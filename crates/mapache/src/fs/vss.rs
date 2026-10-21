//! Volume Shadow Copy Service (VSS) snapshot sessions — Windows only.
//!
//! A VSS session creates a shadow copy of every local volume that hosts a
//! snapshot source path, and registers a global translation table so the
//! source-reading syscalls (metadata, directory enumeration, file opens) read
//! the frozen shadow volume instead of the live one. All higher-level
//! bookkeeping (snapshot paths, tree serialization, parent diffing) stays in
//! the original path space, so incrementals are unaffected and call sites keep
//! consulting paths exactly as before — they just transparently hit the shadow
//! volume for the duration of the session.
//!
//! The session runs the standard full-backup writer protocol: writers are
//! notified and flush their in-memory state before the shadow copy is
//! frozen, so the copies reflect a consistent moment (at least
//! crash-consistent, and effectively application-consistent when writers
//! cooperate). mapache selects no writer components and consumes no writer
//! metadata.
//!
//! Everything here is best-effort and per volume: if VSS is unavailable (not
//! elevated, the service is disabled) the whole session degrades to ordinary
//! live reads with a warning, and a volume that cannot be shadowed (a network
//! drive, a `SUBST` drive, a provider that refuses it) is skipped on its own
//! with a warning while the remaining volumes are still shadowed. Only one
//! session can be active per process: the translation table is global, so a
//! second concurrent session is rejected instead of silently disabling the
//! first one.
//!
//! Known limitation: VSS snapshots are taken per volume. A source path that
//! lives on a volume mounted inside a snapshotted drive (a directory mount
//! point) is not part of that drive's shadow copy, so its reads either fail
//! or resolve through the live mount point. Such layouts need their own
//! volume in the source path list.
//!
//! Implementation notes: the `windows-sys` bindings do not expose the
//! requester-side `IVssBackupComponents` interface (microsoft/win32metadata
//! #1790), so the needed vtable slots are declared in this file, in
//! declaration order of the SDK's `VsBackup.h` (vtable order follows IDL
//! declaration order). All COM traffic — creation and deletion — runs on a
//! single dedicated worker thread with an MTA apartment.

use std::{borrow::Cow, path::Path};

#[cfg(windows)]
use std::path::PathBuf;

/// Translates `path` to its shadow-copy path when a VSS session is active and
/// the path lives on one of the snapshotted volumes.
///
/// Returns the path unchanged, without allocating, in every other case: no
/// active session, an already translated path (`\\?\GLOBALROOT\...` matches no
/// drive prefix), or a path whose volume has no shadow copy. This is on the
/// hot path of the scanner, so the borrow matters.
#[inline]
#[must_use]
pub fn translate(path: &Path) -> Cow<'_, Path> {
    #[cfg(windows)]
    {
        match imp::active_mapping() {
            Some(mapping) => mapping.translate(path),
            None => Cow::Borrowed(path),
        }
    }
    #[cfg(not(windows))]
    {
        Cow::Borrowed(path)
    }
}

/// A session that redirects source reads to a set of shadow-copied volumes.
///
/// Created before source paths are walked; the translation stays registered
/// until the session is dropped or [`VssSession::close`] is awaited, at which
/// point the shadow copies are deleted (best effort) and reads fall back to
/// the live filesystem.
pub struct VssSession {
    inner: Option<imp::SessionInner>,
}

impl VssSession {
    /// An inert session: no translation and no shadow copies.
    ///
    /// Useful when a caller decides the feature is disabled but wants to keep
    /// a uniform session-typed value in scope.
    #[must_use]
    pub fn inert() -> Self {
        Self { inner: None }
    }

    /// Starts a VSS session covering the volumes of `source_paths`.
    ///
    /// Relative paths are resolved against the current directory. This never
    /// fails: on invalid VSS state it logs a warning and returns an inert
    /// session (ordinary live reads). The shadow copies may take a few seconds
    /// to create; the wait happens on the blocking pool.
    pub async fn start<P>(source_paths: &[P]) -> Self
    where
        P: AsRef<Path>,
    {
        #[cfg(windows)]
        {
            let paths: Vec<PathBuf> = source_paths
                .iter()
                .map(|path| path.as_ref().to_path_buf())
                .collect();

            match tokio::task::spawn_blocking(move || imp::start_sync(&paths)).await {
                Ok(Some(inner)) => VssSession { inner: Some(inner) },
                Ok(None) => VssSession::inert(),
                Err(error) => {
                    crate::ui::cli::warning!(
                        "the VSS setup task failed: {error}; continuing with \
                         ordinary live reads"
                    );
                    VssSession::inert()
                }
            }
        }
        #[cfg(not(windows))]
        {
            crate::ui::cli::warning!(
                "--use-fs-snapshot is only supported on Windows; continuing \
                 with ordinary live reads"
            );
            let _ = source_paths;
            VssSession::inert()
        }
    }

    /// Releases the shadow copies, running the (blocking) teardown on the
    /// blocking pool so the async executor is never stalled by VSS.
    ///
    /// Call this as soon as the source reads are done: a shadow copy keeps
    /// consuming copy-on-write space until it is deleted. Dropping the session
    /// without calling `close` still cleans up, but does so synchronously.
    pub async fn close(self) {
        if self.inner.is_none() {
            return;
        }
        // The inner value's destructor joins the VSS worker (which deletes
        // the snapshot set), so let it run on the blocking pool instead of
        // stalling the async executor.
        let _ = tokio::task::spawn_blocking(move || {
            let _session = self;
        })
        .await;
    }
}

/// Non-Windows stub so the `VssSession` field type resolves; sessions there
/// are always inert.
#[cfg(not(windows))]
mod imp {
    pub(super) struct SessionInner;
}

/// Splits `C:\rest` into `(drive-key, "rest")` on the UTF-16 form.
///
/// A leading `\\?\` verbatim prefix is ignored, so verbatim drive paths are
/// translated like their plain counterparts. Returns `None` for anything that
/// is not a drive-letter absolute path: UNC paths, already translated paths,
/// drive-relative forms without a separator, and malformed input.
#[cfg(any(windows, test))]
fn split_drive(wide: &[u16]) -> Option<(char, &[u16])> {
    let wide = strip_verbatim_prefix(wide);

    // `X:\...` — three units minimum, and the drive letter must be ASCII.
    if wide.len() < 3 || wide[1] != u16::from(b':') || wide[0] > 0x7F {
        return None;
    }
    if wide[2] != u16::from(b'\\') && wide[2] != u16::from(b'/') {
        return None;
    }
    let key = (wide[0] as u8 as char).to_ascii_uppercase();
    if !key.is_ascii_alphabetic() {
        return None;
    }
    Some((key, &wide[3..]))
}

/// Strips a leading `\\?\` from a UTF-16 path, if present.
#[cfg(any(windows, test))]
fn strip_verbatim_prefix(wide: &[u16]) -> &[u16] {
    const PREFIX: [u16; 4] = [b'\\' as u16, b'\\' as u16, b'?' as u16, b'\\' as u16];
    if wide.starts_with(&PREFIX) {
        &wide[PREFIX.len()..]
    } else {
        wide
    }
}

/// Prefixes `remainder` with the shadow device path, normalizing `/` to `\`.
///
/// The shadow device path is `\\?\`-prefixed, so the Win32 layer performs no
/// path normalization: separators must already be canonical.
#[cfg(any(windows, test))]
fn join_device_path(device: &[u16], remainder: &[u16]) -> Vec<u16> {
    let mut out = Vec::with_capacity(device.len() + remainder.len());
    out.extend_from_slice(device);
    out.extend(remainder.iter().map(|&unit| {
        if unit == u16::from(b'/') {
            u16::from(b'\\')
        } else {
            unit
        }
    }));
    out
}

/// Reads a NUL-terminated UTF-16 buffer into a `String`, replacing invalid
/// units. The search is bounded by the buffer length.
#[cfg(windows)]
fn read_wide_buffer(buffer: &[u16]) -> String {
    let len = buffer
        .iter()
        .position(|&unit| unit == 0)
        .unwrap_or(buffer.len());
    String::from_utf16_lossy(&buffer[..len])
}

/// Windows implementation: COM plumbing, snapshot-set lifecycle and the
/// active path-translation table.
#[cfg(windows)]
mod imp {
    use std::{
        borrow::Cow,
        ffi::OsString,
        os::windows::ffi::{OsStrExt, OsStringExt},
        path::{Path, PathBuf},
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
            mpsc,
        },
        time::{Duration, Instant},
    };

    use windows_sys::{
        Win32::{
            Foundation::{CloseHandle, GetLastError, RPC_E_CHANGED_MODE},
            Storage::FileSystem::GetVolumeNameForVolumeMountPointW,
            System::Com::{
                COINIT_MULTITHREADED, CoInitializeEx, CoInitializeSecurity, CoUninitialize,
                EOAC_NONE, RPC_C_AUTHN_LEVEL_PKT_PRIVACY, RPC_C_IMP_LEVEL_IMPERSONATE,
            },
        },
        core::{GUID, HRESULT},
    };

    use crate::{
        common::error::{MapacheError, Result},
        fs::get_absolute_normalized_path,
        ui,
    };

    use super::{join_device_path, read_wide_buffer, split_drive};

    /// Translation table registered for the duration of `VssSession`.
    static ACTIVE: parking_lot::RwLock<Option<Arc<ShadowMapping>>> = parking_lot::RwLock::new(None);
    /// Fast, lock-free "is a session active?" probe for the scanner hot path.
    static ACTIVE_SET: AtomicBool = AtomicBool::new(false);

    #[inline]
    pub(super) fn active_mapping() -> Option<Arc<ShadowMapping>> {
        if !ACTIVE_SET.load(Ordering::Acquire) {
            return None;
        }
        ACTIVE.read().clone()
    }

    /// Registers the mapping as the active one. Fails when another session
    /// already owns the global table (only one session can be active).
    fn register(mapping: &Arc<ShadowMapping>) -> bool {
        let mut slot = ACTIVE.write();
        if slot.is_some() {
            return false;
        }
        *slot = Some(mapping.clone());
        ACTIVE_SET.store(true, Ordering::Release);
        true
    }

    /// Clears the table, but only if `mapping` is still the active one, so a
    /// stale session can never disable a newer one.
    fn deregister(mapping: &Arc<ShadowMapping>) {
        let mut slot = ACTIVE.write();
        if slot
            .as_ref()
            .is_some_and(|active| Arc::ptr_eq(active, mapping))
        {
            *slot = None;
            ACTIVE_SET.store(false, Ordering::Release);
        }
    }

    // -----------------------------------------------------------------------
    // Path translation table
    // -----------------------------------------------------------------------

    /// Maps local volume roots (`C:\`) to VSS shadow device strings.
    pub(super) struct ShadowMapping {
        /// One entry per shadowed volume, keyed by the uppercase drive letter.
        volumes: Vec<VolumeShadow>,
    }

    struct VolumeShadow {
        key: char,
        /// Device path of the shadow copy in UTF-16, with a trailing
        /// separator, e.g. `\\?\GLOBALROOT\Device\HarddiskVolumeShadowCopy1\`.
        device_utf16: Vec<u16>,
    }

    impl ShadowMapping {
        fn new(volumes: Vec<VolumeShadow>) -> Self {
            ShadowMapping { volumes }
        }

        /// Translates a `C:\...` path into `\\?\GLOBALROOT\Device\...\...`.
        ///
        /// Drive-relative paths (`C:file`) are never translated: they resolve
        /// against the current directory and are outside the snapshot's
        /// absolute source paths.
        pub(super) fn translate<'a>(&self, path: &'a Path) -> Cow<'a, Path> {
            let wide: Vec<u16> = path.as_os_str().encode_wide().collect();
            let Some((key, remainder)) = split_drive(&wide) else {
                return Cow::Borrowed(path);
            };
            let Some(volume) = self.volumes.iter().find(|volume| volume.key == key) else {
                return Cow::Borrowed(path);
            };

            Cow::Owned(PathBuf::from(OsString::from_wide(&join_device_path(
                &volume.device_utf16,
                remainder,
            ))))
        }
    }

    fn wide_zero_terminated(s: &str) -> Vec<u16> {
        s.encode_utf16().chain(std::iter::once(0)).collect()
    }

    /// Upper bound for scanning a `VSS_PWSZ` string. Device paths are far
    /// shorter; the bound only keeps a corrupted or unterminated string from
    /// being scanned past its allocation.
    const MAX_PWSZ_UNITS: usize = 32 * 1024;

    /// Reads a NUL-terminated wide string (`VSS_PWSZ`) into a `String`,
    /// replacing invalid units.
    ///
    /// The string members of `VSS_SNAPSHOT_PROP` are `VSS_PWSZ` — plain
    /// NUL-terminated `WCHAR` strings, *not* BSTRs — so their length has to be
    /// found by scanning for the terminator. `SysStringLen` would instead read
    /// the BSTR length prefix in front of the allocation, which such a string
    /// does not have, and return an arbitrary length that overruns the buffer.
    fn pwsz_to_string(pwsz: *const u16) -> String {
        if pwsz.is_null() {
            return String::new();
        }
        let mut len = 0;
        // SAFETY: `pwsz` points to a NUL-terminated string owned by the
        // snapshot property structure; the scan stops at the terminator or at
        // the conservative bound above.
        unsafe {
            while len < MAX_PWSZ_UNITS && *pwsz.add(len) != 0 {
                len += 1;
            }
            if len == MAX_PWSZ_UNITS {
                return String::new();
            }
            read_wide_buffer(std::slice::from_raw_parts(pwsz, len))
        }
    }

    // -----------------------------------------------------------------------
    // COM vtables (subset needed to create and delete a snapshot set)
    // -----------------------------------------------------------------------

    mod ffi {
        #![allow(non_snake_case)]
        #![allow(non_camel_case_types)]

        use windows_sys::core::{BSTR, GUID, HRESULT, PCWSTR};

        /// `VSS_PWSZ`: a plain NUL-terminated `WCHAR` string. Note the
        /// difference from a `BSTR`, which carries a byte-length prefix.
        pub type VssPwsz = *const u16;

        /// `IVssBackupComponents` raw pointer (`IUnknown`-like layout).
        #[repr(C)]
        pub struct IVssBackupComponentsRaw {
            pub vtbl: *const IVssBackupComponentsVtbl,
        }

        /// Declaration order follows `VsBackup.h` (vtable order follows IDL
        /// declaration order); every slot is typed for clarity. VSS `BOOLEAN`
        /// parameters are one byte in the IDL (`u8` here), while Win32 `BOOL`
        /// parameters are four bytes (`i32`).
        #[repr(C)]
        pub struct IVssBackupComponentsVtbl {
            // IUnknown
            pub QueryInterface: unsafe extern "system" fn(
                *mut IVssBackupComponentsRaw,
                *const GUID,
                *mut *mut core::ffi::c_void,
            ) -> HRESULT,
            pub AddRef: unsafe extern "system" fn(*mut IVssBackupComponentsRaw) -> u32,
            pub Release: unsafe extern "system" fn(*mut IVssBackupComponentsRaw) -> u32,

            // IVssBackupComponents
            pub GetWriterComponentsCount:
                unsafe extern "system" fn(*mut IVssBackupComponentsRaw, *mut u32) -> HRESULT,
            pub GetWriterComponents: unsafe extern "system" fn(
                *mut IVssBackupComponentsRaw,
                u32,
                *mut *mut core::ffi::c_void,
            ) -> HRESULT,
            pub InitializeForBackup:
                unsafe extern "system" fn(*mut IVssBackupComponentsRaw, BSTR) -> HRESULT,
            // `SetBackupState` uses Win32 `BOOL` (4 bytes) for its first,
            // second and fourth parameters, unlike the `BOOLEAN` (1 byte)
            // parameters elsewhere in this interface.
            pub SetBackupState: unsafe extern "system" fn(
                *mut IVssBackupComponentsRaw,
                i32,
                i32,
                i32,
                i32,
            ) -> HRESULT,
            pub InitializeForRestore:
                unsafe extern "system" fn(*mut IVssBackupComponentsRaw, BSTR) -> HRESULT,
            pub SetRestoreState:
                unsafe extern "system" fn(*mut IVssBackupComponentsRaw, i32) -> HRESULT,
            pub GatherWriterMetadata: unsafe extern "system" fn(
                *mut IVssBackupComponentsRaw,
                *mut *mut IVssAsyncRaw,
            ) -> HRESULT,
            pub GetWriterMetadataCount:
                unsafe extern "system" fn(*mut IVssBackupComponentsRaw, *mut u32) -> HRESULT,
            pub GetWriterMetadata: unsafe extern "system" fn(
                *mut IVssBackupComponentsRaw,
                u32,
                *mut GUID,
                *mut *mut core::ffi::c_void,
            ) -> HRESULT,
            pub FreeWriterMetadata: unsafe extern "system" fn(*mut IVssBackupComponentsRaw),
            pub AddComponent: unsafe extern "system" fn(
                *mut IVssBackupComponentsRaw,
                GUID,
                GUID,
                i32,
                PCWSTR,
                PCWSTR,
            ) -> HRESULT,
            pub PrepareForBackup: unsafe extern "system" fn(
                *mut IVssBackupComponentsRaw,
                *mut *mut IVssAsyncRaw,
            ) -> HRESULT,
            pub AbortBackup: unsafe extern "system" fn(*mut IVssBackupComponentsRaw) -> HRESULT,
            pub GatherWriterStatus: unsafe extern "system" fn(
                *mut IVssBackupComponentsRaw,
                *mut *mut IVssAsyncRaw,
            ) -> HRESULT,
            pub GetWriterStatusCount:
                unsafe extern "system" fn(*mut IVssBackupComponentsRaw, *mut u32) -> HRESULT,
            pub FreeWriterStatus: unsafe extern "system" fn(*mut IVssBackupComponentsRaw),
            pub GetWriterStatus: unsafe extern "system" fn(
                *mut IVssBackupComponentsRaw,
                u32,
                *mut GUID,
                *mut GUID,
                *mut BSTR,
                *mut i32,
                *mut HRESULT,
            ) -> HRESULT,
            pub SetBackupSucceeded: unsafe extern "system" fn(
                *mut IVssBackupComponentsRaw,
                GUID,
                GUID,
                i32,
                PCWSTR,
                PCWSTR,
                u8,
            ) -> HRESULT,
            pub SetBackupOptions: unsafe extern "system" fn(
                *mut IVssBackupComponentsRaw,
                GUID,
                i32,
                PCWSTR,
                PCWSTR,
                PCWSTR,
            ) -> HRESULT,
            pub SetSelectedForRestore: unsafe extern "system" fn(
                *mut IVssBackupComponentsRaw,
                GUID,
                i32,
                PCWSTR,
                PCWSTR,
                u8,
            ) -> HRESULT,
            pub SetRestoreOptions: unsafe extern "system" fn(
                *mut IVssBackupComponentsRaw,
                GUID,
                i32,
                PCWSTR,
                PCWSTR,
                PCWSTR,
            ) -> HRESULT,
            pub SetAdditionalRestores: unsafe extern "system" fn(
                *mut IVssBackupComponentsRaw,
                GUID,
                i32,
                PCWSTR,
                PCWSTR,
                u8,
            ) -> HRESULT,
            pub SetPreviousBackupStamp: unsafe extern "system" fn(
                *mut IVssBackupComponentsRaw,
                GUID,
                i32,
                PCWSTR,
                PCWSTR,
                PCWSTR,
            ) -> HRESULT,
            pub SaveAsXML:
                unsafe extern "system" fn(*mut IVssBackupComponentsRaw, *mut BSTR) -> HRESULT,
            pub BackupComplete: unsafe extern "system" fn(
                *mut IVssBackupComponentsRaw,
                *mut *mut IVssAsyncRaw,
            ) -> HRESULT,
            pub AddAlternativeLocationMapping: unsafe extern "system" fn(
                *mut IVssBackupComponentsRaw,
                GUID,
                i32,
                PCWSTR,
                PCWSTR,
                PCWSTR,
                PCWSTR,
                u8,
                PCWSTR,
            ) -> HRESULT,
            pub AddRestoreSubcomponent: unsafe extern "system" fn(
                *mut IVssBackupComponentsRaw,
                GUID,
                i32,
                PCWSTR,
                PCWSTR,
                PCWSTR,
                PCWSTR,
                u8,
            ) -> HRESULT,
            pub SetFileRestoreStatus: unsafe extern "system" fn(
                *mut IVssBackupComponentsRaw,
                GUID,
                i32,
                PCWSTR,
                PCWSTR,
                i32,
            ) -> HRESULT,
            pub AddNewTarget: unsafe extern "system" fn(
                *mut IVssBackupComponentsRaw,
                GUID,
                i32,
                PCWSTR,
                PCWSTR,
                PCWSTR,
                u8,
                PCWSTR,
            ) -> HRESULT,
            pub SetRangesFilePath: unsafe extern "system" fn(
                *mut IVssBackupComponentsRaw,
                GUID,
                i32,
                PCWSTR,
                PCWSTR,
                u32,
                PCWSTR,
            ) -> HRESULT,
            pub PreRestore: unsafe extern "system" fn(
                *mut IVssBackupComponentsRaw,
                *mut *mut IVssAsyncRaw,
            ) -> HRESULT,
            pub PostRestore: unsafe extern "system" fn(
                *mut IVssBackupComponentsRaw,
                *mut *mut IVssAsyncRaw,
            ) -> HRESULT,
            pub SetContext: unsafe extern "system" fn(*mut IVssBackupComponentsRaw, i32) -> HRESULT,
            pub StartSnapshotSet:
                unsafe extern "system" fn(*mut IVssBackupComponentsRaw, *mut GUID) -> HRESULT,
            pub AddToSnapshotSet: unsafe extern "system" fn(
                *mut IVssBackupComponentsRaw,
                PCWSTR,
                GUID,
                *mut GUID,
            ) -> HRESULT,
            pub DoSnapshotSet: unsafe extern "system" fn(
                *mut IVssBackupComponentsRaw,
                *mut *mut IVssAsyncRaw,
            ) -> HRESULT,
            pub DeleteSnapshots: unsafe extern "system" fn(
                *mut IVssBackupComponentsRaw,
                GUID,
                i32,
                i32,
                *mut i32,
                *mut GUID,
            ) -> HRESULT,
            pub ImportSnapshots: unsafe extern "system" fn(
                *mut IVssBackupComponentsRaw,
                *mut *mut IVssAsyncRaw,
            ) -> HRESULT,
            pub BreakSnapshotSet:
                unsafe extern "system" fn(*mut IVssBackupComponentsRaw, GUID) -> HRESULT,
            pub GetSnapshotProperties: unsafe extern "system" fn(
                *mut IVssBackupComponentsRaw,
                GUID,
                *mut VssSnapshotProp,
            ) -> HRESULT,
            pub Query: unsafe extern "system" fn(
                *mut IVssBackupComponentsRaw,
                GUID,
                i32,
                i32,
                *mut *mut core::ffi::c_void,
            ) -> HRESULT,
            pub IsVolumeSupported: unsafe extern "system" fn(
                *mut IVssBackupComponentsRaw,
                GUID,
                PCWSTR,
                *mut i32,
            ) -> HRESULT,
            pub DisableWriterClasses: unsafe extern "system" fn(
                *mut IVssBackupComponentsRaw,
                *const GUID,
                u32,
            ) -> HRESULT,
            pub EnableWriterClasses: unsafe extern "system" fn(
                *mut IVssBackupComponentsRaw,
                *const GUID,
                u32,
            ) -> HRESULT,
            pub DisableWriterInstances: unsafe extern "system" fn(
                *mut IVssBackupComponentsRaw,
                *const GUID,
                u32,
            ) -> HRESULT,
            pub ExposeSnapshot: unsafe extern "system" fn(
                *mut IVssBackupComponentsRaw,
                GUID,
                PCWSTR,
                i32,
                PCWSTR,
                *mut windows_sys::core::PWSTR,
            ) -> HRESULT,
            pub RevertToSnapshot:
                unsafe extern "system" fn(*mut IVssBackupComponentsRaw, GUID, i32) -> HRESULT,
            pub QueryRevertStatus: unsafe extern "system" fn(
                *mut IVssBackupComponentsRaw,
                PCWSTR,
                *mut *mut IVssAsyncRaw,
            ) -> HRESULT,
        }

        /// `IVssAsync` raw pointer. Method order follows `vss.h`:
        /// `IUnknown`, `Cancel`, `Wait`, `QueryStatus`.
        #[repr(C)]
        pub struct IVssAsyncRaw {
            pub vtbl: *const IVssAsyncVtbl,
        }

        #[repr(C)]
        pub struct IVssAsyncVtbl {
            pub QueryInterface: unsafe extern "system" fn(
                *mut IVssAsyncRaw,
                *const GUID,
                *mut *mut core::ffi::c_void,
            ) -> HRESULT,
            pub AddRef: unsafe extern "system" fn(*mut IVssAsyncRaw) -> u32,
            pub Release: unsafe extern "system" fn(*mut IVssAsyncRaw) -> u32,
            pub Cancel: unsafe extern "system" fn(*mut IVssAsyncRaw) -> HRESULT,
            pub Wait: unsafe extern "system" fn(*mut IVssAsyncRaw, u32) -> HRESULT,
            pub QueryStatus:
                unsafe extern "system" fn(*mut IVssAsyncRaw, *mut HRESULT, *mut i32) -> HRESULT,
        }

        /// Mirrors `VSS_SNAPSHOT_PROP` (`vss.h`). The string members are
        /// `VSS_PWSZ` owned by `vssapi` and released as a whole with
        /// `VssFreeSnapshotPropertiesInternal`; they must not be freed
        /// individually with `SysFreeString`/`CoTaskMemFree`.
        #[repr(C)]
        pub struct VssSnapshotProp {
            pub m_SnapshotId: GUID,
            pub m_SnapshotSetId: GUID,
            pub m_lSnapshotsCount: i32,
            pub m_pwszSnapshotDeviceObject: VssPwsz,
            pub m_pwszOriginalVolumeName: VssPwsz,
            pub m_pwszOriginatingMachine: VssPwsz,
            pub m_pwszServiceMachine: VssPwsz,
            pub m_pwszExposedName: VssPwsz,
            pub m_pwszExposedPath: VssPwsz,
            pub m_ProviderId: GUID,
            pub m_lSnapshotAttributes: i32,
            /// `VSS_TIMESTAMP` is a `LONGLONG` (8-byte aligned). Declaring it
            /// as `FILETIME` (8 bytes, but 4-byte aligned) would shrink this
            /// structure, and VSS writes the full `VSS_SNAPSHOT_PROP` into the
            /// caller's buffer.
            pub m_tsCreationTimestamp: i64,
            pub m_eStatus: i32,
        }
    }

    // SAFETY: `CreateVssBackupComponentsInternal` and
    // `VssFreeSnapshotPropertiesInternal` are the unmangled exports of
    // `vssapi.dll`, declared in `VsBackup.h`; the SDK import library
    // `vssapi.lib` resolves them for MSVC toolchains. The plain-C names in
    // the SDK headers (`CreateVssBackupComponents`,
    // `VssFreeSnapshotProperties`) are `static inline` wrappers around these.
    #[link(name = "vssapi")]
    unsafe extern "system" {
        pub fn CreateVssBackupComponentsInternal(
            ppbackup: *mut *mut ffi::IVssBackupComponentsRaw,
        ) -> HRESULT;
        /// Frees the `VSS_PWSZ` members of a `VSS_SNAPSHOT_PROP` returned by
        /// `GetSnapshotProperties` and zeroes the structure. The strings are
        /// owned by `vssapi`'s allocator, so they must not be freed
        /// individually.
        pub fn VssFreeSnapshotPropertiesInternal(pProp: *mut ffi::VssSnapshotProp);
    }

    /// `VSS_OBJECT_TYPE::SNAPSHOT_SET` (`vss.h`: `UNKNOWN` = 0, `NONE` = 1,
    /// `SNAPSHOT_SET` = 2).
    const VSS_OBJECT_SNAPSHOT_SET: i32 = 2;
    /// `VSS_BACKUP_TYPE::VSS_BT_FULL`.
    const VSS_BT_FULL: i32 = 1;
    /// `VSS_S_ASYNC_FINISHED`: the async operation completed successfully.
    const VSS_S_ASYNC_FINISHED: HRESULT = 0x0004_230A;
    /// `VSS_S_ASYNC_CANCELLED`: the async operation was cancelled.
    const VSS_S_ASYNC_CANCELLED: HRESULT = 0x0004_230B;
    /// `VSS_S_ASYNC_PENDING`: the async operation has not completed.
    const VSS_S_ASYNC_PENDING: HRESULT = 0x0004_2309;
    /// `S_FALSE`, accepted from `IVssAsync::Wait` as "the wait timed out".
    const S_FALSE: HRESULT = 1;
    /// Granularity of the async wait loop, so a stuck operation is noticed
    /// and cancelled instead of blocking forever.
    const WAIT_SLICE_MS: u32 = 30_000;
    /// Upper bound for a single VSS async operation. Generous enough for the
    /// initial copy of a large volume, but bounded so a wedged provider or a
    /// stuck writer cannot hang the snapshot indefinitely; on expiry the
    /// operation is cancelled and the snapshot falls back to live reads.
    const ASYNC_OPERATION_TIMEOUT: Duration = Duration::from_secs(15 * 60);

    fn hr(result: HRESULT, operation: &str) -> Result<()> {
        if result == 0 {
            Ok(())
        } else {
            Err(MapacheError::Backend(format!(
                "VSS {operation} failed: 0x{result:08X}"
            )))
        }
    }

    /// Checks a completed `IVssAsync::QueryStatus` result. The statuses may be
    /// *success* HRs (`VSS_S_ASYNC_*`), so the plain check is not valid here.
    fn hr_async_status(status: HRESULT, operation: &str) -> Result<()> {
        match status {
            VSS_S_ASYNC_FINISHED => Ok(()),
            VSS_S_ASYNC_PENDING => Err(MapacheError::Backend(format!(
                "VSS {operation} is still pending"
            ))),
            VSS_S_ASYNC_CANCELLED => Err(MapacheError::Backend(format!(
                "VSS {operation} was cancelled"
            ))),
            _ => Err(MapacheError::Backend(format!(
                "VSS {operation} failed: 0x{status:08X}"
            ))),
        }
    }

    /// RAII for a live `IVssBackupComponents` pointer.
    struct Components {
        raw: *mut ffi::IVssBackupComponentsRaw,
    }

    impl Components {
        fn create() -> Result<Self> {
            let mut raw: *mut ffi::IVssBackupComponentsRaw = std::ptr::null_mut();
            // SAFETY: the out-pointer is a valid target; on failure `raw`
            // stays null and no `Release` is owed.
            let created = unsafe { CreateVssBackupComponentsInternal(&mut raw) };
            hr(created, "CreateVssBackupComponents")?;
            if raw.is_null() {
                return Err(MapacheError::Backend(
                    "VSS returned a null backup components instance".to_string(),
                ));
            }
            Ok(Components { raw })
        }
    }

    impl Drop for Components {
        fn drop(&mut self) {
            if self.raw.is_null() {
                return;
            }
            // SAFETY: `Release` is `IUnknown` vtable slot 2, and the pointer
            // was returned by the creation call above and never shared.
            unsafe {
                let release = (*(*self.raw).vtbl).Release;
                release(self.raw);
            }
        }
    }

    /// Whether COM got initialized on the worker thread and lifecycle actions
    /// may proceed.
    struct ComInit {
        /// Whether `CoInitializeEx` must be balanced with `CoUninitialize`.
        owned: bool,
    }

    impl ComInit {
        fn init() -> Result<Self> {
            // SAFETY: standard COM initialization.
            let hr = unsafe { CoInitializeEx(std::ptr::null(), COINIT_MULTITHREADED as u32) };
            match hr {
                // S_OK or S_FALSE: we own one init count that `Drop` balances.
                0 | 1 => Ok(ComInit { owned: true }),
                // `RPC_E_CHANGED_MODE`: the thread already runs
                // in the MTA, which is equally valid here, but no count was
                // taken so nothing must be balanced.
                RPC_E_CHANGED_MODE => Ok(ComInit { owned: false }),
                _ => Err(MapacheError::Backend(format!(
                    "COM could not be initialized for VSS: 0x{hr:08X}"
                ))),
            }
        }
    }

    impl Drop for ComInit {
        fn drop(&mut self) {
            if self.owned {
                // SAFETY: balances the successful `CoInitializeEx` call.
                unsafe { CoUninitialize() };
            }
        }
    }

    /// Best-effort process-wide COM security configuration.
    fn init_com_security() {
        // SAFETY: `CoInitializeSecurity` is called before any COM activation
        // on this thread; failure usually means it was already configured for
        // the process, which is harmless for this in-process usage.
        unsafe {
            // Null descriptor, authenticate everything, packet privacy,
            // impersonate, no capabilities.
            let _ = CoInitializeSecurity(
                std::ptr::null_mut(),
                -1,
                std::ptr::null(),
                std::ptr::null(),
                RPC_C_AUTHN_LEVEL_PKT_PRIVACY,
                RPC_C_IMP_LEVEL_IMPERSONATE,
                std::ptr::null(),
                EOAC_NONE as u32,
                std::ptr::null(),
            );
        }
    }

    // -----------------------------------------------------------------------
    // Worker thread: create, serve, delete
    // -----------------------------------------------------------------------

    /// Distinct volume to shadow.
    struct VolumePlan {
        /// `AddToSnapshotSet` name, e.g. `\\?\Volume{1a2b-...}\`.
        volume_name: String,
        /// Drive key used by the translation table.
        key: char,
    }

    pub(super) struct SessionInner {
        /// Kept so teardown can clear *this* mapping only.
        mapping: Arc<ShadowMapping>,
        /// Signals the worker to delete the snapshot set and exit.
        shutdown: Option<mpsc::Sender<()>>,
        worker: Option<std::thread::JoinHandle<()>>,
    }

    impl Drop for SessionInner {
        fn drop(&mut self) {
            deregister(&self.mapping);
            if let Some(shutdown) = self.shutdown.take() {
                let _ = shutdown.send(());
            }
            if let Some(worker) = self.worker.take()
                && worker.join().is_err()
            {
                ui::cli::warning!("the VSS worker thread panicked");
            }
        }
    }

    /// Entry point run on the blocking pool. Never fails outward: any error
    /// becomes a warning and an inert session (live reads).
    pub(super) fn start_sync(source_paths: &[PathBuf]) -> Option<SessionInner> {
        match create_session(source_paths) {
            Ok(inner) => Some(inner),
            Err(error) => {
                ui::cli::warning!(
                    "Continuing without a filesystem snapshot (files that a \
                     running program keeps open for exclusive access may be \
                     skipped): {error}"
                );
                None
            }
        }
    }

    fn create_session(source_paths: &[PathBuf]) -> Result<SessionInner> {
        // Cheap fast path for overlapping snapshots: the set creation below
        // is expensive, and only one session can own the global table anyway.
        if active_mapping().is_some() {
            return Err(session_already_active());
        }
        if !is_elevated() {
            return Err(MapacheError::Backend(
                "--use-fs-snapshot requires administrator privileges (shadow \
                 copies are created with elevated rights)"
                    .to_string(),
            ));
        }

        let volumes = plan_volumes(source_paths);
        if volumes.is_empty() {
            return Err(MapacheError::Backend(
                "no local drive among the source paths supports a VSS shadow \
                 copy (non-drive paths are read from the live filesystem)"
                    .to_string(),
            ));
        }

        let (ready_tx, ready_rx) = mpsc::channel();
        let (shutdown_tx, shutdown_rx) = mpsc::channel();

        let worker = std::thread::spawn(move || {
            worker_run(volumes, ready_tx, shutdown_rx);
        });

        let mapping = match ready_rx.recv() {
            Ok(Ok(mapping)) => mapping,
            Ok(Err(error)) => return Err(error),
            Err(_) => {
                return Err(MapacheError::Backend(
                    "the VSS worker thread died unexpectedly".to_string(),
                ));
            }
        };

        if !register(&mapping) {
            // Another session won the race and owns the global translation
            // table. Release this worker so it deletes the set it just
            // created, and wait for the cleanup before reporting the failure.
            drop(shutdown_tx);
            let _ = worker.join();
            return Err(session_already_active());
        }

        Ok(SessionInner {
            mapping,
            shutdown: Some(shutdown_tx),
            worker: Some(worker),
        })
    }

    fn session_already_active() -> MapacheError {
        MapacheError::Backend(
            "another filesystem snapshot session is already active; these \
             sources will be read from the live filesystem"
                .to_string(),
        )
    }

    /// Resolves the volumes to shadow, skipping (with a warning) any source
    /// path or drive that VSS cannot cover, so one unsupported path never
    /// disables the shadow copy for the others.
    fn plan_volumes(source_paths: &[PathBuf]) -> Vec<VolumePlan> {
        let mut keys: Vec<char> = Vec::new();
        for path in source_paths {
            let normalized = match get_absolute_normalized_path(path) {
                Ok(normalized) => normalized,
                Err(error) => {
                    ui::cli::warning!(
                        "cannot resolve {}: {error}; it will be read from the \
                         live filesystem",
                        path.display()
                    );
                    continue;
                }
            };
            let wide: Vec<u16> = normalized.as_os_str().encode_wide().collect();
            match split_drive(&wide) {
                Some((key, _)) => {
                    if !keys.contains(&key) {
                        keys.push(key);
                    }
                }
                None => {
                    ui::cli::warning!(
                        "VSS snapshots only cover local drives; {} will be \
                         read from the live filesystem",
                        normalized.display()
                    );
                }
            }
        }

        let mut plan = Vec::with_capacity(keys.len());
        for key in keys {
            match volume_guid_path(key) {
                Ok(volume_name) => plan.push(VolumePlan { volume_name, key }),
                Err(error) => {
                    ui::cli::warning!(
                        "{error}; paths on drive {key} will be read from the \
                         live filesystem"
                    );
                }
            }
        }
        plan
    }

    /// `\\?\Volume{GUID}\` for a local drive letter.
    fn volume_guid_path(key: char) -> Result<String> {
        // `GetVolumeNameForVolumeMountPointW` requires a trailing separator.
        let mount = format!("{key}:\\");
        let mut buffer = vec![0u16; 128];
        // SAFETY: the mount-point string is null-terminated UTF-16 and the
        // buffer is writable with the explicit capacity below.
        let ok = unsafe {
            GetVolumeNameForVolumeMountPointW(
                wide_zero_terminated(&mount).as_ptr(),
                buffer.as_mut_ptr(),
                buffer.len() as u32,
            )
        };
        if ok == 0 {
            // SAFETY: `GetLastError` has no preconditions.
            let code = unsafe { GetLastError() };
            return Err(MapacheError::Backend(format!(
                "cannot resolve the volume path of drive {key} (error {code})"
            )));
        }
        Ok(read_wide_buffer(&buffer))
    }

    /// Creates the snapshot set and serves the session until told to stop.
    fn worker_run(
        volumes: Vec<VolumePlan>,
        ready: mpsc::Sender<Result<Arc<ShadowMapping>>>,
        shutdown: mpsc::Receiver<()>,
    ) {
        let fail_with = |error| {
            let _ = ready.send(Err(error));
        };

        let _com = match ComInit::init() {
            Ok(com) => com,
            Err(error) => return fail_with(error),
        };
        init_com_security();

        match create_snapshot_set(&volumes) {
            Ok((components, set_id, mapping)) => {
                let _ = ready.send(Ok(mapping));

                // Serve until the session is dropped, then close the writer
                // protocol and delete the set.
                let _ = shutdown.recv();
                complete_backup(components.raw);
                if let Err(error) = delete_snapshot_set(components.raw, set_id) {
                    ui::cli::warning!(
                        "{error}; the shadow copies are reclaimed when this process \
                         exits, or with `vssadmin delete shadows /all`"
                    );
                }
            }
            Err(error) => fail_with(error),
        }

        // `components` and `_com` drop here, releasing the COM object and the
        // worker's COM apartment.
    }

    /// Brings up the snapshot set for `volumes`: creates the components
    /// object, configures the backup context, engages the writer protocol and
    /// commits the set.
    ///
    /// The writer phases (`SetBackupState`, `GatherWriterMetadata`,
    /// `PrepareForBackup`) are structural prerequisites for `DoSnapshotSet`
    /// in the backup context — without them it fails with `VSS_E_BAD_STATE`,
    /// even though mapache does not consume writer components. On any failure
    /// after `StartSnapshotSet` succeeds, the set is aborted and deleted
    /// best-effort before the error propagates.
    fn create_snapshot_set(
        volumes: &[VolumePlan],
    ) -> Result<(Components, GUID, Arc<ShadowMapping>)> {
        let components = Components::create()?;
        let raw = components.raw;
        initialize_for_backup(raw)?;
        set_backup_context(raw)?;
        configure_backup_state(raw)?;
        gather_writer_metadata(raw)?;
        let set_id = start_snapshot_set(raw)?;

        // Every step past this point may fail after the snapshot set exists,
        // so route the error through the abort/delete path below. This
        // includes `PrepareForBackup`, where writer errors surface.
        let shadows = (|| {
            let added = add_volumes(raw, volumes)?;
            prepare_for_backup(raw)?;
            gather_shadows(raw, &added)
        })();

        match shadows {
            Ok(shadows) => Ok((components, set_id, Arc::new(ShadowMapping::new(shadows)))),
            Err(error) => {
                abort_backup(raw);
                let _ = delete_snapshot_set(raw, set_id);
                Err(error)
            }
        }
    }

    /// Calls `InitializeForBackup` with no writer metadata document.
    fn initialize_for_backup(raw: *mut ffi::IVssBackupComponentsRaw) -> Result<()> {
        // SAFETY: vtable slot method call on the live interface owned by the
        // worker thread, with arguments matching the slot's signature.
        unsafe {
            let initialize = (*(*raw).vtbl).InitializeForBackup;
            hr(initialize(raw, std::ptr::null()), "InitializeForBackup")
        }
    }

    /// Sets the nonpersistent `VSS_CTX_BACKUP` context: the snapshot dies with
    /// the requesting process. This is the default, set explicitly so any
    /// future change is deliberate.
    fn set_backup_context(raw: *mut ffi::IVssBackupComponentsRaw) -> Result<()> {
        // SAFETY: vtable slot method call as in `initialize_for_backup`.
        unsafe { hr(((*(*raw).vtbl).SetContext)(raw, 0), "SetContext") }
    }

    /// Opens a new, empty snapshot set, returning its identifier.
    fn start_snapshot_set(raw: *mut ffi::IVssBackupComponentsRaw) -> Result<GUID> {
        let mut set_id = GUID::default();
        // SAFETY: vtable slot method call as in `initialize_for_backup`;
        // the out-parameter is a valid `GUID` target.
        unsafe {
            let call = ((*(*raw).vtbl).StartSnapshotSet)(raw, &mut set_id);
            hr(call, "StartSnapshotSet")?;
        }
        Ok(set_id)
    }

    /// Adds each planned volume to the set, returning the snapshot ids paired
    /// with the volumes that were actually accepted. A volume the provider
    /// refuses is skipped with a warning instead of failing the whole set.
    fn add_volumes(
        raw: *mut ffi::IVssBackupComponentsRaw,
        volumes: &[VolumePlan],
    ) -> Result<Vec<(&VolumePlan, GUID)>> {
        let mut added: Vec<(&VolumePlan, GUID)> = Vec::with_capacity(volumes.len());
        for volume in volumes {
            if !is_volume_supported(raw, &volume.volume_name) {
                ui::cli::warning!(
                    "drive {} does not support VSS shadow copies; its paths \
                     will be read from the live filesystem",
                    volume.key
                );
                continue;
            }

            let name = wide_zero_terminated(&volume.volume_name);
            let mut snapshot_id = GUID::default();
            // SAFETY: vtable slot method call as in `initialize_for_backup`;
            // `name` is a null-terminated UTF-16 temporary alive until the
            // end of the call and the out-parameter is a valid `GUID` target.
            let call = unsafe {
                ((*(*raw).vtbl).AddToSnapshotSet)(
                    raw,
                    name.as_ptr(),
                    GUID::default(),
                    &mut snapshot_id,
                )
            };
            match hr(call, "AddToSnapshotSet") {
                Ok(()) => added.push((volume, snapshot_id)),
                Err(error) => ui::cli::warning!(
                    "{error}; paths on drive {} will be read from the live \
                     filesystem",
                    volume.key
                ),
            }
        }

        if added.is_empty() {
            return Err(MapacheError::Backend(
                "no volume could be added to the VSS snapshot set".to_string(),
            ));
        }
        Ok(added)
    }

    /// Whether the default provider can shadow `volume_name`.
    ///
    /// Queries are treated as "supported" when the call itself fails, so the
    /// failure is reported by `AddToSnapshotSet` instead of being masked.
    fn is_volume_supported(raw: *mut ffi::IVssBackupComponentsRaw, volume_name: &str) -> bool {
        let name = wide_zero_terminated(volume_name);
        // `IsVolumeSupported`'s out-parameter is a Win32 `BOOL` (4 bytes).
        let mut supported: i32 = 0;
        // SAFETY: vtable slot method call as in `initialize_for_backup`; the
        // out-parameter is a valid `BOOL` target.
        let call = unsafe {
            ((*(*raw).vtbl).IsVolumeSupported)(raw, GUID::default(), name.as_ptr(), &mut supported)
        };
        if call != 0 {
            tracing::debug!(target: "vss", "IsVolumeSupported({volume_name}) failed: 0x{call:08X}");
            return true;
        }
        supported != 0
    }

    /// Sets the writer-relevant backup state: no components selected, no
    /// bootable system state, full backup type, no partial-file support.
    /// `GatherWriterMetadata` behaves oddly without these set first.
    fn configure_backup_state(raw: *mut ffi::IVssBackupComponentsRaw) -> Result<()> {
        // SAFETY: vtable slot method call as in `initialize_for_backup`.
        unsafe {
            let call = ((*(*raw).vtbl).SetBackupState)(raw, 0, 0, VSS_BT_FULL, 0);
            hr(call, "SetBackupState")
        }
    }

    /// Initializes writer communication. `DoSnapshotSet` refuses to commit in
    /// the backup context until writer metadata has been gathered.
    fn gather_writer_metadata(raw: *mut ffi::IVssBackupComponentsRaw) -> Result<()> {
        let mut async_raw: *mut ffi::IVssAsyncRaw = std::ptr::null_mut();
        // SAFETY: `GatherWriterMetadata` writes one interface pointer to the
        // out-parameter.
        let call = unsafe { ((*(*raw).vtbl).GatherWriterMetadata)(raw, &mut async_raw) };
        hr(call, "GatherWriterMetadata")?;
        wait_async(async_raw, "GatherWriterMetadata")
    }

    /// Notifies the writers that a backup is about to run, committing the
    /// set right after; `DoSnapshotSet` requires this notification first.
    fn prepare_for_backup(raw: *mut ffi::IVssBackupComponentsRaw) -> Result<()> {
        let mut async_raw: *mut ffi::IVssAsyncRaw = std::ptr::null_mut();
        // SAFETY: `PrepareForBackup` writes one interface pointer to the
        // out-parameter.
        let call = unsafe { ((*(*raw).vtbl).PrepareForBackup)(raw, &mut async_raw) };
        hr(call, "PrepareForBackup")?;
        wait_async(async_raw, "PrepareForBackup")
    }

    /// Acknowledges the end of the backup to the writers. Not required for
    /// correctness of the shadow copies; ignorable best effort.
    fn complete_backup(raw: *mut ffi::IVssBackupComponentsRaw) {
        let mut async_raw: *mut ffi::IVssAsyncRaw = std::ptr::null_mut();
        // SAFETY: `BackupComplete` writes one interface pointer to the
        // out-parameter.
        let call = unsafe { ((*(*raw).vtbl).BackupComplete)(raw, &mut async_raw) };
        if let Err(error) =
            hr(call, "BackupComplete").and_then(|()| wait_async(async_raw, "BackupComplete"))
        {
            tracing::debug!(target: "vss", "BackupComplete: {error}");
        }
    }

    /// Tells the writers to abandon the backup. Best effort: failures do not
    /// change the fate of the in-flight snapshot set.
    fn abort_backup(raw: *mut ffi::IVssBackupComponentsRaw) {
        // SAFETY: vtable slot method call as in `initialize_for_backup`.
        unsafe {
            let call = ((*(*raw).vtbl).AbortBackup)(raw);
            if call != 0 {
                tracing::debug!(target: "vss", "AbortBackup: 0x{call:08X}");
            }
        }
    }

    /// Commits the set and collects one device string per added volume.
    fn gather_shadows(
        raw: *mut ffi::IVssBackupComponentsRaw,
        added: &[(&VolumePlan, GUID)],
    ) -> Result<Vec<VolumeShadow>> {
        do_snapshot_set(raw)?;

        let mut shadows: Vec<VolumeShadow> = Vec::with_capacity(added.len());
        for (volume, snapshot_id) in added.iter().copied() {
            let device = snapshot_device(raw, snapshot_id)?;
            shadows.push(VolumeShadow {
                key: volume.key,
                device_utf16: device.encode_utf16().collect(),
            });
        }
        Ok(shadows)
    }

    /// `DoSnapshotSet` + wait until the whole set is committed.
    fn do_snapshot_set(raw: *mut ffi::IVssBackupComponentsRaw) -> Result<()> {
        let mut async_raw: *mut ffi::IVssAsyncRaw = std::ptr::null_mut();
        // SAFETY: `DoSnapshotSet` writes one interface pointer to the
        // out-parameter.
        let call = unsafe {
            let method = (*(*raw).vtbl).DoSnapshotSet;
            method(raw, &mut async_raw)
        };
        hr(call, "DoSnapshotSet")?;
        wait_async(async_raw, "DoSnapshotSet")
    }

    /// Blocks until the async operation finished, then checks its status.
    ///
    /// The wait is bounded: it polls in slices and gives up (cancelling the
    /// operation) once `ASYNC_OPERATION_TIMEOUT` elapses, so a wedged
    /// provider or writer cannot hang the snapshot forever.
    fn wait_async(async_raw: *mut ffi::IVssAsyncRaw, operation: &str) -> Result<()> {
        if async_raw.is_null() {
            return Err(MapacheError::Backend(
                "VSS returned a null async handle".to_string(),
            ));
        }
        struct ReleaseGuard(*mut ffi::IVssAsyncRaw);
        impl Drop for ReleaseGuard {
            fn drop(&mut self) {
                // SAFETY: `Release` is `IUnknown` vtable slot 2 on an object
                // we received from a single COM call.
                unsafe {
                    let release = (*(*self.0).vtbl).Release;
                    release(self.0);
                }
            }
        }
        let _guard = ReleaseGuard(async_raw);

        let deadline = Instant::now() + ASYNC_OPERATION_TIMEOUT;
        loop {
            // SAFETY: `Wait` call on the async handle held by the guard above;
            // live interface and correct slot signature.
            let call_wait = unsafe {
                let method = (*(*async_raw).vtbl).Wait;
                method(async_raw, WAIT_SLICE_MS)
            };
            // S_OK and S_FALSE both mean "the wait returned"; the operation's
            // own result is read from `QueryStatus` below.
            if call_wait != 0 && call_wait != S_FALSE {
                cancel_async(async_raw);
                return Err(MapacheError::Backend(format!(
                    "VSS IVssAsync::Wait failed: 0x{call_wait:08X}"
                )));
            }

            let mut status: HRESULT = 0;
            // SAFETY: `QueryStatus` call as `Wait` above; the out-parameter
            // points to a valid `HRESULT` and `pReserved` must be null.
            let call_status = unsafe {
                let method = (*(*async_raw).vtbl).QueryStatus;
                method(async_raw, &mut status, std::ptr::null_mut())
            };
            hr(call_status, "IVssAsync::QueryStatus")?;

            match status {
                VSS_S_ASYNC_FINISHED => return Ok(()),
                VSS_S_ASYNC_PENDING => {
                    if Instant::now() >= deadline {
                        cancel_async(async_raw);
                        return Err(MapacheError::Backend(format!(
                            "VSS {operation} did not finish within {} seconds",
                            ASYNC_OPERATION_TIMEOUT.as_secs()
                        )));
                    }
                }
                _ => return hr_async_status(status, operation),
            }
        }
    }

    /// Asks VSS to cancel an async operation that timed out or failed.
    fn cancel_async(async_raw: *mut ffi::IVssAsyncRaw) {
        // SAFETY: `Cancel` is a vtable slot on the live handle.
        let call = unsafe { ((*(*async_raw).vtbl).Cancel)(async_raw) };
        if call != 0 {
            tracing::debug!(target: "vss", "IVssAsync::Cancel: 0x{call:08X}");
        }
    }

    /// Reads `m_pwszSnapshotDeviceObject` for `snapshot_id`.
    fn snapshot_device(
        raw: *mut ffi::IVssBackupComponentsRaw,
        snapshot_id: GUID,
    ) -> Result<String> {
        // SAFETY: zeroed memory is a valid `VSS_SNAPSHOT_PROP` pre-state; the
        // call overwrites it or fails without side effects.
        let mut prop: ffi::VssSnapshotProp = unsafe { std::mem::zeroed() };
        // SAFETY: `GetSnapshotProperties` fills the property structure.
        let call = unsafe {
            let method = (*(*raw).vtbl).GetSnapshotProperties;
            method(raw, snapshot_id, &mut prop)
        };
        hr(call, "GetSnapshotProperties")?;

        // All string members are `VSS_PWSZ` owned by the property structure:
        // copy the device path out, then release the structure as a whole.
        let mut device = pwsz_to_string(prop.m_pwszSnapshotDeviceObject);
        // SAFETY: `VssFreeSnapshotPropertiesInternal` is the documented way to
        // release the property structure from `GetSnapshotProperties`: it
        // frees every string member with `vssapi`'s allocator and zeroes the
        // struct once.
        unsafe {
            VssFreeSnapshotPropertiesInternal(&mut prop);
        }
        if device.is_empty() {
            return Err(MapacheError::Backend(
                "VSS returned an empty snapshot device path".to_string(),
            ));
        }
        if !device.ends_with('\\') {
            device.push('\\');
        }
        Ok(device)
    }

    fn delete_snapshot_set(raw: *mut ffi::IVssBackupComponentsRaw, set_id: GUID) -> Result<()> {
        let mut deleted: i32 = 0;
        let mut not_deleted = GUID::default();
        // SAFETY: `DeleteSnapshots` writes the out parameters below. Both the
        // object type and the force flag are 4-byte values.
        let call = unsafe {
            let method = (*(*raw).vtbl).DeleteSnapshots;
            method(
                raw,
                set_id,
                VSS_OBJECT_SNAPSHOT_SET,
                1,
                &mut deleted,
                &mut not_deleted,
            )
        };
        hr(call, "DeleteSnapshots")?;
        if deleted <= 0 {
            return Err(MapacheError::Backend(
                "VSS DeleteSnapshots did not delete the snapshot set".to_string(),
            ));
        }
        Ok(())
    }

    /// Whether the running token allows creating shadow copies.
    fn is_elevated() -> bool {
        use windows_sys::Win32::{
            Security::{GetTokenInformation, TOKEN_ELEVATION, TOKEN_QUERY, TokenElevation},
            System::Threading::{GetCurrentProcess, OpenProcessToken},
        };

        // SAFETY: `GetCurrentProcess()` returns a pseudo-handle that does not
        // require closing; the real token handle is closed by `Token`.
        unsafe {
            let mut token: *mut core::ffi::c_void = std::ptr::null_mut();
            if OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) == 0 {
                return false;
            }
            struct Token(*mut core::ffi::c_void);
            impl Drop for Token {
                fn drop(&mut self) {
                    // SAFETY: handle returned by `OpenProcessToken`.
                    unsafe { CloseHandle(self.0) };
                }
            }
            let token = Token(token);

            let mut info = TOKEN_ELEVATION { TokenIsElevated: 0 };
            let mut returned = 0u32;
            let ok = GetTokenInformation(
                token.0,
                TokenElevation,
                (&mut info as *mut TOKEN_ELEVATION).cast(),
                std::mem::size_of::<TOKEN_ELEVATION>() as u32,
                &mut returned,
            );
            ok != 0 && info.TokenIsElevated != 0
        }
    }

    #[cfg(test)]
    mod tests {
        use std::borrow::Cow;

        use super::*;

        #[test]
        fn translation_maps_the_matching_volume_only() {
            let table = ShadowMapping::new(vec![
                VolumeShadow {
                    key: 'C',
                    device_utf16: "\\\\?\\GLOBALROOT\\Device\\HarddiskVolumeShadowCopy1\\"
                        .encode_utf16()
                        .collect(),
                },
                VolumeShadow {
                    key: 'D',
                    device_utf16: "\\\\?\\GLOBALROOT\\Device\\HarddiskVolumeShadowCopy2\\"
                        .encode_utf16()
                        .collect(),
                },
            ]);

            // Mapped drives match case-insensitively, including the root.
            assert_eq!(
                table.translate(Path::new("c:\\Users\\me\\a.txt")).to_str(),
                Some("\\\\?\\GLOBALROOT\\Device\\HarddiskVolumeShadowCopy1\\Users\\me\\a.txt")
            );
            assert_eq!(
                table.translate(Path::new("C:\\")).to_str(),
                Some("\\\\?\\GLOBALROOT\\Device\\HarddiskVolumeShadowCopy1\\")
            );
            assert_eq!(
                table.translate(Path::new("D:\\x")).to_str(),
                Some("\\\\?\\GLOBALROOT\\Device\\HarddiskVolumeShadowCopy2\\x")
            );

            // Unmapped drives and non-drive paths are returned unallocated.
            assert!(matches!(
                table.translate(Path::new("Z:\\x")),
                Cow::Borrowed(_)
            ));
            assert!(matches!(
                table.translate(Path::new("\\\\server\\share\\x")),
                Cow::Borrowed(_)
            ));
        }

        /// Guards the hand-written `VSS_SNAPSHOT_PROP` layout: VSS writes the
        /// whole structure into the caller's buffer, so a missing field or a
        /// differently aligned timestamp would overflow the stack object.
        #[test]
        #[cfg(target_pointer_width = "64")]
        fn snapshot_prop_layout_matches_vss() {
            assert_eq!(std::mem::size_of::<ffi::VssSnapshotProp>(), 128);
            assert_eq!(
                std::mem::offset_of!(ffi::VssSnapshotProp, m_pwszSnapshotDeviceObject),
                40
            );
            assert_eq!(
                std::mem::offset_of!(ffi::VssSnapshotProp, m_tsCreationTimestamp),
                112
            );
            assert_eq!(std::mem::offset_of!(ffi::VssSnapshotProp, m_eStatus), 120);
        }

        /// Guards the `VSS_PWSZ` reading path: the string members of
        /// `VSS_SNAPSHOT_PROP` are plain NUL-terminated strings with **no**
        /// length prefix in front of the allocation, so treating them as BSTRs
        /// (`SysStringLen`) reads unrelated bytes and can overrun the buffer.
        #[test]
        fn reads_vss_pwsz_strings_without_a_length_prefix() {
            let units: Vec<u16> = "\\\\?\\GLOBALROOT\\Device\\HarddiskVolumeShadowCopy7\\"
                .encode_utf16()
                .chain(std::iter::once(0))
                .collect();
            assert_eq!(
                pwsz_to_string(units.as_ptr()),
                "\\\\?\\GLOBALROOT\\Device\\HarddiskVolumeShadowCopy7\\"
            );

            let empty: Vec<u16> = vec![0];
            assert_eq!(pwsz_to_string(empty.as_ptr()), "");
            assert_eq!(pwsz_to_string(std::ptr::null()), "");
        }

        /// Only one session may own the global table, and a session clears it
        /// only while it is still the registered one.
        #[test]
        fn registration_is_single_session_and_owner_scoped() {
            /// Restores the global state even if an assertion panics.
            struct Clear;
            impl Drop for Clear {
                fn drop(&mut self) {
                    *ACTIVE.write() = None;
                    ACTIVE_SET.store(false, Ordering::Release);
                }
            }
            let _clear = Clear;

            let first = Arc::new(ShadowMapping::new(Vec::new()));
            let second = Arc::new(ShadowMapping::new(Vec::new()));

            assert!(register(&first));
            assert!(
                !register(&second),
                "a second concurrent session must be rejected"
            );
            deregister(&second);
            assert!(
                active_mapping().is_some(),
                "a stale session must not clear the table"
            );
            deregister(&first);
            assert!(active_mapping().is_none());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wide(s: &str) -> Vec<u16> {
        s.encode_utf16().collect()
    }

    #[test]
    fn split_drive_parses_drive_absolute_paths() {
        for (input, expected_key, expected_remainder) in [
            ("C:\\Users\\me", 'C', "Users\\me"),
            ("d:\\x", 'D', "x"),
            ("C:/Users/me", 'C', "Users/me"),
            ("\\\\?\\C:\\Users\\me", 'C', "Users\\me"),
        ] {
            let units = wide(input);
            let (key, remainder) =
                split_drive(&units).unwrap_or_else(|| panic!("{input} is a drive path"));
            assert_eq!(key, expected_key, "{input}");
            assert_eq!(
                String::from_utf16(remainder).unwrap(),
                expected_remainder,
                "{input}"
            );
        }
    }

    #[test]
    fn split_drive_rejects_non_drive_paths() {
        assert!(split_drive(&wide("\\\\server\\share\\x")).is_none());
        assert!(split_drive(&wide("\\\\?\\GLOBALROOT\\Device\\Other\\x")).is_none());
        assert!(split_drive(&wide("C:relative")).is_none());
        assert!(split_drive(&wide("C:")).is_none());
        assert!(split_drive(&wide("1:\\x")).is_none());
        assert!(split_drive(&wide("")).is_none());
    }

    #[test]
    fn join_device_path_normalizes_separators() {
        let device = wide("\\\\?\\GLOBALROOT\\Device\\HarddiskVolumeShadowCopy1\\");
        let joined = join_device_path(&device, &wide("Users/me/a.txt"));
        assert_eq!(
            String::from_utf16(&joined).unwrap(),
            "\\\\?\\GLOBALROOT\\Device\\HarddiskVolumeShadowCopy1\\Users\\me\\a.txt"
        );
    }
}
