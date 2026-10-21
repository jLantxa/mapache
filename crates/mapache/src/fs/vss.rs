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
//! Everything here is best-effort: if VSS is unavailable (not elevated, the
//! service is disabled, a volume is unsupported) the session degrades to
//! ordinary live reads with a warning instead of failing the snapshot.
//!
//! Implementation notes: the `windows-sys` bindings do not expose the
//! requester-side `IVssBackupComponents` interface (microsoft/win32metadata
//! #1790), so the needed vtable slots are declared in this file, in
//! declaration order of the SDK's `VsBackup.h` (vtable order follows IDL
//! declaration order). All COM traffic — creation and deletion — runs on a
//! single dedicated worker thread with an MTA apartment.

use std::path::{Path, PathBuf};

#[cfg(windows)]
use crate::common::error::MapacheError;

use crate::common::error::Result;

/// Translates `path` to its shadow-copy path when a VSS session is active and
/// the path lives on one of the snapshotted volumes.
///
/// Returns the path unchanged in every other case: no active session, an
/// already translated path (`\\?\GLOBALROOT\...` matches no drive prefix), or
/// a path whose volume has no shadow copy.
#[inline]
#[must_use]
pub fn translate(path: &Path) -> PathBuf {
    #[cfg(windows)]
    {
        match imp::active_mapping() {
            Some(mapping) => mapping.translate(path),
            None => path.to_path_buf(),
        }
    }
    #[cfg(not(windows))]
    {
        path.to_path_buf()
    }
}

/// A session that redirects source reads to a set of shadow-copied volumes.
///
/// Created before source paths are walked; the translation stays registered
/// until the session is dropped, at which point the shadow copies are deleted
/// (best effort) and reads fall back to the live filesystem.
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
    /// fails on invalid VSS state: it logs a warning and returns an inert
    /// session (ordinary live reads). The shadow copies may take a few
    /// seconds to create; the wait happens on the blocking pool.
    pub async fn start<P>(source_paths: &[P]) -> Result<Self>
    where
        P: AsRef<Path>,
        Self: Sized,
    {
        #[cfg(windows)]
        {
            let paths: Vec<PathBuf> = source_paths
                .iter()
                .map(|p| p.as_ref().to_path_buf())
                .collect();

            match tokio::task::spawn_blocking(move || imp::start_sync(&paths)).await {
                Ok(inner) => Ok(VssSession { inner: Some(inner) }),
                Err(e) => Err(MapacheError::Internal(format!(
                    "VSS session task panicked: {e}"
                ))),
            }
        }
        #[cfg(not(windows))]
        {
            crate::ui::cli::warning!(
                "--use-fs-snapshot is only supported on Windows; continuing \
                 with ordinary live reads"
            );
            let _ = source_paths;
            Ok(VssSession { inner: None })
        }
    }
}

impl Drop for VssSession {
    fn drop(&mut self) {
        // `SessionInner::drop` deregisters the mapping and deletes the
        // snapshot set.
        self.inner = None;
    }
}

/// Non-Windows stub so the `VssSession` field type resolves; sessions there
/// are always inert.
#[cfg(not(windows))]
mod imp {
    pub(super) struct SessionInner;
}

/// Windows implementation: COM plumbing, snapshot-set lifecycle and the
/// active path-translation table.
#[cfg(windows)]
mod imp {
    use std::{
        ffi::OsString,
        os::windows::ffi::{OsStrExt, OsStringExt},
        path::{Path, PathBuf},
        sync::{Arc, mpsc},
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
        ui::{self},
    };

    /// Translation table registered for the duration of `VssSession`.
    static ACTIVE: parking_lot::RwLock<Option<Arc<ShadowMapping>>> = parking_lot::RwLock::new(None);

    #[inline]
    pub(super) fn active_mapping() -> Option<Arc<ShadowMapping>> {
        ACTIVE.read().clone()
    }

    fn set_active(mapping: Option<Arc<ShadowMapping>>) {
        *ACTIVE.write() = mapping;
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
        pub(super) fn translate(&self, path: &Path) -> PathBuf {
            let wide: Vec<u16> = path.as_os_str().encode_wide().collect();
            let (key, remainder) = match split_drive(&wide) {
                Some(split) => split,
                None => return path.to_path_buf(),
            };
            let volume = match self.volumes.iter().find(|v| v.key == key) {
                Some(volume) => volume,
                None => return path.to_path_buf(),
            };

            let mut out = volume.device_utf16.clone();
            out.extend_from_slice(remainder);
            PathBuf::from(OsString::from_wide(&out))
        }
    }

    /// Splits `C:\rest` into `(drive-key, "rest")` on the UTF-16 form.
    ///
    /// Returns `None` for anything that is not a drive-letter absolute path:
    /// UNC paths, `\\?\`-prefixed and already translated paths, drive-relative
    /// forms without a separator, and malformed input.
    fn split_drive(wide: &[u16]) -> Option<(char, &[u16])> {
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

    /// Reads a NUL-terminated UTF-16 string into a `String`, replacing
    /// invalid units; a null pointer reads as empty. Works for plain buffers
    /// and for BSTR contents alike.
    fn read_wide_until_nul(ptr: *const u16) -> String {
        if ptr.is_null() {
            return String::new();
        }
        let mut units: Vec<u16> = Vec::new();
        let mut index = 0usize;
        // SAFETY: `ptr` refers to a null-terminated UTF-16 sequence, which is
        // what the walk below stays inside of.
        unsafe {
            while *ptr.add(index) != 0 {
                units.push(*ptr.add(index));
                index += 1;
            }
        }
        String::from_utf16_lossy(&units)
    }

    fn wide_zero_terminated(s: &str) -> Vec<u16> {
        s.encode_utf16().chain(std::iter::once(0)).collect()
    }

    // -----------------------------------------------------------------------
    // COM vtables (subset needed to create and delete a snapshot set)
    // -----------------------------------------------------------------------

    mod ffi {
        #![allow(non_snake_case)]
        #![allow(non_camel_case_types)]

        use windows_sys::core::{BSTR, GUID, HRESULT, PCWSTR};

        /// `IVssBackupComponents` raw pointer (`IUnknown`-like layout).
        #[repr(C)]
        pub struct IVssBackupComponentsRaw {
            pub vtbl: *const IVssBackupComponentsVtbl,
        }

        /// Declaration order follows `VsBackup.h` (vtable order follows IDL
        /// declaration order); every slot is typed for clarity.
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
            pub SetBackupState:
                unsafe extern "system" fn(*mut IVssBackupComponentsRaw, u8, u8, i32, u8) -> HRESULT,
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

        /// Mirrors `VSS_SNAPSHOT_PROP` (`vss.h`). String members are BSTRs
        /// owned by `vssapi`'s allocator and must be released with
        /// `VssFreeSnapshotProperties`; freeing them with `SysFreeString`
        /// individually corrupts the heap.
        #[repr(C)]
        pub struct VssSnapshotProp {
            pub m_SnapshotId: GUID,
            pub m_SnapshotSetId: GUID,
            pub m_lSnapshotsCount: i32,
            pub m_pwszSnapshotDeviceObject: BSTR,
            pub m_pwszOriginalVolumeName: BSTR,
            pub m_pwszOriginatingMachine: BSTR,
            pub m_pwszServiceMachine: BSTR,
            pub m_pwszExposedName: BSTR,
            pub m_pwszExposedPath: BSTR,
            pub m_ProviderId: GUID,
            pub m_lSnapshotAttributes: i32,
            pub m_tsCreationTimestamp: windows_sys::Win32::Foundation::FILETIME,
            pub m_eStatus: i32,
        }
    }

    // SAFETY: `CreateVssBackupComponentsInternal` is exported by
    // `vssapi.dll` — the unmangled alias of `CreateVssBackupComponents` — and
    // links against the SDK import library `vssapi.lib`, which MSVC toolchains
    // resolve automatically. Both exports point to the same entry point; the
    // documented plain-C name is not present unmangled in either the DLL or
    // any SDK import library.
    #[link(name = "vssapi")]
    unsafe extern "system" {
        pub fn CreateVssBackupComponentsInternal(
            ppbackup: *mut *mut ffi::IVssBackupComponentsRaw,
        ) -> HRESULT;
        /// Frees the BSTR members of a `VSS_SNAPSHOT_PROP` returned by
        /// `GetSnapshotProperties`. The strings are owned by `vssapi`'s
        /// allocator, so `SysFreeString` per member is not the way to
        /// release them.
        ///
        /// This is the SDK import-library alias of the unmangled
        /// `VssFreeSnapshotProperties` export (both resolve to the same
        /// address in `vssapi.dll`).
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
    /// No timeout for `IVssAsync::Wait`.
    const WAIT_FOREVER_MS: u32 = u32::MAX;

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
                "VSS {operation} is still pending after Wait"
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

    /// Distinct volumes to shadow.
    struct VolumePlan {
        /// `AddToSnapshotSet` name, e.g. `\\?\Volume{1a2b-...}\`.
        volume_name: String,
        /// Drive key used by the translation table.
        key: char,
    }

    pub(super) struct SessionInner {
        /// Signals the worker to delete the snapshot set and exit.
        shutdown: Option<mpsc::Sender<()>>,
        worker: Option<std::thread::JoinHandle<()>>,
    }

    impl Drop for SessionInner {
        fn drop(&mut self) {
            set_active(None);
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
    pub(super) fn start_sync(source_paths: &[PathBuf]) -> SessionInner {
        match create_session(source_paths) {
            Ok(inner) => inner,
            Err(error) => {
                ui::cli::warning!(
                    "Continuing without a filesystem snapshot (files that a \
                     running program keeps open for exclusive access may be \
                     skipped): {error}"
                );
                SessionInner {
                    shutdown: None,
                    worker: None,
                }
            }
        }
    }

    fn create_session(source_paths: &[PathBuf]) -> Result<SessionInner> {
        if !is_elevated() {
            return Err(MapacheError::Backend(
                "--use-fs-snapshot requires administrator privileges (shadow \
                 copies are created with elevated rights)"
                    .to_string(),
            ));
        }

        let plan = plan_volumes(source_paths)?;
        if plan.is_empty() {
            return Err(MapacheError::Backend(
                "no local drive among the source paths supports a VSS shadow \
                 copy (non-drive paths are read from the live filesystem)"
                    .to_string(),
            ));
        }

        let (ready_tx, ready_rx) = mpsc::channel();
        let (shutdown_tx, shutdown_rx) = mpsc::channel();

        let volumes = plan;
        let worker = std::thread::spawn(move || {
            worker_run(volumes, ready_tx, shutdown_rx);
        });

        match ready_rx.recv() {
            Ok(Ok(mapping)) => {
                set_active(Some(mapping));
            }
            Ok(Err(error)) => {
                // Dropping `shutdown_tx` at scope end releases the worker,
                // which cleans up and exits.
                return Err(error);
            }
            Err(_) => {
                return Err(MapacheError::Backend(
                    "the VSS worker thread died unexpectedly".to_string(),
                ));
            }
        }

        Ok(SessionInner {
            shutdown: Some(shutdown_tx),
            worker: Some(worker),
        })
    }

    fn plan_volumes(source_paths: &[PathBuf]) -> Result<Vec<VolumePlan>> {
        let mut keys: Vec<char> = Vec::new();
        for path in source_paths {
            let normalized = get_absolute_normalized_path(path)?;
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
            plan.push(VolumePlan {
                volume_name: volume_guid_path(key)?,
                key,
            });
        }
        Ok(plan)
    }

    /// `\\?\Volume{GUID}\` for a local drive letter.
    fn volume_guid_path(key: char) -> Result<String> {
        // `GetVolumeNameForVolumeMountPointW` requires a trailing separator,
        // and a volume GUID path fits comfortably in 128 UTF-16 units.
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
                "cannot resolve the volume path of drive {key}: (error {code})"
            )));
        }
        Ok(read_wide_until_nul(buffer.as_ptr()))
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
        let added = add_volumes(raw, volumes);

        let shadows = match added {
            Ok(snapshot_ids) => {
                prepare_for_backup(raw)?;
                gather_shadows(raw, volumes, &snapshot_ids)
            }
            Err(error) => Err(error),
        };
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

    /// Adds each planned volume to the set, returning the snapshot ids in
    /// the same order.
    fn add_volumes(
        raw: *mut ffi::IVssBackupComponentsRaw,
        volumes: &[VolumePlan],
    ) -> Result<Vec<GUID>> {
        let mut snapshot_ids: Vec<GUID> = Vec::with_capacity(volumes.len());
        for volume in volumes {
            let name = wide_zero_terminated(&volume.volume_name);
            let mut snapshot_id = GUID::default();
            // SAFETY: vtable slot method call as in `initialize_for_backup`;
            // `name` is a null-terminated UTF-16 temporary alive until the
            // end of the call and the out-parameter is a valid `GUID` target.
            unsafe {
                let call = ((*(*raw).vtbl).AddToSnapshotSet)(
                    raw,
                    name.as_ptr(),
                    GUID::default(),
                    &mut snapshot_id,
                );
                hr(call, "AddToSnapshotSet")?;
            }
            snapshot_ids.push(snapshot_id);
        }
        Ok(snapshot_ids)
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
            hr(call, "BackupComplete").and_then(|_| wait_async(async_raw, "BackupComplete"))
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

    /// Commits the set and collects one device string per planned volume.
    fn gather_shadows(
        raw: *mut ffi::IVssBackupComponentsRaw,
        volumes: &[VolumePlan],
        snapshot_ids: &[GUID],
    ) -> Result<Vec<VolumeShadow>> {
        do_snapshot_set(raw)?;

        let mut shadows: Vec<VolumeShadow> = Vec::with_capacity(snapshot_ids.len());
        for (volume, &snapshot_id) in volumes.iter().zip(snapshot_ids) {
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

        // SAFETY: `Wait` call on the async handle held by the guard above;
        // live interface and correct slot signature.
        let call_wait = unsafe {
            let method = (*(*async_raw).vtbl).Wait;
            method(async_raw, WAIT_FOREVER_MS)
        };
        hr(call_wait, "IVssAsync::Wait")?;

        let mut status: HRESULT = 0;
        // SAFETY: `QueryStatus` call as `Wait` above; the out-parameter points
        // to a valid `HRESULT` and `pReserved` must be null.
        let call_status = unsafe {
            let method = (*(*async_raw).vtbl).QueryStatus;
            method(async_raw, &mut status, std::ptr::null_mut())
        };
        hr(call_status, "IVssAsync::QueryStatus")?;
        hr_async_status(status, operation)
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

        // All string members are BSTRs owned by the requester: copy the device
        // path out, then free every one of them.
        let mut device = read_wide_until_nul(prop.m_pwszSnapshotDeviceObject);
        // SAFETY: `VssFreeSnapshotProperties` is the documented way to release
        // the property structure from `GetSnapshotProperties`: it frees every
        // BSTR member with `vssapi`'s allocator and zeroes the struct once.
        unsafe {
            VssFreeSnapshotPropertiesInternal(&mut prop);
        }
        if !device.is_empty() && !device.ends_with('\\') {
            device.push('\\');
        }
        Ok(device)
    }

    fn delete_snapshot_set(raw: *mut ffi::IVssBackupComponentsRaw, set_id: GUID) -> Result<()> {
        let mut deleted: i32 = 0;
        let mut not_deleted = GUID::default();
        // SAFETY: `DeleteSnapshots` writes the out parameters below.
        let call = unsafe {
            let method = (*(*raw).vtbl).DeleteSnapshots;
            method(
                raw,
                set_id,
                VSS_OBJECT_SNAPSHOT_SET,
                1, // force delete
                &mut deleted,
                &mut not_deleted,
            )
        };
        hr(call, "DeleteSnapshots")
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
        use super::*;

        fn mapping(pairs: &[(&str, &str)]) -> ShadowMapping {
            ShadowMapping::new(
                pairs
                    .iter()
                    .map(|&(key, device)| VolumeShadow {
                        key: key.chars().next().unwrap_or('!'),
                        device_utf16: device.encode_utf16().collect(),
                    })
                    .collect(),
            )
        }

        #[test]
        fn translates_drive_paths() {
            let table = mapping(&[(
                "C",
                "\\\\?\\GLOBALROOT\\Device\\HarddiskVolumeShadowCopy1\\",
            )]);

            let translated = table.translate(Path::new("C:\\Users\\me\\a.txt"));
            assert_eq!(
                translated.to_str(),
                Some("\\\\?\\GLOBALROOT\\Device\\HarddiskVolumeShadowCopy1\\Users\\me\\a.txt")
            );
            assert_eq!(
                table.translate(Path::new("C:\\")).to_str(),
                Some("\\\\?\\GLOBALROOT\\Device\\HarddiskVolumeShadowCopy1\\")
            );
        }

        #[test]
        fn transcribes_case_and_passthrough() {
            let table = mapping(&[(
                "D",
                "\\\\?\\GLOBALROOT\\Device\\HarddiskVolumeShadowCopy2\\",
            )]);

            // Case-insensitive volume keys.
            assert_eq!(
                table.translate(Path::new("d:\\x")).to_str(),
                Some("\\\\?\\GLOBALROOT\\Device\\HarddiskVolumeShadowCopy2\\x")
            );
            // Unmapped volume passes through.
            assert_eq!(table.translate(Path::new("Z:\\x")).to_str(), Some("Z:\\x"));
            // UNC and drive-relative forms pass through unmapped.
            assert_eq!(
                table.translate(Path::new("\\\\server\\share\\x")).to_str(),
                Some("\\\\server\\share\\x")
            );
            assert_eq!(
                table.translate(Path::new("D:relative")).to_str(),
                Some("D:relative")
            );
            // Already translated paths pass through unchanged.
            assert_eq!(
                table
                    .translate(Path::new("\\\\?\\GLOBALROOT\\Device\\Other\\x"))
                    .to_str(),
                Some("\\\\?\\GLOBALROOT\\Device\\Other\\x")
            );
        }
    }
}
