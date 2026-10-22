#![allow(non_snake_case)]
#![allow(non_camel_case_types)]

use windows_sys::core::{BSTR, GUID, HRESULT, PCWSTR};

/// `VSS_PWSZ`: a plain NUL-terminated `WCHAR` string. Note the difference
/// from a `BSTR`, which carries a byte-length prefix.
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
    pub SetBackupState:
        unsafe extern "system" fn(*mut IVssBackupComponentsRaw, i32, i32, i32, i32) -> HRESULT,
    pub InitializeForRestore:
        unsafe extern "system" fn(*mut IVssBackupComponentsRaw, BSTR) -> HRESULT,
    pub SetRestoreState: unsafe extern "system" fn(*mut IVssBackupComponentsRaw, i32) -> HRESULT,
    pub GatherWriterMetadata:
        unsafe extern "system" fn(*mut IVssBackupComponentsRaw, *mut *mut IVssAsyncRaw) -> HRESULT,
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
    pub PrepareForBackup:
        unsafe extern "system" fn(*mut IVssBackupComponentsRaw, *mut *mut IVssAsyncRaw) -> HRESULT,
    pub AbortBackup: unsafe extern "system" fn(*mut IVssBackupComponentsRaw) -> HRESULT,
    pub GatherWriterStatus:
        unsafe extern "system" fn(*mut IVssBackupComponentsRaw, *mut *mut IVssAsyncRaw) -> HRESULT,
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
    pub SaveAsXML: unsafe extern "system" fn(*mut IVssBackupComponentsRaw, *mut BSTR) -> HRESULT,
    pub BackupComplete:
        unsafe extern "system" fn(*mut IVssBackupComponentsRaw, *mut *mut IVssAsyncRaw) -> HRESULT,
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
    pub PreRestore:
        unsafe extern "system" fn(*mut IVssBackupComponentsRaw, *mut *mut IVssAsyncRaw) -> HRESULT,
    pub PostRestore:
        unsafe extern "system" fn(*mut IVssBackupComponentsRaw, *mut *mut IVssAsyncRaw) -> HRESULT,
    pub SetContext: unsafe extern "system" fn(*mut IVssBackupComponentsRaw, i32) -> HRESULT,
    pub StartSnapshotSet:
        unsafe extern "system" fn(*mut IVssBackupComponentsRaw, *mut GUID) -> HRESULT,
    pub AddToSnapshotSet:
        unsafe extern "system" fn(*mut IVssBackupComponentsRaw, PCWSTR, GUID, *mut GUID) -> HRESULT,
    pub DoSnapshotSet:
        unsafe extern "system" fn(*mut IVssBackupComponentsRaw, *mut *mut IVssAsyncRaw) -> HRESULT,
    pub DeleteSnapshots: unsafe extern "system" fn(
        *mut IVssBackupComponentsRaw,
        GUID,
        i32,
        i32,
        *mut i32,
        *mut GUID,
    ) -> HRESULT,
    pub ImportSnapshots:
        unsafe extern "system" fn(*mut IVssBackupComponentsRaw, *mut *mut IVssAsyncRaw) -> HRESULT,
    pub BreakSnapshotSet: unsafe extern "system" fn(*mut IVssBackupComponentsRaw, GUID) -> HRESULT,
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
    pub IsVolumeSupported:
        unsafe extern "system" fn(*mut IVssBackupComponentsRaw, GUID, PCWSTR, *mut i32) -> HRESULT,
    pub DisableWriterClasses:
        unsafe extern "system" fn(*mut IVssBackupComponentsRaw, *const GUID, u32) -> HRESULT,
    pub EnableWriterClasses:
        unsafe extern "system" fn(*mut IVssBackupComponentsRaw, *const GUID, u32) -> HRESULT,
    pub DisableWriterInstances:
        unsafe extern "system" fn(*mut IVssBackupComponentsRaw, *const GUID, u32) -> HRESULT,
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

/// Mirrors `VSS_SNAPSHOT_PROP` (`vss.h`). The string members are `VSS_PWSZ`
/// owned by `vssapi` and released as a whole with
/// `VssFreeSnapshotPropertiesInternal`; they must not be freed individually
/// with `SysFreeString`/`CoTaskMemFree`.
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
    /// `VSS_TIMESTAMP` is a `LONGLONG` (8-byte aligned). Declaring it as
    /// `FILETIME` (8 bytes, but 4-byte aligned) would shrink this structure,
    /// and VSS writes the full `VSS_SNAPSHOT_PROP` into the caller's buffer.
    pub m_tsCreationTimestamp: i64,
    pub m_eStatus: i32,
}
