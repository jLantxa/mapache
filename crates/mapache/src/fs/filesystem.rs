use std::{io, path::Path};

use crate::{
    common::error::Result,
    fs::node::{Node, NodeType},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanMetadata {
    pub node_type: NodeType,
    pub size: u64,
    pub dev: Option<u64>,
}

/// Returns scan metadata without following symlinks or reading extended attributes.
pub fn scan_metadata(path: &Path) -> Result<ScanMetadata> {
    let (metadata, node_type) = Node::fetch_metadata_and_type_sync(path, false, false)?;
    Ok(ScanMetadata {
        node_type,
        size: metadata.size,
        dev: metadata.dev,
    })
}

/// Fills `dev` with a filesystem device identifier when it is not known yet.
///
/// On Unix the device ID is part of every `lstat` result, so this is a no-op.
/// On Windows `std` does not expose device identifiers on stable builds, so
/// the volume serial number is queried on demand. Callers only do this for
/// `--one-file-system`, because the extra handle open per queried directory
/// would otherwise slow down every snapshot. Failures leave `dev` as `None`;
/// `--one-file-system` then treats the entry as staying on the same
/// filesystem (and rejects source paths whose device ID cannot be read).
pub fn fill_device_id(dev: &mut Option<u64>, path: &Path) {
    #[cfg(windows)]
    {
        if dev.is_none() {
            *dev = volume_serial_number(path);
        }
    }
    #[cfg(not(windows))]
    {
        let _ = (dev, path);
    }
}

/// Reads the volume serial number of `path` without following reparse points.
///
/// The handle is opened the way `lstat` observes a path, so symlinks and
/// junctions report the volume they live on instead of their target's volume.
#[cfg(windows)]
fn volume_serial_number(path: &Path) -> Option<u64> {
    use std::{iter, mem, os::windows::ffi::OsStrExt};

    use windows_sys::Win32::{
        Foundation::{CloseHandle, INVALID_HANDLE_VALUE},
        Storage::FileSystem::{
            BY_HANDLE_FILE_INFORMATION, CreateFileW, FILE_FLAG_BACKUP_SEMANTICS,
            FILE_FLAG_OPEN_REPARSE_POINT, FILE_READ_ATTRIBUTES, FILE_SHARE_DELETE, FILE_SHARE_READ,
            FILE_SHARE_WRITE, GetFileInformationByHandle, OPEN_EXISTING,
        },
    };

    let wide: Vec<u16> = path
        .as_os_str()
        .encode_wide()
        .chain(iter::once(0))
        .collect();

    // SAFETY: `wide` is null-terminated; the handle is validated before use.
    let handle = unsafe {
        CreateFileW(
            wide.as_ptr(),
            FILE_READ_ATTRIBUTES,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            std::ptr::null(),
            OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT,
            std::ptr::null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return None;
    }

    let mut info = mem::MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::uninit();
    // SAFETY: `handle` is valid and `info` points to writable memory.
    let queried = unsafe { GetFileInformationByHandle(handle, info.as_mut_ptr()) };
    // SAFETY: `CloseHandle` is safe for any handle returned by `CreateFileW`.
    unsafe { CloseHandle(handle) };
    if queried == 0 {
        return None;
    }

    // SAFETY: initialized by the successful query above.
    Some(unsafe { info.assume_init() }.dwVolumeSerialNumber as u64)
}

/// Opens a file with platform-specific sequential-read and access-time flags.
pub fn open_for_sequential_read(path: &Path) -> io::Result<std::fs::File> {
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        const FILE_FLAG_SEQUENTIAL_SCAN: u32 = 0x0800_0000;
        std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(FILE_FLAG_SEQUENTIAL_SCAN)
            .open(path)
    }
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        use std::os::unix::fs::OpenOptionsExt;

        let file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOATIME)
            .open(path)
            .or_else(|error| {
                if matches!(error.raw_os_error(), Some(libc::EPERM) | Some(libc::EINVAL)) {
                    std::fs::File::open(path)
                } else {
                    Err(error)
                }
            })?;
        let fd = file.as_raw_fd();

        unsafe {
            // SAFETY: `fd` is valid and the advice range covers the open file.
            libc::posix_fadvise(fd, 0, 0, libc::POSIX_FADV_SEQUENTIAL);
        }

        Ok(file)
    }
    #[cfg(all(unix, not(target_os = "linux")))]
    {
        std::fs::File::open(path)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use super::{ScanMetadata, open_for_sequential_read, scan_metadata};

    #[cfg(windows)]
    use super::fill_device_id;

    #[test]
    fn basic_metadata_matches_full_metadata() -> crate::common::error::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("input");
        std::fs::write(&path, b"filesystem test")?;
        for path in [dir.path(), path.as_path()] {
            let node = super::Node::from_path_sync(path, false)?;
            assert_eq!(
                scan_metadata(path)?,
                ScanMetadata {
                    node_type: node.node_type,
                    size: node.metadata.size,
                    dev: node.metadata.dev,
                }
            );
        }
        assert!(scan_metadata(&dir.path().join("missing")).is_err());
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn basic_metadata_does_not_follow_symlinks() -> crate::common::error::Result<()> {
        let dir = tempfile::tempdir()?;
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(dir.path().join("missing"), &link)?;
        assert_eq!(scan_metadata(&link)?.node_type, super::NodeType::Symlink);
        Ok(())
    }

    #[test]
    fn opens_and_reads_file() -> std::io::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("input");
        std::fs::write(&path, b"filesystem test")?;

        let mut file = open_for_sequential_read(&path)?;
        let mut contents = Vec::new();
        file.read_to_end(&mut contents)?;

        assert_eq!(contents, b"filesystem test");
        Ok(())
    }

    #[cfg(windows)]
    #[test]
    fn fills_device_id_for_directories_and_files() -> crate::common::error::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("input");
        std::fs::write(&path, b"device id test")?;

        let mut dir_dev = None;
        fill_device_id(&mut dir_dev, dir.path());
        let mut file_dev = None;
        fill_device_id(&mut file_dev, &path);

        assert!(dir_dev.is_some());
        // A file always lives on its parent directory's volume.
        assert_eq!(dir_dev, file_dev);

        // Known device IDs are never overwritten.
        let mut kept = Some(u64::MAX);
        fill_device_id(&mut kept, &path);
        assert_eq!(kept, Some(u64::MAX));
        Ok(())
    }

    #[cfg(windows)]
    #[test]
    fn fills_device_id_for_junctions() -> crate::common::error::Result<()> {
        let dir = tempfile::tempdir()?;
        let target = dir.path().join("target");
        std::fs::create_dir(&target)?;
        let junction = dir.path().join("junction");
        // mklink /J does not require administrator rights or developer mode.
        let status = std::process::Command::new("cmd")
            .args(["/c", "mklink", "/J"])
            .arg(&junction)
            .arg(&target)
            .status()?;
        assert!(status.success());

        let mut junction_dev = None;
        fill_device_id(&mut junction_dev, &junction);
        let mut parent_dev = None;
        fill_device_id(&mut parent_dev, dir.path());

        assert!(junction_dev.is_some());
        // The junction itself lives on the parent volume; it is not followed.
        assert_eq!(junction_dev, parent_dev);
        Ok(())
    }
}
