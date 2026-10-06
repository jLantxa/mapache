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
}
