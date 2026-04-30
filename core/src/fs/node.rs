use std::{
    collections::BTreeMap,
    fs::Metadata as FsMetadata,
    path::{Path, PathBuf},
    time::SystemTime,
};

#[cfg(unix)]
use std::os::unix::fs::{FileTypeExt, MetadataExt};

#[cfg(target_os = "linux")]
use std::os::unix::{ffi::OsStrExt, io::AsRawFd};

#[cfg(windows)]
use std::os::windows::fs::MetadataExt;

use anyhow::{Context, Result, bail};
use colored::Colorize;
use serde::{Deserialize, Serialize};

use crate::{mapache::ID, utils};

/// The type of a node (file, directory, symlink, etc.)
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeType {
    #[default]
    File,
    Directory,
    Symlink,
    BlockDevice,
    CharDevice,
    Fifo,
    Socket,
}

/// A node in the file system tree. This struct is serialized; keep field order stable.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Node {
    pub name: String,

    #[serde(rename = "type")]
    pub node_type: NodeType,

    pub metadata: Metadata,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub symlink_info: Option<SymlinkInfo>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub blobs: Option<Vec<ID>>, // For files

    #[serde(skip_serializing_if = "Option::is_none")]
    pub tree: Option<ID>, // For directories
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SymlinkInfo {
    pub target_path: PathBuf, // Target path
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_type: Option<NodeType>, // Type of node referenced by the symlink (necessary for restoration in Windows)
}

/// Node metadata. This struct is serialized; keep field order stable.
///
/// We ignore the accessed time. This field changes everytime we analyze a file for backup,
/// altering the hash of the node. The accessed time will be updated after restoring the
/// file anyway. We don't include it in the metadata, but we still have it here.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct Metadata {
    /// Size in bytes
    pub size: u64,

    /// Accessed time
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accessed_time: Option<SystemTime>,
    /// Created time
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_time: Option<SystemTime>,
    /// Modified time
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modified_time: Option<SystemTime>,

    /// Unix mode
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<u32>,
    // Unix owner user id
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner_uid: Option<u32>,
    /// Unix owner group id
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner_gid: Option<u32>,

    // The unique file serial number on a given device
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inode: Option<u64>,

    // The device ID containing the file
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dev: Option<u64>,

    // The number of hard links pointing to this inode
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nlink: Option<u64>,

    // Raw device ID for block/char devices
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rdev: Option<u64>,

    /// Extended attributes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extended_attributes: Option<BTreeMap<String, Vec<u8>>>,

    /// Windows file attributes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub windows_attributes: Option<u32>,

    /// Linux file flags (chattr)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub linux_flags: Option<u32>,
}

/// Minimal manual implementation of the Linux `statx` syscall.
///
/// This is necessary because some libc versions (like older musl versions used in static
/// Rust builds) do not provide the `statx` wrapper or its associated types.
/// By talking directly to the kernel, we can reliably retrieve the birth time (creation time)
/// of files, which is otherwise unavailable in such environments.
#[cfg(target_os = "linux")]
#[allow(dead_code)]
mod linux_statx {
    pub const STATX_TYPE: u32 = 0x0001;
    pub const STATX_MODE: u32 = 0x0002;
    pub const STATX_NLINK: u32 = 0x0004;
    pub const STATX_UID: u32 = 0x0008;
    pub const STATX_GID: u32 = 0x0010;
    pub const STATX_ATIME: u32 = 0x0020;
    pub const STATX_MTIME: u32 = 0x0040;
    pub const STATX_CTIME: u32 = 0x0080;
    pub const STATX_INO: u32 = 0x0100;
    pub const STATX_SIZE: u32 = 0x0200;
    pub const STATX_BLOCKS: u32 = 0x0400;
    /// Mask for all "basic" stats available in a standard stat call.
    pub const STATX_BASIC_STATS: u32 = 0x07ff;
    /// Mask for the file birth time (creation time).
    pub const STATX_BTIME: u32 = 0x0800;

    /// Don't sync attributes with the server (for network filesystems).
    pub const AT_STATX_DONT_SYNC: i32 = 0x4000;

    #[repr(C)]
    #[derive(Debug, Clone, Copy)]
    /// Timestamp format used by the `statx` syscall.
    pub struct statx_timestamp {
        pub tv_sec: i64,
        pub tv_nsec: u32,
        pub __reserved: i32,
    }

    #[repr(C)]
    /// The structure returned by the `statx` syscall.
    /// See `man statx(2)` for details on field meanings.
    pub struct statx {
        pub stx_mask: u32,
        pub stx_blksize: u32,
        pub stx_attributes: u64,
        pub stx_nlink: u32,
        pub stx_uid: u32,
        pub stx_gid: u32,
        pub stx_mode: u16,
        pub __spare0: [u16; 1],
        pub stx_ino: u64,
        pub stx_size: u64,
        pub stx_blocks: u64,
        pub stx_attributes_mask: u64,
        pub stx_atime: statx_timestamp,
        pub stx_btime: statx_timestamp,
        pub stx_ctime: statx_timestamp,
        pub stx_mtime: statx_timestamp,
        pub stx_rdev_major: u32,
        pub stx_rdev_minor: u32,
        pub stx_dev_major: u32,
        pub stx_dev_minor: u32,
        pub __spare2: [u64; 14],
    }

    /// Invokes the `statx` syscall directly via `libc::syscall`.
    ///
    /// # Safety
    /// This is a raw syscall wrapper. The caller must ensure that `pathname` points to a
    /// valid C-string and `statxbuf` points to a valid `statx` structure.
    pub unsafe fn statx(
        dirfd: i32,
        pathname: *const libc::c_char,
        flags: i32,
        mask: u32,
        statxbuf: *mut statx,
    ) -> i32 {
        #[cfg(target_arch = "x86_64")]
        const SYS_STATX: libc::c_long = 332;
        #[cfg(target_arch = "aarch64")]
        const SYS_STATX: libc::c_long = 291;
        #[cfg(target_arch = "riscv64")]
        const SYS_STATX: libc::c_long = 291;

        unsafe { libc::syscall(SYS_STATX, dirfd, pathname, flags, mask, statxbuf) as i32 }
    }
}

impl Metadata {
    fn from_fs(meta: &FsMetadata) -> Self {
        Self {
            size: meta.len(),
            accessed_time: None, // atime is disabled
            created_time: meta.created().ok(),
            modified_time: meta.modified().ok(),

            #[cfg(unix)]
            mode: Some(meta.mode()),
            #[cfg(not(unix))]
            mode: None,

            #[cfg(unix)]
            owner_uid: Some(meta.uid()),
            #[cfg(not(unix))]
            owner_uid: None,

            #[cfg(unix)]
            owner_gid: Some(meta.gid()),
            #[cfg(not(unix))]
            owner_gid: None,

            #[cfg(unix)]
            inode: Some(meta.ino()),
            #[cfg(not(unix))]
            inode: None,

            #[cfg(unix)]
            dev: Some(meta.dev()),
            #[cfg(not(unix))]
            dev: None,

            #[cfg(unix)]
            nlink: Some(meta.nlink()),
            #[cfg(not(unix))]
            nlink: None,

            #[cfg(unix)]
            rdev: Some(meta.rdev()),
            #[cfg(not(unix))]
            rdev: None,

            #[cfg(windows)]
            windows_attributes: Some(meta.file_attributes()),
            #[cfg(not(windows))]
            windows_attributes: None,

            linux_flags: None,
            extended_attributes: None,
        }
    }

    #[cfg(target_os = "linux")]
    fn statx_to_system_time(ts: linux_statx::statx_timestamp) -> SystemTime {
        if ts.tv_sec >= 0 {
            SystemTime::UNIX_EPOCH + std::time::Duration::new(ts.tv_sec as u64, ts.tv_nsec)
        } else {
            SystemTime::UNIX_EPOCH - std::time::Duration::new(ts.tv_sec.unsigned_abs(), ts.tv_nsec)
        }
    }

    #[cfg(target_os = "linux")]
    fn makedev(major: u32, minor: u32) -> u64 {
        ((minor & 0xff) as u64)
            | (((major & 0xfff) as u64) << 8)
            | (((minor & !0xff) as u64) << 12)
            | (((major & !0xfff) as u64) << 32)
    }

    #[cfg(target_os = "linux")]
    fn from_statx(sx: &linux_statx::statx) -> Self {
        let mut m = Self {
            size: sx.stx_size,
            accessed_time: None, // atime is disabled
            modified_time: Some(Self::statx_to_system_time(sx.stx_mtime)),
            mode: Some(sx.stx_mode as u32),
            owner_uid: Some(sx.stx_uid),
            owner_gid: Some(sx.stx_gid),
            inode: Some(sx.stx_ino),
            nlink: Some(sx.stx_nlink as u64),
            dev: Some(Self::makedev(sx.stx_dev_major, sx.stx_dev_minor)),
            ..Default::default()
        };

        if (sx.stx_mask & linux_statx::STATX_BTIME) != 0 {
            m.created_time = Some(Self::statx_to_system_time(sx.stx_btime));
        }

        // STATX_BLOCKS is 0x0400, but here we want to check if stx_rdev_* are valid.
        // In the kernel, STATX_RDEV is 0x0400.
        if (sx.stx_mask & 0x0400) != 0 {
            m.rdev = Some(Self::makedev(sx.stx_rdev_major, sx.stx_rdev_minor));
        }

        m
    }

    #[inline]
    pub fn times_match(&self, t1: Option<SystemTime>, t2: Option<SystemTime>) -> bool {
        match (t1, t2) {
            (None, None) => true,
            (Some(t1), Some(t2)) => {
                if t1 == t2 {
                    return true;
                }

                #[cfg(windows)]
                {
                    // On Windows, mtime can have some precision issues or small updates
                    // from the OS (like 100ns vs 1ms vs 15.6ms).
                    // We use a 1-second tolerance, which is common for backup tools on Windows.
                    let d1 = t1.duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
                    let d2 = t2.duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
                    let diff = d1.abs_diff(d2);
                    diff.as_secs() < 1
                }
                #[cfg(not(windows))]
                {
                    false
                }
            }
            _ => false,
        }
    }
}

impl Node {
    /// Build a `Node` from any path on disk.
    pub async fn from_path(path: &Path) -> Result<Self> {
        let (metadata, node_type) = Self::fetch_metadata_and_type(path, false).await?;

        let name = path
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| {
                // If it's a drive root or system root, extract a sensible name.
                if let Some(std::path::Component::Prefix(p)) = path.components().next() {
                    p.as_os_str().to_string_lossy().replace(':', "")
                } else {
                    path.to_string_lossy().into_owned()
                }
            });

        let mut node = Self {
            name,
            node_type,
            metadata,
            ..Default::default()
        };

        node.fetch_xattrs(path);
        node.fetch_linux_flags(path);

        if node.is_symlink() {
            node.populate_symlink_info(path).await?;
        }

        Ok(node)
    }

    async fn fetch_metadata_and_type(
        path: &Path,
        follow_symlinks: bool,
    ) -> Result<(Metadata, NodeType)> {
        #[cfg(target_os = "linux")]
        {
            let path_owned = path.to_owned();
            let res = tokio::task::spawn_blocking(move || {
                let c_path = std::ffi::CString::new(path_owned.as_os_str().as_bytes())?;
                let mut sx: linux_statx::statx = unsafe { std::mem::zeroed() };
                let flags = if follow_symlinks {
                    0
                } else {
                    libc::AT_SYMLINK_NOFOLLOW
                } | linux_statx::AT_STATX_DONT_SYNC;

                let res = unsafe {
                    linux_statx::statx(
                        libc::AT_FDCWD,
                        c_path.as_ptr(),
                        flags,
                        linux_statx::STATX_BASIC_STATS | linux_statx::STATX_BTIME,
                        &mut sx,
                    )
                };

                if res == 0 {
                    let meta = Metadata::from_statx(&sx);
                    let mode = sx.stx_mode as u32;
                    let node_type = if (mode & libc::S_IFMT) == libc::S_IFDIR {
                        NodeType::Directory
                    } else if (mode & libc::S_IFMT) == libc::S_IFREG {
                        NodeType::File
                    } else if (mode & libc::S_IFMT) == libc::S_IFLNK {
                        NodeType::Symlink
                    } else if (mode & libc::S_IFMT) == libc::S_IFBLK {
                        NodeType::BlockDevice
                    } else if (mode & libc::S_IFMT) == libc::S_IFCHR {
                        NodeType::CharDevice
                    } else if (mode & libc::S_IFMT) == libc::S_IFIFO {
                        NodeType::Fifo
                    } else if (mode & libc::S_IFMT) == libc::S_IFSOCK {
                        NodeType::Socket
                    } else {
                        bail!("Unsupported file type");
                    };
                    Ok(Some((meta, node_type)))
                } else {
                    let err = std::io::Error::last_os_error();
                    if err.raw_os_error() == Some(libc::ENOSYS) {
                        Ok(None) // Fallback to std::fs
                    } else {
                        Err(anyhow::Error::from(err))
                    }
                }
            })
            .await??;

            if let Some(res) = res {
                return Ok(res);
            }
        }

        // Fallback or non-Linux implementation
        let meta = if follow_symlinks {
            tokio::fs::metadata(path).await
        } else {
            tokio::fs::symlink_metadata(path).await
        }?;
        let node_type = get_node_type(&meta)?;
        Ok((Metadata::from_fs(&meta), node_type))
    }

    pub fn fetch_xattrs(&mut self, path: &Path) {
        #[cfg(unix)]
        {
            let mut xattrs = BTreeMap::new();
            // Use standard variants to get attributes of the node itself (including symlinks)
            if let Ok(iter) = xattr::list(path) {
                for name in iter {
                    if let Ok(Some(value)) = xattr::get(path, &name) {
                        xattrs.insert(name.to_string_lossy().into_owned(), value);
                    }
                }
            }
            if !xattrs.is_empty() {
                self.metadata.extended_attributes = Some(xattrs);
            }
        }
        #[cfg(not(unix))]
        let _ = path;
    }

    pub fn fetch_linux_flags(&mut self, path: &Path) {
        #[cfg(target_os = "linux")]
        {
            if self.is_symlink() {
                return;
            }

            // Using standard fs::File for ioctl.
            // Opening as read-only is enough for GETFLAGS.
            if let Ok(file) = std::fs::File::open(path) {
                let mut flags: libc::c_int = 0;
                const FS_IOC_GETFLAGS: libc::Ioctl = 0x80086601u32 as libc::Ioctl;
                unsafe {
                    if libc::ioctl(file.as_raw_fd(), FS_IOC_GETFLAGS, &mut flags) == 0 {
                        self.metadata.linux_flags = Some(flags as u32);
                    }
                }
            }
        }
        #[cfg(not(target_os = "linux"))]
        let _ = path;
    }

    async fn populate_symlink_info(&mut self, path: &Path) -> Result<()> {
        let target = tokio::fs::read_link(path)
            .await
            .with_context(|| format!("Failed to read symlink target for: {}", path.display()))?;

        let mut info = SymlinkInfo {
            target_path: target.clone(),
            target_type: None,
        };

        // Cross-Platform Support: Windows requires knowing if the target is a dir.
        // We probe the target type only if it exists.
        if let Some(parent) = path.parent() {
            let full_target_path = parent.join(&target);
            if let Ok((_, target_type)) =
                Self::fetch_metadata_and_type(&full_target_path, true).await
            {
                info.target_type = Some(target_type);
            }
        }

        self.symlink_info = Some(info);
        Ok(())
    }

    #[inline]
    pub fn is_dir(&self) -> bool {
        matches!(self.node_type, NodeType::Directory)
    }

    #[inline]
    pub fn is_file(&self) -> bool {
        matches!(self.node_type, NodeType::File)
    }

    #[inline]
    pub fn is_symlink(&self) -> bool {
        matches!(self.node_type, NodeType::Symlink)
    }

    #[inline]
    pub fn is_block_device(&self) -> bool {
        matches!(self.node_type, NodeType::BlockDevice)
    }

    #[inline]
    pub fn is_char_device(&self) -> bool {
        matches!(self.node_type, NodeType::CharDevice)
    }

    #[inline]
    pub fn is_fifo(&self) -> bool {
        matches!(self.node_type, NodeType::Fifo)
    }

    #[inline]
    pub fn is_socket(&self) -> bool {
        matches!(self.node_type, NodeType::Socket)
    }

    pub async fn from_dir_entry(path: &Path, e: &tokio::fs::DirEntry) -> Result<Self> {
        let name = e.file_name().to_string_lossy().into_owned();

        // Metadata semantics:
        // - symlink -> symlink_metadata
        // - otherwise -> entry.metadata
        // Our fetch_metadata_and_type handles this correctly.
        let (metadata, node_type) = Self::fetch_metadata_and_type(path, false).await?;

        let mut node = Self {
            name,
            node_type,
            metadata,
            ..Default::default()
        };

        node.fetch_xattrs(path);
        node.fetch_linux_flags(path);

        if node.is_symlink() {
            node.populate_symlink_info(path).await?;
        }

        Ok(node)
    }

    /// Compare this metadata with the metadata of another node.
    /// Returns `true` iff any key metadata differs, which could indicate that the
    /// node contents have changed or the node has been replaced.
    #[inline]
    pub fn is_modified_hint(&self, other: &Self) -> bool {
        let this_meta = &self.metadata;
        let other_meta = &other.metadata;

        this_meta.size != other_meta.size
            || !this_meta.times_match(this_meta.modified_time, other_meta.modified_time)
            || !other_meta.times_match(this_meta.created_time, other_meta.created_time)
            || this_meta.inode != other_meta.inode
    }
}

/// Returns the NodeType for a metadata entry
fn get_node_type(meta: &FsMetadata) -> Result<NodeType> {
    let file_type = meta.file_type();

    let node_type = if file_type.is_dir() {
        NodeType::Directory
    } else if file_type.is_file() {
        NodeType::File
    } else if file_type.is_symlink() {
        NodeType::Symlink
    } else {
        #[cfg(unix)]
        {
            // Special unix file types
            if file_type.is_block_device() {
                NodeType::BlockDevice
            } else if file_type.is_char_device() {
                NodeType::CharDevice
            } else if file_type.is_fifo() {
                NodeType::Fifo
            } else if file_type.is_socket() {
                NodeType::Socket
            } else {
                bail!("Found unsupported Unix file type")
            }
        }
        #[cfg(not(unix))]
        bail!("Found unsupported non-Unix file type")
    };

    Ok(node_type)
}

/// Returns a colorized node name.
/// This function follows the color code convention of ls, but it is not comprehensive.
fn get_colorized_node_name(node: &Node, name: String) -> String {
    if node.is_dir() {
        format!("{}", name.bold().blue())
    } else if node.is_symlink() {
        match &node.symlink_info {
            None => format!("{}", name.cyan()),
            Some(symlink_info) => {
                format!("{} -> {}", name.cyan(), symlink_info.target_path.display())
            }
        }
    } else if node.is_block_device() || node.is_char_device() {
        format!("{}", name.yellow().on_black())
    } else {
        name
    }
}

/// Prints the relevant metadata of a node as a single line, similar to the Unix ls command.
pub(crate) fn node_to_string(
    node: &Node,
    full_path: Option<&Path>,
    long: bool,
    human_readable: bool,
) -> String {
    let node_name_str = match full_path {
        Some(path) => get_colorized_node_name(node, path.to_string_lossy().to_string()),
        None => get_colorized_node_name(node, node.name.clone()),
    };

    if long {
        let size_str = match human_readable {
            true => utils::format_size_binary(node.metadata.size, 3),
            false => node.metadata.size.to_string(),
        };

        const NA: &str = "_";

        format!(
            "{:10} {:3} {:7}  {:7}  {:>14}  {:12}  {}",
            node.metadata.mode.map_or(NA.to_string(), |mode| {
                utils::mode_to_permissions_string(mode)
            }),
            node.metadata
                .nlink
                .map_or(NA.to_string(), |nlink| nlink.to_string()),
            node.metadata
                .owner_uid
                .map_or(NA.to_string(), |uid| uid.to_string()),
            node.metadata
                .owner_gid
                .map_or(NA.to_string(), |gid| gid.to_string()),
            size_str,
            node.metadata.modified_time.map_or(NA.to_string(), |mtime| {
                utils::pretty_print_system_time(mtime, None)
                    .unwrap_or_else(|_| String::from("Error"))
            }),
            node_name_str
        )
    } else {
        node_name_str
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;

    #[test]
    fn test_node_type_predicates() {
        let mut node = Node {
            node_type: NodeType::Directory,
            ..Default::default()
        };

        assert!(node.is_dir());
        assert!(!node.is_file());

        node.node_type = NodeType::File;
        assert!(node.is_file());
        assert!(!node.is_dir());

        node.node_type = NodeType::Symlink;
        assert!(node.is_symlink());

        node.node_type = NodeType::BlockDevice;
        assert!(node.is_block_device());

        node.node_type = NodeType::CharDevice;
        assert!(node.is_char_device());

        node.node_type = NodeType::Fifo;
        assert!(node.is_fifo());

        node.node_type = NodeType::Socket;
        assert!(node.is_socket());
    }

    #[test]
    fn test_metadata_times_match() {
        let t1 = SystemTime::UNIX_EPOCH;
        let t2 = t1 + std::time::Duration::from_millis(500);
        let m = Metadata::default();

        assert!(m.times_match(Some(t1), Some(t1)));

        #[cfg(windows)]
        assert!(m.times_match(Some(t1), Some(t2)));
        #[cfg(not(windows))]
        assert!(!m.times_match(Some(t1), Some(t2)));

        assert!(!m.times_match(Some(t1), None));
        assert!(!m.times_match(None, Some(t1)));
        assert!(m.times_match(None, None));
    }

    #[tokio::test]
    async fn test_node_from_path() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let file_path = tmp_dir.path().join("file.txt");
        std::fs::write(&file_path, "hello").unwrap();

        let node = Node::from_path(&file_path).await.unwrap();
        assert_eq!(node.name, "file.txt");
        assert!(node.is_file());
        assert_eq!(node.metadata.size, 5);

        let dir_path = tmp_dir.path().join("subdir");
        std::fs::create_dir(&dir_path).unwrap();
        let node_dir = Node::from_path(&dir_path).await.unwrap();
        assert_eq!(node_dir.name, "subdir");
        assert!(node_dir.is_dir());
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn test_metadata_statx_parity() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let file_path = tmp_dir.path().join("parity.txt");
        std::fs::write(&file_path, "parity test").unwrap();

        let node = Node::from_path(&file_path).await.unwrap();
        let meta = std::fs::symlink_metadata(&file_path).unwrap();

        assert_eq!(node.metadata.size, meta.len());
        assert_eq!(node.metadata.mode, Some(meta.mode()));
        assert_eq!(node.metadata.owner_uid, Some(meta.uid()));
        assert_eq!(node.metadata.owner_gid, Some(meta.gid()));
        assert_eq!(node.metadata.inode, Some(meta.ino()));
        assert_eq!(node.metadata.dev, Some(meta.dev()));
        assert_eq!(node.metadata.nlink, Some(meta.nlink()));

        // mtime should match within reasonable precision
        assert!(
            node.metadata
                .times_match(node.metadata.modified_time, meta.modified().ok())
        );

        // btime should be present on most modern Linux filesystems
        if let Ok(created) = meta.created() {
            assert!(
                node.metadata
                    .times_match(node.metadata.created_time, Some(created))
            );
        } else {
            // If std::fs couldn't get it, our statx might have (on musl) or not.
            // But on glibc (likely where tests run), they should behave similarly.
            println!("Creation time not supported by std::fs on this filesystem");
        }
    }

    #[test]
    fn test_node_to_string_short() {
        let node = Node {
            name: "test_file".to_string(),
            node_type: NodeType::File,
            ..Default::default()
        };
        // Short mode doesn't use metadata, only name (and colorization if it were a dir/symlink)
        assert_eq!(node_to_string(&node, None, false, false), "test_file");
    }

    #[test]
    fn test_node_to_string_long() {
        let node = Node {
            name: "test_file".to_string(),
            node_type: NodeType::File,
            metadata: Metadata {
                size: 1024,
                mode: Some(0o100644),
                owner_uid: Some(1000),
                owner_gid: Some(1000),
                nlink: Some(1),
                modified_time: Some(SystemTime::UNIX_EPOCH),
                ..Default::default()
            },
            ..Default::default()
        };
        let s = node_to_string(&node, None, true, false);
        // "-rw-r--r--   1 1000     1000              1024  1970-01-01 00:00:00  test_file"
        assert!(s.contains("-rw-r--r--"));
        assert!(s.contains("1024"));
        assert!(s.contains("test_file"));
    }

    #[test]
    fn test_node_to_string_human_readable() {
        let node = Node {
            name: "test_file".to_string(),
            node_type: NodeType::File,
            metadata: Metadata {
                size: 1024 * 1024,
                mode: Some(0o100644),
                modified_time: Some(SystemTime::UNIX_EPOCH),
                ..Default::default()
            },
            ..Default::default()
        };
        let s = node_to_string(&node, None, true, true);
        assert!(s.contains("1.000 MiB"));
    }

    #[test]
    fn test_node_name_from_windows_prefix() {
        // We can't easily test actual Windows paths on Linux, but we can test
        // the logic of how we extract the name from components.
        let path = Path::new("C:\\");
        let name = if let Some(std::path::Component::Prefix(p)) = path.components().next() {
            p.as_os_str().to_string_lossy().replace(':', "")
        } else {
            // Fallback for Linux or if no prefix
            path.file_name()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_else(|| "/".to_string())
        };

        if cfg!(windows) {
            assert_eq!(name, "C");
        } else {
            // On Linux, "C:\" is just a filename
            assert_eq!(name, "C:\\");
        }
    }
}
