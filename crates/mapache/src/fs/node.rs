#[cfg(unix)]
use std::os::unix::fs::{FileTypeExt, MetadataExt};
#[cfg(target_os = "linux")]
use std::os::unix::{ffi::OsStrExt, io::AsRawFd};
#[cfg(windows)]
use std::os::windows::fs::MetadataExt;
use std::{
    collections::BTreeMap,
    fs::Metadata as FsMetadata,
    path::{Path, PathBuf},
    time::SystemTime,
};

use serde::{Deserialize, Serialize};

use crate::{
    common::ID,
    common::error::{MapacheError, Result},
    ui::cli::color::Colorize,
    utils,
    utils::binary::{
        get_array, get_exact, get_string, get_time, get_u8, get_u16, get_u32, get_u64, put_str,
        put_time, put_u8, put_u16, put_u32, put_u64,
    },
};

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

impl NodeType {
    pub(crate) fn to_u8(self) -> u8 {
        match self {
            NodeType::File => 0,
            NodeType::Directory => 1,
            NodeType::Symlink => 2,
            NodeType::BlockDevice => 3,
            NodeType::CharDevice => 4,
            NodeType::Fifo => 5,
            NodeType::Socket => 6,
        }
    }

    pub(crate) fn from_u8(v: u8) -> Result<Self> {
        match v {
            0 => Ok(NodeType::File),
            1 => Ok(NodeType::Directory),
            2 => Ok(NodeType::Symlink),
            3 => Ok(NodeType::BlockDevice),
            4 => Ok(NodeType::CharDevice),
            5 => Ok(NodeType::Fifo),
            6 => Ok(NodeType::Socket),
            _ => Err(MapacheError::Format(format!("invalid NodeType value: {v}"))),
        }
    }
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

impl Node {
    pub(crate) fn to_binary(&self, buf: &mut Vec<u8>) {
        put_u8(buf, self.node_type.to_u8());
        put_str(buf, &self.name);

        let mut flags: u8 = 0;
        if self.symlink_info.is_some() {
            flags |= 0x01;
        }
        if self.blobs.is_some() {
            flags |= 0x02;
        }
        if self.tree.is_some() {
            flags |= 0x04;
        }
        put_u8(buf, flags);

        if let Some(ref si) = self.symlink_info {
            si.to_binary(buf);
        }
        if let Some(ref blobs) = self.blobs {
            put_u32(buf, blobs.len() as u32);
            for id in blobs {
                buf.extend_from_slice(id.as_slice());
            }
        }
        if let Some(ref tree_id) = self.tree {
            buf.extend_from_slice(tree_id.as_slice());
        }

        self.metadata.to_binary(buf);
    }

    pub(crate) fn from_binary(buf: &mut &[u8]) -> Result<Self> {
        let node_type = NodeType::from_u8(get_u8(buf)?)?;
        let name = get_string(buf)?;
        let flags = get_u8(buf)?;

        let symlink_info = if flags & 0x01 != 0 {
            Some(SymlinkInfo::from_binary(buf)?)
        } else {
            None
        };

        let blobs = if flags & 0x02 != 0 {
            let count = get_u32(buf)? as usize;
            let mut vec = Vec::with_capacity(count);
            for _ in 0..count {
                let bytes = get_array::<32>(buf)?;
                vec.push(ID::from_bytes(bytes));
            }
            Some(vec)
        } else {
            None
        };

        let tree = if flags & 0x04 != 0 {
            let bytes = get_array::<32>(buf)?;
            Some(ID::from_bytes(bytes))
        } else {
            None
        };

        let metadata = Metadata::from_binary(buf)?;

        Ok(Node {
            name,
            node_type,
            metadata,
            symlink_info,
            blobs,
            tree,
        })
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SymlinkInfo {
    pub target_path: PathBuf, // Target path
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_type: Option<NodeType>, // Type of node referenced by the symlink (necessary for restoration in Windows)
}

impl SymlinkInfo {
    pub(crate) fn to_binary(&self, buf: &mut Vec<u8>) {
        put_str(buf, &self.target_path.to_string_lossy());
        match &self.target_type {
            Some(nt) => {
                put_u8(buf, 1);
                put_u8(buf, nt.to_u8());
            }
            None => {
                put_u8(buf, 0);
            }
        }
    }

    pub(crate) fn from_binary(buf: &mut &[u8]) -> Result<Self> {
        let target_path = PathBuf::from(get_string(buf)?);
        let has_type = get_u8(buf)?;
        let target_type = if has_type != 0 {
            Some(NodeType::from_u8(get_u8(buf)?)?)
        } else {
            None
        };
        Ok(SymlinkInfo {
            target_path,
            target_type,
        })
    }
}

/// Node metadata. This struct is serialized; keep field order stable.
///
/// The accessed time is only captured when `--with-atime` is passed to the snapshot command.
/// Without this flag, atime is omitted to avoid metadata churn on incremental backups,
/// since reading files during backup would update atime anyway (unless O_NOATIME is used).
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

impl Metadata {
    const FLAG_ACCESSED_TIME: u16 = 1 << 0;
    const FLAG_CREATED_TIME: u16 = 1 << 1;
    const FLAG_MODIFIED_TIME: u16 = 1 << 2;
    const FLAG_MODE: u16 = 1 << 3;
    const FLAG_OWNER_UID: u16 = 1 << 4;
    const FLAG_OWNER_GID: u16 = 1 << 5;
    const FLAG_INODE: u16 = 1 << 6;
    const FLAG_DEV: u16 = 1 << 7;
    const FLAG_NLINK: u16 = 1 << 8;
    const FLAG_RDEV: u16 = 1 << 9;
    const FLAG_XATTR: u16 = 1 << 10;
    const FLAG_WIN_ATTR: u16 = 1 << 11;
    const FLAG_LINUX_FLAGS: u16 = 1 << 12;

    pub(crate) fn to_binary(&self, buf: &mut Vec<u8>) {
        put_u64(buf, self.size);

        let mut flags: u16 = 0;
        if self.accessed_time.is_some() {
            flags |= Self::FLAG_ACCESSED_TIME;
        }
        if self.created_time.is_some() {
            flags |= Self::FLAG_CREATED_TIME;
        }
        if self.modified_time.is_some() {
            flags |= Self::FLAG_MODIFIED_TIME;
        }
        if self.mode.is_some() {
            flags |= Self::FLAG_MODE;
        }
        if self.owner_uid.is_some() {
            flags |= Self::FLAG_OWNER_UID;
        }
        if self.owner_gid.is_some() {
            flags |= Self::FLAG_OWNER_GID;
        }
        if self.inode.is_some() {
            flags |= Self::FLAG_INODE;
        }
        if self.dev.is_some() {
            flags |= Self::FLAG_DEV;
        }
        if self.nlink.is_some() {
            flags |= Self::FLAG_NLINK;
        }
        if self.rdev.is_some() {
            flags |= Self::FLAG_RDEV;
        }
        if self.extended_attributes.is_some() {
            flags |= Self::FLAG_XATTR;
        }
        if self.windows_attributes.is_some() {
            flags |= Self::FLAG_WIN_ATTR;
        }
        if self.linux_flags.is_some() {
            flags |= Self::FLAG_LINUX_FLAGS;
        }

        put_u16(buf, flags);

        if let Some(v) = &self.accessed_time {
            put_time(buf, v);
        }
        if let Some(v) = &self.created_time {
            put_time(buf, v);
        }
        if let Some(v) = &self.modified_time {
            put_time(buf, v);
        }
        if let Some(v) = self.mode {
            put_u32(buf, v);
        }
        if let Some(v) = self.owner_uid {
            put_u32(buf, v);
        }
        if let Some(v) = self.owner_gid {
            put_u32(buf, v);
        }
        if let Some(v) = self.inode {
            put_u64(buf, v);
        }
        if let Some(v) = self.dev {
            put_u64(buf, v);
        }
        if let Some(v) = self.nlink {
            put_u64(buf, v);
        }
        if let Some(v) = self.rdev {
            put_u64(buf, v);
        }
        if let Some(ref xattrs) = self.extended_attributes {
            put_u32(buf, xattrs.len() as u32);
            for (k, v) in xattrs {
                put_str(buf, k);
                put_u32(buf, v.len() as u32);
                buf.extend_from_slice(v);
            }
        }
        if let Some(v) = self.windows_attributes {
            put_u32(buf, v);
        }
        if let Some(v) = self.linux_flags {
            put_u32(buf, v);
        }
    }

    pub(crate) fn from_binary(buf: &mut &[u8]) -> Result<Self> {
        let size = get_u64(buf)?;
        let flags = get_u16(buf)?;

        let accessed_time = if flags & Self::FLAG_ACCESSED_TIME != 0 {
            Some(get_time(buf)?)
        } else {
            None
        };
        let created_time = if flags & Self::FLAG_CREATED_TIME != 0 {
            Some(get_time(buf)?)
        } else {
            None
        };
        let modified_time = if flags & Self::FLAG_MODIFIED_TIME != 0 {
            Some(get_time(buf)?)
        } else {
            None
        };
        let mode = if flags & Self::FLAG_MODE != 0 {
            Some(get_u32(buf)?)
        } else {
            None
        };
        let owner_uid = if flags & Self::FLAG_OWNER_UID != 0 {
            Some(get_u32(buf)?)
        } else {
            None
        };
        let owner_gid = if flags & Self::FLAG_OWNER_GID != 0 {
            Some(get_u32(buf)?)
        } else {
            None
        };
        let inode = if flags & Self::FLAG_INODE != 0 {
            Some(get_u64(buf)?)
        } else {
            None
        };
        let dev = if flags & Self::FLAG_DEV != 0 {
            Some(get_u64(buf)?)
        } else {
            None
        };
        let nlink = if flags & Self::FLAG_NLINK != 0 {
            Some(get_u64(buf)?)
        } else {
            None
        };
        let rdev = if flags & Self::FLAG_RDEV != 0 {
            Some(get_u64(buf)?)
        } else {
            None
        };
        let extended_attributes = if flags & Self::FLAG_XATTR != 0 {
            let count = get_u32(buf)? as usize;
            let mut map = BTreeMap::new();
            for _ in 0..count {
                let key = get_string(buf)?;
                let val_len = get_u32(buf)? as usize;
                let val = get_exact(buf, val_len)?.to_vec();
                map.insert(key, val);
            }
            Some(map)
        } else {
            None
        };
        let windows_attributes = if flags & Self::FLAG_WIN_ATTR != 0 {
            Some(get_u32(buf)?)
        } else {
            None
        };
        let linux_flags = if flags & Self::FLAG_LINUX_FLAGS != 0 {
            Some(get_u32(buf)?)
        } else {
            None
        };

        Ok(Metadata {
            size,
            accessed_time,
            created_time,
            modified_time,
            mode,
            owner_uid,
            owner_gid,
            inode,
            dev,
            nlink,
            rdev,
            extended_attributes,
            windows_attributes,
            linux_flags,
        })
    }
}

/// Minimal manual implementation of the Linux `statx` syscall.
///
/// This is necessary because some libc versions (like older musl versions used in static
/// Rust builds) do not provide the `statx` wrapper or its associated types.
/// By talking directly to the kernel, we can reliably retrieve the birth time (creation time)
/// of files, which is otherwise unavailable in such environments.
#[cfg(target_os = "linux")]
mod linux_statx {
    pub const STATX_TYPE: u32 = 0x0001;
    pub const STATX_MODE: u32 = 0x0002;
    pub const STATX_NLINK: u32 = 0x0004;
    pub const STATX_UID: u32 = 0x0008;
    pub const STATX_GID: u32 = 0x0010;
    pub const STATX_ATIME: u32 = 0x0020;
    pub const STATX_MTIME: u32 = 0x0040;
    pub const STATX_INO: u32 = 0x0100;
    pub const STATX_SIZE: u32 = 0x0200;
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
        unsafe {
            // SAFETY: FFI call to syscall(statx) with valid pointers and descriptors.
            libc::syscall(libc::SYS_statx, dirfd, pathname, flags, mask, statxbuf) as i32
        }
    }
}

impl Metadata {
    fn from_fs(meta: &FsMetadata, with_atime: bool) -> Self {
        Self {
            size: meta.len(),
            accessed_time: if with_atime {
                meta.accessed().ok()
            } else {
                None
            },
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
    fn from_statx(sx: &linux_statx::statx, with_atime: bool) -> Self {
        let mut m = Self {
            size: sx.stx_size,
            accessed_time: if with_atime && (sx.stx_mask & linux_statx::STATX_ATIME) != 0 {
                Some(Self::statx_to_system_time(sx.stx_atime))
            } else {
                None
            },
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

        // Populate linux_flags from stx_attributes.
        // This avoids a separate ioctl() call later for the most common flags.
        // Many statx attributes (like IMMUTABLE, APPEND) have the same bit values
        // as the FS_*_FL constants used by the ioctl.
        const STATX_ATTR_COMPRESSED: u64 = 0x0004;
        const STATX_ATTR_IMMUTABLE: u64 = 0x0010;
        const STATX_ATTR_APPEND: u64 = 0x0020;
        const STATX_ATTR_NODUMP: u64 = 0x0040;
        const STATX_ATTR_NOATIME: u64 = 0x0080;
        const STATX_ATTR_ENCRYPTED: u64 = 0x0800;
        const STATX_ATTR_VERITY: u64 = 0x100000;
        const STATX_ATTR_DAX: u64 = 0x200000;

        const SUPPORTED_FLAGS_MASK: u64 = STATX_ATTR_COMPRESSED
            | STATX_ATTR_IMMUTABLE
            | STATX_ATTR_APPEND
            | STATX_ATTR_NODUMP
            | STATX_ATTR_NOATIME
            | STATX_ATTR_ENCRYPTED
            | STATX_ATTR_VERITY
            | STATX_ATTR_DAX;

        m.linux_flags = Some((sx.stx_attributes & SUPPORTED_FLAGS_MASK) as u32);

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

    /// Exact timestamp comparison with no tolerance. Used by the archiver to
    /// detect file changes that must trigger re-chunking, where even small
    /// mtime differences indicate the content may have changed.
    #[inline]
    pub fn times_match_exact(t1: Option<SystemTime>, t2: Option<SystemTime>) -> bool {
        t1 == t2
    }
}

impl Node {
    /// Build a `Node` from any path on disk, optionally capturing access time.
    pub async fn from_path(path: &Path, with_atime: bool) -> Result<Self> {
        let path_owned = path.to_owned();
        tokio::task::spawn_blocking(move || Self::from_path_sync(&path_owned, with_atime))
            .await
            .map_err(|e| MapacheError::Internal(format!("node creation panicked: {}", e)))?
    }

    /// Synchronous version of `from_path`, optionally capturing access time.
    pub fn from_path_sync(path: &Path, with_atime: bool) -> Result<Self> {
        let (metadata, node_type) = Self::fetch_metadata_and_type_sync(path, false, with_atime)?;

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
            node.populate_symlink_info_sync(path)?;
        }

        Ok(node)
    }

    async fn fetch_metadata_and_type(
        path: &Path,
        follow_symlinks: bool,
        with_atime: bool,
    ) -> Result<(Metadata, NodeType)> {
        let path_owned = path.to_owned();
        tokio::task::spawn_blocking(move || {
            Self::fetch_metadata_and_type_sync(&path_owned, follow_symlinks, with_atime)
        })
        .await
        .map_err(|e| MapacheError::Internal(format!("metadata fetching panicked: {}", e)))?
    }

    fn fetch_metadata_and_type_sync(
        path: &Path,
        follow_symlinks: bool,
        with_atime: bool,
    ) -> Result<(Metadata, NodeType)> {
        #[cfg(target_os = "linux")]
        {
            let c_path = std::ffi::CString::new(path.as_os_str().as_bytes())
                .map_err(|e| MapacheError::Format(e.to_string()))?;
            // SAFETY: `statx` is a C struct containing only integer types; zeroed is
            // a valid initial state. It is overwritten by `statx()` before any reads.
            let mut sx: linux_statx::statx = unsafe { std::mem::zeroed() };
            let flags = if follow_symlinks {
                0
            } else {
                libc::AT_SYMLINK_NOFOLLOW
            } | linux_statx::AT_STATX_DONT_SYNC;

            let mut req_mask = linux_statx::STATX_TYPE
                | linux_statx::STATX_MODE
                | linux_statx::STATX_NLINK
                | linux_statx::STATX_UID
                | linux_statx::STATX_GID
                | linux_statx::STATX_MTIME
                | linux_statx::STATX_INO
                | linux_statx::STATX_SIZE
                | linux_statx::STATX_BTIME;

            if with_atime {
                req_mask |= linux_statx::STATX_ATIME;
            }

            let res = unsafe {
                // SAFETY: FFI call to statx.
                linux_statx::statx(libc::AT_FDCWD, c_path.as_ptr(), flags, req_mask, &mut sx)
            };

            if res == 0 {
                let meta = Metadata::from_statx(&sx, with_atime);
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
                    return Err(MapacheError::Repo("unsupported file type".to_string()));
                };
                return Ok((meta, node_type));
            } else {
                let err = std::io::Error::last_os_error();
                if err.raw_os_error() != Some(libc::ENOSYS) {
                    return Err(MapacheError::Io(err));
                }
                // Fallback to std::fs if statx is not supported
            }
        }

        // Fallback or non-Linux implementation
        let meta = if follow_symlinks {
            std::fs::metadata(path)
        } else {
            std::fs::symlink_metadata(path)
        }?;
        let node_type = get_node_type(&meta)?;
        Ok((Metadata::from_fs(&meta, with_atime), node_type))
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
                    // SAFETY: FFI call to ioctl to get file flags. file and flags pointer are valid.
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
        let path_owned = path.to_owned();
        let mut node = self.clone();
        let updated_node = tokio::task::spawn_blocking(move || {
            node.populate_symlink_info_sync(&path_owned)?;
            Ok::<_, MapacheError>(node)
        })
        .await
        .map_err(|e| {
            MapacheError::Internal(format!("symlink info populating panicked: {}", e))
        })??;
        *self = updated_node;
        Ok(())
    }

    fn populate_symlink_info_sync(&mut self, path: &Path) -> Result<()> {
        let target = std::fs::read_link(path)?;

        let mut info = SymlinkInfo {
            target_path: target.clone(),
            target_type: None,
        };

        // Cross-Platform Support: Windows requires knowing if the target is a dir.
        // We probe the target type only if it exists.
        if let Some(parent) = path.parent() {
            let full_target_path = parent.join(&target);
            if let Ok((_, target_type)) =
                Self::fetch_metadata_and_type_sync(&full_target_path, true, false)
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
        let (metadata, node_type) = Self::fetch_metadata_and_type(path, false, false).await?;

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
            || !Metadata::times_match_exact(this_meta.modified_time, other_meta.modified_time)
            || !Metadata::times_match_exact(this_meta.created_time, other_meta.created_time)
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
                return Err(MapacheError::Repo(
                    "found unsupported Unix file type".to_string(),
                ));
            }
        }
        #[cfg(not(unix))]
        return Err(MapacheError::Repo(
            "found unsupported non-Unix file type".to_string(),
        ));
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
    use std::time::SystemTime;

    use super::*;

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

        let node = Node::from_path(&file_path, false).await.unwrap();
        assert_eq!(node.name, "file.txt");
        assert!(node.is_file());
        assert_eq!(node.metadata.size, 5);

        let dir_path = tmp_dir.path().join("subdir");
        std::fs::create_dir(&dir_path).unwrap();
        let node_dir = Node::from_path(&dir_path, false).await.unwrap();
        assert_eq!(node_dir.name, "subdir");
        assert!(node_dir.is_dir());
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn test_metadata_statx_parity() {
        let tmp_dir = tempfile::tempdir().unwrap();
        let file_path = tmp_dir.path().join("parity.txt");
        std::fs::write(&file_path, "parity test").unwrap();

        let node = Node::from_path(&file_path, false).await.unwrap();
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
            eprintln!("Creation time not supported by std::fs on this filesystem");
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
