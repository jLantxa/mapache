use std::{
    fs::Metadata as FsMetadata,
    path::{Path, PathBuf},
    time::SystemTime,
};

#[cfg(unix)]
use std::os::unix::fs::{FileTypeExt, MetadataExt};

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

    // The number of hard links pointing to this inode
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nlink: Option<u64>,

    // Raw device ID for block/char devices
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rdev: Option<u64>,
}

impl Metadata {
    #[inline]
    pub fn from_fs(meta: &FsMetadata) -> Self {
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
            nlink: Some(meta.nlink()),
            #[cfg(not(unix))]
            nlink: None,

            #[cfg(unix)]
            rdev: Some(meta.rdev()),
            #[cfg(not(unix))]
            rdev: None,
        }
    }

    /// Compare this metadata with the metadata of another node.
    /// Returns `true` iff any metadata differs, which could indicate that the
    /// node contents have changed or the node has been replaced.
    #[inline]
    pub fn is_modified(&self, other: &Self) -> bool {
        if self.size != other.size {
            return true;
        }

        // Check times with tolerance
        if !self.times_match(self.modified_time, other.modified_time) {
            return true;
        }

        if !self.times_match(self.created_time, other.created_time) {
            return true;
        }

        // Compare other fields
        self.mode != other.mode
            || self.owner_uid != other.owner_uid
            || self.owner_gid != other.owner_gid
            || self.inode != other.inode
            || self.nlink != other.nlink
            || self.rdev != other.rdev
    }

    #[inline]
    fn times_match(&self, t1: Option<SystemTime>, t2: Option<SystemTime>) -> bool {
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
        let meta = tokio::fs::symlink_metadata(path).await.with_context(|| {
            format!(
                "Failed to get symlink metadata for path: {}",
                path.display()
            )
        })?;

        let node_type = get_node_type(&meta)?;

        let name = path
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| "/".to_string());

        let mut node = Self {
            name,
            node_type,
            metadata: Metadata::from_fs(&meta),
            ..Default::default()
        };

        if node.is_symlink() {
            node.populate_symlink_info(path).await?;
        }

        Ok(node)
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
            if let Ok(target_meta) = tokio::fs::metadata(&full_target_path).await {
                info.target_type = Some(get_node_type(&target_meta).with_context(|| {
                    format!(
                        "Failed to resolve target type for symlink: {}",
                        full_target_path.display()
                    )
                })?);
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
        let ft = e
            .file_type()
            .await
            .with_context(|| format!("Failed to get file type for entry: {}", path.display()))?;

        // Name is cheap: file_name is already an OsString
        let name = e.file_name().to_string_lossy().into_owned();

        let node_type = if ft.is_dir() {
            NodeType::Directory
        } else if ft.is_file() {
            NodeType::File
        } else if ft.is_symlink() {
            NodeType::Symlink
        } else {
            #[cfg(unix)]
            {
                use std::os::unix::fs::FileTypeExt;
                if ft.is_block_device() {
                    NodeType::BlockDevice
                } else if ft.is_char_device() {
                    NodeType::CharDevice
                } else if ft.is_fifo() {
                    NodeType::Fifo
                } else if ft.is_socket() {
                    NodeType::Socket
                } else {
                    bail!("Unsupported Unix file type for entry: {}", path.display());
                }
            }
            #[cfg(not(unix))]
            bail!(
                "Unsupported non-Unix file type for entry: {}",
                path.display()
            )
        };

        // Metadata semantics:
        // - symlink -> symlink_metadata
        // - otherwise -> entry.metadata
        let meta = if node_type == NodeType::Symlink {
            tokio::fs::symlink_metadata(path)
                .await
                .with_context(|| format!("Failed to get symlink metadata: {}", path.display()))?
        } else {
            e.metadata().await.with_context(|| {
                format!(
                    "Failed to get metadata for directory entry: {}",
                    path.display()
                )
            })?
        };

        let mut node = Self {
            name,
            node_type,
            metadata: Metadata::from_fs(&meta),
            ..Default::default()
        };

        if node.is_symlink() {
            node.populate_symlink_info(path).await?;
        }

        Ok(node)
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
    fn test_metadata_is_modified() {
        let m1 = Metadata {
            size: 100,
            modified_time: Some(SystemTime::UNIX_EPOCH),
            ..Default::default()
        };
        let mut m2 = m1.clone();
        assert!(!m1.is_modified(&m2));

        m2.size = 200;
        assert!(m1.is_modified(&m2));
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
}
