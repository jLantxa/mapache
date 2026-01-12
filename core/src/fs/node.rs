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
        self != other
    }
}

impl Node {
    /// Build a `Node` from any path on disk.
    pub fn from_path(path: &Path) -> Result<Self> {
        let meta = std::fs::symlink_metadata(path)
            .with_context(|| format!("Stat failed: {}", path.display()))?;

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
            node.populate_symlink_info(path)?;
        }

        Ok(node)
    }

    fn populate_symlink_info(&mut self, path: &Path) -> Result<()> {
        if let Ok(target) = std::fs::read_link(path) {
            let mut info = SymlinkInfo {
                target_path: target.clone(),
                target_type: None,
            };

            // Cross-Platform Support: Windows requires knowing if the target is a dir.
            // We probe the target type only if it exists.
            if let Some(parent) = path.parent()
                && let Ok(target_meta) = std::fs::metadata(parent.join(&target))
            {
                info.target_type = Some(get_node_type(&target_meta)?);
            }
            self.symlink_info = Some(info);
        }
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
                bail!("Unsupported file type {file_type:?}")
            }
        }
        #[cfg(not(unix))]
        bail!("Unsupported file type {:?}", file_type)
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
                utils::pretty_print_system_time(mtime, None).unwrap_or(String::from("Error"))
            }),
            node_name_str
        )
    } else {
        node_name_str.to_string()
    }
}
