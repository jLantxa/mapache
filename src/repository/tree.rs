// [backup] is an incremental backup tool
// Copyright (C) 2025  Javier Lancha Vázquez <javier.lancha@gmail.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use std::{
    fs::Metadata as FsMetadata,
    path::{Path, PathBuf},
    time::SystemTime,
};

#[cfg(unix)]
use std::os::unix::fs::FileTypeExt;

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::global::ObjectType;

use super::{ID, RepositoryBackend};

/// The type of a node (file, directory, symlink, etc.)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeType {
    File,
    Directory,
    Symlink,
    BlockDevice,
    CharDevice,
    Fifo,
    Socket,
}

/// A node in the file system tree. This struct is serialized; keep field order stable.
#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymlinkInfo {
    pub target_path: PathBuf, // Target path
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_type: Option<NodeType>, // Type of node referenced by the symlink (necessary for restoration in Windows)
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Tree {
    pub nodes: Vec<Node>,
}

/// Node metadata. This struct is serialized; keep field order stable.
///
/// We ignore the accessed time. This field changes everytime we analyze a file for backup,
/// altering the hash of the node. The accessed time will be updated after restoring the
/// file anyway. We don't include it in the metadata, but we still have it here.
#[derive(Debug, Clone, Default, Serialize, Deserialize, Eq, PartialEq)]
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

    /// Returns `true` iff any relevant metadata field differs.
    #[inline]
    pub fn has_changed(&self, other: &Self) -> bool {
        self.modified_time != other.modified_time
            || self.size != other.size
            || self.mode != other.mode
            || self.owner_uid != other.owner_uid
            || self.owner_gid != other.owner_gid
            || self.inode != other.inode
    }
}

impl Node {
    /// Build a `Node` from any path on disk.
    pub fn from_path(path: &Path) -> Result<Self> {
        let name = path
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();

        // symlink_metadata does not follow symlinks
        let meta = std::fs::symlink_metadata(&path)
            .with_context(|| format!("Cannot stat {}", path.display()))?;
        let node_type = get_node_type(&meta)?;

        let mut node = Self {
            name,
            node_type,
            metadata: Metadata::from_fs(&meta),
            blobs: None,
            tree: None,
            symlink_info: None,
        };

        if node.is_symlink() {
            let target_path = std::fs::read_link(&path)
                .with_context(|| format!("Cannot read symlink target for {}", path.display()))?;

            let target_type = match target_path.symlink_metadata() {
                Ok(meta) => Some(get_node_type(&meta)?),
                Err(_) => None,
            };

            let symlink_info = SymlinkInfo {
                target_path,
                target_type,
            };

            node.symlink_info = Some(symlink_info);
        }

        Ok(node)
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
                bail!("Unsupported file type {:?}", file_type)
            }
        }
        #[cfg(not(unix))]
        bail!("Unsupported file type {:?}", file_type)
    };

    Ok(node_type)
}

impl Tree {
    /// Creates a new empty tree.
    pub fn new() -> Self {
        Self { nodes: Vec::new() }
    }

    /// Add a node to the tree. This function makes sure to order the list of
    /// nodes alphabetically by name. If a node with the same name already exists,
    /// it is replaced.
    pub fn add_node(&mut self, node: Node) {
        match self
            .nodes
            .binary_search_by(|probe| probe.name.cmp(&node.name))
        {
            Ok(idx) => {
                self.nodes[idx] = node;
            }
            Err(idx) => {
                self.nodes.insert(idx, node);
            }
        }
    }

    /// Saves a tree in the repository. This function should be called when a tree is complete,
    /// that is, when all the contents and/or tree hashes have been resolved.
    pub fn save_to_repo(&self, repo: &dyn RepositoryBackend) -> Result<(ID, u64, u64)> {
        let tree_json = serde_json::to_string(self)?.as_bytes().to_vec();
        let (id, raw_size, encoded_size) = repo.save_blob(
            ObjectType::Tree,
            tree_json,
            crate::global::SaveID::CalculateID,
        )?;
        Ok((id, raw_size, encoded_size))
    }

    /// Load a tree from the repository.
    pub fn load_from_repo(repo: &dyn RepositoryBackend, root_id: &ID) -> Result<Tree> {
        let tree_object = repo.load_blob(root_id)?;
        let tree: Tree = serde_json::from_slice(&tree_object)?;
        Ok(tree)
    }
}
