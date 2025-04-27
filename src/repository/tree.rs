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

use std::{fs::DirEntry, time::SystemTime};

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use super::backend::{BlobId, TreeId};

/// Node metadata. This struct is serialized, so the order of the fields
/// must remain constant.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Metadata {
    pub size: u64,

    pub created_time: Option<SystemTime>,
    pub modified_time: Option<SystemTime>,
    pub accessed_time: Option<SystemTime>,
    pub mode: Option<u32>,

    pub user: Option<String>,
    pub group: Option<String>,
    pub owner_uid: Option<u32>,
    pub owner_gid: Option<u32>,
}

impl Metadata {
    pub fn from_fs_meta(os_meta: &std::fs::Metadata) -> Self {
        Self {
            size: os_meta.len(),

            created_time: os_meta.created().ok(),
            modified_time: os_meta.modified().ok(),
            accessed_time: os_meta.accessed().ok(),

            #[cfg(unix)]
            mode: Some(os_meta.mode()),
            #[cfg(not(unix))]
            mode: None,

            #[cfg(unix)]
            owner_uid: Some(os_meta.uid()),
            #[cfg(not(unix))]
            owner_uid: None,

            user: None,  // TODO
            group: None, // TODO

            #[cfg(unix)]
            owner_gid: Some(os_meta.gid()),
            #[cfg(not(unix))]
            owner_gid: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum NodeType {
    File,
    Directory,
    Simlink,
}

/// A Node, representing an item in a filesystem.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub name: String,
    pub node_type: NodeType,

    #[serde(flatten)]
    pub metadata: Metadata,

    pub contents: Option<Vec<BlobId>>,
    pub tree: Option<TreeId>,
}

impl Node {
    /// Create a new node from a DirEntry
    pub fn from_dir_entry(entry: &DirEntry) -> Result<Node> {
        let name = entry.file_name().to_string_lossy().into_owned();

        // Read the metadata. This is an IO operation that could be async
        let os_meta = entry.metadata().with_context(|| {
            format!(
                "Could not read metadata for directory \'{}\'",
                entry.path().display()
            )
        })?;

        let node_type = if os_meta.is_dir() {
            NodeType::Directory
        } else if os_meta.is_file() {
            NodeType::File
        } else if os_meta.is_symlink() {
            NodeType::Simlink
        } else {
            bail!(
                "Unsupported dir entry encountered for path \'{}\'",
                entry.path().display()
            )
        };

        Ok(Self {
            name,
            node_type,
            metadata: Metadata::from_fs_meta(&os_meta),
            contents: None,
            tree: None,
        })
    }

    pub fn is_dir(&self) -> bool {
        self.node_type == NodeType::Directory
    }

    pub fn is_file(&self) -> bool {
        self.node_type == NodeType::File
    }
}

/// A tree, represented as a collection of its immediate children nodes.
#[derive(Debug, Serialize, Deserialize)]
pub struct Tree {
    pub nodes: Vec<Node>,
}
