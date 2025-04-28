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

use std::path::{Path, PathBuf};
use std::{fs::DirEntry, time::SystemTime};

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

use anyhow::{Context, Result, anyhow, bail};
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
    Symlink,
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
            NodeType::Symlink
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

    /// Returns true if the node is a directory
    pub fn is_dir(&self) -> bool {
        self.node_type == NodeType::Directory
    }

    /// Returns true if the node is a file
    pub fn is_file(&self) -> bool {
        self.node_type == NodeType::File
    }

    /// Returns true if the node is a symlink
    pub fn is_symlink(&self) -> bool {
        self.node_type == NodeType::Symlink
    }
}

/// A tree, represented as a collection of its immediate children nodes.
#[derive(Debug, Serialize, Deserialize)]
pub struct Tree {
    pub nodes: Vec<Node>,
}

pub struct FilesystemNodeStreamer {
    stack: Vec<PathBuf>,
}

impl FilesystemNodeStreamer {
    pub fn new(root_path: &Path) -> Result<Self> {
        if !root_path.exists() {
            bail!(
                "Failed to create streamer for path \'{}\'. The path doesn't exist.",
                root_path.display()
            );
        }

        Ok(FilesystemNodeStreamer {
            stack: vec![root_path.to_path_buf()],
        })
    }

    fn sort_dir_entries(dir_path: &Path) -> Result<Vec<PathBuf>> {
        let read_dir_iterator = std::fs::read_dir(dir_path)
            .with_context(|| format!("Failed to read directory: {}", dir_path.display()))?;

        let mut entries: Vec<PathBuf> = Vec::new();

        for entry_result in read_dir_iterator {
            let entry = entry_result.with_context(|| {
                format!(
                    "Failed to read an entry in directory: {}",
                    dir_path.display()
                )
            })?;
            entries.push(entry.path());
        }

        let mut sorted_entries = entries;
        sorted_entries.sort_by(|first, second| first.file_name().cmp(&second.file_name()));

        Ok(sorted_entries)
    }
}

impl Iterator for FilesystemNodeStreamer {
    type Item = Result<(PathBuf, Node)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.stack.pop() {
            None => None, // The stack is empty. We are finished.
            Some(path) => {
                let name = path
                    .file_name()
                    .map(|o| o.to_string_lossy().into_owned())
                    .unwrap();

                let os_meta = match std::fs::metadata(&path) {
                    Ok(meta) => meta,
                    Err(_) => {
                        return Some(Err(anyhow!(
                            "Failed to read metadata for path \'{}\'",
                            path.display()
                        )));
                    }
                };

                let node_type = if os_meta.is_dir() {
                    // Push children to the stack in reverse alphabetical order
                    let sorted_children = Self::sort_dir_entries(&path);
                    match sorted_children {
                        Ok(list) => self.stack.extend(list.into_iter().rev()),
                        Err(_) => {
                            return Some(Err(anyhow!(
                                "Failed to list directory \'{}\'",
                                &path.display()
                            )));
                        }
                    }

                    NodeType::Directory
                } else if os_meta.is_file() {
                    NodeType::File
                } else if os_meta.is_symlink() {
                    NodeType::Symlink
                } else {
                    return Some(Err(anyhow!(
                        "Unsupported dir entry encountered for path \'{}\'",
                        &path.display()
                    )));
                };

                let node = Node {
                    name,
                    node_type: node_type,
                    metadata: Metadata::from_fs_meta(&os_meta),
                    contents: None,
                    tree: None,
                };

                Some(Ok((path, node)))
            }
        }
    }
}
