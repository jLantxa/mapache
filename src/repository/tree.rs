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
    cmp::Ordering,
    fs::{self, Metadata as FsMetadata},
    path::{Path, PathBuf},
    time::SystemTime,
};

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};

use super::backend::{BlobId, RepositoryBackend, TreeId};

/// Node metadata. This struct is serialized; keep field order stable.
#[derive(Debug, Clone, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct Metadata {
    pub size: u64,
    pub created_time: Option<SystemTime>,
    pub modified_time: Option<SystemTime>,
    pub accessed_time: Option<SystemTime>,
    pub mode: Option<u32>,
    pub owner_uid: Option<u32>,
    pub owner_gid: Option<u32>,
}

impl Metadata {
    #[inline]
    pub fn from_fs(meta: &FsMetadata) -> Self {
        Self {
            size: meta.len(),
            created_time: meta.created().ok(),
            modified_time: meta.modified().ok(),
            accessed_time: meta.accessed().ok(),
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
        }
    }

    /// Returns `true` iff any relevant metadata field differs.
    #[inline]
    pub fn has_changed(&self, other: &Self) -> bool {
        self.size != other.size
            || self.modified_time != other.modified_time
            || self.mode != other.mode
            || self.owner_uid != other.owner_uid
            || self.owner_gid != other.owner_gid
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeType {
    File,
    Directory,
    Symlink,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub name: String,
    #[serde(rename = "type")]
    pub node_type: NodeType,
    #[serde(flatten)]
    pub metadata: Metadata,
    pub contents: Option<Vec<BlobId>>, // populated lazily for files
    pub tree: Option<TreeId>,          // populated lazily for dirs
}

impl Node {
    /// Build a `Node` from any path on disk.
    pub fn from_path(path: PathBuf) -> Result<Self> {
        let name = path
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();

        // `symlink_metadata` does *not* follow symlinks – that is what we need
        let meta = fs::symlink_metadata(&path)
            .with_context(|| format!("Cannot stat {}", path.display()))?;

        let node_type = if meta.is_dir() {
            NodeType::Directory
        } else if meta.is_file() {
            NodeType::File
        } else if meta.file_type().is_symlink() {
            NodeType::Symlink
        } else {
            bail!("Unsupported file type at {}", path.display());
        };

        Ok(Self {
            name,
            node_type,
            metadata: Metadata::from_fs(&meta),
            contents: None,
            tree: None,
        })
    }

    /// Convenience helpers.
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
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Tree {
    nodes: Vec<Node>,
}

/// A depth‑first *pre‑order* filesystem streamer.
/// Items are produced in lexicographical order of their *full* paths.
#[derive(Debug)]
pub struct FSNodeStreamer {
    stack: Vec<PathBuf>,
}

impl FSNodeStreamer {
    /// Creates an FSNodeStreamer from one root path
    pub fn from_root(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref();
        if !root.exists() {
            bail!("Path {} does not exist", root.display());
        }
        Ok(Self {
            stack: vec![root.to_path_buf()],
        })
    }

    /// Creates an FSNodeStreamer from multiple root paths. The paths are iterated in lexicographical order.
    pub fn from_paths(paths: &Vec<PathBuf>) -> Result<Self> {
        for path in paths {
            if !path.exists() {
                bail!("Path {} does not exist", path.display());
            }
        }

        let mut roots = paths.clone();
        roots.sort_by(|a, b| b.cmp(a));
        Ok(Self { stack: roots })
    }

    fn sorted_children(dir: &Path) -> Result<Vec<PathBuf>> {
        let mut children: Vec<_> = fs::read_dir(dir)?
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<_, _>>()?;
        children.sort_by(|a, b| a.file_name().cmp(&b.file_name()));
        Ok(children)
    }
}

impl Iterator for FSNodeStreamer {
    type Item = Result<(PathBuf, Node)>;

    fn next(&mut self) -> Option<Self::Item> {
        let path = self.stack.pop()?;
        let res = (|| {
            let node = Node::from_path(path.clone())?;
            if node.is_dir() {
                for child in Self::sorted_children(&path)?.into_iter().rev() {
                    self.stack.push(child);
                }
            }
            Ok((path, node))
        })();
        Some(res)
    }
}

pub struct SerializedNodeStreamer<'a> {
    repo: &'a dyn RepositoryBackend,
    stack: Vec<(PathBuf, Node)>,
}

impl<'a> SerializedNodeStreamer<'a> {
    pub fn new(repo: &'a dyn RepositoryBackend, root_id: TreeId) -> Result<Self> {
        // Load root tree and push its children to the stack in reverse order
        let root_tree = repo.load_tree(&root_id)?;
        let mut stack = Vec::new();
        for node in root_tree.nodes.iter().rev() {
            stack.push((PathBuf::new(), node.clone()));
        }

        Ok(Self { repo, stack })
    }
}

impl<'a> Iterator for SerializedNodeStreamer<'a> {
    type Item = Result<(PathBuf, Node)>;

    fn next(&mut self) -> Option<Self::Item> {
        let (parent_path, node) = self.stack.pop()?;
        let res = (|| {
            // Build the full path to this node first
            let current_path = parent_path.join(&node.name);

            // If it’s a subtree, push its children *under* current_path
            if let Some(subtree_id) = &node.tree {
                let subtree = self.repo.load_tree(subtree_id)?;
                for subnode in subtree.nodes.iter().rev() {
                    self.stack.push((current_path.clone(), subnode.clone()));
                }
            }

            // Now emit the correctly-built path + node
            Ok((current_path, node))
        })();
        Some(res)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeDiff {
    New,
    Deleted,
    Changed,
    Unchanged,
}

/// Streaming diff between two ordered node streams.
pub struct NodeDiffStreamer<P, I>
where
    P: Iterator<Item = Result<(PathBuf, Node)>>,
    I: Iterator<Item = Result<(PathBuf, Node)>>,
{
    prev: P,
    next: I,
    head_prev: Option<Result<(PathBuf, Node)>>,
    head_next: Option<Result<(PathBuf, Node)>>,
}

impl<P, I> NodeDiffStreamer<P, I>
where
    P: Iterator<Item = Result<(PathBuf, Node)>>,
    I: Iterator<Item = Result<(PathBuf, Node)>>,
{
    pub fn new(mut prev: P, mut next: I) -> Self {
        Self {
            head_prev: prev.next(),
            head_next: next.next(),
            prev,
            next,
        }
    }
}

impl<P, I> Iterator for NodeDiffStreamer<P, I>
where
    P: Iterator<Item = Result<(PathBuf, Node)>>,
    I: Iterator<Item = Result<(PathBuf, Node)>>,
{
    type Item = Result<(PathBuf, Option<Node>, Option<Node>, NodeDiff)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match (&self.head_prev, &self.head_next) {
                (None, None) => return None,
                (Some(Err(_)), _) => {
                    let err = self.head_prev.take().unwrap();
                    self.head_prev = self.prev.next();
                    return Some(Err(anyhow!("Previous node error: {}", err.unwrap_err())));
                }
                (_, Some(Err(_))) => {
                    let err = self.head_next.take().unwrap();
                    self.head_next = self.next.next();
                    return Some(Err(anyhow!("Next node error: {}", err.unwrap_err())));
                }
                (Some(Ok((p_path, p_node))), Some(Ok((n_path, n_node)))) => {
                    match p_path.cmp(n_path) {
                        Ordering::Equal => {
                            let diff = if p_node.metadata.has_changed(&n_node.metadata) {
                                NodeDiff::Changed
                            } else {
                                NodeDiff::Unchanged
                            };
                            let item = Ok((
                                p_path.clone(),
                                Some(p_node.clone()),
                                Some(n_node.clone()),
                                diff,
                            ));
                            self.head_prev = self.prev.next();
                            self.head_next = self.next.next();
                            return Some(item);
                        }
                        Ordering::Less => {
                            let item = Ok((
                                p_path.clone(),
                                Some(p_node.clone()),
                                None,
                                NodeDiff::Deleted,
                            ));
                            self.head_prev = self.prev.next();
                            return Some(item);
                        }
                        Ordering::Greater => {
                            let item =
                                Ok((n_path.clone(), None, Some(n_node.clone()), NodeDiff::New));
                            self.head_next = self.next.next();
                            return Some(item);
                        }
                    }
                }
                (Some(Ok((p_path, p_node))), None) => {
                    let item = Ok((
                        p_path.clone(),
                        Some(p_node.clone()),
                        None,
                        NodeDiff::Deleted,
                    ));
                    self.head_prev = self.prev.next();
                    return Some(item);
                }
                (None, Some(Ok((n_path, n_node)))) => {
                    let item = Ok((n_path.clone(), None, Some(n_node.clone()), NodeDiff::New));
                    self.head_next = self.next.next();
                    return Some(item);
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use tempfile::tempdir;

    use super::*;

    // Create a filesystem tree for testing. root should be the path to a temporary folder
    fn create_tree(root: &Path) -> Result<()> {
        // dir_a
        // |____ dir0
        // |____ dir1
        // |____ dir2
        // |      |____ file1
        // |____ file0
        //
        // |dir_b
        // |____ file2

        std::fs::create_dir_all(root.join("dir_a").join("dir0"))?;
        std::fs::create_dir_all(root.join("dir_a").join("dir1"))?;
        std::fs::File::create(root.join("dir_a").join("file0"))?;
        std::fs::create_dir_all(root.join("dir_a").join("dir2"))?;
        std::fs::File::create(root.join("dir_a").join("dir2").join("file1"))?;
        std::fs::create_dir(root.join("dir_b"))?;
        std::fs::File::create(root.join("dir_b").join("file2"))?;

        Ok(())
    }

    #[test]
    fn test_fs_node_streamer_with_root() -> Result<()> {
        let temp_dir = tempdir()?;
        let tmp_path = temp_dir.path();
        create_tree(tmp_path)?;

        let streamer = FSNodeStreamer::from_root(tmp_path.join("dir_a"))?;
        let nodes: Vec<Result<(PathBuf, Node)>> = streamer.collect();

        assert_eq!(nodes.len(), 6);
        assert_eq!(nodes[0].as_ref().unwrap().0, tmp_path.join("dir_a"));
        assert_eq!(
            nodes[1].as_ref().unwrap().0,
            tmp_path.join("dir_a").join("dir0")
        );
        assert_eq!(
            nodes[2].as_ref().unwrap().0,
            tmp_path.join("dir_a").join("dir1")
        );
        assert_eq!(
            nodes[3].as_ref().unwrap().0,
            tmp_path.join("dir_a").join("dir2")
        );
        assert_eq!(
            nodes[4].as_ref().unwrap().0,
            tmp_path.join("dir_a").join("dir2").join("file1")
        );
        assert_eq!(
            nodes[5].as_ref().unwrap().0,
            tmp_path.join("dir_a").join("file0")
        );

        Ok(())
    }

    #[test]
    fn test_fs_node_streamer_with_many_roots() -> Result<()> {
        let temp_dir = tempdir()?;
        let tmp_path = temp_dir.path();
        create_tree(tmp_path)?;

        let streamer =
            FSNodeStreamer::from_paths(&vec![tmp_path.join("dir_a"), tmp_path.join("dir_b")])?;
        let nodes: Vec<Result<(PathBuf, Node)>> = streamer.collect();

        assert_eq!(nodes.len(), 8);
        assert_eq!(nodes[0].as_ref().unwrap().0, tmp_path.join("dir_a"));
        assert_eq!(
            nodes[1].as_ref().unwrap().0,
            tmp_path.join("dir_a").join("dir0")
        );
        assert_eq!(
            nodes[2].as_ref().unwrap().0,
            tmp_path.join("dir_a").join("dir1")
        );
        assert_eq!(
            nodes[3].as_ref().unwrap().0,
            tmp_path.join("dir_a").join("dir2")
        );
        assert_eq!(
            nodes[4].as_ref().unwrap().0,
            tmp_path.join("dir_a").join("dir2").join("file1")
        );
        assert_eq!(
            nodes[5].as_ref().unwrap().0,
            tmp_path.join("dir_a").join("file0")
        );
        assert_eq!(nodes[6].as_ref().unwrap().0, tmp_path.join("dir_b"));
        assert_eq!(
            nodes[7].as_ref().unwrap().0,
            tmp_path.join("dir_b").join("file2")
        );

        Ok(())
    }

    #[test]
    fn test_diff_different_trees() -> Result<()> {
        let temp_dir = tempdir()?;
        let tmp_path = temp_dir.path();
        create_tree(tmp_path)?;

        let dir_a = FSNodeStreamer::from_root(tmp_path.join("dir_a"))?;
        let dir_b = FSNodeStreamer::from_root(tmp_path.join("dir_b"))?;
        let diff_streamer = NodeDiffStreamer::new(dir_a, dir_b);
        let diffs: Vec<Result<(PathBuf, Option<Node>, Option<Node>, NodeDiff)>> =
            diff_streamer.collect();

        assert_eq!(diffs.len(), 8);
        assert_eq!(diffs[0].as_ref().unwrap().3, NodeDiff::Deleted);
        assert_eq!(diffs[1].as_ref().unwrap().3, NodeDiff::Deleted);
        assert_eq!(diffs[2].as_ref().unwrap().3, NodeDiff::Deleted);
        assert_eq!(diffs[3].as_ref().unwrap().3, NodeDiff::Deleted);
        assert_eq!(diffs[4].as_ref().unwrap().3, NodeDiff::Deleted);
        assert_eq!(diffs[5].as_ref().unwrap().3, NodeDiff::Deleted);
        assert_eq!(diffs[6].as_ref().unwrap().3, NodeDiff::New);
        assert_eq!(diffs[7].as_ref().unwrap().3, NodeDiff::New);

        Ok(())
    }

    #[test]
    fn test_diff_same_tree() -> Result<()> {
        let temp_dir = tempdir()?;
        let tmp_path = temp_dir.path();
        create_tree(tmp_path)?;

        let dir_a1 = FSNodeStreamer::from_root(tmp_path.join("dir_a"))?;
        let dir_a2 = FSNodeStreamer::from_root(tmp_path.join("dir_a"))?;
        let diff_streamer = NodeDiffStreamer::new(dir_a1, dir_a2);
        let diffs: Vec<Result<(PathBuf, Option<Node>, Option<Node>, NodeDiff)>> =
            diff_streamer.collect();

        assert_eq!(diffs.len(), 6);
        assert_eq!(diffs[0].as_ref().unwrap().3, NodeDiff::Unchanged);
        assert_eq!(diffs[1].as_ref().unwrap().3, NodeDiff::Unchanged);
        assert_eq!(diffs[2].as_ref().unwrap().3, NodeDiff::Unchanged);
        assert_eq!(diffs[3].as_ref().unwrap().3, NodeDiff::Unchanged);
        assert_eq!(diffs[4].as_ref().unwrap().3, NodeDiff::Unchanged);
        assert_eq!(diffs[5].as_ref().unwrap().3, NodeDiff::Unchanged);

        Ok(())
    }
}
