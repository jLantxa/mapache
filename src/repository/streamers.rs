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
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, anyhow, bail};

use crate::{global::ID, utils};

use super::{
    RepositoryBackend,
    tree::{Node, Tree},
};

#[derive(Debug)]
pub struct StreamNode {
    pub node: Node,
    pub num_children: usize,
}

pub type StreamNodeInfo = (PathBuf, StreamNode);

/// A depth‑first *pre‑order* filesystem streamer.
/// Items are produced in lexicographical order of their *full* paths.
#[derive(Debug)]
pub struct FSNodeStreamer {
    stack: Vec<PathBuf>,
    intermediate_paths: Vec<(PathBuf, usize)>,
}

impl FSNodeStreamer {
    /// Creates an FSNodeStreamer from multiple root paths. The paths are iterated in lexicographical order.
    pub fn from_paths(mut paths: Vec<PathBuf>) -> Result<Self> {
        for path in &paths {
            if !path.exists() {
                bail!("Path {} does not exist", path.display());
            }
        }

        // Calculate intermediate paths and count children (root included)
        let common_root = utils::calculate_lcp(&paths);
        let (_root_children_count, intermediate_path_set) =
            utils::intermediate_paths(&common_root, &paths);
        let mut intermediate_paths: Vec<(PathBuf, usize)> =
            intermediate_path_set.into_iter().collect();

        // Sort paths in reverse order
        paths.sort_by(|a, b| b.cmp(a));
        intermediate_paths.sort_by(|(a, _), (b, _)| b.cmp(&a));

        Ok(Self {
            stack: paths,
            intermediate_paths,
        })
    }

    // Get all children sorted in reverse lexicographical order.
    fn get_children_rev_sorted(dir: &Path) -> Result<Vec<PathBuf>> {
        let mut children: Vec<_> = std::fs::read_dir(dir)?
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<_, _>>()?;
        children.sort_by(|a, b| b.file_name().cmp(&a.file_name()));
        Ok(children)
    }
}

impl Iterator for FSNodeStreamer {
    type Item = Result<StreamNodeInfo>;

    fn next(&mut self) -> Option<Self::Item> {
        // Helper to peek the next path in each list
        fn peek_path(entry: &(PathBuf, usize)) -> &PathBuf {
            &entry.0
        }

        // Decide which source has the lexicographically smaller “next” element
        let take_intermediate = match (self.intermediate_paths.last(), self.stack.last()) {
            (Some(iv @ _), Some(sv @ _)) => peek_path(iv).cmp(sv) == std::cmp::Ordering::Less,
            (Some(_), None) => true,
            _ => false,
        };

        if take_intermediate {
            // pop from intermediate_paths
            let (path, num_children) = self.intermediate_paths.pop().unwrap();

            return Some(Ok((
                path.clone(),
                StreamNode {
                    node: Node::from_path(path.clone()).unwrap(),
                    num_children,
                },
            )));
        }

        // Otherwise pop from the DFS stack as before
        let path = self.stack.pop()?;
        let result = (|| {
            let node = Node::from_path(path.clone())?;

            let num_children = if node.is_dir() {
                let children = Self::get_children_rev_sorted(&path)?;
                let n = children.len();
                // push in *reverse* so that the very first child is at the top of the stack
                for child in children.into_iter() {
                    self.stack.push(child);
                }
                n
            } else {
                0
            };

            Ok((path, StreamNode { node, num_children }))
        })();

        Some(result)
    }
}

pub struct SerializedNodeStreamer {
    repo: Arc<dyn RepositoryBackend>,
    stack: Vec<StreamNodeInfo>,
}

impl SerializedNodeStreamer {
    pub fn new(
        repo: Arc<dyn RepositoryBackend>,
        root_id: Option<ID>,
        base_path: PathBuf,
    ) -> Result<Self> {
        let mut stack = Vec::new();

        if let Some(id) = root_id {
            let tree = Tree::load_from_repo(repo.as_ref(), &id)
                .with_context(|| format!("Failed to load root tree with ID {}", id))?;

            for node in tree.nodes.into_iter().rev() {
                stack.push((
                    base_path.clone(),
                    StreamNode {
                        node,

                        // Actual child count will be determined when this node is processed by `next`.
                        // Initialize to 0 for consistency with how FSNodeStreamer initializes non-directories.
                        num_children: 0,
                    },
                ));
            }
        }

        Ok(Self { repo, stack })
    }
}

impl Iterator for SerializedNodeStreamer {
    type Item = Result<StreamNodeInfo>;

    fn next(&mut self) -> Option<Self::Item> {
        let (parent_path, mut stream_node) = self.stack.pop()?;

        let res = (|| {
            let current_path = parent_path.join(&stream_node.node.name);

            // If it’s a subtree (i.e., a directory), load its children and push them.
            // Also, update the current `stream_node`'s `num_children` with its actual count.
            if let Some(subtree_id) = &stream_node.node.tree {
                let subtree = Tree::load_from_repo(self.repo.as_ref(), subtree_id)?;
                let num_children_of_this_dir = subtree.nodes.len();

                // Push children for the next iteration. Their `num_children` starts at 0.
                for subnode in subtree.nodes.into_iter().rev() {
                    // Use into_iter() for efficiency
                    self.stack.push((
                        current_path.clone(),
                        StreamNode {
                            node: subnode,

                            // Children nodes initially have 0, their own children
                            // count is set when *they* are processed
                            num_children: 0,
                        },
                    ));
                }

                // Update the current stream_node's num_children before emitting it
                stream_node.num_children = num_children_of_this_dir;
            } else {
                // For files or symlinks, ensure num_children is 0.
                stream_node.num_children = 0;
            }

            Ok((current_path, stream_node))
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
    P: Iterator<Item = Result<(PathBuf, StreamNode)>>,
    I: Iterator<Item = Result<(PathBuf, StreamNode)>>,
{
    prev: P,
    next: I,
    head_prev: Option<Result<(PathBuf, StreamNode)>>,
    head_next: Option<Result<(PathBuf, StreamNode)>>,
}

impl<P, I> NodeDiffStreamer<P, I>
where
    P: Iterator<Item = Result<(PathBuf, StreamNode)>>,
    I: Iterator<Item = Result<(PathBuf, StreamNode)>>,
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
    P: Iterator<Item = Result<(PathBuf, StreamNode)>>,
    I: Iterator<Item = Result<(PathBuf, StreamNode)>>,
{
    type Item = Result<(PathBuf, Option<StreamNode>, Option<StreamNode>, NodeDiff)>;

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
                (Some(Ok(item_a_ref)), Some(Ok(item_b_ref))) => {
                    let path_a = &item_a_ref.0;
                    let path_b = &item_b_ref.0;

                    match path_a.cmp(path_b) {
                        Ordering::Less => {
                            let item = self.head_prev.take().unwrap().unwrap();
                            let (previous_path, previous_node) = item;

                            self.head_prev = self.prev.next();

                            return Some(Ok((
                                previous_path,
                                Some(previous_node),
                                None,
                                NodeDiff::Deleted,
                            )));
                        }
                        Ordering::Greater => {
                            let item = self.head_next.take().unwrap().unwrap();
                            let (incoming_path, incoming_node) = item;

                            self.head_next = self.next.next();

                            return Some(Ok((
                                incoming_path,
                                None,
                                Some(incoming_node),
                                NodeDiff::New,
                            )));
                        }
                        Ordering::Equal => {
                            let item_a = self.head_prev.take().unwrap().unwrap();
                            let (previous_path, previous_node) = item_a;

                            let item_b = self.head_next.take().unwrap().unwrap();
                            let (_, incoming_node) = item_b;

                            self.head_prev = self.prev.next();
                            self.head_next = self.next.next();

                            let diff_type = if previous_node
                                .node
                                .metadata
                                .has_changed(&incoming_node.node.metadata)
                            {
                                NodeDiff::Changed
                            } else {
                                NodeDiff::Unchanged
                            };

                            return Some(Ok((
                                previous_path,
                                Some(previous_node),
                                Some(incoming_node),
                                diff_type,
                            )));
                        }
                    }
                }
                (Some(Ok(_)), None) => {
                    let item = self.head_prev.take().unwrap().unwrap();
                    let (previous_path, previous_node) = item;
                    self.head_prev = self.prev.next();

                    return Some(Ok((
                        previous_path,
                        Some(previous_node),
                        None,
                        NodeDiff::Deleted,
                    )));
                }
                (None, Some(Ok(_))) => {
                    let item = self.head_next.take().unwrap().unwrap();
                    let (incoming_path, incoming_node) = item;
                    self.head_next = self.next.next();

                    return Some(Ok((
                        incoming_path,
                        None,
                        Some(incoming_node),
                        NodeDiff::New,
                    )));
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
        // dir_b
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

        let streamer = FSNodeStreamer::from_paths(vec![tmp_path.join("dir_a")])?;
        let nodes: Vec<Result<(PathBuf, StreamNode)>> = streamer.collect();

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
            FSNodeStreamer::from_paths(vec![tmp_path.join("dir_a"), tmp_path.join("dir_b")])?;
        let nodes: Vec<Result<(PathBuf, StreamNode)>> = streamer.collect();

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
    fn test_fs_node_streamer_with_intermediate_paths() -> Result<()> {
        let temp_dir = tempdir()?;
        let tmp_path = temp_dir.path();
        create_tree(tmp_path)?;

        let streamer = FSNodeStreamer::from_paths(vec![
            tmp_path.join("dir_a").join("file0"),
            tmp_path.join("dir_a").join("dir2").join("file1"),
        ])?;
        let nodes: Vec<Result<(PathBuf, StreamNode)>> = streamer.collect();

        assert_eq!(nodes.len(), 3);
        assert_eq!(
            nodes[0].as_ref().unwrap().0,
            tmp_path.join("dir_a").join("dir2")
        );
        assert_eq!(
            nodes[1].as_ref().unwrap().0,
            tmp_path.join("dir_a").join("dir2").join("file1")
        );
        assert_eq!(
            nodes[2].as_ref().unwrap().0,
            tmp_path.join("dir_a").join("file0")
        );

        Ok(())
    }

    #[test]
    fn test_diff_different_trees() -> Result<()> {
        let temp_dir = tempdir()?;
        let tmp_path = temp_dir.path();
        create_tree(tmp_path)?;

        let dir_a = FSNodeStreamer::from_paths(vec![tmp_path.join("dir_a")])?;
        let dir_b = FSNodeStreamer::from_paths(vec![tmp_path.join("dir_b")])?;
        let diff_streamer = NodeDiffStreamer::new(dir_a, dir_b);
        let diffs: Vec<Result<(PathBuf, Option<StreamNode>, Option<StreamNode>, NodeDiff)>> =
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

        let dir_a1 = FSNodeStreamer::from_paths(vec![tmp_path.join("dir_a")])?;
        let dir_a2 = FSNodeStreamer::from_paths(vec![tmp_path.join("dir_a")])?;
        let diff_streamer = NodeDiffStreamer::new(dir_a1, dir_a2);
        let diffs: Vec<Result<(PathBuf, Option<StreamNode>, Option<StreamNode>, NodeDiff)>> =
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
