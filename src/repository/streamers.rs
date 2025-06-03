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
///
/// Items are produced in lexicographical order of their *full* paths. The root path is not emitted.
/// The internal stack only stores the nodes strictly necessary for iteration. The full tree is not
/// stored in memory. The iteration with a stack avoids recursive calls.
///
/// This streamer will emit all the merged nodes as if they belong to the same tree,
/// intercalating intermediate paths between disjoint branches.
/// This streamer also allows excluding a list of paths. Paths in this list, and their
/// children, are never explored nor emitted.
#[derive(Debug)]
pub struct FSNodeStreamer {
    stack: Vec<PathBuf>,
    intermediate_paths: Vec<(PathBuf, usize)>,
    exclude_paths: Vec<PathBuf>,
}

impl FSNodeStreamer {
    /// Creates an FSNodeStreamer from multiple root paths. The paths are iterated in lexicographical order.
    /// Exclude paths and their children are neither emitted nor explored into.
    pub fn from_paths(mut paths: Vec<PathBuf>, mut exclude_paths: Vec<PathBuf>) -> Result<Self> {
        for path in &paths {
            if !path.exists() {
                bail!("Path {} does not exist", path.display());
            }
        }

        exclude_paths.sort_unstable();
        paths.retain(|path| utils::filter_path(path, None, Some(&exclude_paths)));

        // Calculate intermediate paths and count children (root included)
        let common_root = utils::calculate_lcp(&paths);
        let (_root_children_count, intermediate_path_set) =
            utils::intermediate_paths(&common_root, &paths);

        // Filter intermediate paths based on exclude_paths and collect
        let mut intermediate_paths: Vec<(PathBuf, usize)> = intermediate_path_set
            .into_iter()
            .filter(|(path, _)| utils::filter_path(path, None, Some(&exclude_paths)))
            .collect();

        // Sort paths in reverse order
        paths.sort_by(|a, b| b.cmp(a));
        intermediate_paths.sort_by(|(a, _), (b, _)| b.cmp(&a));

        Ok(Self {
            stack: paths,
            intermediate_paths,
            exclude_paths,
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
        let take_intermediate = loop {
            match (self.intermediate_paths.last(), self.stack.last()) {
                (Some(iv @ _), Some(sv @ _)) => {
                    let iv_path = peek_path(iv);
                    let sv_path = sv;

                    // Skip intermediate if it's excluded
                    if !utils::filter_path(iv_path, None, Some(&self.exclude_paths)) {
                        self.intermediate_paths.pop();
                        continue;
                    }
                    // Skip stack path if it's excluded
                    if !utils::filter_path(sv_path, None, Some(&self.exclude_paths)) {
                        self.stack.pop();
                        continue;
                    }

                    break iv_path.cmp(sv_path) == std::cmp::Ordering::Less;
                }
                (Some(iv @ _), None) => {
                    let iv_path = peek_path(iv);
                    if !utils::filter_path(iv_path, None, Some(&self.exclude_paths)) {
                        self.intermediate_paths.pop();
                        continue;
                    }
                    break true;
                }
                (None, Some(sv @ _)) => {
                    if !utils::filter_path(sv, None, Some(&self.exclude_paths)) {
                        self.stack.pop();
                        continue;
                    }
                    break false;
                }
                (None, None) => return None, // Both are empty
            }
        };

        if take_intermediate {
            let (path, num_children) = self.intermediate_paths.pop().unwrap();
            let node = match Node::from_path(&path) {
                Ok(n) => n,
                Err(e) => return Some(Err(e)),
            };

            return Some(Ok((path.clone(), StreamNode { node, num_children })));
        }

        // Otherwise pop from the DFS stack as before
        let path = self.stack.pop().unwrap(); // We know it's not None due to the loop logic
        let result = (|| {
            let node = Node::from_path(&path)?;

            let num_children = if node.is_dir() {
                let children = Self::get_children_rev_sorted(&path)?;
                let mut valid_children_count = 0;

                for child in children.into_iter() {
                    if utils::filter_path(&child, None, Some(&self.exclude_paths)) {
                        self.stack.push(child);
                        valid_children_count += 1;
                    }
                }
                valid_children_count
            } else {
                0
            };

            Ok((path, StreamNode { node, num_children }))
        })();

        Some(result)
    }
}

/// A depth‑first *pre‑order* streamer of serialized nodes.
///
/// Items are produced in lexicographical order of their *full* paths. The root node is not emitted.
/// Trees are loaded from the repository as they are needed. The full tree is not  stored in memory.
/// The iteration with a stack avoids recursive calls.
///
/// This streamer also allows including and excluding a list of paths. Paths in the exclude list, and their
/// children, are never explored nor emitted. If the include list is not empty, only nodes in the same branch
/// (children and parents (intermediate nodes to reach the included path)) as those paths will be emitted.
pub struct SerializedNodeStreamer {
    repo: Arc<dyn RepositoryBackend>,
    stack: Vec<StreamNodeInfo>,
    include: Vec<PathBuf>,
    exclude: Vec<PathBuf>,
}

impl SerializedNodeStreamer {
    pub fn new(
        repo: Arc<dyn RepositoryBackend>,
        root_id: Option<ID>,
        base_path: PathBuf,
        include: Vec<PathBuf>,
        exclude: Vec<PathBuf>,
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

        Ok(Self {
            repo,
            stack,
            include,
            exclude,
        })
    }
}

impl Iterator for SerializedNodeStreamer {
    type Item = Result<StreamNodeInfo>;

    fn next(&mut self) -> Option<Self::Item> {
        let (current_path, mut stream_node) = loop {
            let (cpath, node) = match self.stack.pop() {
                None => return None,
                Some((parent_path, stream_node)) => {
                    let current_path = parent_path.join(&stream_node.node.name);
                    (current_path, stream_node)
                }
            };

            if utils::filter_path(&cpath, Some(&self.include), Some(&self.exclude)) {
                break (cpath, node);
            }
        };

        let res = (|| {
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

/// A depth‑first *pre‑order* streamer of node differences.
///
/// Items are produced in lexicographical order of their *full* paths. The root node is not emitted.
///
/// This treamer accepts any iterator of `(PathBuf, StreamNode)` and produces a stream of differences
/// between a `previous` stream and a `next`. The differences between two nodes can be:
///
/// - New: `next` has a node not present in `previous`.
/// - Deleted: `prev` has a node not present in `next`.
/// - Changed: `previous` and `next` share a node, but they are deemed to be different (by comparing metadata).
/// - Unchanged: `previous` and `next` share a node and they are deemed to be the same (by comparing metadata).
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

        let streamer = FSNodeStreamer::from_paths(vec![tmp_path.join("dir_a")], Vec::new())?;
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

        let streamer = FSNodeStreamer::from_paths(
            vec![tmp_path.join("dir_a"), tmp_path.join("dir_b")],
            Vec::new(),
        )?;
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

        let streamer = FSNodeStreamer::from_paths(
            vec![
                tmp_path.join("dir_a").join("file0"),
                tmp_path.join("dir_a").join("dir2").join("file1"),
            ],
            Vec::new(),
        )?;
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

        let dir_a = FSNodeStreamer::from_paths(vec![tmp_path.join("dir_a")], Vec::new())?;
        let dir_b = FSNodeStreamer::from_paths(vec![tmp_path.join("dir_b")], Vec::new())?;
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

        let dir_a1 = FSNodeStreamer::from_paths(vec![tmp_path.join("dir_a")], Vec::new())?;
        let dir_a2 = FSNodeStreamer::from_paths(vec![tmp_path.join("dir_a")], Vec::new())?;
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

    #[test]
    fn test_fs_node_streamer_with_exclude_paths() -> Result<()> {
        let temp_dir = tempdir()?;
        let tmp_path = temp_dir.path();
        create_tree(tmp_path)?;

        let streamer = FSNodeStreamer::from_paths(
            vec![tmp_path.join("dir_a"), tmp_path.join("dir_b")],
            vec![tmp_path.join("dir_b")],
        )?;
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
}
