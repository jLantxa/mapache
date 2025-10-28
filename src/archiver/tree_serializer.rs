use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result};

use crate::{
    fs::{
        node::{Node, NodeType},
        tree::{StreamNode, Tree},
    },
    mapache::ID,
    repository::repo::Repository,
    utils,
};

/// Represents the expected number of children for a directory node.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ExpectedChildren {
    /// The number of children is known.
    Known(usize),
    /// The number of children is not yet known (e.g., for the root before stream processing).
    Unknown,
}

impl From<isize> for ExpectedChildren {
    fn from(value: isize) -> Self {
        if value < 0 {
            ExpectedChildren::Unknown
        } else {
            ExpectedChildren::Known(value as usize)
        }
    }
}

/// Represents a directory node that is being built bottom-up during the snapshot process.
/// It holds the directory's own node information (if available), the collected child nodes,
/// and the number of children expected from the stream.
#[derive(Debug)]
pub(crate) struct PendingTree {
    pub num_expected_children: ExpectedChildren,
    pub node: Option<Node>,
    pub children: HashMap<String, Node>,
}

impl PendingTree {
    /// Returns true if this directory node is still waiting to receive children.
    fn is_pending(&self) -> bool {
        match self.num_expected_children {
            ExpectedChildren::Unknown => true,
            ExpectedChildren::Known(expected_count) => self.children.len() < expected_count,
        }
    }
}

pub(crate) fn init_pending_trees(
    snapshot_root_path: &Path,
    paths: &[PathBuf],
) -> HashMap<PathBuf, PendingTree> {
    let mut pending_trees = HashMap::new();

    // We need to know ahead how many children the root is expecting, because the FSNodeStreamer
    // does not emit it (the root node).
    let (root_children_count, _) = utils::get_intermediate_paths(snapshot_root_path, paths);

    // The tree root, has no node
    pending_trees.insert(
        snapshot_root_path.to_path_buf(),
        PendingTree {
            node: None,
            children: HashMap::new(),
            num_expected_children: ExpectedChildren::Known(root_children_count),
        },
    );

    pending_trees
}

/// A struct responsible for managing directory state and serializing completed
/// directory trees to the repository in a bottom-up fashion.
pub(crate) struct TreeSerializer {
    repo: Arc<Repository>,
    pending_trees: HashMap<PathBuf, PendingTree>,
    snapshot_root_path: PathBuf,
    root_tree_id: Option<ID>,
}

impl TreeSerializer {
    pub fn new(repo: Arc<Repository>, snapshot_root_path: PathBuf, paths: &[PathBuf]) -> Self {
        Self {
            repo,
            pending_trees: init_pending_trees(&snapshot_root_path, paths),
            snapshot_root_path,
            root_tree_id: None,
        }
    }

    pub fn root_tree(&self) -> Option<ID> {
        self.root_tree_id
    }

    pub(crate) fn handle_processed_item(
        &mut self,
        (path, stream_node): (PathBuf, StreamNode),
    ) -> Result<(u64, u64)> {
        let mut dir_path = utils::extract_parent(&path)
            .with_context(|| format!("Could not extract parent path for {}", path.display()))?;

        match stream_node.node.node_type {
            NodeType::File
            | NodeType::Symlink
            | NodeType::BlockDevice
            | NodeType::CharDevice
            | NodeType::Fifo
            | NodeType::Socket => {
                self.insert_finalized_node(&dir_path, stream_node.node);
            }
            NodeType::Directory => {
                // If the path is a directory, insert/update its own PendingTree entry
                self.pending_trees
                    .entry(path.clone())
                    .and_modify(|pt| {
                        // The entry exists because a child was inserted before this dir node was processed
                        pt.node = Some(stream_node.node.clone());
                        pt.num_expected_children =
                            ExpectedChildren::Known(stream_node.num_children);
                    })
                    .or_insert_with(|| PendingTree {
                        // The entry did not exist (e.g., if the directory was empty)
                        node: Some(stream_node.node),
                        children: HashMap::new(),
                        num_expected_children: ExpectedChildren::Known(stream_node.num_children),
                    });

                dir_path = path;
            }
        }

        self.finalize_if_complete(&dir_path)
    }

    // Helper function to encapsulate the core finalization and serialization logic,
    // handling both root and non-root directories.
    fn finalize_and_save(
        &mut self,
        dir_path: PathBuf,
        pending_tree: PendingTree,
    ) -> Result<(Option<Node>, Option<PathBuf>, (u64, u64))> {
        let mut completed_tree = Tree {
            nodes: pending_tree.children.into_values().collect(),
        };

        let (tree_id, sizes) = completed_tree.save_to_repo(&self.repo)?;
        let is_root = dir_path.as_path() == self.snapshot_root_path.as_path();
        if is_root {
            self.root_tree_id = Some(tree_id);
            return Ok((None, None, sizes));
        }

        // Non-root case: Needs a parent path and a node with the tree_id
        let parent_path = utils::extract_parent(&dir_path).with_context(|| {
            format!(
                "Could not extract parent path for finalized directory '{}'",
                dir_path.display()
            )
        })?;

        let mut completed_dir_node = pending_tree.node.with_context(|| {
            format!(
                "Non-root finalized tree should have a node. dir_path: {}",
                dir_path.display()
            )
        })?;
        completed_dir_node.tree = Some(tree_id);

        Ok((Some(completed_dir_node), Some(parent_path), sizes))
    }

    pub(crate) fn finalize_if_complete(&mut self, dir_path: &Path) -> Result<(u64, u64)> {
        // Check if pending
        let is_pending = match self.pending_trees.get(dir_path) {
            Some(tree) => tree.is_pending(),
            None => return Ok((0, 0)),
        };

        if is_pending {
            return Ok((0, 0));
        }

        // Now we know it's complete, consume the entry.
        // Use `remove_entry` with `&Path` to avoid cloning `dir_path` just for removal.
        let (dir_path_key, this_pending_tree) =
            self.pending_trees.remove_entry(dir_path).with_context(|| {
                format!(
                    "Completed tree for path '{}' not found in map during removal.",
                    dir_path.display()
                )
            })?;

        let (completed_dir_node_opt, parent_path_opt, sizes) =
            self.finalize_and_save(dir_path_key, this_pending_tree)?;

        // If it was the root, the options will be None, and we stop.
        if let (Some(completed_dir_node), Some(parent_path)) =
            (completed_dir_node_opt, parent_path_opt)
        {
            self.insert_finalized_node(&parent_path, completed_dir_node);
            self.finalize_if_complete(&parent_path)?;
        }

        Ok(sizes)
    }

    pub(crate) fn finalize_root(&mut self) -> Result<(u64, u64)> {
        let root = self.snapshot_root_path.clone();
        self.finalize_if_complete(&root)
    }

    #[inline]
    fn insert_finalized_node(&mut self, parent_path: &Path, node: Node) {
        let parent_pending_tree = self
            .pending_trees
            .entry(parent_path.to_path_buf())
            .or_insert_with(|| PendingTree {
                node: None,
                children: HashMap::new(),
                num_expected_children: ExpectedChildren::Unknown,
            });
        parent_pending_tree.children.insert(node.name.clone(), node);
    }
}
