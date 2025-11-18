use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result};

use crate::{
    fs::{
        node::Node,
        tree::{StreamNode, Tree},
    },
    mapache::ID,
    repository::repo::Repository,
    utils,
};

/// Represents the expected number of children for a directory node.
#[derive(Debug, PartialEq, Eq)]
enum ExpectedChildren {
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
struct PendingTree {
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

fn init_pending_trees(
    snapshot_root_path: &Path,
    paths: &[PathBuf],
) -> HashMap<PathBuf, PendingTree> {
    let mut pending_trees = HashMap::new();

    // We need to know ahead how many children the root is expecting, because the FSNodeStream
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
    pub(crate) fn new(
        repo: Arc<Repository>,
        snapshot_root_path: PathBuf,
        paths: &[PathBuf],
    ) -> Self {
        Self {
            repo,
            pending_trees: init_pending_trees(&snapshot_root_path, paths),
            snapshot_root_path,
            root_tree_id: None,
        }
    }

    pub(crate) fn root_tree(&self) -> Option<ID> {
        self.root_tree_id
    }

    pub(crate) fn handle_processed_item(
        &mut self,
        (path, stream_node): (&Path, StreamNode),
    ) -> Result<(u64, u64)> {
        let parent_path = utils::extract_parent(path)
            .with_context(|| format!("Could not extract parent path for {}", path.display()))?;

        // Determine the directory path that will receive the finalized node.
        let target_dir_path = if stream_node.node.is_dir() {
            // If the processed item is a directory (NodeType::Directory),
            // we update its own PendingTree entry.
            self.pending_trees
                .entry(path.to_path_buf())
                .or_insert_with(|| PendingTree {
                    node: None,
                    children: HashMap::new(),
                    num_expected_children: ExpectedChildren::Unknown,
                })
                .node = Some(stream_node.node);

            // The number of expected children is now known.
            self.pending_trees
                .get_mut(path)
                .unwrap() // We just inserted or modified it, so it must exist.
                .num_expected_children = ExpectedChildren::Known(stream_node.num_children);

            path
        } else {
            // For non-directory nodes (Files, Symlinks, etc.), we finalize the item
            // and insert it into the parent's PendingTree.
            self.insert_finalized_node(&parent_path, stream_node.node);

            &parent_path
        };

        self.finalize_if_complete(target_dir_path)
    }

    // Helper function to encapsulate the core finalization and serialization logic,
    // handling both root and non-root directories.
    #[allow(clippy::type_complexity)]
    fn finalize_and_save(
        &mut self,
        dir_path: PathBuf,
        pending_tree: PendingTree,
    ) -> Result<(Option<(PathBuf, Node)>, (u64, u64))> {
        let mut completed_tree = Tree {
            nodes: pending_tree.children.into_values().collect(),
        };

        let (tree_id, sizes) = completed_tree.save_to_repo(&self.repo)?;
        let is_root = dir_path.as_path() == self.snapshot_root_path.as_path();

        if is_root {
            self.root_tree_id = Some(tree_id);
            return Ok((None, sizes));
        }

        // Non-root case
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

        // Return the parent path and the node to be inserted there.
        Ok((Some((parent_path, completed_dir_node)), sizes))
    }

    pub(crate) fn finalize_if_complete(&mut self, dir_path: &Path) -> Result<(u64, u64)> {
        // Check if the directory is present and complete.
        let pending_tree_entry = self
            .pending_trees
            .get(dir_path)
            .filter(|tree| !tree.is_pending());

        if pending_tree_entry.is_none() {
            return Ok((0, 0));
        }

        // Now we know it's complete, remove and process.
        let (dir_path_key, this_pending_tree) =
            self.pending_trees.remove_entry(dir_path).with_context(|| {
                format!(
                    "Completed tree for path '{}' not found in map during removal.",
                    dir_path.display()
                )
            })?;

        let (parent_info_opt, sizes) = self.finalize_and_save(dir_path_key, this_pending_tree)?;

        // Recursively handle the parent if it was not the root.
        if let Some((parent_path, completed_dir_node)) = parent_info_opt {
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
