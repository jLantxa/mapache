//! The tree_serializer module implements a bottom-up tree builder that collects
//! processed nodes and serializes them into repository trees.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, ensure};
use futures::{FutureExt, future::BoxFuture};

use crate::{
    fs::{
        extract_parent, get_intermediate_paths,
        node::Node,
        tree::{StreamNode, Tree},
    },
    mapache::ID,
    mapache::traits::BlobSaver,
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
    children: Vec<Node>,
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
    let (root_children_count, _) = get_intermediate_paths(snapshot_root_path, paths);

    // The tree root. It has no node.
    pending_trees.insert(
        snapshot_root_path.to_path_buf(),
        PendingTree {
            node: None,
            children: Vec::with_capacity(root_children_count),
            num_expected_children: ExpectedChildren::Known(root_children_count),
        },
    );

    pending_trees
}

/// A struct responsible for managing directory state and serializing completed
/// directory trees to the repository in a bottom-up fashion.
/// It maintains a stack of "pending" trees that are finalized once all their
/// children have been processed.
pub(crate) struct TreeSerializer {
    blob_saver: Arc<dyn BlobSaver>,
    pending_trees: HashMap<PathBuf, PendingTree>,
    snapshot_root_path: PathBuf,
    root_tree_id: Option<ID>,
}

impl TreeSerializer {
    pub(crate) fn new(
        blob_saver: Arc<dyn BlobSaver>,
        snapshot_root_path: PathBuf,
        paths: &[PathBuf],
    ) -> Self {
        Self {
            blob_saver,
            pending_trees: init_pending_trees(&snapshot_root_path, paths),
            snapshot_root_path,
            root_tree_id: None,
        }
    }

    pub(crate) fn root_tree(&self) -> Option<ID> {
        self.root_tree_id
    }

    pub(crate) async fn handle_processed_item(
        &mut self,
        (path, stream_node): (&Path, StreamNode),
    ) -> Result<()> {
        // Determine the directory path that will receive the finalized node.
        let target_dir_path = if stream_node.node.is_dir() {
            // If the processed item is a directory (NodeType::Directory),
            // we update its own PendingTree entry.
            let pending = self
                .pending_trees
                .entry(path.to_path_buf())
                .or_insert_with(|| PendingTree {
                    node: None,
                    children: Vec::new(),
                    num_expected_children: ExpectedChildren::Unknown,
                });

            // The number of expected children is now known.
            pending.node = Some(stream_node.node);
            pending.num_expected_children = ExpectedChildren::Known(stream_node.num_children);

            if pending.children.capacity() < stream_node.num_children {
                pending
                    .children
                    .reserve(stream_node.num_children - pending.children.len());
            }

            path.to_path_buf()
        } else {
            // For non-directory nodes (Files, Symlinks, etc.), we finalize the item
            // and insert it into the parent's PendingTree.
            let parent_path = extract_parent(path)
                .with_context(|| format!("Could not extract parent path for {}", path.display()))?;

            self.insert_finalized_node(&parent_path, stream_node.node);
            parent_path
        };

        self.finalize_if_complete(&target_dir_path).await
    }

    // Helper function to encapsulate the core finalization and serialization logic,
    // handling both root and non-root directories.
    #[allow(clippy::type_complexity)]
    async fn finalize_and_save(
        &mut self,
        dir_path: PathBuf,
        pending_tree: PendingTree,
    ) -> Result<Option<(PathBuf, Node)>> {
        // Invariant check: Ensure we actually have the expected number of children
        if let ExpectedChildren::Known(expected) = pending_tree.num_expected_children {
            let actual = pending_tree.children.len();
            ensure!(
                actual == expected,
                "Integrity error for {}: expected {} children but got {}",
                dir_path.display(),
                expected,
                actual
            );
        }

        if pending_tree
            .children
            .windows(2)
            .any(|w| w[0].name == w[1].name)
        {
            anyhow::bail!("Duplicate child name in {}", dir_path.display());
        }

        let mut completed_tree = Tree::new(pending_tree.children);

        let tree_id = completed_tree
            .save_to_store(self.blob_saver.clone())
            .await
            .with_context(|| format!("Failed to save tree for {}", dir_path.display()))?;

        let is_root = dir_path.as_path() == self.snapshot_root_path.as_path();

        if is_root {
            self.root_tree_id = Some(tree_id);
            return Ok(None);
        }

        // Non-root case
        let parent_path = match extract_parent(&dir_path) {
            Some(parent) => parent,
            None if self.snapshot_root_path.as_os_str().is_empty() => {
                // If the root is empty (virtual root), top-level drives/roots
                // should be children of the empty path.
                PathBuf::new()
            }
            None => {
                anyhow::bail!(
                    "Could not extract parent path for finalized directory '{}'",
                    dir_path.display()
                );
            }
        };

        let mut completed_dir_node = pending_tree.node.with_context(|| {
            format!(
                "Non-root finalized tree should have a node. dir_path: {}",
                dir_path.display()
            )
        })?;

        completed_dir_node.tree = Some(tree_id);

        // Return the parent path and the node to be inserted there.
        Ok(Some((parent_path, completed_dir_node)))
    }

    pub(crate) fn finalize_if_complete<'a>(
        &'a mut self,
        dir_path: &'a Path,
    ) -> BoxFuture<'a, Result<()>> {
        async move {
            // Check if the directory is present and complete.
            let is_complete = self
                .pending_trees
                .get(dir_path)
                .is_some_and(|tree| !tree.is_pending());

            if !is_complete {
                return Ok(());
            }

            // Now we know it's complete, remove and process.
            let (dir_path_key, this_pending_tree) =
                self.pending_trees.remove_entry(dir_path).with_context(|| {
                    format!(
                        "Completed tree for path '{}' not found in map during removal.",
                        dir_path.display()
                    )
                })?;

            let parent_info_opt = self
                .finalize_and_save(dir_path_key, this_pending_tree)
                .await?;

            // Recursively handle the parent if it was not the root.
            if let Some((parent_path, completed_dir_node)) = parent_info_opt {
                self.insert_finalized_node(&parent_path, completed_dir_node);

                // This is the fix: Call recursively and use .boxed()
                self.finalize_if_complete(&parent_path).await?;
            }

            Ok(())
        }
        .boxed()
    }

    pub(crate) async fn finalize_root(&mut self) -> Result<()> {
        let root = self.snapshot_root_path.clone();
        self.finalize_if_complete(&root).await
    }

    #[inline]
    fn insert_finalized_node(&mut self, parent_path: &Path, node: Node) {
        let parent_pending_tree = self
            .pending_trees
            .entry(parent_path.to_path_buf())
            .or_insert_with(|| PendingTree {
                node: None,
                children: Vec::new(),
                num_expected_children: ExpectedChildren::Unknown,
            });
        parent_pending_tree.children.push(node);
    }
}
