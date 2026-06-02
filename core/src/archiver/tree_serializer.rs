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
        tracing::info!(target: "archiver", "Finalizing root tree");
        let root = self.snapshot_root_path.clone();
        self.finalize_if_complete(&root).await
    }

    /// Allow tests to inspect the pending trees map
    #[cfg(test)]
    fn pending_count(&self) -> usize {
        self.pending_trees.len()
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

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use anyhow::Result;
    use serde_json;

    use crate::{
        backend::WriteContents,
        fs::node::{Node, NodeType},
        fs::tree::{StreamNode, Tree},
        mapache::{BlobType, ID, SaveID, traits::BlobSaver},
    };

    use super::*;

    /// In-memory BlobSaver for testing. Stores blobs by ID.
    struct MockBlobSaver {
        blobs: Mutex<std::collections::HashMap<ID, Vec<u8>>>,
    }

    impl MockBlobSaver {
        fn new() -> Self {
            Self {
                blobs: Mutex::new(std::collections::HashMap::new()),
            }
        }

        fn get(&self, id: &ID) -> Option<Vec<u8>> {
            self.blobs.lock().unwrap().get(id).cloned()
        }
    }

    impl BlobSaver for MockBlobSaver {
        fn save_blob(
            &self,
            _blob_type: BlobType,
            data: WriteContents<'_>,
            save_id: SaveID,
        ) -> Result<ID> {
            let owned = match data {
                WriteContents::Borrowed(d) => d.to_vec(),
                WriteContents::Owned(d) => d,
            };
            let id = match save_id {
                SaveID::CalculateID => ID::from_content(&owned),
                SaveID::WithID(id) => id,
            };
            self.blobs.lock().unwrap().insert(id, owned);
            Ok(id)
        }
    }

    fn file_node(name: &str) -> Node {
        Node {
            name: name.to_string(),
            node_type: NodeType::File,
            ..Default::default()
        }
    }

    fn dir_node(name: &str) -> Node {
        Node {
            name: name.to_string(),
            node_type: NodeType::Directory,
            ..Default::default()
        }
    }

    fn make_ts(blob_saver: Arc<MockBlobSaver>, root: &str, paths: &[&str]) -> TreeSerializer {
        let root_path = PathBuf::from(root);
        let path_bufs: Vec<PathBuf> = paths.iter().map(|p| PathBuf::from(p)).collect();
        TreeSerializer::new(blob_saver as Arc<dyn BlobSaver>, root_path, &path_bufs)
    }

    #[tokio::test]
    async fn test_empty_snapshot() -> Result<()> {
        let saver = Arc::new(MockBlobSaver::new());
        let mut ts = make_ts(saver.clone(), "/", &[]);

        ts.finalize_root().await?;
        let root_id = ts.root_tree().expect("root tree should be set");
        let bytes = saver.get(&root_id).expect("root tree blob should exist");
        let tree: Tree = serde_json::from_slice(&bytes)?;
        assert!(tree.nodes.is_empty(), "empty snapshot has no nodes");
        Ok(())
    }

    #[tokio::test]
    async fn test_single_file_at_root() -> Result<()> {
        let saver = Arc::new(MockBlobSaver::new());
        let mut ts = make_ts(saver.clone(), "/", &["/file.txt"]);

        ts.handle_processed_item((
            Path::new("/file.txt"),
            StreamNode {
                node: file_node("file.txt"),
                num_children: 0,
            },
        ))
        .await?;

        let root_id = ts.root_tree().expect("root should be finalized");
        let bytes = saver.get(&root_id).unwrap();
        let tree: Tree = serde_json::from_slice(&bytes)?;
        assert_eq!(tree.nodes.len(), 1);
        assert_eq!(tree.nodes[0].name, "file.txt");
        assert_eq!(tree.nodes[0].node_type, NodeType::File);
        Ok(())
    }

    #[tokio::test]
    async fn test_single_directory_with_files() -> Result<()> {
        let saver = Arc::new(MockBlobSaver::new());
        let mut ts = make_ts(saver.clone(), "/", &["/dir/a.txt", "/dir/b.txt"]);

        // Feed directory first
        ts.handle_processed_item((
            Path::new("/dir"),
            StreamNode {
                node: dir_node("dir"),
                num_children: 2,
            },
        ))
        .await?;
        assert_eq!(ts.pending_count(), 2, "dir + root pending");

        // Feed first file
        ts.handle_processed_item((
            Path::new("/dir/a.txt"),
            StreamNode {
                node: file_node("a.txt"),
                num_children: 0,
            },
        ))
        .await?;
        // dir still waiting for b.txt
        assert_eq!(ts.pending_count(), 2);

        // Feed second file → triggers dir finalization → root finalization
        ts.handle_processed_item((
            Path::new("/dir/b.txt"),
            StreamNode {
                node: file_node("b.txt"),
                num_children: 0,
            },
        ))
        .await?;

        assert!(ts.pending_count() == 0, "all trees should be finalized");

        let root_id = ts.root_tree().expect("root tree should be set");
        let bytes = saver.get(&root_id).unwrap();
        let tree: Tree = serde_json::from_slice(&bytes)?;
        assert_eq!(tree.nodes.len(), 1);
        assert_eq!(tree.nodes[0].name, "dir");

        // Verify the dir's tree was saved
        let dir_tree_id = tree.nodes[0].tree.expect("dir should have a tree");
        let dir_bytes = saver.get(&dir_tree_id).unwrap();
        let dir_tree: Tree = serde_json::from_slice(&dir_bytes)?;
        assert_eq!(dir_tree.nodes.len(), 2);
        assert_eq!(dir_tree.nodes[0].name, "a.txt");
        assert_eq!(dir_tree.nodes[1].name, "b.txt");
        Ok(())
    }

    #[tokio::test]
    async fn test_deeply_nested_directories() -> Result<()> {
        let saver = Arc::new(MockBlobSaver::new());
        let mut ts = make_ts(saver.clone(), "/", &["/a/b/c/file.txt"]);

        // Feed a → b → c → file.txt
        ts.handle_processed_item((
            Path::new("/a"),
            StreamNode {
                node: dir_node("a"),
                num_children: 1,
            },
        ))
        .await?;

        ts.handle_processed_item((
            Path::new("/a/b"),
            StreamNode {
                node: dir_node("b"),
                num_children: 1,
            },
        ))
        .await?;

        ts.handle_processed_item((
            Path::new("/a/b/c"),
            StreamNode {
                node: dir_node("c"),
                num_children: 1,
            },
        ))
        .await?;

        ts.handle_processed_item((
            Path::new("/a/b/c/file.txt"),
            StreamNode {
                node: file_node("file.txt"),
                num_children: 0,
            },
        ))
        .await?;

        // All trees should be finalized by now (leaf triggered cascade)
        assert!(ts.pending_count() == 0, "all trees finalized");

        let root_id = ts.root_tree().expect("root tree");
        let root_bytes = saver.get(&root_id).unwrap();
        let root_tree: Tree = serde_json::from_slice(&root_bytes)?;
        assert_eq!(root_tree.nodes.len(), 1);
        assert_eq!(root_tree.nodes[0].name, "a");

        // Walk down: a → b → c → file.txt
        let a_id = root_tree.nodes[0].tree.unwrap();
        let a_bytes = saver.get(&a_id).unwrap();
        let a_tree: Tree = serde_json::from_slice(&a_bytes)?;
        assert_eq!(a_tree.nodes.len(), 1);
        assert_eq!(a_tree.nodes[0].name, "b");

        let b_id = a_tree.nodes[0].tree.unwrap();
        let b_bytes = saver.get(&b_id).unwrap();
        let b_tree: Tree = serde_json::from_slice(&b_bytes)?;
        assert_eq!(b_tree.nodes.len(), 1);
        assert_eq!(b_tree.nodes[0].name, "c");

        let c_id = b_tree.nodes[0].tree.unwrap();
        let c_bytes = saver.get(&c_id).unwrap();
        let c_tree: Tree = serde_json::from_slice(&c_bytes)?;
        assert_eq!(c_tree.nodes.len(), 1);
        assert_eq!(c_tree.nodes[0].name, "file.txt");
        assert_eq!(c_tree.nodes[0].node_type, NodeType::File);
        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_root_items() -> Result<()> {
        let saver = Arc::new(MockBlobSaver::new());
        let mut ts = make_ts(saver.clone(), "/", &["/a.txt", "/b.txt", "/c.txt"]);

        ts.handle_processed_item((
            Path::new("/a.txt"),
            StreamNode {
                node: file_node("a.txt"),
                num_children: 0,
            },
        ))
        .await?;

        ts.handle_processed_item((
            Path::new("/b.txt"),
            StreamNode {
                node: file_node("b.txt"),
                num_children: 0,
            },
        ))
        .await?;

        ts.handle_processed_item((
            Path::new("/c.txt"),
            StreamNode {
                node: file_node("c.txt"),
                num_children: 0,
            },
        ))
        .await?;

        let root_id = ts.root_tree().expect("root tree");
        let bytes = saver.get(&root_id).unwrap();
        let tree: Tree = serde_json::from_slice(&bytes)?;
        assert_eq!(tree.nodes.len(), 3);
        assert_eq!(tree.nodes[0].name, "a.txt");
        assert_eq!(tree.nodes[1].name, "b.txt");
        assert_eq!(tree.nodes[2].name, "c.txt");
        Ok(())
    }

    #[tokio::test]
    async fn test_empty_directory() -> Result<()> {
        let saver = Arc::new(MockBlobSaver::new());
        let mut ts = make_ts(saver.clone(), "/", &["/empty_dir"]);

        ts.handle_processed_item((
            Path::new("/empty_dir"),
            StreamNode {
                node: dir_node("empty_dir"),
                num_children: 0,
            },
        ))
        .await?;

        let root_id = ts.root_tree().expect("root tree");
        let root_tree: Tree = serde_json::from_slice(&saver.get(&root_id).unwrap())?;
        assert_eq!(root_tree.nodes.len(), 1);
        assert_eq!(root_tree.nodes[0].name, "empty_dir");

        // Empty dir tree should have no nodes
        let dir_id = root_tree.nodes[0].tree.unwrap();
        let dir_tree: Tree = serde_json::from_slice(&saver.get(&dir_id).unwrap())?;
        assert!(dir_tree.nodes.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_duplicate_child_name_error() -> Result<()> {
        let saver = Arc::new(MockBlobSaver::new());
        let mut ts = make_ts(saver.clone(), "/", &["/dir/a.txt", "/dir/a.txt"]);

        ts.handle_processed_item((
            Path::new("/dir"),
            StreamNode {
                node: dir_node("dir"),
                num_children: 2,
            },
        ))
        .await?;

        ts.handle_processed_item((
            Path::new("/dir/a.txt"),
            StreamNode {
                node: file_node("a.txt"),
                num_children: 0,
            },
        ))
        .await?;

        let result = ts
            .handle_processed_item((
                Path::new("/dir/a.txt"),
                StreamNode {
                    node: file_node("a.txt"),
                    num_children: 0,
                },
            ))
            .await;

        assert!(result.is_err(), "duplicate names should be rejected");
        assert!(
            result.unwrap_err().to_string().contains("Duplicate"),
            "error should mention duplicate"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_incomplete_tree_not_finalized() -> Result<()> {
        let saver = Arc::new(MockBlobSaver::new());
        let mut ts = make_ts(saver.clone(), "/", &["/dir/a.txt", "/dir/b.txt"]);

        ts.handle_processed_item((
            Path::new("/dir"),
            StreamNode {
                node: dir_node("dir"),
                num_children: 2,
            },
        ))
        .await?;

        // Only feed one of the two children
        ts.handle_processed_item((
            Path::new("/dir/a.txt"),
            StreamNode {
                node: file_node("a.txt"),
                num_children: 0,
            },
        ))
        .await?;

        // incomplete trees should not be finalized
        assert_eq!(ts.pending_count(), 2, "dir + root still pending");

        ts.finalize_root().await?;
        // Root should NOT be set — tree is incomplete
        assert!(ts.root_tree().is_none(), "root should not be finalized");
        Ok(())
    }

    #[tokio::test]
    async fn test_blob_saver_deterministic_ids() -> Result<()> {
        let saver = Arc::new(MockBlobSaver::new());
        let mut ts = make_ts(saver.clone(), "/", &["/a.txt"]);

        ts.handle_processed_item((
            Path::new("/a.txt"),
            StreamNode {
                node: file_node("a.txt"),
                num_children: 0,
            },
        ))
        .await?;

        let id1 = ts.root_tree().unwrap();
        let _ = saver.get(&id1).unwrap();

        // Same input should produce the same tree ID
        let saver2 = Arc::new(MockBlobSaver::new());
        let mut ts2 = make_ts(saver2.clone(), "/", &["/a.txt"]);
        ts2.handle_processed_item((
            Path::new("/a.txt"),
            StreamNode {
                node: file_node("a.txt"),
                num_children: 0,
            },
        ))
        .await?;
        let id2 = ts2.root_tree().unwrap();

        assert_eq!(id1, id2, "identical trees should have the same ID");
        Ok(())
    }

    #[tokio::test]
    async fn test_pending_count_tracking() -> Result<()> {
        let saver = Arc::new(MockBlobSaver::new());
        let mut ts = make_ts(saver.clone(), "/", &["/dir/sub/file.txt"]);

        assert_eq!(ts.pending_count(), 1, "only root initially");

        ts.handle_processed_item((
            Path::new("/dir"),
            StreamNode {
                node: dir_node("dir"),
                num_children: 1,
            },
        ))
        .await?;
        assert_eq!(ts.pending_count(), 2, "root + dir");

        ts.handle_processed_item((
            Path::new("/dir/sub"),
            StreamNode {
                node: dir_node("sub"),
                num_children: 1,
            },
        ))
        .await?;
        assert_eq!(ts.pending_count(), 3, "root + dir + sub");

        ts.handle_processed_item((
            Path::new("/dir/sub/file.txt"),
            StreamNode {
                node: file_node("file.txt"),
                num_children: 0,
            },
        ))
        .await?;
        assert_eq!(ts.pending_count(), 0, "all finalized after leaf insertion");
        Ok(())
    }
}
