use std::{
    collections::HashMap,
    ffi::OsString,
    io::Read,
    path::{Component, Path, PathBuf},
    sync::{Arc, atomic::AtomicBool},
};

use futures::StreamExt;
use tokio::io::AsyncReadExt;

use crate::{
    archiver::{
        processor::chunk_and_store_file, progress::SnapshotProgress,
        tree_serializer::TreeSerializer,
    },
    common::{
        ID,
        compression_policy::CompressionHint,
        error::{MapacheError, Result},
        traits::BlobSaver,
    },
    fs::{
        filter::PathFilter,
        node::Node,
        tree::{NodeDiff, SerializedNodeDataReader, SerializedNodeStream},
    },
    repository::{repo::Repository, snapshot::Snapshot},
    ui::events::{BackupEvent, Event, EventSender},
};

pub(crate) struct RewriteCtx {
    pub progress: Arc<SnapshotProgress>,
    pub event_sender: EventSender,
    pub shutdown_signal: Arc<AtomicBool>,
}

/// Express a source path in the coordinate space used by serialized node
/// streams for a given snapshot root.
///
/// Snapshots with several roots have a virtual (empty) root and store each root
/// as a logical node name (`C`, `D` on Windows; the first directory name on
/// Unix). Their serialized paths are relative (`C/prj/file`), while
/// `snapshot.paths` and user-supplied excludes may be absolute
/// (`C:\prj\file`, `/prj/file`). Mapping both to the stream's coordinate space
/// keeps filters and the tree serializer in sync; otherwise the serializer can
/// create synthetic root nodes with empty names.
fn path_in_snapshot_space(root: &Path, path: &Path) -> PathBuf {
    if !root.as_os_str().is_empty() || !path.is_absolute() {
        return path.to_path_buf();
    }

    let mut relative = PathBuf::new();
    for component in path.components() {
        match component {
            // A drive prefix becomes a logical node name (`C:` -> `C`). UNC
            // prefixes use their share name, matching `Node::from_path_sync`.
            Component::Prefix(prefix) => {
                let name = Path::new(prefix.as_os_str())
                    .file_name()
                    .map(|name| name.to_os_string())
                    .unwrap_or_else(|| {
                        OsString::from(prefix.as_os_str().to_string_lossy().trim_end_matches(':'))
                    });
                relative.push(name);
            }
            // The virtual root is represented by the top-level node names, so
            // absolute-root components must not become tree paths.
            Component::RootDir => {}
            other => relative.push(other.as_os_str()),
        }
    }
    relative
}

/// Rewrites an entire snapshot. It rechunks and reindexes the data.
/// Data is read from the existing snapshot and written into a new one.
pub(crate) async fn rewrite_snapshot_tree(
    repo: Arc<Repository>,
    snapshot: &mut Snapshot,
    excludes: Option<&Vec<PathBuf>>,
    rechunk: bool,
    mut rechunked_blobs_list_map: Option<&mut HashMap<Vec<ID>, Vec<ID>>>,
    ctx: RewriteCtx,
) -> Result<()> {
    // Express excludes in the same coordinate space as the serialized stream.
    // With a virtual root, joining a relative exclude would not make it
    // absolute, and an absolute drive path would not match the logical drive
    // names used by the tree.
    let canonical_excludes: Option<Vec<PathBuf>> = excludes.map(|exclude_paths| {
        exclude_paths
            .iter()
            .map(|path| {
                let resolved = if path.is_absolute() || snapshot.root.as_os_str().is_empty() {
                    path.clone()
                } else {
                    snapshot.root.join(path)
                };
                path_in_snapshot_space(&snapshot.root, &resolved)
            })
            .collect()
    });

    let path_filter = PathFilter::new(None, canonical_excludes.clone());

    // Filter paths to retain only those allowed. Keep the original absolute
    // paths for snapshot metadata, but use stream-compatible paths for the
    // tree serializer and the node stream.
    let mut retained_paths = Vec::with_capacity(snapshot.paths.len());
    let mut paths = Vec::with_capacity(snapshot.paths.len());
    for path in &snapshot.paths {
        let stream_path = path_in_snapshot_space(&snapshot.root, path);
        if path_filter.allow(&stream_path) {
            retained_paths.push(path.clone());
            paths.push(stream_path);
        }
    }
    snapshot.paths = retained_paths;

    // Restrict the stream to the retained source paths as well. Without this,
    // a logical drive whose only source paths were excluded would still be
    // emitted as an empty drive node and break the serializer's child count.
    let mut tree_serializer = TreeSerializer::new(repo.clone(), snapshot.root.clone(), &paths);

    // Initialize the stream of nodes from the existing snapshot
    let mut node_stream = SerializedNodeStream::new(
        repo.clone(),
        Some(snapshot.tree),
        snapshot.root.clone(),
        Some(paths),
        canonical_excludes,
    )
    .await?;

    snapshot.summary.processed_items_count = 0;
    snapshot.summary.processed_bytes = 0;

    // Iterate through the nodes in the tree
    while let Some(res) = node_stream.next().await {
        let (path, stream_node_res) = res?;
        let mut stream_node = stream_node_res?;

        let size_hint = Some(stream_node.node.metadata.size);
        let diff = if rechunk {
            NodeDiff::Changed
        } else {
            NodeDiff::Unchanged
        };
        (ctx.event_sender)(Event::Backup(BackupEvent::NodeProcessing {
            path: path.clone(),
            diff,
            size_hint,
        }));

        if stream_node.node.is_file() {
            if !rechunk {
                ctx.progress.processed_bytes(stream_node.node.metadata.size);
                (ctx.event_sender)(Event::Backup(BackupEvent::BytesProcessed(
                    stream_node.node.metadata.size,
                )));
            } else {
                let blobs = stream_node.node.blobs.as_ref().ok_or_else(|| {
                    MapacheError::Integrity("file node must have contents".to_string())
                })?;

                let rechunked_blobs = if let Some(map) = rechunked_blobs_list_map.as_deref_mut() {
                    if let Some(rechunked) = map.get(blobs) {
                        ctx.progress.processed_bytes(stream_node.node.metadata.size);
                        (ctx.event_sender)(Event::Backup(BackupEvent::BytesProcessed(
                            stream_node.node.metadata.size,
                        )));
                        rechunked.clone()
                    } else {
                        let rechunked = run_rechunk_task(
                            repo.clone(),
                            stream_node.node.clone(),
                            ctx.progress.clone(),
                            ctx.event_sender.clone(),
                            ctx.shutdown_signal.clone(),
                        )
                        .await?;
                        map.insert(blobs.clone(), rechunked.clone());
                        rechunked
                    }
                } else {
                    run_rechunk_task(
                        repo.clone(),
                        stream_node.node.clone(),
                        ctx.progress.clone(),
                        ctx.event_sender.clone(),
                        ctx.shutdown_signal.clone(),
                    )
                    .await?
                };

                stream_node.node.blobs = Some(rechunked_blobs);
            }

            snapshot.summary.processed_bytes += stream_node.node.metadata.size;
        }

        tree_serializer
            .handle_processed_item((&path, stream_node))
            .await?;

        (ctx.event_sender)(Event::Backup(BackupEvent::NodeProcessed {
            path: path.clone(),
            diff,
            size_hint,
        }));
        snapshot.summary.processed_items_count += 1;
    }

    tree_serializer.finalize_root().await?;
    snapshot.tree = tree_serializer
        .root_tree()
        .ok_or_else(|| MapacheError::Internal("failed to serialize root tree".to_string()))?;

    Ok(())
}

/// A bridge to convert AsyncRead into std::io::Read by blocking the thread.
/// This must only be used inside spawn_blocking.
struct BlockingBridge<R: tokio::io::AsyncRead + Unpin> {
    inner: R,
}

impl<R: tokio::io::AsyncRead + Unpin> Read for BlockingBridge<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // Use the futures executor to block on the async read operation
        futures::executor::block_on(async { self.inner.read(buf).await })
    }
}

/// Bridge helper to run the synchronous chunker in a background thread pool.
async fn run_rechunk_task(
    repo: Arc<Repository>,
    node: Node,
    progress: Arc<SnapshotProgress>,
    event_sender: EventSender,
    shutdown_signal: Arc<AtomicBool>,
) -> Result<Vec<ID>> {
    let reader = SerializedNodeDataReader::new(repo.clone(), &node).await?;
    let sync_reader = BlockingBridge { inner: reader };

    // Preserve the original file name so compression patterns still apply when
    // re-chunking existing data.
    let node_path = PathBuf::from(&node.name);
    let hint = repo
        .compression_policy()
        .decide_by_path(&node_path)
        .unwrap_or(CompressionHint::Auto);
    let res = tokio::task::spawn_blocking(move || {
        chunk_and_store_file(
            repo.as_ref(),
            sync_reader,
            node.metadata.size,
            hint,
            progress.as_ref(),
            &event_sender,
            shutdown_signal.as_ref(),
        )
    })
    .await
    .map_err(|e| MapacheError::task_panicked("rechunk", e))??;

    Ok(res.0)
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use super::*;
    use crate::{
        archiver::progress::SnapshotProgress,
        backend::StorageBackend,
        backend::mock::MockBackend,
        common::defaults::TEST_REPO_CONFIG,
        fs::{
            node::{Metadata, Node, NodeType},
            tree::Tree,
        },
        repository::{
            repo::{Auth, Repository, THIS_REPOSITORY_VERSION},
            snapshot::Snapshot,
        },
        ui::events::EventSender,
    };

    fn test_ctx() -> RewriteCtx {
        let event_sender: EventSender = Arc::new(|_| {});
        RewriteCtx {
            progress: Arc::new(SnapshotProgress::new()),
            event_sender,
            shutdown_signal: Arc::new(AtomicBool::new(false)),
        }
    }

    async fn virtual_root_repo() -> Result<Arc<Repository>> {
        let auth = Auth {
            username: "test".to_string(),
            password: "test".to_string().into(),
        };
        let backend: Arc<dyn StorageBackend> = Arc::new(MockBackend::new());
        Repository::init(
            THIS_REPOSITORY_VERSION,
            &auth,
            None,
            backend.clone(),
            None,
            false,
        )
        .await?;
        let (repo, _, lock) =
            Repository::try_open_with_lock(&auth, None, backend, TEST_REPO_CONFIG, false, None)
                .await?;
        drop(lock);
        repo.init_pack_saver(1)?;
        Ok(repo)
    }

    fn file_node(name: &str) -> Node {
        Node {
            name: name.to_string(),
            node_type: NodeType::File,
            metadata: Metadata {
                size: 4,
                ..Default::default()
            },
            blobs: Some(vec![ID::from_content(b"data")]),
            ..Default::default()
        }
    }

    fn dir_node(name: &str, tree: ID) -> Node {
        Node {
            name: name.to_string(),
            node_type: NodeType::Directory,
            tree: Some(tree),
            ..Default::default()
        }
    }

    async fn save_tree(repo: &Arc<Repository>, nodes: Vec<Node>) -> Result<ID> {
        let mut tree = Tree::new(nodes);
        let id = tree.save_to_store(repo.clone()).await?;
        Ok(id)
    }

    /// Builds a virtual-root snapshot with two logical roots (`C` and `D`) and
    /// one file under each. `c_dirs` are the intermediate directories between
    /// `C` and `file.txt` in the tree; `c_path` and `d_path` are the recorded
    /// source paths, which may be absolute or relative to the virtual root.
    async fn virtual_root_snapshot(
        repo: &Arc<Repository>,
        c_dirs: &[&str],
        c_path: PathBuf,
        d_path: PathBuf,
    ) -> Result<Snapshot> {
        let mut c_id = save_tree(repo, vec![file_node("file.txt")]).await?;
        for dir in c_dirs.iter().rev() {
            c_id = save_tree(repo, vec![dir_node(dir, c_id)]).await?;
        }
        let d_id = save_tree(repo, vec![file_node("data.bin")]).await?;
        let root_id = save_tree(repo, vec![dir_node("C", c_id), dir_node("D", d_id)]).await?;
        // Make the fixture trees visible to the loader before rewriting.
        repo.flush_and_finalize_pack_saver().await?;
        repo.init_pack_saver(1)?;

        Ok(Snapshot {
            root: PathBuf::new(),
            tree: root_id,
            paths: vec![c_path, d_path],
            ..Default::default()
        })
    }

    /// A single virtual-root regression: rewrites must keep filters and the
    /// tree serializer in the same coordinate space, with or without excludes.
    /// On Windows the source paths are absolute drive paths (the reported
    /// case); elsewhere the equivalent logical relative paths are used.
    #[tokio::test]
    async fn rewrite_virtual_root_filters_and_roots() -> Result<()> {
        let (c_path, d_path, c_dirs): (PathBuf, PathBuf, &[&str]) = if cfg!(windows) {
            (
                PathBuf::from(r"C:\Users\me\file.txt"),
                PathBuf::from(r"D:\data.bin"),
                &["Users", "me"],
            )
        } else {
            (
                PathBuf::from("C/prj/file.txt"),
                PathBuf::from("D/data.bin"),
                &["prj"],
            )
        };

        #[cfg(windows)]
        let cases = vec![
            (Some(vec![PathBuf::from(r"D:\")]), vec!["C"]),
            (Some(vec![PathBuf::from("D")]), vec!["C"]),
            (None, vec!["C", "D"]),
        ];
        #[cfg(not(windows))]
        let cases = vec![
            (Some(vec![PathBuf::from("D")]), vec!["C"]),
            (None, vec!["C", "D"]),
        ];

        for (excludes, expected_roots) in cases {
            let repo = virtual_root_repo().await?;
            let mut snapshot =
                virtual_root_snapshot(&repo, c_dirs, c_path.clone(), d_path.clone()).await?;

            rewrite_snapshot_tree(
                repo.clone(),
                &mut snapshot,
                excludes.as_ref(),
                false,
                None,
                test_ctx(),
            )
            .await?;
            repo.flush_and_finalize_pack_saver().await?;

            let root = Tree::load_from_repo(&repo, &snapshot.tree).await?;
            assert_eq!(
                root.nodes
                    .iter()
                    .map(|n| n.name.as_str())
                    .collect::<Vec<_>>(),
                expected_roots
            );
            assert_eq!(snapshot.paths.len(), expected_roots.len());

            if expected_roots == ["C"] {
                assert_eq!(snapshot.paths, vec![c_path.clone()]);
                let mut tree =
                    Tree::load_from_repo(&repo, root.nodes[0].tree.as_ref().unwrap()).await?;
                for dir in c_dirs {
                    assert_eq!(tree.nodes[0].name, *dir);
                    tree =
                        Tree::load_from_repo(&repo, tree.nodes[0].tree.as_ref().unwrap()).await?;
                }
                assert_eq!(tree.nodes[0].name, "file.txt");
            }
        }
        Ok(())
    }
}
