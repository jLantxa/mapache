use std::{
    collections::HashMap,
    io::Read,
    path::PathBuf,
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
        error::{MapacheError, Result},
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
    // Canonicalize exclude paths relative to snapshot root
    let canonical_excludes: Option<Vec<PathBuf>> = excludes.map(|exclude_paths| {
        exclude_paths
            .iter()
            .map(|path| snapshot.root.join(path))
            .collect()
    });

    let path_filter = PathFilter::new(None, canonical_excludes.clone());

    // Filter paths to retain only those allowed
    let mut paths = snapshot.paths.clone();
    paths.retain(|p| path_filter.allow(p));

    let mut tree_serializer = TreeSerializer::new(
        repo.clone(),
        repo.repo_version(),
        snapshot.root.clone(),
        &paths,
    );

    // Initialize the stream of nodes from the existing snapshot
    let mut node_stream = SerializedNodeStream::new(
        repo.clone(),
        Some(snapshot.tree),
        snapshot.root.clone(),
        None,
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

    let size = node.metadata.size;
    let res = tokio::task::spawn_blocking(move || {
        chunk_and_store_file(
            repo.as_ref(),
            sync_reader,
            size,
            progress.as_ref(),
            &event_sender,
            shutdown_signal.as_ref(),
        )
    })
    .await
    .map_err(|e| MapacheError::task_panicked("rechunk", e))??;

    Ok(res)
}
