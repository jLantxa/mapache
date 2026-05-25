//! The archiver module implements the core logic for creating new repository snapshots.
//! It orchestrates a pipeline that scans the local filesystem, compares it with
//! a parent snapshot (if available), processes changed files into chunks, and
//! serializes the resulting directory tree.

pub(crate) mod chunker_pool;
pub(crate) mod processor;
pub(crate) mod progress;
pub(crate) mod tree_serializer;

use std::{
    collections::BTreeSet,
    path::Path,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use anyhow::{Context, Result, anyhow, bail};
use chrono::Local;
use futures::StreamExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::{
    archiver::{progress::SnapshotProgress, tree_serializer::TreeSerializer},
    fs::{
        filter::PathFilter,
        node::Node,
        tree::{FSNodeStream, NodeDiff, NodeDiffStream, SerializedNodeStream},
    },
    mapache::{global::THIS_MAPACHE_VERSION, traits::BlobSaver},
    repository::{
        repo::Repository,
        snapshot::{Snapshot, SnapshotPair, SnapshotSummary},
    },
    ui::SnapshotProgressReporter,
    utils,
};

/// Options for creating a new snapshot.
#[derive(Clone)]
pub struct SnapshotOptions<'a> {
    /// List of absolute paths to include in the snapshot.
    pub absolute_source_paths: Vec<PathBuf>,
    /// The root path within the repository's virtual filesystem where the snapshot is placed.
    pub snapshot_root_path: PathBuf,
    /// Glob patterns for paths to exclude from the snapshot.
    pub exclude_paths: Vec<PathBuf>,
    /// An optional parent snapshot to use for incremental backups.
    pub parent_snapshot: Option<&'a SnapshotPair>,
    /// Tags associated with the new snapshot.
    pub tags: BTreeSet<String>,
    /// An optional text description for the snapshot.
    pub description: Option<String>,
    /// If true, skip the initial filesystem scan (estimated progress will be less accurate).
    pub no_scan: bool,
    /// If true, store the access time (atime) for all files and directories.
    pub with_atime: bool,
}

/// Internal state used to coordinate multiple concurrent tasks in the archiver pipeline.
/// It tracks progress, completion status, and fatal errors.
struct PipelineStatus {
    /// Signal that the snapshot is finished.
    finished_flag: AtomicBool,

    /// This error flag signals a fatal error to all running tasks so they can
    /// abort execution early. Only fatal, unrecoverable errors should be signaled.
    fatal_error_flag: AtomicBool,
    /// Stores the first error that triggered the shutdown to report back to the user.
    first_error: Mutex<Option<anyhow::Error>>,
    shutdown_signal: Arc<AtomicBool>,

    progress_reporter: Arc<dyn SnapshotProgressReporter>,
    progress: Arc<SnapshotProgress>,
}

impl PipelineStatus {
    fn new(
        progress: Arc<SnapshotProgress>,
        progress_reporter: Arc<dyn SnapshotProgressReporter>,
        shutdown_signal: Arc<AtomicBool>,
    ) -> Self {
        Self {
            finished_flag: AtomicBool::new(false),
            fatal_error_flag: AtomicBool::new(false),
            first_error: Mutex::new(None),
            shutdown_signal,
            progress_reporter,
            progress,
        }
    }

    fn signal_finished(&self) {
        self.finished_flag.store(true, Ordering::SeqCst);
    }

    fn signal_fatal(&self, err: anyhow::Error) {
        let already_errored = self.fatal_error_flag.swap(true, Ordering::SeqCst);
        if !already_errored {
            // Only the first task to "flip" the switch gets to log the error.
            self.progress_reporter.error(&format!("{err:#}"));
            tracing::error!(target: "archiver", "Fatal error in pipeline: {err:#}");
            if let Ok(mut guard) = self.first_error.lock() {
                *guard = Some(err);
            }
        }
    }

    fn is_finished(&self) -> bool {
        self.finished_flag.load(Ordering::Relaxed)
    }

    fn is_failed(&self) -> bool {
        self.fatal_error_flag.load(Ordering::Relaxed) || self.shutdown_signal.load(Ordering::SeqCst)
    }
}

/// The main entry point for creating a new snapshot.
/// This function sets up the processing pipeline and waits for its completion.
pub(crate) async fn snapshot(
    repo: Arc<Repository>,
    snapshot_options: SnapshotOptions<'_>,
    num_readers: usize,
    progress: Arc<SnapshotProgress>,
    progress_reporter: Arc<dyn SnapshotProgressReporter>,
    shutdown_signal: Arc<AtomicBool>,
) -> Result<Snapshot> {
    tracing::info!(target: "archiver", "Starting snapshot archival (root={:?})", snapshot_options.snapshot_root_path);
    let status = Arc::new(PipelineStatus::new(
        progress,
        progress_reporter.clone(),
        shutdown_signal,
    ));

    // Start Background Scanner early so it counts files concurrently
    // with the stream setup and the backup pipeline.
    tracing::info!(target: "archiver", "Starting background scanner");
    spawn_scanner_task(
        snapshot_options.no_scan,
        snapshot_options.absolute_source_paths.clone(),
        snapshot_options.exclude_paths.clone(),
        status.clone(),
        progress_reporter.clone(),
    );

    // Setup Input Streams
    tracing::info!(target: "archiver", "Setting up input streams");
    let fs_stream = FSNodeStream::from_paths(
        snapshot_options.absolute_source_paths.clone(),
        snapshot_options.exclude_paths.clone(),
        snapshot_options.with_atime,
    )
    .await?;

    let previous_tree_stream = SerializedNodeStream::new(
        repo.clone(),
        snapshot_options.parent_snapshot.map(|p| p.snapshot.tree),
        snapshot_options.snapshot_root_path.clone(),
        None,
        None,
    )
    .await?;

    let mut diff_stream = NodeDiffStream::new(previous_tree_stream, fs_stream);

    // ---------------------------------------------------------------------
    // Pipeline Channels & Chunker Pool
    // ---------------------------------------------------------------------
    // We use a larger buffer for many small files to reduce backpressure on the producer.
    let (diff_tx, diff_rx) = mpsc::channel(num_readers * 128);
    let (processed_tx, mut processed_rx) = mpsc::channel(4096);

    // Dedicated threadpool for CPU-bound chunking work.
    // This avoids contention with tokio's blocking pool (used by FSNodeStream).
    tracing::info!(target: "archiver", "Starting chunker pool ({} threads)", num_readers);
    let chunker_pool = chunker_pool::ChunkerPool::new(num_readers);

    // ---------------------------------------------------------------------
    // Stage 1: Diff Producer Task
    // ---------------------------------------------------------------------
    tracing::info!(target: "archiver", "Starting Stage 1: Diff Producer");
    let producer_status = status.clone();
    let producer_task = tokio::spawn(async move {
        tracing::trace!(target: "archiver", "Diff Producer task started");
        while let Some(item) = diff_stream.next().await {
            if producer_status.is_failed() {
                tracing::trace!(target: "archiver", "Diff Producer: status failed, aborting");
                break;
            }

            match item {
                Ok(diff) => {
                    tracing::trace!(target: "archiver", "Diff Producer: sending item {:?}", diff.0);
                    if diff_tx.send(diff).await.is_err() {
                        tracing::trace!(target: "archiver", "Diff Producer: channel closed");
                        break;
                    }
                }
                Err(e) => {
                    tracing::error!(target: "archiver", "Diff Producer error: {e}");
                    producer_status.signal_fatal(e);
                    break;
                }
            }
        }
        tracing::trace!(target: "archiver", "Diff Producer task finished");
    });

    // ---------------------------------------------------------------------
    // Stage 2a: Coordinator Task (lightweight routing)
    // ---------------------------------------------------------------------
    // Items that need chunking (new/changed files) are sent to the dedicated
    // chunker threadpool. Everything else (unchanged, deleted, directories)
    // is processed inline — no spawn_blocking overhead.
    tracing::info!(target: "archiver", "Starting Stage 2: Coordinator ({} readers)", num_readers);
    let coordinator_blob_saver: Arc<dyn BlobSaver> = repo.clone();
    let coordinator_status = status.clone();
    let coordinator_tx = processed_tx.clone();
    let forwarder_tx = processed_tx.clone();
    let coordinator_task = tokio::spawn(async move {
        tracing::trace!(target: "archiver", "Coordinator task started");
        let stream = ReceiverStream::new(diff_rx);

        stream
            .for_each_concurrent(num_readers * 2, |(path, prev_res, next_res, diff)| {
                let blob_saver = coordinator_blob_saver.clone();
                let status = coordinator_status.clone();
                let pool_sender = chunker_pool.sender.clone();
                let tx = coordinator_tx.clone();
                let progress = status.progress.clone();
                let progress_reporter = status.progress_reporter.clone();
                let shutdown_signal = status.shutdown_signal.clone();

                async move {
                    if status.is_failed() {
                        return;
                    }

                    // Resolve result wrappers
                    let next_node = match next_res {
                        Some(Err(e)) => {
                            progress_reporter.warning(&format!("Skipping {}: {}", path.display(), e));
                            tracing::warn!(target: "archiver", "Skipping {}: {}", path.display(), e);
                            progress.processed_node();
                            return;
                        }
                        Some(Ok(n)) => Some(n),
                        None => None,
                    };
                    let prev_node = match prev_res {
                        Some(Ok(n)) => Some(n),
                        Some(Err(_)) => None,
                        None => None,
                    };

                    let needs_chunking = matches!(diff, NodeDiff::New | NodeDiff::Changed)
                        && next_node.as_ref().is_some_and(|n| n.node.is_file());

                    if needs_chunking {
                        let job = chunker_pool::ChunkerJob {
                            path,
                            prev_node,
                            next_node,
                            diff_type: diff,
                            blob_saver,
                            progress,
                            progress_reporter,
                            shutdown_signal,
                        };
                        if pool_sender.send(job).is_err() {
                            status.signal_fatal(anyhow!("Chunker pool channel closed"));
                        }
                    } else {
                        tracing::trace!(target: "archiver", "Processing item inline: {:?}", path);
                        match processor::process_item_sync(
                            path.as_path(),
                            prev_node.as_ref(),
                            next_node.as_ref(),
                            diff,
                            blob_saver,
                            &progress,
                            &*progress_reporter,
                            &shutdown_signal,
                        ) {
                            Ok(Some(node)) => {
                                tracing::trace!(target: "archiver", "Item processed inline: {:?}", path);
                                if tx.send((path, node)).await.is_err() {
                                    tracing::trace!(target: "archiver", "Serializer channel closed");
                                }
                            }
                            Ok(None) => {
                                tracing::trace!(target: "archiver", "Item skipped (no node): {:?}", path);
                            }
                            Err(e) => {
                                tracing::error!(target: "archiver", "Inline processor error for {:?}: {e}", path);
                                status.signal_fatal(e);
                            }
                        }
                    }
                }
            })
            .await;

        tracing::trace!(target: "archiver", "Coordinator task finished");
    });

    // ---------------------------------------------------------------------
    // Stage 2b: Chunker Result Forwarder
    // ---------------------------------------------------------------------
    // Forwards processed results from the dedicated chunker pool to the
    // tree serializer. Runs in a single spawn_blocking (not one per file).
    let forwarder_status = status.clone();
    let forwarder_task = tokio::spawn(async move {
        tracing::trace!(target: "archiver", "Chunker forwarder task started");
        let receiver = chunker_pool.receiver.clone();

        let result = tokio::task::spawn_blocking(move || {
            while let Ok(chunker_result) = receiver.recv() {
                if forwarder_status.is_failed() {
                    return Ok::<(), anyhow::Error>(());
                }

                match chunker_result.result {
                    Ok(Some(node)) => {
                        if forwarder_tx
                            .blocking_send((chunker_result.path, node))
                            .is_err()
                        {
                            break;
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        forwarder_status.signal_fatal(e);
                        break;
                    }
                }
            }
            Ok(())
        })
        .await;

        if let Err(e) = result {
            tracing::error!(target: "archiver", "Chunker forwarder panicked: {e}");
        }
        tracing::trace!(target: "archiver", "Chunker forwarder task finished");
    });

    // Drop our sender so the channel closes when all workers finish
    drop(processed_tx);

    // ---------------------------------------------------------------------
    // Stage 3: Tree Serializer (Main Loop)
    // ---------------------------------------------------------------------
    tracing::info!(target: "archiver", "Starting Stage 3: Tree Serializer");
    let mut tree_serializer = TreeSerializer::new(
        repo.clone() as Arc<dyn BlobSaver>,
        snapshot_options.snapshot_root_path.clone(),
        &snapshot_options.absolute_source_paths,
    );

    while let Some((path, stream_node)) = processed_rx.recv().await {
        if status.is_failed() {
            break;
        }

        if let Err(e) = tree_serializer
            .handle_processed_item((path.as_path(), stream_node))
            .await
        {
            status.signal_fatal(e.context(format!("Serializer error at {path:?}")));
            break;
        }
    }

    let _ = tokio::join!(producer_task, coordinator_task, forwarder_task);

    // ---------------------------------------------------------------------
    // Finalization
    // ---------------------------------------------------------------------
    tracing::info!(target: "archiver", "Finalizing snapshot tree");

    if status.is_failed() {
        if let Some(err) = status.first_error.lock().unwrap().take() {
            return Err(err);
        }
        bail!("Snapshot aborted by user or fatal error");
    }

    tree_serializer.finalize_root().await?;
    let root_tree_id = tree_serializer
        .root_tree()
        .context("Root tree ID not set")?;

    status.signal_finished();
    tracing::info!(target: "archiver", "Snapshot tree finalized: {root_tree_id}");
    let (hostname, username) = utils::get_system_info();

    Ok(Snapshot {
        timestamp: Local::now(),
        parent: snapshot_options.parent_snapshot.map(|pair| pair.id),
        tree: root_tree_id,
        root: snapshot_options.snapshot_root_path,
        paths: snapshot_options.absolute_source_paths,
        hostname,
        username,
        version: Some(THIS_MAPACHE_VERSION.to_string()),
        tags: snapshot_options.tags,
        description: snapshot_options.description,
        summary: SnapshotSummary::default(),
    })
}

/// Spawns a background scanner task to estimate the total size and item count.
/// This uses a sequential walk so it does not compete with the main backup
/// pipeline for rayon's global threadpool.
fn spawn_scanner_task(
    no_scan: bool,
    absolute_source_paths: Vec<PathBuf>,
    exclude_paths: Vec<PathBuf>,
    status: Arc<PipelineStatus>,
    progress_reporter: Arc<dyn SnapshotProgressReporter>,
) {
    tokio::spawn(async move {
        if no_scan {
            return;
        }

        let filter = Arc::new(PathFilter::new(None, Some(exclude_paths)));

        let status_for_blocking = status.clone();
        let reporter_for_blocking = progress_reporter.clone();
        let filter_for_blocking = filter.clone();

        let res = tokio::task::spawn_blocking(move || {
            for path in &absolute_source_paths {
                scan_recursive(
                    path,
                    filter_for_blocking.clone(),
                    status_for_blocking.clone(),
                    reporter_for_blocking.clone(),
                );
            }
        })
        .await;

        if let Err(e) = res {
            status.signal_fatal(anyhow!("Scanner panicked or failed: {e}"));
        }

        tracing::info!(target: "archiver", "Background scanner finished");
        progress_reporter.scan_finished();
    });
}

fn scan_recursive(
    path: &Path,
    filter: Arc<PathFilter>,
    status: Arc<PipelineStatus>,
    progress_reporter: Arc<dyn SnapshotProgressReporter>,
) {
    // Exit early if user requested shutdown or another part of the pipeline failed
    if status.is_failed() || status.is_finished() {
        return;
    }

    if !filter.allow(path) {
        return;
    }

    match Node::from_path_sync(path, false) {
        Ok(node) => {
            progress_reporter.add_expected_items(1);
            if node.is_file() {
                progress_reporter.add_expected_bytes(node.metadata.size);
            }

            if node.is_dir() {
                if let Ok(entries) = std::fs::read_dir(path) {
                    for entry in entries.flatten() {
                        scan_recursive(
                            &entry.path(),
                            filter.clone(),
                            status.clone(),
                            progress_reporter.clone(),
                        );
                    }
                } else {
                    // read_dir already failed, collect error string outside the move closure
                    let msg = format!("Error reading directory {}", path.display());
                    progress_reporter.warning(&msg);
                }
            }
        }
        Err(e) => {
            progress_reporter.warning(&format!("Error scanning {}: {}", path.display(), e));
        }
    }
}
