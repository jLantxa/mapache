//! The archiver module implements the core logic for creating new repository snapshots.
//! It orchestrates a pipeline that scans the local filesystem, compares it with
//! a parent snapshot (if available), processes changed files into chunks, and
//! serializes the resulting directory tree.

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
    fs::tree::{FSNodeStream, NodeDiffStream, SerializedNodeStream},
    mapache::global::THIS_MAPACHE_VERSION,
    repository::{
        repo::Repository,
        snapshot::{Snapshot, SnapshotPair, SnapshotSummary},
    },
    ui::snapshot::SnapshotProgressReporter,
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
    let status = Arc::new(PipelineStatus::new(
        progress,
        progress_reporter.clone(),
        shutdown_signal,
    ));

    // Setup Input Streams
    let fs_stream = FSNodeStream::from_paths(
        snapshot_options.absolute_source_paths.clone(),
        snapshot_options.exclude_paths.clone(),
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

    // Start Background Scanner (Progress only)
    spawn_scanner_task(
        snapshot_options.no_scan,
        snapshot_options.absolute_source_paths.clone(),
        snapshot_options.exclude_paths.clone(),
        status.clone(),
        progress_reporter.clone(),
    );

    // ---------------------------------------------------------------------
    // Pipeline Channels
    // ---------------------------------------------------------------------
    // We use a larger buffer for many small files to reduce backpressure on the producer.
    let (diff_tx, diff_rx) = mpsc::channel(num_readers * 128);
    let (processed_tx, mut processed_rx) = mpsc::channel(4096);

    // ---------------------------------------------------------------------
    // Stage 1: Diff Producer Task
    // ---------------------------------------------------------------------
    let producer_status = status.clone();
    let producer_task = tokio::spawn(async move {
        while let Some(item) = diff_stream.next().await {
            // Check for shutdown/failure before every send
            if producer_status.is_failed() {
                break;
            }

            match item {
                Ok(diff) => {
                    if diff_tx.send(diff).await.is_err() {
                        break;
                    }
                }
                Err(e) => {
                    producer_status.signal_fatal(e);
                    break;
                }
            }
        }
    });

    // ---------------------------------------------------------------------
    // Stage 2: Concurrent Processor Task
    // ---------------------------------------------------------------------
    let processor_repo = repo.clone();
    let processor_status = status.clone();
    let processor_task = tokio::spawn(async move {
        let stream = ReceiverStream::new(diff_rx);

        stream
            .for_each_concurrent(num_readers, |(path, prev, next, diff)| {
                let repo = processor_repo.clone();
                let status = processor_status.clone();
                let tx = processed_tx.clone();

                async move {
                    if status.is_failed() {
                        return;
                    }

                    match processor::process_item(
                        (path.as_path(), prev, next, diff),
                        repo,
                        status.progress.clone(),
                        status.progress_reporter.clone(),
                        status.shutdown_signal.clone(),
                    )
                    .await
                    {
                        Ok(Some(node)) => {
                            let _ = tx.send((path, node)).await;
                        }
                        Ok(None) => {}
                        Err(e) => status.signal_fatal(e),
                    }
                }
            })
            .await;

        drop(processed_tx);
    });

    // ---------------------------------------------------------------------
    // Stage 3: Tree Serializer (Main Loop)
    // ---------------------------------------------------------------------
    let mut tree_serializer = TreeSerializer::new(
        repo.clone(),
        snapshot_options.snapshot_root_path.clone(),
        &snapshot_options.absolute_source_paths,
    );

    // Receive nodes and build the tree structure
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

    // Wait for worker tasks to cleanup/exit
    let _ = tokio::join!(producer_task, processor_task);

    // ---------------------------------------------------------------------
    // Finalization
    // ---------------------------------------------------------------------

    // Check if we aborted due to failure or shutdown signal
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
/// This implementation uses a parallel recursive walk with `rayon` for maximum speed.
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

        let filter = Arc::new(crate::fs::filter::PathFilter::new(
            None,
            Some(exclude_paths),
        ));

        let status_for_blocking = status.clone();
        let reporter_for_blocking = progress_reporter.clone();
        let filter_for_blocking = filter.clone();

        let res = tokio::task::spawn_blocking(move || {
            use rayon::prelude::*;
            absolute_source_paths.into_par_iter().for_each(|path| {
                parallel_scan_recursive(
                    &path,
                    filter_for_blocking.clone(),
                    status_for_blocking.clone(),
                    reporter_for_blocking.clone(),
                );
            });
        })
        .await;

        if let Err(e) = res {
            status.signal_fatal(anyhow!("Scanner panicked or failed: {e}"));
        }

        progress_reporter.scan_finished();
    });
}

fn parallel_scan_recursive(
    path: &Path,
    filter: Arc<crate::fs::filter::PathFilter>,
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

    match crate::fs::node::Node::from_path_sync(path) {
        Ok(node) => {
            progress_reporter.add_expected_items(1);
            if node.is_file() {
                progress_reporter.add_expected_bytes(node.metadata.size);
            }

            if node.is_dir() {
                match std::fs::read_dir(path) {
                    Ok(entries) => {
                        use rayon::prelude::*;
                        // par_bridge allows us to process the directory entries in parallel.
                        entries.par_bridge().for_each(|entry_res| {
                            if let Ok(entry) = entry_res {
                                parallel_scan_recursive(
                                    &entry.path(),
                                    filter.clone(),
                                    status.clone(),
                                    progress_reporter.clone(),
                                );
                            }
                        });
                    }
                    Err(e) => {
                        progress_reporter.warning(&format!(
                            "Error reading directory {}: {}",
                            path.display(),
                            e
                        ));
                    }
                }
            }
        }
        Err(e) => {
            progress_reporter.warning(&format!("Error scanning {}: {}", path.display(), e));
        }
    }
}
