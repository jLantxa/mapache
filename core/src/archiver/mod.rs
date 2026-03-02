pub(crate) mod processor;
pub(crate) mod progress;
pub(crate) mod tree_serializer;

use std::{
    collections::BTreeSet,
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

#[derive(Clone)]
pub struct SnapshotOptions<'a> {
    pub absolute_source_paths: Vec<PathBuf>,
    pub snapshot_root_path: PathBuf,
    pub exclude_paths: Vec<PathBuf>,
    pub parent_snapshot: Option<&'a SnapshotPair>,
    pub tags: BTreeSet<String>,
    pub description: Option<String>,
    pub no_scan: bool,
}

/// Internal state to coordinate graceful shutdowns and error reporting across tasks.
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

/// Orchestrates the backup snapshot process, building a new snapshot of the source paths.
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
    let (diff_tx, diff_rx) = mpsc::channel(num_readers * 2);
    let (processed_tx, mut processed_rx) = mpsc::channel(num_readers);

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

/// Spawns a scanner task.
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

        let mut scan_stream =
            match FSNodeStream::from_paths(absolute_source_paths, exclude_paths).await {
                Ok(stream) => stream,
                Err(e) => {
                    status.signal_fatal(anyhow!("Scanner failed to start: {e}"));
                    return;
                }
            };

        while let Some(item) = scan_stream.next().await {
            // Exit if user requested shutdown or another part of the pipeline failed
            if status.is_failed() || status.is_finished() {
                break;
            }

            match item {
                Ok((_path, Ok(stream_node))) => {
                    let node = stream_node.node;
                    progress_reporter.add_expected_items(1);
                    if node.is_file() {
                        progress_reporter.add_expected_bytes(node.metadata.size);
                    }
                }
                Ok((path, Err(e))) => {
                    progress_reporter.warning(&format!("Error scanning {}: {}", path.display(), e));
                }
                Err(e) => {
                    status.signal_fatal(
                        anyhow!(e).context("The scanner failed to traverse target paths"),
                    );
                    return;
                }
            }
        }
        progress_reporter.scan_finished();
    });
}
