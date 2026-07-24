//! The archiver module implements the core logic for creating new repository snapshots.
//! It orchestrates a pipeline that scans the local filesystem, compares it with
//! a parent snapshot (if available), processes changed files into chunks, and
//! serializes the resulting directory tree.

pub(crate) mod chunker_pool;
pub(crate) mod processor;
pub(crate) mod progress;
pub(crate) mod rewrite;
pub(crate) mod tree_serializer;

use std::{
    collections::{BTreeSet, VecDeque},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::SystemTime,
};

use chrono::Local;
use futures::StreamExt;
use parking_lot::Mutex;
use tokio::sync::mpsc;

use crate::{
    archiver::{
        chunker_pool::{BATCH_SIZE, ChunkerPoolMsg},
        progress::SnapshotProgress,
        tree_serializer::TreeSerializer,
    },
    common::{
        self,
        error::{self, Result},
        global::MAPACHE_VERSION_INFO,
        traits::BlobSaver,
    },
    fs::{
        filter::PathFilter,
        node::{Metadata, Node, NodeType},
        tree::{FSNodeStream, NodeDiff, NodeDiffStream, SerializedNodeStream, StreamNode},
    },
    repository::{
        repo::Repository,
        snapshot::{Snapshot, SnapshotPair, SnapshotSummary},
    },
    ui::events::{BackupEvent, Event, EventSender, emit_event},
    utils::{self, stream::ReceiverStream},
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
    /// If true, read backup data from stdin as a single file at /stdin.
    pub stdin: bool,
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
    first_error: Mutex<Option<error::MapacheError>>,
    shutdown_signal: Arc<AtomicBool>,

    event_sender: EventSender,
    progress: Arc<SnapshotProgress>,
}

impl PipelineStatus {
    fn new(
        progress: Arc<SnapshotProgress>,
        event_sender: EventSender,
        shutdown_signal: Arc<AtomicBool>,
    ) -> Self {
        Self {
            finished_flag: AtomicBool::new(false),
            fatal_error_flag: AtomicBool::new(false),
            first_error: Mutex::new(None),
            shutdown_signal,
            event_sender,
            progress,
        }
    }

    fn signal_finished(&self) {
        self.finished_flag.store(true, Ordering::Release);
    }

    fn signal_fatal(&self, err: error::MapacheError) {
        let already_errored = self.fatal_error_flag.swap(true, Ordering::Release);
        if !already_errored {
            // Only the first task to "flip" the switch gets to log the error.
            emit_event(
                &self.event_sender,
                Event::Backup(BackupEvent::Error(format!("{err:#}"))),
            );
            tracing::error!(target: "archiver", "Fatal error in pipeline: {err:#}");
            *self.first_error.lock() = Some(err);
        }
    }

    fn is_finished(&self) -> bool {
        self.finished_flag.load(Ordering::Relaxed)
    }

    fn is_failed(&self) -> bool {
        self.fatal_error_flag.load(Ordering::Acquire)
            || self.shutdown_signal.load(Ordering::Acquire)
    }
}

/// The main entry point for creating a new snapshot.
/// This function sets up the processing pipeline and waits for its completion.
pub(crate) async fn snapshot(
    repo: Arc<Repository>,
    snapshot_options: SnapshotOptions<'_>,
    num_readers: usize,
    event_sender: EventSender,
    shutdown_signal: Arc<AtomicBool>,
) -> Result<Snapshot> {
    let progress = Arc::new(SnapshotProgress::new());
    tracing::info!(target: "archiver", "Starting snapshot archival (root={:?})", snapshot_options.snapshot_root_path);
    let status = Arc::new(PipelineStatus::new(
        progress.clone(),
        event_sender.clone(),
        shutdown_signal,
    ));

    let is_stdin = snapshot_options.stdin;

    let scanner_handle = if !is_stdin {
        // Start Background Scanner early so it counts files concurrently
        // with the stream setup and the backup pipeline.
        tracing::info!(target: "archiver", "Starting background scanner");
        Some(spawn_scanner_task(
            snapshot_options.no_scan,
            snapshot_options.absolute_source_paths.clone(),
            snapshot_options.exclude_paths.clone(),
            status.clone(),
            event_sender.clone(),
        ))
    } else {
        tracing::info!(target: "archiver", "Reading backup data from stdin (scanner skipped)");
        emit_event(
            &event_sender,
            Event::Backup(BackupEvent::ScanFinished {
                total_items: 0,
                total_bytes: 0,
            }),
        );
        None
    };

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
    let producer_task = if is_stdin {
        tokio::spawn(async move {
            tracing::trace!(target: "archiver", "Stdin Producer task started");
            let node = Node {
                name: "stdin".to_string(),
                node_type: NodeType::File,
                metadata: Metadata {
                    size: 0, // We don't know the size upfront
                    modified_time: Some(SystemTime::now()),
                    mode: Some(0o100644),
                    ..Default::default()
                },
                ..Default::default()
            };
            let stream_node = StreamNode {
                node,
                num_children: 0,
            };
            let item = (
                PathBuf::from("/stdin"),
                None,
                Some(Ok(stream_node)),
                NodeDiff::New,
            );
            if diff_tx.send(item).await.is_err() {
                tracing::warn!(target: "archiver", "Stdin Producer: channel closed");
            }
            tracing::trace!(target: "archiver", "Stdin Producer task finished");
        })
    } else {
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

        tokio::spawn(async move {
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
        })
    };

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

    // Shared batch accumulator for small files.
    // Pool threads process batches sequentially, amortizing thread handoff overhead.
    let batch_lock: Arc<Mutex<Vec<chunker_pool::ChunkerJob>>> = Arc::new(Mutex::new(Vec::new()));

    let coordinator_is_stdin = is_stdin;
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
                let event_sender = status.event_sender.clone();
                let shutdown_signal = status.shutdown_signal.clone();
                let batch_lock = batch_lock.clone();
                let is_stdin = coordinator_is_stdin;

                async move {
                    if status.is_failed() {
                        return;
                    }

                    // Resolve result wrappers
                    let next_node = match next_res {
                        Some(Err(e)) => {
                            emit_event(&event_sender, Event::Backup(BackupEvent::Warning(format!("Skipping {}: {}", path.display(), e))));
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
                        let is_stdin_item = is_stdin
                            && path == Path::new("/stdin");
                        let is_small = !is_stdin_item
                            && next_node
                                .as_ref()
                                .is_some_and(|n| n.node.metadata.size <= common::defaults::MIN_CHUNK_SIZE);

                        let job = chunker_pool::ChunkerJob {
                            path,
                            prev_node,
                            next_node,
                            diff_type: diff,
                            blob_saver,
                            progress,
                            event_sender,
                            shutdown_signal,
                            is_stdin: is_stdin_item,
                        };

                        if is_small {
                            // Accumulate small files into a batch.
                            let mut batch = batch_lock.lock();
                            batch.push(job);
                            if batch.len() >= BATCH_SIZE {
                                let to_send = std::mem::take(&mut *batch);
                                drop(batch);
                                if pool_sender.send(ChunkerPoolMsg::Batch(to_send)).is_err()
                                    && !status.is_failed() {
                                        status.signal_fatal(error::MapacheError::Chunking(
                                            "chunker pool channel closed".to_string(),
                                        ));
                                }
                            }
                        } else {
                            // Large file: flush any pending batch first, then send as Single.
                            let pending = {
                                let mut batch = batch_lock.lock();
                                std::mem::take(&mut *batch)
                            };
                            if !pending.is_empty()
                                && pool_sender.send(ChunkerPoolMsg::Batch(pending)).is_err()
                                && !status.is_failed() {
                                    status.signal_fatal(error::MapacheError::Chunking(
                                        "chunker pool channel closed".to_string(),
                                    ));

                                return;
                            }
                            if pool_sender.send(ChunkerPoolMsg::Single(Box::new(job))).is_err()
                                && !status.is_failed() {
                                    status.signal_fatal(error::MapacheError::Chunking(
                                        "chunker pool channel closed".to_string(),
                                    ));
                            }
                        }
                    } else {
                        tracing::trace!(target: "archiver", "Processing item inline: {:?}", path);
                        let mut ctx = processor::ItemContext {
                            blob_saver,
                            progress: &progress,
                            event_sender: &event_sender,
                            shutdown_signal: &shutdown_signal,
                            bufs: None,
                        };
                        match processor::process_item_sync(
                            path.as_path(),
                            prev_node.as_ref(),
                            next_node.as_ref(),
                            diff,
                            &mut ctx,
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

        // Flush any remaining batch
        let remaining = {
            let mut batch = batch_lock.lock();
            std::mem::take(&mut *batch)
        };
        if !remaining.is_empty()
            && chunker_pool
                .sender
                .send(ChunkerPoolMsg::Batch(remaining))
                .is_err()
        {
            tracing::warn!(target: "archiver", "Coordinator flush failed: chunker channel closed");
        }

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
                    return Ok::<(), error::MapacheError>(());
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
        repo.repo_version(),
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
            status.signal_fatal(error::MapacheError::Repo(format!(
                "serializer error at {:?}: {}",
                path, e
            )));
            break;
        }
    }

    let results = tokio::join!(producer_task, coordinator_task, forwarder_task);
    for (name, result) in [
        ("producer", results.0),
        ("coordinator", results.1),
        ("forwarder", results.2),
    ] {
        if let Err(e) = result {
            tracing::error!(target: "archiver", "{name} task panicked: {e}");
            status.signal_fatal(error::MapacheError::Internal(format!(
                "{name} task panicked"
            )));
        }
    }

    if let Some(handle) = scanner_handle
        && let Err(e) = handle.await
    {
        tracing::warn!(target: "archiver", "Scanner task panicked: {e}");
    }

    // ---------------------------------------------------------------------
    // Finalization
    // ---------------------------------------------------------------------
    tracing::info!(target: "archiver", "Finalizing snapshot tree");

    if status.is_failed() {
        if let Some(err) = status.first_error.lock().take() {
            return Err(err);
        }
        return Err(error::MapacheError::Interrupted);
    }

    tree_serializer.finalize_root().await?;
    let root_tree_id = tree_serializer
        .root_tree()
        .ok_or_else(|| error::MapacheError::Internal("root tree ID not set".to_string()))?;

    status.signal_finished();
    let summary = progress.summary();
    emit_event(
        &event_sender,
        Event::Backup(BackupEvent::Finished(summary.clone())),
    );
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
        version: Some(MAPACHE_VERSION_INFO.clone()),
        tags: snapshot_options.tags,
        description: snapshot_options.description,
        summary: summary.into(),
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
    event_sender: EventSender,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if no_scan {
            return;
        }

        let filter = Arc::new(PathFilter::new(None, Some(exclude_paths)));

        let status_for_blocking = status.clone();
        let sender_for_blocking = event_sender.clone();
        let filter_for_blocking = filter.clone();

        let mut scan_items = 0u64;
        let mut scan_bytes = 0u64;

        let res = tokio::task::spawn_blocking(move || {
            for path in &absolute_source_paths {
                scan_recursive(
                    path,
                    filter_for_blocking.clone(),
                    status_for_blocking.clone(),
                    sender_for_blocking.clone(),
                    &mut scan_items,
                    &mut scan_bytes,
                );
            }
        })
        .await;

        if let Err(e) = res {
            status.signal_fatal(error::MapacheError::Internal(format!(
                "scanner panicked or failed: {e}"
            )));
        }

        tracing::info!(target: "archiver", "Background scanner finished");
        emit_event(
            &event_sender,
            Event::Backup(BackupEvent::ScanFinished {
                total_items: scan_items,
                total_bytes: scan_bytes,
            }),
        );
    })
}

fn scan_recursive(
    path: &Path,
    filter: Arc<PathFilter>,
    status: Arc<PipelineStatus>,
    event_sender: EventSender,
    scan_items: &mut u64,
    scan_bytes: &mut u64,
) {
    let mut stack = VecDeque::new();
    stack.push_back(path.to_path_buf());

    while let Some(current) = stack.pop_front() {
        if status.is_failed() || status.is_finished() {
            return;
        }

        if !filter.allow(&current) {
            continue;
        }

        match Node::from_path_sync(&current, false) {
            Ok(node) => {
                *scan_items += 1;
                let size = if node.is_file() {
                    node.metadata.size
                } else {
                    0
                };
                *scan_bytes += size;

                emit_event(
                    &event_sender,
                    Event::Backup(BackupEvent::ScanProgress {
                        items: 1,
                        bytes: size,
                    }),
                );

                if node.is_dir() {
                    if let Ok(entries) = std::fs::read_dir(&current) {
                        for entry in entries.flatten() {
                            stack.push_back(entry.path());
                        }
                    } else {
                        let msg = format!("error reading directory {}", current.display());
                        emit_event(&event_sender, Event::Backup(BackupEvent::Warning(msg)));
                    }
                }
            }
            Err(e) => {
                emit_event(
                    &event_sender,
                    Event::Backup(BackupEvent::Warning(format!(
                        "error scanning {}: {}",
                        current.display(),
                        e
                    ))),
                );
            }
        }
    }
}

impl From<progress::SnapshotProcessSummary> for SnapshotSummary {
    fn from(s: progress::SnapshotProcessSummary) -> Self {
        SnapshotSummary {
            processed_items_count: s.processed_items_count,
            processed_bytes: s.processed_bytes,
            diff_counts: s.diff_counts,
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;
    use zeroize::Zeroizing;

    use crate::{
        backend::{
            StorageHint,
            mock::{BackendOp, MockBackend, MockEffect},
        },
        common::{ContentIdType, ID, defaults::TEST_REPO_CONFIG},
        repository::repo::{Auth, THIS_REPOSITORY_VERSION},
        ui::events::noop_sender,
    };

    use super::*;
    #[tokio::test]
    async fn test_archiver_atomic_ordering() -> Result<()> {
        let auth = Auth {
            username: "user".to_string(),
            password: Zeroizing::new("pass".to_string()),
        };
        let backend = Arc::new(MockBackend::new());
        Repository::init(THIS_REPOSITORY_VERSION, &auth, None, backend.clone()).await?;
        let (repo, _) =
            Repository::try_open_unlocked(&auth, None, backend.clone(), TEST_REPO_CONFIG).await?;

        repo.init_pack_saver(1)?;

        let tmp = tempdir()?;
        let f1 = tmp.path().join("f1.txt");
        fs::write(&f1, b"hello world")?;

        let options = SnapshotOptions {
            absolute_source_paths: vec![tmp.path().to_path_buf()],
            snapshot_root_path: PathBuf::from("/"),
            exclude_paths: Vec::new(),
            parent_snapshot: None,
            tags: BTreeSet::new(),
            description: None,
            no_scan: false,
            with_atime: false,
            stdin: false,
        };

        let new_snapshot = snapshot(
            repo.clone(),
            options,
            1,
            noop_sender(),
            Arc::new(AtomicBool::new(false)),
        )
        .await?;

        // Finalize packs and indices
        repo.flush_and_finalize_pack_saver().await?;

        // Save snapshot file
        repo.save_file(
            &common::SaveID::CalculateID,
            serde_json::to_string(&new_snapshot)?.as_bytes(),
            StorageHint {
                is_metadata: true,
                file_type: ContentIdType::Snapshot,
            },
            None,
        )
        .await?;

        let history = backend.history();

        // Find the index and snapshot writes
        let snapshot_write_idx = history
            .iter()
            .rposition(|e| {
                if let BackendOp::Write { path, .. } = &e.op {
                    path.to_string_lossy().contains("snapshots")
                } else {
                    false
                }
            })
            .expect("Should have written a snapshot");

        // Verify that all data packs and index files were written BEFORE the snapshot
        for (i, entry) in history.iter().enumerate() {
            if let BackendOp::Write { path, .. } = &entry.op {
                let p = path.to_string_lossy();
                // Data packs are in "data/" or similar, indices in "index/"
                if p.contains("data") || p.contains("index") {
                    assert!(
                        i < snapshot_write_idx,
                        "Data/Index pack {} written after snapshot!",
                        path.display()
                    );
                }
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_archiver_handles_write_failure() -> Result<()> {
        let auth = Auth {
            username: "user".to_string(),
            password: Zeroizing::new("pass".to_string()),
        };
        let backend = Arc::new(MockBackend::new());
        Repository::init(THIS_REPOSITORY_VERSION, &auth, None, backend.clone()).await?;
        let (repo, _) =
            Repository::try_open_unlocked(&auth, None, backend.clone(), TEST_REPO_CONFIG).await?;

        repo.init_pack_saver(1)?;

        // Inject failure on writing any file to the backend
        backend.add_hook(Arc::new(|op| {
            if let BackendOp::Write { .. } = op {
                return MockEffect {
                    result_override: Some(Err(error::MapacheError::Backend(
                        "write failed".to_string(),
                    ))),
                    ..Default::default()
                };
            }
            MockEffect::default()
        }));

        let tmp = tempdir()?;
        fs::write(tmp.path().join("f1.txt"), b"some data")?;

        let options = SnapshotOptions {
            absolute_source_paths: vec![tmp.path().to_path_buf()],
            snapshot_root_path: PathBuf::from("/"),
            exclude_paths: Vec::new(),
            parent_snapshot: None,
            tags: BTreeSet::new(),
            description: None,
            no_scan: false,
            with_atime: false,
            stdin: false,
        };

        let res = snapshot(
            repo.clone(),
            options,
            1,
            noop_sender(),
            Arc::new(AtomicBool::new(false)),
        )
        .await;

        if res.is_ok() {
            let flush_res = repo.flush_and_finalize_pack_saver().await;
            assert!(flush_res.is_err(), "Flush should have failed");
        }

        // Verify that at least one write operation failed in the backend.
        let history = backend.history();
        let write_failed = history
            .iter()
            .any(|entry| matches!(&entry.op, BackendOp::Write { .. }) && entry.result.is_err());
        assert!(
            write_failed,
            "Expected at least one failed write in backend history"
        );

        Ok(())
    }

    // ------------------------------------------------------------------
    // Stdin snapshot integration test (cross-platform)
    // ------------------------------------------------------------------
    // Uses platform-specific APIs to redirect stdin to a pipe with test
    // data, then runs the full archiver::snapshot pipeline.

    /// Global mutex to serialize tests that modify process-global stdin.
    static STDIN_LOCK: std::sync::LazyLock<tokio::sync::Mutex<()>> =
        std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));

    /// RAII guard that redirects stdin to a pipe containing `data`,
    /// and restores the original stdin on drop.
    struct StdinPipe {
        #[cfg(unix)]
        saved_stdin: i32,
        #[cfg(windows)]
        saved_stdin: windows_sys::Win32::Foundation::HANDLE,
        #[cfg(windows)]
        read_handle: windows_sys::Win32::Foundation::HANDLE,
    }

    #[cfg(unix)]
    impl StdinPipe {
        fn new(data: &[u8]) -> Self {
            let mut fds = [0i32; 2];

            let ret = unsafe {
                // SAFETY: FFI call to pipe() with valid pointer to fds.
                libc::pipe(fds.as_mut_ptr())
            };
            assert_eq!(ret, 0, "pipe() failed");

            let read_fd = fds[0];
            let write_fd = fds[1];

            let written = unsafe {
                // SAFETY: FFI call to write() with valid pipe end and data buffer.
                libc::write(write_fd, data.as_ptr() as *const libc::c_void, data.len())
            };
            assert_eq!(written as usize, data.len(), "write() failed");
            unsafe {
                // SAFETY: FFI call to close() with valid pipe end.
                libc::close(write_fd)
            };

            let saved_stdin = unsafe {
                // SAFETY: FFI call to dup() to save current stdin.
                libc::dup(0)
            };
            assert!(saved_stdin >= 0, "dup(0) failed");

            let ret = unsafe {
                // SAFETY: FFI call to dup2() to redirect stdin from pipe.
                libc::dup2(read_fd, 0)
            };
            assert_eq!(ret, 0, "dup2() failed");

            unsafe {
                // SAFETY: FFI call to close() with valid pipe end.
                libc::close(read_fd)
            };

            StdinPipe { saved_stdin }
        }
    }

    #[cfg(unix)]
    impl Drop for StdinPipe {
        fn drop(&mut self) {
            unsafe {
                // SAFETY: FFI calls to restore original stdin and close the backup descriptor.
                libc::dup2(self.saved_stdin, 0);
                libc::close(self.saved_stdin);
            }
        }
    }

    #[cfg(windows)]
    impl StdinPipe {
        fn new(data: &[u8]) -> Self {
            use windows_sys::Win32::{
                Foundation::CloseHandle,
                Storage::FileSystem::WriteFile,
                System::{
                    Console::{GetStdHandle, STD_INPUT_HANDLE, SetStdHandle},
                    Pipes::CreatePipe,
                },
            };

            let mut read_handle = std::ptr::null_mut();
            let mut write_handle = std::ptr::null_mut();

            let ret = unsafe {
                // SAFETY: FFI call to CreatePipe with valid handle pointers.
                CreatePipe(&mut read_handle, &mut write_handle, std::ptr::null(), 0)
            };
            assert_ne!(ret, 0, "CreatePipe failed");

            let mut bytes_written: u32 = 0;

            let ret = unsafe {
                // SAFETY: FFI call to WriteFile with valid pipe handle and data buffer.
                WriteFile(
                    write_handle,
                    data.as_ptr() as *const _,
                    data.len() as u32,
                    &mut bytes_written,
                    std::ptr::null_mut(),
                )
            };
            assert_ne!(ret, 0, "WriteFile failed");
            assert_eq!(bytes_written as usize, data.len());

            unsafe {
                // SAFETY: FFI call to CloseHandle for the write end of the pipe.
                CloseHandle(write_handle)
            };

            let saved_stdin = unsafe {
                // SAFETY: FFI call to GetStdHandle to save current stdin handle.
                GetStdHandle(STD_INPUT_HANDLE)
            };
            unsafe {
                // SAFETY: FFI call to SetStdHandle to redirect stdin from pipe.
                SetStdHandle(STD_INPUT_HANDLE, read_handle)
            };

            StdinPipe {
                saved_stdin,
                read_handle,
            }
        }
    }

    #[cfg(windows)]
    impl Drop for StdinPipe {
        fn drop(&mut self) {
            use windows_sys::Win32::{
                Foundation::CloseHandle,
                System::Console::{STD_INPUT_HANDLE, SetStdHandle},
            };
            unsafe {
                // SAFETY: FFI calls to restore original stdin and close the pipe handle.
                SetStdHandle(STD_INPUT_HANDLE, self.saved_stdin);
                CloseHandle(self.read_handle);
            }
        }
    }

    /// Test that archiver::snapshot with stdin=true correctly creates a
    /// snapshot by piping data through a real stdin redirect.
    #[tokio::test]
    async fn test_stdin_snapshot_pipeline() {
        let _guard = STDIN_LOCK.lock().await;

        // --- Setup repo ---
        let auth = Auth {
            username: "stdin_test".to_string(),
            password: Zeroizing::new("stdin_test".to_string()),
        };
        let backend = Arc::new(MockBackend::new());
        Repository::init(THIS_REPOSITORY_VERSION, &auth, None, backend.clone())
            .await
            .unwrap();
        let (repo, _) =
            Repository::try_open_unlocked(&auth, None, backend.clone(), TEST_REPO_CONFIG)
                .await
                .unwrap();
        repo.init_pack_saver(1).unwrap();

        // --- Pipe test data via stdin redirect ---
        let test_data = b"mapache stdin integration test data";
        let _stdin_pipe = StdinPipe::new(test_data);

        // --- Run archiver with stdin=true ---
        let snapshot_result = snapshot(
            repo.clone(),
            SnapshotOptions {
                absolute_source_paths: vec![PathBuf::from("/stdin")],
                snapshot_root_path: PathBuf::from("/"),
                exclude_paths: Vec::new(),
                parent_snapshot: None,
                tags: BTreeSet::new(),
                description: Some("stdin test snapshot".to_string()),
                no_scan: true,
                with_atime: false,
                stdin: true,
            },
            1,
            noop_sender(),
            Arc::new(AtomicBool::new(false)),
        )
        .await;

        // StdinPipe's Drop restores original stdin (even on panic).

        // --- Assertions ---
        let snapshot = snapshot_result.expect("snapshot should succeed");
        assert_eq!(snapshot.root, PathBuf::from("/"));
        assert_ne!(snapshot.tree, ID::default(), "tree ID should not be nil");

        // Cleanup
        repo.flush_and_finalize_pack_saver().await.unwrap();
    }
}
