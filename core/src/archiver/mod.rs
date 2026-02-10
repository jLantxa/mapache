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
    thread::JoinHandle,
};

use anyhow::{Result, anyhow, bail};
use chrono::Local;

use crate::{
    archiver::{progress::SnapshotProgress, tree_serializer::TreeSerializer},
    fs::tree::{FSNodeStream, NodeDiff, NodeDiffStream, SerializedNodeStream, StreamNode},
    mapache::{ID, global::THIS_MAPACHE_VERSION},
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

/// Internal state to coordinate graceful shutdowns and error reporting across threads.
struct PipelineStatus {
    /// Signal that the snapshot is finished.
    finished_flag: AtomicBool,

    /// This error flag signals a fatal error to all running threads so they can
    /// abort execution early. Only fatal, unrecoverable errors should be signaled.
    fatal_error_flag: AtomicBool,
    /// Stores the first error that triggered the shutdown to report back to the user.
    first_error: Mutex<Option<anyhow::Error>>,

    progress_reporter: Arc<dyn SnapshotProgressReporter>,
    progress: Arc<SnapshotProgress>,
}

impl PipelineStatus {
    fn new(
        progress: Arc<SnapshotProgress>,
        progress_reporter: Arc<dyn SnapshotProgressReporter>,
    ) -> Self {
        Self {
            finished_flag: AtomicBool::new(false),
            fatal_error_flag: AtomicBool::new(false),
            first_error: Mutex::new(None),
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
            // Only the first thread to "flip" the switch gets to log the error.
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
        self.fatal_error_flag.load(Ordering::Relaxed)
    }
}

/// Orchestrates the backup snapshot process, building a new snapshot of the source paths.
///
/// This implementation utilizes a multi-threaded, channel-based architecture to manage
/// the workflow. Dedicated threads handle generating the difference stream, processing
/// individual file and directory changes, and serializing the resulting tree structure
/// bottom-up to create the final snapshot.
pub(crate) fn snapshot(
    repo: Arc<Repository>,
    snapshot_options: SnapshotOptions,
    num_readers: usize,
    progress: Arc<SnapshotProgress>,
    progress_reporter: Arc<dyn SnapshotProgressReporter>,
) -> Result<Snapshot> {
    let status = Arc::new(PipelineStatus::new(progress, progress_reporter.clone()));

    // Extract parent snapshot tree id
    let parent_tree_id: Option<ID> = snapshot_options
        .parent_snapshot
        .as_ref()
        .map(|snapshot_pair| snapshot_pair.snapshot.tree);

    // Create streams
    let fs_stream = match FSNodeStream::from_paths(
        snapshot_options.absolute_source_paths.clone(),
        snapshot_options.exclude_paths.clone(),
    ) {
        Ok(stream) => stream,
        Err(e) => bail!(anyhow!("Failed to initialize filesystem node stream: {e}")),
    };

    let previous_tree_stream = match SerializedNodeStream::new(
        repo.clone(),
        parent_tree_id,
        snapshot_options.snapshot_root_path.clone(),
        None,
        None,
    ) {
        Ok(stream) => stream,
        Err(e) => bail!(anyhow!(
            "Failed to initialize previous snapshot tree stream: {e}"
        )),
    };

    // Channels
    let (diff_tx, diff_rx) =
        crossbeam_channel::bounded::<(PathBuf, Option<StreamNode>, Option<StreamNode>, NodeDiff)>(
            4 * num_readers,
        );
    let (process_item_tx, process_item_rx) =
        crossbeam_channel::bounded::<(PathBuf, StreamNode)>(16 * num_readers);

    // Spawn archiver threads
    let scanner_thread = spawn_scanner_thread(
        snapshot_options.no_scan,
        snapshot_options.absolute_source_paths.clone(),
        snapshot_options.exclude_paths.clone(),
        status.clone(),
        progress_reporter.clone(),
    );
    let diff_thread = spawn_diff_thread(status.clone(), previous_tree_stream, fs_stream, diff_tx);
    let processor_thread = spawn_processor_thread(
        num_readers,
        repo.clone(),
        status.clone(),
        diff_rx,
        process_item_tx,
    );
    let tree_serializer_thread = spawn_serializer_thread(
        repo.clone(),
        status.clone(),
        snapshot_options.snapshot_root_path.clone(),
        snapshot_options.absolute_source_paths.clone(),
        process_item_rx,
    );

    // Join threads and handle potential panics or errors
    if let Some(e) = scanner_thread
        .join()
        .unwrap_or_else(|_| Err(anyhow!("Archiver scanner thread panicked")))
        .err()
    {
        status.signal_fatal(e.context("Archiver scanner thread error"))
    }
    if diff_thread.join().err().is_some() {
        status.signal_fatal(anyhow!("Archiver diff thread panicked"))
    }
    if processor_thread.join().err().is_some() {
        status.signal_fatal(anyhow!("Archiver processor thread panicked"))
    }

    let root_tree_id = tree_serializer_thread
        .join()
        .map_err(|_| anyhow!("Archiver serializer thread panicked"))?;

    // Return now if fatal error occurred in any thread
    if let Some(err) = status.first_error.lock().unwrap().take() {
        return Err(err);
    }

    let (hostname, username) = utils::get_system_info();

    match root_tree_id {
        Some(tree_id) => Ok(Snapshot {
            timestamp: Local::now(),
            parent: snapshot_options
                .parent_snapshot
                .map(|snapshot_pair| snapshot_pair.id),
            tree: tree_id,
            root: snapshot_options.snapshot_root_path,
            paths: snapshot_options.absolute_source_paths,
            hostname,
            username,
            version: Some(THIS_MAPACHE_VERSION.to_string()),
            tags: snapshot_options.tags,
            description: snapshot_options.description,
            summary: SnapshotSummary::default(),
        }),
        None => Err(anyhow!(
            "Failed to finalize snapshot: No root tree ID was generated."
        )),
    }
}

/// Spawns a scanner thread.
/// This thread scans the filesystem to collect stats about the targets.
/// This information is only used to display the total number of bytes and items to process
/// in the progress bar. To save time, this is started concurrently with the archiver pipeline.
fn spawn_scanner_thread(
    no_scan: bool,
    absolute_source_paths: Vec<PathBuf>,
    exclude_paths: Vec<PathBuf>,
    status: Arc<PipelineStatus>,
    progress_reporter: Arc<dyn SnapshotProgressReporter>,
) -> JoinHandle<Result<()>> {
    std::thread::spawn(move || {
        if no_scan {
            return Ok(());
        }
        let scan_stream = match FSNodeStream::from_paths(absolute_source_paths, exclude_paths) {
            Ok(stream) => stream,
            Err(e) => bail!(anyhow!("Scanner failed to start: {e}")),
        };

        for item in scan_stream {
            if status.is_failed() || status.is_finished() {
                break;
            }

            match item {
                Ok((_path, stream_node)) => {
                    let node = stream_node.node;
                    progress_reporter.add_expected_items(1);
                    if node.is_file() {
                        progress_reporter.add_expected_bytes(node.metadata.size);
                    }
                }
                Err(e) => {
                    return Err(
                        anyhow!(e).context("The scanner failed to traverse the target paths")
                    );
                }
            }
        }
        progress_reporter.scan_finished();
        Ok(())
    })
}

/// Spawns the diff thread.
/// This thread iterates the NodeDiffStream and passes the items to the item
/// processor thread.
fn spawn_diff_thread(
    status: Arc<PipelineStatus>,
    previous_tree_stream: SerializedNodeStream,
    fs_stream: FSNodeStream,
    diff_tx: crossbeam_channel::Sender<(PathBuf, Option<StreamNode>, Option<StreamNode>, NodeDiff)>,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let diff_stream = NodeDiffStream::new(previous_tree_stream, fs_stream);

        for diff_result in diff_stream {
            if status.is_failed() {
                break;
            }

            match diff_result {
                Ok(diff_item) => {
                    if diff_tx.send(diff_item).is_err() {
                        break;
                    }
                }
                Err(e) => {
                    status.signal_fatal(anyhow!(e).context("Archiver diff stream error"));
                    break;
                }
            }
        }
    })
}

/// Spawns the item processor thread.
/// This thread receives diffs and processes them in parallel using a rayon
/// parallel iterator, chunking and saving files in the process. The resultin
/// processed nodes are passed to the serializer thread. A thread of pool is
/// installed in order to enforce the concurrency limit.
fn spawn_processor_thread(
    num_readers: usize,
    repo: Arc<Repository>,
    status: Arc<PipelineStatus>,
    diff_rx: crossbeam_channel::Receiver<(
        PathBuf,
        Option<StreamNode>,
        Option<StreamNode>,
        NodeDiff,
    )>,
    process_item_tx: crossbeam_channel::Sender<(PathBuf, StreamNode)>,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let mut handles = Vec::with_capacity(num_readers);

        for _ in 0..num_readers {
            let rx = diff_rx.clone();
            let repo = repo.clone();
            let status = status.clone();
            let process_item_tx = process_item_tx.clone();

            handles.push(std::thread::spawn(move || {
                let mut ctx = match repo.get_encoding_context() {
                    Ok(ctx) => ctx,
                    Err(e) => {
                        status.signal_fatal(anyhow!(e).context("Failed to create thread context"));
                        return;
                    }
                };

                while let Ok((path, prev, next, diff)) = rx.recv() {
                    if status.is_failed() {
                        break;
                    }

                    match processor::process_item(
                        (path.as_path(), prev, next, diff),
                        repo.clone(),
                        &mut ctx,
                        status.progress.as_ref(),
                        status.progress_reporter.as_ref(),
                    ) {
                        Ok(Some(stream_node)) => {
                            if let Err(e) = process_item_tx.send((path, stream_node)) {
                                status.signal_fatal(anyhow!(
                                    "Archiver error sending item {:?}: {e:?}",
                                    e.0.0
                                ));
                                break;
                            }
                        }
                        Ok(None) => {}
                        Err(e) => {
                            status.signal_fatal(
                                e.context(format!("Archiver error processing item {path:?}")),
                            );
                            break;
                        }
                    }
                }
            }));
        }

        drop(process_item_tx); // allow serializer to terminate when workers finish

        for h in handles {
            if h.join().is_err() {
                status.signal_fatal(anyhow!("Archiver processor worker thread panicked"));
            }
        }
    })
}

/// Spawns the serializer thread.
/// This thread receives processed items and serializes tree nodes as they
/// become finalized, bottom-up.
fn spawn_serializer_thread(
    repo: Arc<Repository>,
    status: Arc<PipelineStatus>,
    snapshot_root_path: PathBuf,
    absolute_source_paths: Vec<PathBuf>,
    process_item_rx: crossbeam_channel::Receiver<(PathBuf, StreamNode)>,
) -> JoinHandle<Option<ID>> {
    std::thread::spawn(move || {
        let mut tree_serializer =
            TreeSerializer::new(repo.clone(), snapshot_root_path, &absolute_source_paths);

        let mut encoding_context = match repo.get_encoding_context() {
            Ok(ctx) => ctx,
            Err(e) => {
                status.signal_fatal(
                    anyhow!(e).context("Serializer thread failed to initialize encoding context"),
                );
                return None;
            }
        };

        while let Ok((path, stream_node)) = process_item_rx.recv() {
            if status.is_failed() {
                break;
            }

            if let Err(e) =
                tree_serializer.handle_processed_item((&path, stream_node), &mut encoding_context)
            {
                status.signal_fatal(e.context(format!(
                    "Archiver serializer thread errored handling processed item {path:?}"
                )));
                return None;
            }
        }

        if status.is_failed() {
            return None;
        }

        match tree_serializer.finalize_root(&mut encoding_context) {
            Ok(_) => {
                status.signal_finished(); // Signal completion to other threads (mainly the scanner)
                tree_serializer.root_tree()
            }
            Err(e) => {
                status.signal_fatal(
                    e.context("Archiver serializer thread errored finalizing root tree"),
                );
                None
            }
        }
    })
}
