pub(crate) mod processor;
pub(crate) mod tree_serializer;

use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
};

use anyhow::{Result, anyhow, bail};
use chrono::Local;
use rayon::iter::{ParallelBridge, ParallelIterator};

use crate::{
    archiver::tree_serializer::TreeSerializer,
    fs::tree::{FSNodeStream, NodeDiff, NodeDiffStream, SerializedNodeStream, StreamNode},
    mapache::ID,
    repository::{
        repo::Repository,
        snapshot::{Snapshot, SnapshotPair},
    },
    ui::snapshot_progress::SnapshotProgressReporter,
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

/// Orchestrates the backup snapshot process, building a new snapshot of the source paths.
///
/// This implementation utilizes a multi-threaded, channel-based architecture to manage
/// the workflow. Dedicated threads handle generating the difference stream, processing
/// individual file and directory changes, and serializing the resulting tree structure
/// bottom-up to create the final snapshot.
pub(crate) fn snapshot(
    repo: Arc<Repository>,
    snapshot_options: SnapshotOptions,
    (read_concurrency, write_concurrency): (usize, usize),
    progress_reporter: Arc<SnapshotProgressReporter>,
) -> Result<Snapshot> {
    // This error flag signals a fatal error to all running threads so they can
    // abort execution early. Only fatal, unrecoverable errors should be signaled.
    let fatal_error_flag = Arc::new(AtomicBool::new(false));

    // Extract parent snapshot tree id
    let parent_tree_id: Option<ID> = snapshot_options
        .parent_snapshot
        .as_ref()
        .map(|snapshot_pair| snapshot_pair.snapshot.tree);

    // Create streams
    let fs_stream = FSNodeStream::from_paths(
        snapshot_options.absolute_source_paths.clone(),
        snapshot_options.exclude_paths.clone(),
    )?;
    let previous_tree_stream = SerializedNodeStream::new(
        repo.clone(),
        parent_tree_id,
        snapshot_options.snapshot_root_path.clone(),
        None,
        None,
    )?;

    repo.init_pack_saver(write_concurrency);

    // Channels
    let (diff_tx, diff_rx) =
        crossbeam_channel::bounded::<(PathBuf, Option<StreamNode>, Option<StreamNode>, NodeDiff)>(
            4 * read_concurrency,
        );
    let (process_item_tx, process_item_rx) =
        crossbeam_channel::bounded::<(PathBuf, StreamNode)>(16 * read_concurrency);

    // Spawn archiver threads
    let scanner_thread = spawn_scanner_thread(
        snapshot_options.no_scan,
        snapshot_options.absolute_source_paths.clone(),
        snapshot_options.exclude_paths.clone(),
        progress_reporter.clone(),
    );
    let diff_thread = spawn_diff_thread(
        progress_reporter.clone(),
        fatal_error_flag.clone(),
        previous_tree_stream,
        fs_stream,
        diff_tx,
    );
    let processor_thread = spawn_processor_thread(
        read_concurrency,
        repo.clone(),
        progress_reporter.clone(),
        fatal_error_flag.clone(),
        snapshot_options.snapshot_root_path.clone(),
        diff_rx,
        process_item_tx,
    );
    let tree_serializer_thread = spawn_serializer_thread(
        repo.clone(),
        progress_reporter.clone(),
        fatal_error_flag.clone(),
        snapshot_options.snapshot_root_path.clone(),
        snapshot_options.absolute_source_paths.clone(),
        process_item_rx,
    );

    // Join threads
    scanner_thread
        .join()
        .map_err(|_| anyhow!("Archiver scanner thread panicked"))??;
    diff_thread
        .join()
        .map_err(|_| anyhow!("Archiver diff thread panicked"))?;
    processor_thread
        .join()
        .map_err(|_| anyhow!("Archiver processor thread panicked"))?;
    let root_tree_id = tree_serializer_thread
        .join()
        .map_err(|_| anyhow!("Archiver serializer thread panicked"))?;

    // Return now if fatal error
    if fatal_error_flag.load(Ordering::Relaxed) {
        bail!("A fatal error occurred. Aborting snapshot.");
    }

    // Flush repo and finalize pack saver
    let flushed_meta_size = repo.flush()?;
    progress_reporter.written_meta_bytes(flushed_meta_size);
    repo.finalize_pack_saver()?;

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
            tags: snapshot_options.tags,
            description: snapshot_options.description,
            summary: progress_reporter.get_summary(),
        }),
        None => Err(anyhow!(
            "Failed to finalize snapshot: No root tree ID was generated."
        )),
    }
}

fn strip_prefix<'a>(full_path: &'a Path, root_path: &'a Path) -> &'a Path {
    full_path.strip_prefix(root_path).unwrap_or(full_path)
}

fn signal_fatal_error(
    progress_reporter: &Arc<SnapshotProgressReporter>,
    fatal_error_flag: &AtomicBool,
    msg: &str,
) {
    progress_reporter.error(msg);
    fatal_error_flag.store(true, Ordering::SeqCst);
}

/// Spawns a scanner thread.
/// This thread scans the filesystem to collect stats about the targets.
/// This information is only used to display the total number of bytes and items to process
/// in the progress bar. To save time, this is started concurrently with the archiver pipeline.
fn spawn_scanner_thread(
    no_scan: bool,
    absolute_source_paths: Vec<PathBuf>,
    exclude_paths: Vec<PathBuf>,
    progress_reporter: Arc<SnapshotProgressReporter>,
) -> JoinHandle<Result<()>> {
    std::thread::spawn(move || {
        if no_scan {
            return Ok(());
        }
        match FSNodeStream::from_paths(absolute_source_paths, exclude_paths) {
            Ok(scan_stream) => {
                for (_path, stream_node) in scan_stream.flatten() {
                    let node = stream_node.node;
                    progress_reporter.add_expected_items(1);
                    if node.is_file() {
                        progress_reporter.add_expected_bytes(node.metadata.size);
                    }
                }
                progress_reporter.scan_finished();
                Ok(())
            }
            Err(e) => {
                progress_reporter.error(&format!(
                    "The scanner failed to traverse the target paths: {e:#?}"
                ));
                Err(e)
            }
        }
    })
}

/// Spawns the diff thread.
/// This thread iterates the NodeDiffStream and passes the items to the item
/// processor thread.
fn spawn_diff_thread(
    progress_reporter: Arc<SnapshotProgressReporter>,
    fatal_error_flag: Arc<AtomicBool>,
    previous_tree_stream: SerializedNodeStream,
    fs_stream: FSNodeStream,
    diff_tx: crossbeam_channel::Sender<(PathBuf, Option<StreamNode>, Option<StreamNode>, NodeDiff)>,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let diff_stream = NodeDiffStream::new(previous_tree_stream, fs_stream);

        for diff_result in diff_stream {
            if fatal_error_flag.load(Ordering::Relaxed) {
                break;
            }

            match diff_result {
                Ok(diff_item) => {
                    if let Err(e) = diff_tx.send(diff_item) {
                        signal_fatal_error(
                            &progress_reporter,
                            &fatal_error_flag,
                            &format!("Archiver errored sending diff: {e:#?}"),
                        );
                    }
                }
                Err(e) => {
                    progress_reporter.error(&format!("{e:#?}"));
                }
            }
        }
        drop(diff_tx);
    })
}

/// Spawns the item processor thread.
/// This thread receives diffs and processes them in parallel using a rayon
/// parallel iterator, chunking and saving files in the process. The resultin
/// processed nodes are passed to the serializer thread. A thread of pool is
/// installed in order to enforce the concurrency limit.
fn spawn_processor_thread(
    read_concurrency: usize,
    repo: Arc<Repository>,
    progress_reporter: Arc<SnapshotProgressReporter>,
    fatal_error_flag: Arc<AtomicBool>,
    snapshot_root_path: PathBuf,
    diff_rx: crossbeam_channel::Receiver<(
        PathBuf,
        Option<StreamNode>,
        Option<StreamNode>,
        NodeDiff,
    )>,
    process_item_tx: crossbeam_channel::Sender<(PathBuf, StreamNode)>,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(read_concurrency)
            .build()
            .expect("Failed to create Rayon read concurrency pool");

        pool.install(|| {
            let _ = diff_rx.into_iter().par_bridge().try_for_each_init(
                || {
                    repo.get_encoding_context()
                        .expect("Failed to create thread context")
                },
                |ctx, (path, prev, next, diff)| {
                    if fatal_error_flag.load(Ordering::Relaxed) {
                        return Err(());
                    }

                    let stripped_path = strip_prefix(&path, &snapshot_root_path);
                    progress_reporter.processing_node(stripped_path, diff);

                    match processor::process_item(
                        (path.as_path(), prev, next, diff),
                        repo.clone(),
                        ctx,
                        progress_reporter.as_ref(),
                    ) {
                        Ok(Some(stream_node)) => {
                            // re-wrap with the owned PathBuf for the channel
                            let processed_item: (PathBuf, StreamNode) = (path, stream_node);

                            if let Err(e) = process_item_tx.send(processed_item) {
                                progress_reporter.error(&format!(
                                    "Archiver error sending item {:?}: {e:#?}",
                                    e.0.0
                                ));
                            }
                        }
                        Ok(None) => {}
                        Err(e) => {
                            progress_reporter
                                .error(&format!("Archiver error processing item {path:?}: {e:#?}"));
                        }
                    }

                    Ok(())
                },
            );
        });
    })
}

/// Spawns the serializer thread.
/// This thread receives processed items and serializes tree nodes as they
/// become finalized, bottom-up.
fn spawn_serializer_thread(
    repo: Arc<Repository>,
    progress_reporter: Arc<SnapshotProgressReporter>,
    fatal_error_flag: Arc<AtomicBool>,
    snapshot_root_path: PathBuf,
    absolute_source_paths: Vec<PathBuf>,
    process_item_rx: crossbeam_channel::Receiver<(PathBuf, StreamNode)>,
) -> JoinHandle<Option<ID>> {
    std::thread::spawn(move || {
        let mut tree_serializer = TreeSerializer::new(
            repo.clone(),
            snapshot_root_path.clone(),
            &absolute_source_paths,
        );

        let mut encoding_context = match repo.get_encoding_context() {
            Ok(ctx) => ctx,
            Err(e) => {
                signal_fatal_error(
                    &progress_reporter,
                    &fatal_error_flag,
                    &format!("Serializer thread failed to initialize encoding context: {e:#}"),
                );
                return None; // Exit the thread early
            }
        };

        while let Ok((path, stream_node)) = process_item_rx.recv() {
            if fatal_error_flag.load(Ordering::Relaxed) {
                break;
            }

            match tree_serializer.handle_processed_item((&path, stream_node), &mut encoding_context)
            {
                Ok(size) => progress_reporter.written_meta_bytes(size),
                Err(e) => {
                    signal_fatal_error(
                        &progress_reporter,
                        &fatal_error_flag,
                        &format!(
                            "Archiver serializer thread errored handling processed item {path:?}: {e:#}"
                        ),
                    );
                }
            }
        }

        if fatal_error_flag.load(Ordering::Relaxed) {
            return None;
        }

        match tree_serializer.finalize_root(&mut encoding_context) {
            Ok(size) => {
                progress_reporter.written_meta_bytes(size);
                tree_serializer.root_tree()
            }
            Err(e) => {
                progress_reporter.error(&format!(
                    "Archiver serializer thread errored finalizing root tree: {e:#}"
                ));
                None
            }
        }
    })
}
