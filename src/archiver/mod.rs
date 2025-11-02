pub(crate) mod processor;
pub(crate) mod tree_serializer;

use std::{
    collections::BTreeSet,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use anyhow::{Result, anyhow, bail};
use chrono::Local;
use rayon::iter::{ParallelBridge, ParallelIterator};

use crate::{
    archiver::tree_serializer::TreeSerializer,
    fs::tree::{FSNodeStreamer, NodeDiff, NodeDiffStreamer, SerializedNodeStreamer, StreamNode},
    mapache::ID,
    repository::{repo::Repository, snapshot::Snapshot},
    ui::snapshot_progress::SnapshotProgressReporter,
    utils,
};

pub struct SnapshotOptions {
    pub absolute_source_paths: Vec<PathBuf>,
    pub snapshot_root_path: PathBuf,
    pub exclude_paths: Vec<PathBuf>,
    pub parent_snapshot: Option<(ID, Snapshot)>,
    pub tags: BTreeSet<String>,
    pub description: Option<String>,
}

pub struct Archiver {
    repo: Arc<Repository>,
    snapshot_options: SnapshotOptions,
    read_concurrency: usize,
    write_concurrency: usize,
    progress_reporter: Arc<SnapshotProgressReporter>,
}

impl Archiver {
    pub fn new(
        repo: Arc<Repository>,
        snapshot_options: SnapshotOptions,
        (read_concurrency, write_concurrency): (usize, usize),
        progress_reporter: Arc<SnapshotProgressReporter>,
    ) -> Self {
        Self {
            repo,
            snapshot_options,
            read_concurrency,
            write_concurrency,
            progress_reporter,
        }
    }

    /// Orchestrates the backup snapshot process, building a new snapshot of the source paths.
    ///
    /// This implementation utilizes a multi-threaded, channel-based architecture to manage
    /// the workflow.Dedicated threads handle generating the difference stream, processing
    /// individual file and directory changes, and serializing the resulting tree structure
    /// bottom-up to create the final snapshot.
    pub fn snapshot(self) -> Result<Snapshot> {
        let arch = Arc::from(self);

        // This error flag signals a fatal error to all running threads so they can
        // abort execution early. Only fatal, unrecoverable errors should be signaled.
        let fatal_error_flag = Arc::new(AtomicBool::new(false));

        // Extract parent snapshot tree id
        let parent_tree_id: Option<ID> = arch
            .snapshot_options
            .parent_snapshot
            .as_ref()
            .map(|(_id, snapshot)| snapshot.tree);

        // Create streamers
        let fs_streamer = FSNodeStreamer::from_paths(
            arch.snapshot_options.absolute_source_paths.clone(),
            arch.snapshot_options.exclude_paths.clone(),
        )?;
        let previous_tree_streamer = SerializedNodeStreamer::new(
            arch.repo.clone(),
            parent_tree_id,
            arch.snapshot_options.snapshot_root_path.clone(),
            None,
            None,
        )?;

        arch.repo.init_pack_saver(arch.write_concurrency);

        // Channels
        let (diff_tx, diff_rx) = crossbeam_channel::bounded::<(
            PathBuf,
            Option<StreamNode>,
            Option<StreamNode>,
            NodeDiff,
        )>(4 * arch.read_concurrency);
        let (process_item_tx, process_item_rx) =
            crossbeam_channel::bounded::<(PathBuf, StreamNode)>(4 * arch.read_concurrency);

        // Diff thread. This thread iterates the NodeDiffStreamer and passes the
        // items to the item processor thread.
        let diff_progress_reporter_clone = arch.progress_reporter.clone();
        let diff_fatal_error_flag = fatal_error_flag.clone();
        let diff_thread = std::thread::spawn(move || {
            let diff_streamer = NodeDiffStreamer::new(previous_tree_streamer, fs_streamer);

            for diff_result in diff_streamer {
                if diff_fatal_error_flag.load(Ordering::Relaxed) {
                    break;
                }

                match diff_result {
                    Ok((path, prev, next, diff)) => {
                        if let Err(e) = diff_tx.send((path, prev, next, diff)) {
                            diff_progress_reporter_clone
                                .error(&format!("Archiver errored sending diff: {e:#?}"));
                            diff_fatal_error_flag.store(true, Ordering::SeqCst);
                        }
                    }
                    Err(e) => {
                        diff_progress_reporter_clone.error(&format!("{e:#?}"));
                    }
                }
            }

            // Exclicitly drop the diff tx.
            drop(diff_tx);
        });

        // Item processor thread. This thread receives diffs and processes them in parallel
        // using a rayon parallel iterator, chunking and saving files in the process.
        // The resulting processed nodes are passed to the serializer thread.
        // A thread of pool is installed in order to enforce the concurrency limit.
        let process_item_tx_clone = process_item_tx.clone();
        let repo_clone = arch.repo.clone();
        let processor_progress_reporter_clone = arch.progress_reporter.clone();
        let processor_fatal_error_flag = fatal_error_flag.clone();
        let snapshot_root_path_clone = arch.snapshot_options.snapshot_root_path.clone();

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(arch.read_concurrency)
            .build()
            .expect("Failed to create Rayon read concurrency pool");

        let processor_thread = std::thread::spawn(move || {
            pool.install(|| {
                let _ = diff_rx
                    .into_iter()
                    .par_bridge()
                    .try_for_each(|(path, prev, next, diff)| {
                        if processor_fatal_error_flag.load(Ordering::Relaxed) {
                            return Err(());
                        }

                        let inner_repo_clone = repo_clone.clone();
                        let inner_progress_reporter_clone = processor_progress_reporter_clone.clone();

                        let stripped_path = path
                            .strip_prefix(&snapshot_root_path_clone)
                            .unwrap_or(&path)
                            .to_path_buf();

                        inner_progress_reporter_clone.processing_node(&stripped_path, diff);

                        let processed_item_result = processor::process_item(
                            (path, prev, next, diff),
                            inner_repo_clone,
                            inner_progress_reporter_clone.clone(),
                        );

                        match processed_item_result {
                            Ok(Some(processed_item)) => {
                                if let Err(e) = process_item_tx_clone.send(processed_item) {
                                    inner_progress_reporter_clone.error(&
                                  format!(
                                        "Archiver processor task thread errored sending processing item: {e:#?}"
                                    ));
                                    processor_fatal_error_flag.store(true,Ordering::SeqCst);
                                }
                            }
                            Ok(None) => {}
                            Err(e) => {
                                inner_progress_reporter_clone.error(&format!(
                                    "Archiver thread errored processing item: {e:#?}"
                                ));
                                processor_fatal_error_flag.store(true,Ordering::SeqCst);
                            }
                        }

                        Ok(())
                    });
                });
        });

        // Drop the original senders/receivers that are not used by the main thread.
        // The cloned versions are held by the spawned threads.
        drop(process_item_tx);

        // Serializer thread. This thread receives processed items and serializes tree nodes as they
        // become finalized, bottom-up.
        let repo_clone = arch.repo.clone();
        let serializer_progress_reporter_clone = arch.progress_reporter.clone();
        let serializer_snapshot_root_path_clone = arch.snapshot_options.snapshot_root_path.clone();
        let arch_clone = arch.clone();
        let serializer_fatal_error_flag = fatal_error_flag.clone();
        let tree_serializer_thread = std::thread::spawn(move || -> Option<ID> {
            let mut tree_serializer = TreeSerializer::new(
                repo_clone,
                serializer_snapshot_root_path_clone.clone(),
                &arch_clone.snapshot_options.absolute_source_paths,
            );

            while let Ok(item) = process_item_rx.recv() {
                if serializer_fatal_error_flag.load(Ordering::Relaxed) {
                    break;
                }

                // Notify reporter
                let (item_path, _) = &item;
                serializer_progress_reporter_clone.processed_node(
                    item_path
                        .strip_prefix(serializer_snapshot_root_path_clone.clone())
                        .unwrap_or(item_path),
                );

                match tree_serializer.handle_processed_item(item) {
                    Ok((raw_tree_size, encoded_tree_size)) => serializer_progress_reporter_clone
                        .written_meta_bytes(raw_tree_size, encoded_tree_size),
                    Err(e) => {
                        serializer_progress_reporter_clone.error(&format!(
                            "Archiver serializer thread errored handling processed item: {e:#}"
                        ));
                        serializer_fatal_error_flag.store(true, Ordering::SeqCst);
                    }
                }
            }

            // Now finalize the root tree.
            match tree_serializer.finalize_root() {
                Ok((raw, encoded)) => {
                    serializer_progress_reporter_clone.written_meta_bytes(raw, encoded);
                    tree_serializer.root_tree()
                }
                Err(e) => {
                    serializer_progress_reporter_clone.error(&format!(
                        "Archiver serializer thread errored finalizing root tree: {e:#}"
                    ));

                    None
                }
            }
        });

        // Join threads
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

        // Unwrap the archiver Arc to avoid cloning the contents.
        // Archiver cannot implement Debug, so unwrap is not available.
        // This match is unavoidable.
        let archiver = match Arc::try_unwrap(arch) {
            Ok(a) => a,
            // This error is never supposed to happen because at this point
            // only one copy of the Arc exists.
            Err(_) => bail!("Fatal error occurred unwrapping the Archiver pointer."),
        };

        // Flush repo and finalize pack saver
        let (flushed_raw_meta_size, flushed_encode_meta_size) = archiver.repo.flush()?;
        archiver
            .progress_reporter
            .written_meta_bytes(flushed_raw_meta_size, flushed_encode_meta_size);
        archiver.repo.finalize_pack_saver();

        let (hostname, username) = utils::get_system_info();

        match root_tree_id {
            Some(tree_id) => Ok(Snapshot {
                timestamp: Local::now(),
                parent: archiver.snapshot_options.parent_snapshot.map(|(id, _)| id),
                tree: tree_id,
                root: archiver.snapshot_options.snapshot_root_path,
                paths: archiver.snapshot_options.absolute_source_paths,
                hostname,
                username,
                tags: archiver.snapshot_options.tags,
                description: archiver.snapshot_options.description,
                summary: archiver.progress_reporter.get_summary(),
            }),
            None => Err(anyhow!(
                "Failed to finalize snapshot: No root tree ID was generated."
            )),
        }
    }
}
