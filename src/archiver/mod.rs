// mapache is an incremental backup tool
// Copyright (C) 2025  Javier Lancha Vázquez <javier.lancha@gmail.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

mod processor;
mod tree_serializer;

use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use anyhow::{Result, anyhow, bail};
use chrono::Local;
use tree_serializer::finalize_if_complete;

use crate::{
    global::ID,
    repository::{
        RepositoryBackend,
        snapshot::Snapshot,
        streamers::{
            FSNodeStreamer, NodeDiff, NodeDiffStreamer, SerializedNodeStreamer, StreamNode,
        },
    },
    ui,
    ui::snapshot_progress::SnapshotProgressReporter,
};

pub struct SnapshotOptions {
    pub absolute_source_paths: Vec<PathBuf>,
    pub snapshot_root_path: PathBuf,
    pub exclude_paths: Vec<PathBuf>,
    pub parent_snapshot: Option<Snapshot>,
    pub tags: Vec<String>,
    pub description: Option<String>,
}

pub struct Archiver {
    repo: Arc<dyn RepositoryBackend>,
    snapshot_options: SnapshotOptions,
    read_concurrency: usize,
    write_concurrency: usize,
    progress_reporter: Arc<SnapshotProgressReporter>,
}

impl Archiver {
    pub fn new(
        repo: Arc<dyn RepositoryBackend>,
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

        // Extract parent snapshot tree id
        let parent_tree_id: Option<ID> = arch
            .snapshot_options
            .parent_snapshot
            .as_ref()
            .map(|snapshot| snapshot.tree.clone());

        // Create streamers
        let fs_streamer = match FSNodeStreamer::from_paths(
            arch.snapshot_options.absolute_source_paths.clone(),
            arch.snapshot_options.exclude_paths.clone(),
        ) {
            Ok(stream) => stream,
            Err(e) => bail!("Failed to create FSNodeStreamer: {:?}", e.to_string()),
        };
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
        )>(arch.read_concurrency);
        let (process_item_tx, process_item_rx) =
            crossbeam_channel::bounded::<(PathBuf, StreamNode)>(arch.read_concurrency);

        let error_flag = Arc::new(AtomicBool::new(false));

        // Diff thread. This thread iterates the NodeDiffStreamer and passes the
        // items to the item processor thread.
        let error_flag_clone = error_flag.clone();
        let diff_thread = std::thread::spawn(move || {
            let diff_streamer = NodeDiffStreamer::new(previous_tree_streamer, fs_streamer);

            for diff_result in diff_streamer {
                if error_flag_clone.load(Ordering::Acquire) {
                    break;
                }

                if let Ok((path, prev, next, diff)) = diff_result {
                    if let Err(e) = diff_tx.send((path, prev, next, diff)) {
                        error_flag_clone.store(true, Ordering::Release);
                        ui::cli::error!(
                            "Archiver diff thread errored sending diff: {:?}",
                            e.to_string()
                        );
                        break;
                    }
                } else {
                    ui::cli::error!("Archiver diff thread errored getting next diff");
                    break;
                }
            }
        });

        // Item processor thread pool. These threads receive diffs and process them, chunking and
        // saving files in the process. The resulting processed nodes are passed to the serializer
        // thread.
        let diff_rx_clone = diff_rx.clone();
        let process_item_tx_clone = process_item_tx.clone();
        let repo_clone = arch.repo.clone();
        let error_flag_clone = error_flag.clone();
        let processor_progress_reporter_clone = arch.progress_reporter.clone();
        let snapshot_root_path_clone = arch.snapshot_options.snapshot_root_path.clone();

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(arch.read_concurrency)
            .build()
            .expect("Failed to build thread pool");

        let processor_thread = std::thread::spawn(move || {
            pool.scope(|s| {
                while let Ok((path, prev, next, diff)) = diff_rx_clone.recv() {
                    if error_flag_clone.load(Ordering::Acquire) {
                        break;
                    }

                    let inner_process_item_tx_clone = process_item_tx_clone.clone();
                    let inner_repo_clone = repo_clone.clone();
                    let inner_error_flag_clone = error_flag_clone.clone();
                    let inner_progress_reporter_clone = processor_progress_reporter_clone.clone();
                    let inner_snapshot_root_path_clone = snapshot_root_path_clone.clone();

                    s.spawn(move |_| {
                        let stripped_path = path.strip_prefix(&inner_snapshot_root_path_clone).unwrap().to_path_buf();
                        inner_progress_reporter_clone.processing_file(
                            stripped_path, diff
                        );


                        let processed_item_result = processor::process_item(
                            (path, prev, next, diff),
                            inner_repo_clone,
                            inner_progress_reporter_clone,
                        );

                        match processed_item_result {
                            Ok(Some(processed_item)) => {
                                if let Err(e) = inner_process_item_tx_clone.send(processed_item) {
                                    inner_error_flag_clone.store(true, Ordering::Release);
                                    ui::cli::error!(
                                        "Archiver processor task thread errored sending processing item: {:?}",
                                        e.to_string()
                                    );
                                }
                            }
                            Ok(None) => {}
                            Err(e) => {
                                inner_error_flag_clone.store(true, Ordering::Release);
                                ui::cli::error!(
                                    "Archiver thread errored processing item: {:?}",
                                    e.to_string()
                                );
                            }
                        }
                    });
                }
            });
        });

        // Drop the original senders/receivers that are not used by the main thread.
        // The cloned versions are held by the spawned threads.
        drop(process_item_tx);

        // Serializer thread. This thread receives processed items and serializes tree nodes as they
        // become finalized, bottom-up.
        let error_flag_clone = error_flag.clone();
        let repo_clone = arch.repo.clone();
        let serializer_progress_reporter_clone = arch.progress_reporter.clone();
        let serializer_snapshot_root_path_clone = arch.snapshot_options.snapshot_root_path.clone();
        let arch_clone = arch.clone();
        let tree_serializer_thread = std::thread::spawn(move || -> Option<ID> {
            let mut final_root_tree_id: Option<ID> = None;
            let mut pending_trees = tree_serializer::init_pending_trees(
                &serializer_snapshot_root_path_clone,
                &arch_clone.snapshot_options.absolute_source_paths,
            );

            while let Ok(item) = process_item_rx.recv() {
                if error_flag_clone.load(Ordering::Acquire) {
                    break;
                }

                // Notify reporter
                let (item_path, _) = &item;
                serializer_progress_reporter_clone.processed_file(
                    item_path
                        .strip_prefix(serializer_snapshot_root_path_clone.clone())
                        .unwrap(),
                );

                if let Err(e) = tree_serializer::handle_processed_item(
                    item,
                    repo_clone.as_ref(),
                    &mut pending_trees,
                    &mut final_root_tree_id,
                    &serializer_snapshot_root_path_clone,
                    &serializer_progress_reporter_clone,
                ) {
                    error_flag_clone.store(true, Ordering::Release);
                    ui::cli::error!(
                        "Archiver serializer thread errored handling processed item: {:?}",
                        e.to_string()
                    );
                    break;
                }
            }

            // After the loop, if no error occurred, finalize the root tree.
            if !error_flag_clone.load(Ordering::Acquire) {
                if let Err(e) = finalize_if_complete(
                    serializer_snapshot_root_path_clone.clone(),
                    repo_clone.as_ref(),
                    &mut pending_trees,
                    &mut final_root_tree_id,
                    &serializer_snapshot_root_path_clone,
                    &serializer_progress_reporter_clone,
                ) {
                    error_flag_clone.store(true, Ordering::Release);
                    ui::cli::error!(
                        "Archiver serializer thread errored finalizing root tree: {:?}",
                        e.to_string()
                    );
                }
            }

            final_root_tree_id
        });

        // Join threads
        let _ = diff_thread.join();
        let _ = processor_thread.join();
        let root_tree_id = tree_serializer_thread.join().unwrap();

        let arch = match Arc::try_unwrap(arch) {
            Ok(a) => a,
            Err(_) => bail!("Cosa"),
        };

        // Flush repo and finalize pack saver
        let (index_raw_data, index_encoded_data) = arch.repo.flush()?;
        arch.progress_reporter
            .written_meta_bytes(index_raw_data, index_encoded_data);
        arch.repo.finalize_pack_saver();

        match root_tree_id {
            Some(tree_id) => Ok(Snapshot {
                timestamp: Local::now(),
                tree: tree_id,
                root: arch.snapshot_options.snapshot_root_path,
                paths: arch.snapshot_options.absolute_source_paths,
                tags: arch.snapshot_options.tags,
                description: arch.snapshot_options.description,
                summary: arch.progress_reporter.get_summary(),
            }),
            None => {
                if error_flag.load(Ordering::Acquire) {
                    Err(anyhow!("Snapshot creation failed due to a previous error."))
                } else {
                    Err(anyhow!(
                        "Failed to finalize snapshot: No root tree ID was generated."
                    ))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {}
