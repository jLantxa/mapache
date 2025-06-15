// [backup] is an incremental backup tool
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

pub struct Archiver {
    repo: Arc<dyn RepositoryBackend>,
    absolute_source_paths: Vec<PathBuf>,
    snapshot_root_path: PathBuf,
    exclude_paths: Vec<PathBuf>,
    parent_snapshot: Option<Snapshot>,
    read_concurrency: usize,
    write_concurrency: usize,
    progress_reporter: Arc<SnapshotProgressReporter>,
}

impl Archiver {
    pub fn new(
        repo: Arc<dyn RepositoryBackend>,
        absolute_source_paths: Vec<PathBuf>,
        snapshot_root_path: PathBuf,
        exclude_paths: Vec<PathBuf>,
        parent_snapshot: Option<Snapshot>,
        (read_concurrency, write_concurrency): (usize, usize),
        progress_reporter: Arc<SnapshotProgressReporter>,
    ) -> Self {
        Self {
            repo,
            absolute_source_paths,
            snapshot_root_path,
            exclude_paths,
            parent_snapshot,
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
        let parent_tree_id: Option<ID> = match &arch.parent_snapshot {
            None => None,
            Some(snapshot) => Some(snapshot.tree.clone()),
        };

        // Create streamers
        let fs_streamer = match FSNodeStreamer::from_paths(
            arch.absolute_source_paths.clone(),
            arch.exclude_paths.clone(),
        ) {
            Ok(stream) => stream,
            Err(e) => bail!("Failed to create FSNodeStreamer: {:?}", e.to_string()),
        };
        let previous_tree_streamer = SerializedNodeStreamer::new(
            arch.repo.clone(),
            parent_tree_id,
            arch.snapshot_root_path.clone(),
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
        )>(arch.read_concurrency * 2);
        let (process_item_tx, process_item_rx) =
            crossbeam_channel::bounded::<(PathBuf, StreamNode)>(arch.read_concurrency * 2);

        let error_flag = Arc::new(AtomicBool::new(false));

        // Diff thread. This thread iterates the NodeDiffStreamer and passes the
        // items to the item processor thread.
        let error_flag_clone = error_flag.clone();
        let diff_thread = std::thread::spawn(move || {
            let diff_streamer = NodeDiffStreamer::new(previous_tree_streamer, fs_streamer);

            for diff_result in diff_streamer {
                if error_flag_clone.load(std::sync::atomic::Ordering::Acquire) {
                    break;
                }

                if let Ok(diff) = diff_result {
                    if let Err(e) = diff_tx.send(diff) {
                        error_flag_clone.store(true, std::sync::atomic::Ordering::Release);
                        ui::cli::error!(
                            "Archiver diff thread errored sending diff: {:?}",
                            e.to_string()
                        );
                        break;
                    }
                } else {
                    error_flag_clone.store(true, std::sync::atomic::Ordering::Release);
                    ui::cli::error!("Archiver diff thread errored getting next diff");
                    break;
                }
            }

            drop(diff_tx); // Signal that no more diffs will be sent
        });

        // Processor thread using Rayon for parallel processing with controlled concurrency.
        // These threads receive diffs and process them, chunking and saving files in the process.
        // The resulting processed nodes are passed to the serializer thread.
        let diff_rx_clone = diff_rx.clone();
        let process_item_tx_clone = process_item_tx.clone();
        let repo_clone = arch.repo.clone();
        let error_flag_clone_for_processor = error_flag.clone();
        let processor_progress_reporter_clone = arch.progress_reporter.clone();
        let snapshot_root_path_clone = arch.snapshot_root_path.clone();
        let read_concurrency = arch.read_concurrency;

        let processor_thread = std::thread::spawn(move || {
            // Create a dedicated Rayon thread pool for read concurrency
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(read_concurrency)
                .build()
                .expect("Failed to build Rayon thread pool for processing.");

            // Run processing within the dedicated pool
            pool.scope(|s| {
                for diff_tuple in diff_rx_clone.iter() {
                    if error_flag_clone_for_processor.load(std::sync::atomic::Ordering::Acquire) {
                        break;
                    }

                    let inner_process_item_tx_clone = process_item_tx_clone.clone();
                    let inner_repo_clone = repo_clone.clone();
                    let inner_error_flag_clone = error_flag_clone_for_processor.clone();
                    let inner_progress_reporter_clone = processor_progress_reporter_clone.clone();
                    let inner_snapshot_root_path_clone = snapshot_root_path_clone.clone();


                    s.spawn(move |_| {
                        // Notify reporter
                        let (item_path, _, _, _) = &diff_tuple;
                        inner_progress_reporter_clone.processing_file(
                            item_path
                                .strip_prefix(inner_snapshot_root_path_clone.clone())
                                .unwrap()
                                .to_path_buf(),
                        );

                        let processed_item_result = processor::process_item(
                            diff_tuple,
                            inner_repo_clone,
                            inner_progress_reporter_clone,
                        );

                        match processed_item_result {
                            Ok(processed_item_opt) => {
                                if let Some(processed_item) = processed_item_opt {
                                    if let Err(e) = inner_process_item_tx_clone.send(processed_item) {
                                        inner_error_flag_clone.store(true, Ordering::Release);
                                        ui::cli::error!(
                                            "Archiver processor task errored sending processed item: {:?}",
                                            e.to_string()
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                inner_error_flag_clone.store(true, Ordering::Release);
                                ui::cli::error!(
                                    "Archiver processor task errored processing item: {:?}",
                                    e.to_string()
                                );
                            }
                        }
                    });
                }
            }); // `pool.scope` blocks until all spawned tasks complete

            drop(process_item_tx_clone); // Signal serializer that no more items will be sent
        });

        // Drop the original senders/receivers that are not used by the main thread.
        // The cloned versions are held by the spawned threads.
        drop(diff_rx);
        drop(process_item_tx);

        // Serializer thread. This thread receives processed items and serializes tree nodes as they
        // become finalized, bottom-up.
        let error_flag_clone = error_flag.clone();
        let repo_clone = arch.repo.clone();
        let serializer_progress_reporter_clone = arch.progress_reporter.clone();
        let serializer_snapshot_root_path_clone = arch.snapshot_root_path.clone();
        let arch_clone = arch.clone();
        let tree_serializer_thread = std::thread::spawn(move || {
            let mut final_root_tree_id: Option<ID> = None;
            let mut pending_trees = tree_serializer::init_pending_trees(
                &serializer_snapshot_root_path_clone,
                &arch.absolute_source_paths,
            );

            for item in process_item_rx.iter() {
                if error_flag_clone.load(std::sync::atomic::Ordering::Acquire) {
                    break;
                }

                // Notify reporter
                let (item_path, _) = &item;
                serializer_progress_reporter_clone.processed_file(
                    item_path
                        .clone()
                        .strip_prefix(serializer_snapshot_root_path_clone.clone())
                        .unwrap()
                        .to_path_buf(),
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

            match final_root_tree_id {
                Some(tree_id) => {
                    let (index_raw_data, index_encoded_data) = arch_clone.repo.flush()?;
                    arch_clone
                        .progress_reporter
                        .written_meta_bytes(index_raw_data, index_encoded_data);

                    arch_clone.repo.finalize_pack_saver();

                    Ok(Snapshot {
                        timestamp: Local::now(),
                        tree: tree_id.clone(),
                        root: arch_clone.snapshot_root_path.clone(),
                        paths: arch_clone.absolute_source_paths.clone(),
                        description: None,
                        summary: arch_clone.progress_reporter.get_summary(),
                    })
                }
                None => {
                    if error_flag_clone.load(Ordering::Acquire) {
                        Err(anyhow!("Snapshot creation failed due to a previous error."))
                    } else {
                        Err(anyhow!(
                            "Failed to finalize snapshot: No root tree ID was generated."
                        ))
                    }
                }
            }
        });

        // Join threads
        let _ = diff_thread.join();
        let _ = processor_thread.join();
        tree_serializer_thread.join().unwrap()
    }
}

#[cfg(test)]
mod tests {}
