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

pub mod node_restorer;

use std::{
    path::{Path, PathBuf},
    sync::{Arc, atomic::AtomicBool},
};

use anyhow::{Result, anyhow, bail};
use clap::ValueEnum;

use crate::{
    repository::{
        RepositoryBackend,
        snapshot::Snapshot,
        streamers::{SerializedNodeStreamer, StreamNodeInfo},
    },
    ui::{self, restore_progress::RestoreProgressReporter},
};

#[derive(Debug, Clone, PartialEq, ValueEnum)]
pub enum Resolution {
    Skip,
    Overwrite,
    Fail,
}

pub struct Restorer {}

impl Restorer {
    pub fn restore(
        repo: Arc<dyn RepositoryBackend>,
        snapshot: &Snapshot,
        resolution: &Resolution,
        target_path: &Path,
        progress_reporter: Arc<RestoreProgressReporter>,
    ) -> Result<()> {
        let num_threads = std::cmp::max(1, num_cpus::get());
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build()?;

        let (tx, rx) = crossbeam_channel::bounded::<StreamNodeInfo>(num_threads);

        let error_flag = Arc::new(AtomicBool::new(false));

        let tree = snapshot.tree.clone();
        let error_flag_clone = error_flag.clone();
        let repo_clone = repo.clone();
        let streamer_thread = std::thread::spawn(move || -> Result<()> {
            let node_streamer =
                SerializedNodeStreamer::new(repo_clone, Some(tree), PathBuf::new())?;

            for node_res in node_streamer {
                if error_flag_clone.load(std::sync::atomic::Ordering::Acquire) {
                    break;
                }

                match node_res {
                    Ok(stream_node) => tx.send(stream_node)?,
                    Err(e) => {
                        error_flag_clone.fetch_and(true, std::sync::atomic::Ordering::AcqRel);
                        bail!("Failed to iterate snapshot tree: {}", e.to_string());
                    }
                }
            }

            Ok(())
        });

        let resolution_clone = resolution.clone();
        let target_path_clone = target_path.to_path_buf();
        let repo_clone = repo.clone();
        let restorer_thread = std::thread::spawn(move || -> Result<()> {
            while let Ok((path, stream_node)) = rx.recv() {
                // If an error has already occurred, stop processing new tasks
                // and break out of the loop.
                if error_flag.load(std::sync::atomic::Ordering::Acquire) {
                    break;
                }

                let error_flag_clone = error_flag.clone();
                let progress_reporter_clone = progress_reporter.clone();
                let repo_clone_internal = repo_clone.clone();
                let target_path_clone_internal = target_path_clone.to_path_buf();
                let resolution_clone_internal = resolution_clone.clone();

                pool.spawn(move || {
                    let restore_path = target_path_clone_internal.join(&path);

                    if restore_path.exists() {
                        match resolution_clone_internal {
                            Resolution::Skip => {
                                progress_reporter_clone.processed_file(restore_path);
                                if stream_node.node.is_file() {
                                    progress_reporter_clone
                                        .processed_bytes(stream_node.node.metadata.size);
                                }
                                return;
                            }
                            Resolution::Overwrite => { /* Continue */ }
                            Resolution::Fail => {
                                ui::cli::log_error(&format!(
                                    "Target \'{}\' already exists",
                                    restore_path.display()
                                ));

                                error_flag_clone.store(true, std::sync::atomic::Ordering::SeqCst);
                                return;
                            }
                        }
                    }

                    // Attempt to restore the node.
                    if let Err(e) = node_restorer::restore_node_to_path(
                        repo_clone_internal.as_ref(),
                        &stream_node.node,
                        &restore_path,
                    ) {
                        ui::cli::log_error(&format!(
                            "Failed to restore item \'{}\': {}",
                            restore_path.display(),
                            e
                        ));
                        error_flag_clone.store(true, std::sync::atomic::Ordering::SeqCst);
                    } else {
                        progress_reporter_clone.processed_file(restore_path);
                        if stream_node.node.is_file() {
                            progress_reporter_clone.processed_bytes(stream_node.node.metadata.size);
                        }
                    }
                });
            }

            if error_flag.load(std::sync::atomic::Ordering::SeqCst) {
                bail!("One or more errors occurred during the restore process.");
            }

            Ok(())
        });

        streamer_thread
            .join()
            .map_err(|e| anyhow!("Streamer thread panicked: {:?}", e))??;

        restorer_thread
            .join()
            .map_err(|e| anyhow!("Restorer thread panicked: {:?}", e))??;

        Ok(())
    }
}
