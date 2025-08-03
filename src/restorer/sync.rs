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

use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result};
use colored::Colorize;

use crate::{fs::tree::SerializedTreeStreamer, global::ID, repository::repo::Repository, ui};

/// Delete all local nodes not present in a snapshot tree
pub fn delete_nodes(
    repo: Arc<Repository>,
    target_path: PathBuf,
    root_tree_id: &ID,
    include: Option<Vec<PathBuf>>,
    exclude: Option<Vec<PathBuf>>,
    dry_run: bool,
) -> Result<()> {
    ui::cli::log!("Starting sync delete for '{}'", target_path.display());

    let tree_streamer =
        SerializedTreeStreamer::new(repo, root_tree_id, target_path.clone(), include, exclude)
            .with_context(|| {
                format!("Failed to initialize snapshot tree streamer for root ID {root_tree_id:?}")
            })?;

    // NOTE:
    // It is crucial that the first item (the root tree) is skipped. The root path was never a part
    // of the snapshot, only the selected nodes that were explicitly added. We MUST skip the root
    // directory or there will be catastrophic consequences, i.e., deleting all other nodes in
    // the root path that were never backed up.
    for item_result in tree_streamer.skip(1) {
        // Handle potential errors from the streamer itself.
        // If an error occurs, log a warning and skip to the next item,
        // rather than bailing out entirely, which seems to be the intended behavior.
        let (dir_path, snapshot_tree) = match item_result {
            Ok(data) => data,
            Err(e) => {
                ui::cli::warning!("Could not read snapshot subtree entry: {e}");
                continue; // Skip to the next item in the stream
            }
        };

        // Pre-process snapshot tree nodes into a HashSet for fast lookups
        let snapshot_node_names: HashSet<&str> = snapshot_tree
            .nodes
            .iter()
            .map(|node| node.name.as_str())
            .collect();

        // Delegate the processing of the local directory to a helper function
        process_local_directory(&dir_path, &snapshot_node_names, dry_run)?
    }

    ui::cli::log!("Finished sync delete for '{}'\n", target_path.display());
    Ok(())
}

// Helper function to process a single local directory to sync
fn process_local_directory(
    local_dir_path: &Path,
    snapshot_node_names: &HashSet<&str>,
    dry_run: bool,
) -> Result<()> {
    let local_readdir = match local_dir_path.read_dir() {
        Ok(readdir) => readdir,
        Err(e) => {
            // If the directory does not exist, there's nothing to delete in it.
            if e.kind() == std::io::ErrorKind::NotFound {
                ui::cli::verbose_1!(
                    "Local directory '{}' not found, skipping.",
                    local_dir_path.display()
                );
                return Ok(());
            } else {
                // For other errors (e.g., permission denied), propagate the error.
                return Err(e).with_context(|| {
                    format!(
                        "Could not read local directory '{}'",
                        local_dir_path.display()
                    )
                });
            }
        }
    };

    // The rest of the function remains the same, as it only executes if read_dir was successful
    for node_res in local_readdir {
        let dir_entry = match node_res {
            Ok(entry) => entry,
            Err(e) => {
                ui::cli::warning!(
                    "Failed to read local node in '{}': {e}",
                    local_dir_path.display()
                );
                continue;
            }
        };

        let local_name = dir_entry.file_name();
        let local_name_str = local_name.to_string_lossy();
        let local_path = dir_entry.path();

        if !snapshot_node_names.contains(local_name_str.as_ref()) {
            perform_deletion(&local_path, dry_run)?;
        }
    }

    Ok(())
}

// Helper function to perform the actual file/directory deletion or log dry run
fn perform_deletion(path_to_delete: &Path, dry_run: bool) -> Result<()> {
    if dry_run {
        ui::cli::log!(
            "{} Would delete '{}'",
            "[DRY RUN]".bold().purple(),
            path_to_delete.display()
        );
    } else if path_to_delete.is_dir() {
        ui::cli::verbose_1!("Deleting directory '{}'", path_to_delete.display(),);
        std::fs::remove_dir_all(path_to_delete).with_context(|| {
            format!(
                "Failed to delete local directory '{}'",
                path_to_delete.display()
            )
        })?;
    } else {
        ui::cli::verbose_1!("Deleting file '{}'", path_to_delete.display(),);
        std::fs::remove_file(path_to_delete).with_context(|| {
            format!("Failed to delete local file '{}'", path_to_delete.display())
        })?;
    }

    Ok(())
}
