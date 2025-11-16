use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result};
use colored::Colorize;

use crate::{fs::tree::SerializedTreeStreamer, mapache::ID, repository::repo::Repository, ui};

/// Delete all local nodes not present in a snapshot tree
pub fn delete_nodes(
    repo: Arc<Repository>,
    target_path: PathBuf,
    root_tree_id: &ID,
    include: Option<Vec<PathBuf>>,
    exclude: Option<Vec<PathBuf>>,
    dry_run: bool,
) -> Result<()> {
    let tree_streamer =
        SerializedTreeStreamer::new(repo, root_tree_id, PathBuf::new(), include.clone(), exclude)
            .with_context(|| {
            format!("Failed to initialize snapshot tree stream for root ID {root_tree_id:?}")
        })?;

    for item_result in tree_streamer {
        // Handle potential errors from the stream itself.
        // If an error occurs, log a warning and skip to the next item,
        // rather than bailing out entirely, which seems to be the intended behavior.
        let (path, snapshot_tree) = match item_result {
            Ok(data) => data,
            Err(e) => {
                ui::cli::warning!("Could not read snapshot subtree entry: {e}");
                continue; // Skip to the next item in the stream
            }
        };

        // Only delete nodes within the include paths
        // The intermediate tree nodes are emitted so the children can be reached.
        // This doesn't mean they must be considered.
        if !path_is_below_includes(&path, include.as_ref()) {
            continue;
        }

        // Pre-process snapshot tree nodes into a HashSet for fast lookups
        let snapshot_node_names: HashSet<&str> = snapshot_tree
            .nodes
            .iter()
            .map(|node| node.name.as_str())
            .collect();

        // Delegate the processing of the local directory to a helper function
        let local_dir = &target_path.join(path);
        process_local_directory(local_dir, &snapshot_node_names, dry_run)?
    }

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
        ui::cli::verbose_1!("Deleted {path_to_delete:?}");
        std::fs::remove_dir_all(path_to_delete).with_context(|| {
            format!(
                "Failed to delete local directory '{}'",
                path_to_delete.display()
            )
        })?;
    } else {
        ui::cli::verbose_1!("Deleted {path_to_delete:?}");
        std::fs::remove_file(path_to_delete)
            .with_context(|| format!("Failed to delete local file {path_to_delete:?}"))?;
    }

    Ok(())
}

/// Returns true if a path is contained by the include paths
fn path_is_below_includes(path: &Path, include: Option<&Vec<PathBuf>>) -> bool {
    if include.is_none() {
        return true;
    }

    for ipath in include.unwrap() {
        if path.starts_with(ipath) {
            return true;
        }
    }

    false
}
