use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use anyhow::{Context, Result, bail};
use futures::StreamExt;

use crate::{
    fs::tree::SerializedTreeStream, mapache::ID, repository::repo::Repository,
    ui::RestoreProgressReporter,
};

/// Delete all local nodes not present in a snapshot tree.
#[allow(clippy::too_many_arguments)]
pub async fn delete_nodes(
    repo: Arc<Repository>,
    target_path: PathBuf,
    root_tree_id: &ID,
    include: Option<Vec<PathBuf>>,
    exclude: Option<Vec<PathBuf>>,
    dry_run: bool,
    no_preserve_root: bool,
    shutdown_signal: Arc<AtomicBool>,
    reporter: Arc<dyn RestoreProgressReporter>,
) -> Result<()> {
    let mut tree_stream =
        SerializedTreeStream::new(repo, root_tree_id, PathBuf::new(), include.clone(), exclude)
            .await
            .with_context(|| {
                format!("Failed to initialize snapshot tree stream for root ID {root_tree_id:?}")
            })?;

    if !no_preserve_root {
        let _ = tree_stream.next().await;
    }

    while let Some(item_result) = tree_stream.next().await {
        if shutdown_signal.load(Ordering::Acquire) {
            bail!("Interrupted");
        }

        let (path, snapshot_tree) = match item_result {
            Ok(data) => data,
            Err(e) => {
                reporter.warning(&format!("Could not read snapshot subtree entry: {e}"));
                tracing::warn!(target: "restorer", "Could not read snapshot subtree entry: {e}");
                continue;
            }
        };

        if !path_is_below_includes(&path, include.as_ref()) {
            continue;
        }

        tracing::debug!(target: "restorer", "Syncing local directory {:?} with snapshot tree", path);

        let snapshot_node_names: HashSet<&str> = snapshot_tree
            .nodes
            .iter()
            .map(|node| node.name.as_str())
            .collect();

        let local_dir = &target_path.join(path);
        process_local_directory(local_dir, &snapshot_node_names, dry_run, reporter.clone())?
    }

    tracing::info!(target: "restorer", "Sync deletion finished");
    Ok(())
}

/// Helper function to process a single local directory to sync.
fn process_local_directory(
    local_dir_path: &Path,
    snapshot_node_names: &HashSet<&str>,
    dry_run: bool,
    reporter: Arc<dyn RestoreProgressReporter>,
) -> Result<()> {
    let local_readdir = match local_dir_path.read_dir() {
        Ok(readdir) => readdir,
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                reporter.verbose_1(format!(
                    "Local directory '{}' not found, skipping.",
                    local_dir_path.display()
                ));
                return Ok(());
            } else {
                return Err(e).with_context(|| {
                    format!(
                        "Could not read local directory '{}'",
                        local_dir_path.display()
                    )
                });
            }
        }
    };

    for node_res in local_readdir {
        let dir_entry = match node_res {
            Ok(entry) => entry,
            Err(e) => {
                reporter.warning(&format!(
                    "Failed to read local node in '{}': {e}",
                    local_dir_path.display()
                ));
                continue;
            }
        };

        let local_name = dir_entry.file_name();
        let local_name_str = local_name.to_string_lossy();
        let local_path = dir_entry.path();

        if !snapshot_node_names.contains(local_name_str.as_ref()) {
            perform_deletion(&local_path, dry_run, reporter.clone())?;
        }
    }

    Ok(())
}

/// Helper function to perform the actual file/directory deletion or log dry run.
fn perform_deletion(
    path_to_delete: &Path,
    dry_run: bool,
    reporter: Arc<dyn RestoreProgressReporter>,
) -> Result<()> {
    if dry_run {
        reporter.log(format!(
            "[DRY RUN] Would delete '{}'",
            path_to_delete.display()
        ));
        tracing::debug!(target: "restorer", "Dry run: would delete {:?}", path_to_delete);
    } else if path_to_delete.is_dir() {
        reporter.verbose_1(format!("Deleted {path_to_delete:?}"));
        tracing::debug!(target: "restorer", "Deleting directory {:?}", path_to_delete);
        std::fs::remove_dir_all(path_to_delete).with_context(|| {
            format!(
                "Failed to delete local directory '{}'",
                path_to_delete.display()
            )
        })?;
    } else {
        reporter.verbose_1(format!("Deleted {path_to_delete:?}"));
        tracing::debug!(target: "restorer", "Deleting file {:?}", path_to_delete);
        std::fs::remove_file(path_to_delete)
            .with_context(|| format!("Failed to delete local file {path_to_delete:?}"))?;
    }

    Ok(())
}

/// Returns true if a path is contained by any of the include paths.
fn path_is_below_includes(path: &Path, include: Option<&Vec<PathBuf>>) -> bool {
    let Some(includes) = include else {
        return true;
    };

    for ipath in includes {
        if path.starts_with(ipath) {
            return true;
        }
    }

    false
}
