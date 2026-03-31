pub(crate) mod node_restorer;
pub(crate) mod sync;

use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use anyhow::{Context, Result, bail};
use clap::ValueEnum;
use futures::StreamExt;

use crate::{
    fs::{self, tree::SerializedNodeStream},
    repository::{repo::Repository, snapshot::Snapshot},
    ui::restore_progress::RestoreProgressReporter,
};

#[derive(Debug, Clone, PartialEq, ValueEnum)]
pub enum Strategy {
    Fail,
    Overwrite,
    Skip,
    Newer,
}

pub struct RestoreOptions {
    pub strategy: Strategy,
    pub strip_prefix: Option<PathBuf>,
    pub dry_run: bool,
    pub quit_on_error: bool,
}

#[allow(clippy::too_many_arguments)]
pub async fn restore(
    repo: Arc<Repository>,
    snapshot: &Snapshot,
    target_path: &Path,
    include: Option<Vec<PathBuf>>,
    exclude: Option<Vec<PathBuf>>,
    opts: RestoreOptions,
    progress_reporter: Arc<RestoreProgressReporter>,
    shutdown_signal: Arc<AtomicBool>,
) -> Result<()> {
    let tree = snapshot.tree;
    let mut node_stream =
        SerializedNodeStream::new(repo.clone(), Some(tree), PathBuf::new(), include, exclude)
            .await?;

    // Create the restore target directory
    if !opts.dry_run {
        std::fs::create_dir_all(target_path)?;
    }

    // Directories to restore metadata later
    let mut restored_dirs = Vec::new();

    while let Some(node_res) = node_stream.next().await {
        if shutdown_signal.load(Ordering::Relaxed) {
            bail!("Interrupted");
        }

        let (mut path, stream_node_res) = node_res?;
        let stream_node = stream_node_res?;
        let node = &stream_node.node;

        if let Some(prefix) = &opts.strip_prefix {
            path = match path.strip_prefix(prefix) {
                Ok(stripped_path) => {
                    if stripped_path.as_os_str().is_empty() {
                        continue;
                    }
                    stripped_path.to_path_buf()
                }
                Err(_) => {
                    continue;
                }
            };
        }

        let restore_path = target_path.join(&path);
        progress_reporter.processing_node(&path);

        let mut should_restore = true;
        if fs::path_exists(&restore_path).await {
            match opts.strategy {
                Strategy::Overwrite => (),
                Strategy::Skip => {
                    should_restore = false;
                }
                Strategy::Newer => {
                    let local_metadata =
                        std::fs::symlink_metadata(&restore_path).with_context(|| {
                            format!(
                                "Failed to get metadata for local file {}",
                                restore_path.display()
                            )
                        })?;

                    if let Some(repo_mtime) = node.metadata.modified_time {
                        let local_mtime = local_metadata.modified().with_context(|| {
                            format!(
                                "Failed to get modified time for local file {}",
                                restore_path.display()
                            )
                        })?;

                        if local_mtime >= repo_mtime {
                            should_restore = false;
                        }
                    }
                }
                Strategy::Fail => {
                    if !node.is_dir() || !restored_dirs.iter().any(|(p, _)| p == &restore_path) {
                        bail!("Target {} exists already", restore_path.display());
                    }
                }
            }
        }

        if !should_restore {
            progress_reporter.processed_item(&path);
            if node.is_file() {
                progress_reporter.processed_bytes(node.metadata.size);
            }
            continue;
        }

        if node.is_dir() {
            restored_dirs.push((restore_path.clone(), node.clone()));
        }

        if let Err(e) = node_restorer::restore_node_to_path(
            repo.as_ref(),
            progress_reporter.clone(),
            node,
            &restore_path,
            opts.dry_run,
        )
        .await
        {
            let error_msg = e.to_string();
            if opts.quit_on_error {
                bail!(error_msg);
            }
            progress_reporter.error(&error_msg);
        }

        progress_reporter.processed_item(&path);
    }

    if !opts.dry_run {
        // Sort directories by path length descending to ensure bottom-up restoration
        restored_dirs
            .sort_unstable_by(|(a, _), (b, _)| b.as_os_str().len().cmp(&a.as_os_str().len()));

        for (dir_path, node) in restored_dirs {
            if shutdown_signal.load(Ordering::Relaxed) {
                bail!("Interrupted");
            }

            node_restorer::try_restore_node_metadata(&node, &dir_path, &progress_reporter);
        }
    }

    Ok(())
}
