use std::{io, path::PathBuf};

use clap::Args;
use futures::StreamExt;
use serde::Serialize;

use crate::{
    backend::new_backend_with_prompt,
    commands::{
        GlobalArgs, ToExitCode, UseSnapshot, cleanup::CleanupHandler, find_use_snapshot,
        with_repository_lock,
    },
    common::{ID, error::MapacheError, find_in_snapshot},
    fs::node::{Node, node_to_string},
    repository::snapshot::{Snapshot, SnapshotStream},
    ui,
};

#[derive(Debug, thiserror::Error)]
pub enum FindError {
    #[error("failed to open repository: {0}")]
    RepoOpenFail(String),
    #[error("search failed: {0}")]
    FindFailed(String),
    #[error(transparent)]
    Repo(#[from] MapacheError),
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl ToExitCode for FindError {
    fn to_exit_code(&self) -> i32 {
        match self {
            FindError::RepoOpenFail(_) => 10,
            FindError::FindFailed(_) => 20,
            FindError::Repo(_) => 1,
            FindError::Io(_) => 1,
        }
    }
}

#[derive(Args, Debug)]
#[clap(about = "Find files and directories in the repository")]
pub struct CmdArgs {
    /// Target
    #[arg()]
    pub target: String,

    /// Snapshot ID to search in. If omitted, searches in ALL snapshots.
    /// Use 'latest' for the most recent one.
    #[clap(long, value_parser = clap::value_parser!(UseSnapshot))]
    pub snapshot: Option<UseSnapshot>,
}

#[derive(Serialize)]
struct FindEntry {
    snapshot_id: String,
    path: PathBuf,
    node: Node,
}

#[derive(Serialize)]
struct FindOutput {
    entries: Vec<FindEntry>,
}

pub async fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<(), FindError> {
    with_repository_lock(
        global_args.auth_file.as_ref(),
        global_args.key.as_ref(),
        new_backend_with_prompt(global_args.backend_options(false)).await?,
        global_args.to_repo_config(),
        false,
        global_args.retry_lock_duration,
        global_args.no_lock,
        |repo, _secure_storage, lock_handle| async move {
            let cleanup_handler = CleanupHandler::new();
            cleanup_handler.add_lock(lock_handle);

            repo.reload_master_index().await?;

            let snapshots: Vec<(ID, Snapshot)> = if let Some(use_snap) = &args.snapshot {
                find_use_snapshot(repo.clone(), use_snap)
                    .await?
                    .into_iter()
                    .collect()
            } else {
                let mut snapshot_stream = SnapshotStream::new(repo.clone()).await?;
                let mut snaps = Vec::new();
                while let Some(res) = snapshot_stream.next().await {
                    snaps.push(res?);
                }
                snaps
            };

            if snapshots.is_empty() {
                if global_args.json {
                    ui::json::emit_static(
                        "find",
                        &FindOutput {
                            entries: Vec::new(),
                        },
                    );
                } else {
                    ui::cli::log!("No snapshots found.");
                }
                return Ok(());
            }

            if global_args.json {
                let mut entries = Vec::new();
                for (id, snap) in snapshots {
                    let found = find_in_snapshot(repo.clone(), &snap, &args.target)
                        .await
                        .map_err(|e| FindError::FindFailed(format!("Search failed: {e}")))?;
                    for (path, node) in found {
                        entries.push(FindEntry {
                            snapshot_id: id.to_hex(),
                            path,
                            node,
                        });
                    }
                }
                ui::json::emit_static("find", &FindOutput { entries });
            } else {
                for (id, snap) in snapshots {
                    let found = find_in_snapshot(repo.clone(), &snap, &args.target)
                        .await
                        .map_err(|e| FindError::FindFailed(format!("Search failed: {e}")))?;
                    if !found.is_empty() {
                        ui::cli::log!("Found in snapshot {}", id.to_hex());
                        for (path, node) in found {
                            ui::cli::log!("{}", node_to_string(&node, Some(&path), true, true));
                        }
                        ui::cli::log!();
                    }
                }
            }

            Ok(())
        },
    )
    .await
}
