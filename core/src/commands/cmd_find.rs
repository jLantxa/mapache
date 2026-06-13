use std::path::PathBuf;

use anyhow::Result;
use clap::Args;
use futures::StreamExt;
use serde::Serialize;

use crate::{
    backend::new_backend_with_prompt,
    commands::{
        GlobalArgs, ToExitCode, UseSnapshot, cleanup::CleanupHandler, fail, find_use_snapshot,
        with_repository_lock,
    },
    fs::node::{Node, node_to_string},
    mapache::{ID, find_in_snapshot},
    repository::snapshot::{Snapshot, SnapshotStream},
    ui,
};

#[derive(Debug, Clone, Copy)]
pub enum FindError {
    RepoOpenFail = 10,
    FindFailed = 20,
}

impl ToExitCode for FindError {
    fn to_exit_code(&self) -> i32 {
        *self as i32
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

pub async fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    with_repository_lock(
        global_args.auth_file.as_ref(),
        global_args.key.as_ref(),
        new_backend_with_prompt(global_args.backend_options(false))
            .await
            .map_err(|e| {
                fail(
                    format!("Failed to initialize backend: {}", e),
                    FindError::FindFailed,
                )
            })?,
        global_args.to_repo_config(),
        false,
        global_args.retry_lock_duration,
        global_args.no_lock,
        |repo, _secure_storage, lock_handle| async move {
            let cleanup_handler = CleanupHandler::new().map_err(|e| {
                fail(
                    format!("Failed to initialize cleanup handler: {}", e),
                    FindError::FindFailed,
                )
            })?;
            cleanup_handler.add_lock(lock_handle.clone());

            repo.reload_master_index().await.map_err(|e| {
                fail(
                    format!("Failed to reload master index: {}", e),
                    FindError::FindFailed,
                )
            })?;

            let snapshots: Vec<(ID, Snapshot)> = if let Some(use_snap) = &args.snapshot {
                find_use_snapshot(repo.clone(), use_snap)
                    .await
                    .map_err(|e| {
                        fail(
                            format!("Failed to find snapshot: {}", e),
                            FindError::FindFailed,
                        )
                    })?
                    .into_iter()
                    .collect()
            } else {
                let mut snapshot_stream = SnapshotStream::new(repo.clone()).await.map_err(|e| {
                    fail(
                        format!("Failed to open snapshot stream: {}", e),
                        FindError::FindFailed,
                    )
                })?;
                let mut snaps = Vec::new();
                while let Some(res) = snapshot_stream.next().await {
                    snaps.push(res.map_err(|e| {
                        fail(
                            format!("Failed to read snapshot: {}", e),
                            FindError::FindFailed,
                        )
                    })?);
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
                        .map_err(|e| {
                            fail(format!("Search failed: {}", e), FindError::FindFailed)
                        })?;
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
                        .map_err(|e| {
                            fail(format!("Search failed: {}", e), FindError::FindFailed)
                        })?;
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
    .map_err(|e| {
        if e.is::<crate::commands::error::MapacheError>() {
            e
        } else {
            fail(
                format!("Failed to open repository: {}", e),
                FindError::RepoOpenFail,
            )
        }
    })
}
