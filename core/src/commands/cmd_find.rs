use anyhow::Result;
use clap::Args;
use futures::StreamExt;

use crate::{
    backend::new_backend_with_prompt,
    commands::{
        GlobalArgs, UseSnapshot, cleanup::CleanupHandler, find_use_snapshot, with_repository_lock,
    },
    fs::node::node_to_string,
    mapache::{ID, find_in_snapshot},
    repository::snapshot::{Snapshot, SnapshotStream},
    ui,
};

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

pub async fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    with_repository_lock(
        global_args.auth_file.as_ref(),
        global_args.key.as_ref(),
        new_backend_with_prompt(global_args.backend_options(false)).await?,
        global_args.to_repo_config(),
        false,
        global_args.retry_lock_duration,
        |repo, _secure_storage, lock_handle| async move {
            let cleanup_handler = CleanupHandler::new()?;
            cleanup_handler.add_lock(lock_handle.clone());

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
                ui::cli::log!("No snapshots found.");
                return Ok(());
            }

            for (id, snap) in snapshots {
                let found = find_in_snapshot(repo.clone(), &snap, &args.target).await?;
                if !found.is_empty() {
                    ui::cli::log!("Found in snapshot {}", id.to_hex());
                    for (path, node) in found {
                        ui::cli::log!("{}", node_to_string(&node, Some(&path), true, true));
                    }
                    ui::cli::log!();
                }
            }

            Ok(())
        },
    )
    .await
}
