use anyhow::Result;
use clap::Args;
use futures::StreamExt;

use crate::{
    backend::new_backend_with_prompt,
    commands::{GlobalArgs, UseSnapshot, cleanup::CleanupHandler, find_use_snapshot},
    fs::node::node_to_string,
    mapache::{ID, find_in_snapshot},
    repository::{
        repo::{RepoConfig, Repository},
        snapshot::{Snapshot, SnapshotStream},
    },
    ui,
    utils::{self, size},
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
    let auth = utils::get_auth(&global_args.auth_file)?;
    let backend = new_backend_with_prompt(global_args.backend_options(false)).await?;

    let config = RepoConfig {
        pack_size: (global_args.pack_size_mib * size::MiB as f32) as u64,
        use_cache: !global_args.no_cache,
        compression: global_args.compression_level,
    };
    let (repo, _, mut lock_handle) = Repository::try_open_with_lock(
        auth.as_ref(),
        global_args.key.as_ref(),
        backend,
        config,
        false,
        global_args.retry_lock_duration,
    )
    .await?;

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
        lock_handle.unlock().await;
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

    lock_handle.unlock().await;

    Ok(())
}
