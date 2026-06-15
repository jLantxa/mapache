use std::{io::Write, path::PathBuf};

use anyhow::{Context, Result};
use clap::Args;

use crate::{
    backend::new_backend_with_prompt,
    commands::{
        GlobalArgs, ToExitCode, UseSnapshot, cleanup::CleanupHandler, fail, find_use_snapshot,
        with_repository_lock,
    },
    fs::tree::find_serialized_node,
};

#[derive(Debug, Clone, Copy)]
pub enum DumpError {
    RepoOpenFail = 10,
    SnapshotNotFound = 20,
    PathNotFound = 21,
    DumpFailed = 30,
}

impl ToExitCode for DumpError {
    fn to_exit_code(&self) -> i32 {
        *self as i32
    }
}

#[derive(Args, Debug, Clone)]
#[clap(about = "Print the contents of a file from a snapshot to stdout")]
pub struct CmdArgs {
    /// Snapshot ID (prefix) or 'latest'
    #[clap(value_parser, default_value_t = UseSnapshot::Latest)]
    pub snapshot: UseSnapshot,

    /// Path to the file inside the snapshot
    #[clap(long, value_parser)]
    pub path: PathBuf,
}

pub async fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    with_repository_lock(
        global_args.auth_file.as_ref(),
        global_args.key.as_ref(),
        new_backend_with_prompt(global_args.backend_options(false)).await?,
        global_args.to_repo_config(),
        false,
        global_args.retry_lock_duration,
        global_args.no_lock,
        |repo, _, lock_handle| async move {
            let cleanup_handler = CleanupHandler::new()?;
            cleanup_handler.add_lock(lock_handle);

            repo.reload_master_index().await?;

            let (_snapshot_id, snap) = find_use_snapshot(repo.clone(), &args.snapshot)
                .await
                .map_err(|_| fail("Snapshot not found", DumpError::SnapshotNotFound))?
                .ok_or_else(|| fail("Snapshot not found", DumpError::SnapshotNotFound))?;

            let node = find_serialized_node(repo.as_ref(), &snap.tree, &args.path)
                .await?
                .ok_or_else(|| fail("Path not found", DumpError::PathNotFound))?;

            if !node.is_file() {
                return Err(fail("Path is not a regular file", DumpError::DumpFailed));
            }

            let mut stdout = std::io::stdout();
            if let Some(blob_ids) = &node.blobs {
                for blob_id in blob_ids {
                    let data = repo
                        .load_blob(blob_id)
                        .await
                        .with_context(|| format!("Failed to load blob {blob_id}"))?;
                    stdout.write_all(&data)?;
                }
            }

            Ok(())
        },
    )
    .await
}
