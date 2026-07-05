use std::{io, io::Write, path::PathBuf};

use clap::Args;

use crate::{
    backend::new_backend_with_prompt,
    commands::{
        GlobalArgs, ToExitCode, UseSnapshot, cleanup::CleanupHandler, find_use_snapshot,
        with_repository_lock,
    },
    common::error::MapacheError,
    fs::tree::find_serialized_node,
};

#[derive(Debug, thiserror::Error)]
pub enum DumpError {
    #[error("failed to open repository: {0}")]
    RepoOpenFail(String),
    #[error("snapshot not found: {0}")]
    SnapshotNotFound(String),
    #[error("path not found: {0}")]
    PathNotFound(String),
    #[error("dump failed: {0}")]
    DumpFailed(String),
    #[error(transparent)]
    Repo(#[from] MapacheError),
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl ToExitCode for DumpError {
    fn to_exit_code(&self) -> i32 {
        match self {
            DumpError::RepoOpenFail(_) => 10,
            DumpError::SnapshotNotFound(_) => 20,
            DumpError::PathNotFound(_) => 21,
            DumpError::DumpFailed(_) => 30,
            DumpError::Repo(_) => 1,
            DumpError::Io(_) => 1,
        }
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

pub async fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<(), DumpError> {
    with_repository_lock(
        global_args.auth_file.as_ref(),
        global_args.key.as_ref(),
        new_backend_with_prompt(global_args.backend_options(false)).await?,
        global_args.to_repo_config(),
        false,
        global_args.retry_lock_duration,
        global_args.no_lock,
        |repo, _, lock_handle| async move {
            let cleanup_handler = CleanupHandler::new();
            cleanup_handler.add_lock(lock_handle);

            repo.reload_master_index().await?;

            let (_snapshot_id, snap) = find_use_snapshot(repo.clone(), &args.snapshot)
                .await
                .map_err(|e| DumpError::SnapshotNotFound(e.inner()))?
                .ok_or_else(|| {
                    DumpError::SnapshotNotFound(
                        "no snapshot matches the given identifier".to_string(),
                    )
                })?;

            let node = find_serialized_node(repo.as_ref(), &snap.tree, &args.path)
                .await?
                .ok_or_else(|| {
                    DumpError::PathNotFound(format!(
                        "'{}' does not exist in snapshot",
                        args.path.display()
                    ))
                })?;

            if !node.is_file() {
                return Err(DumpError::DumpFailed(
                    "path is not a regular file".to_string(),
                ));
            }

            let mut stdout = std::io::stdout();
            if let Some(blob_ids) = &node.blobs {
                for blob_id in blob_ids {
                    let data = repo.load_blob(blob_id).await.map_err(|e| {
                        DumpError::DumpFailed(format!(
                            "failed to load blob {blob_id}: {}",
                            e.inner()
                        ))
                    })?;
                    stdout.write_all(&data)?;
                }
            }

            Ok(())
        },
    )
    .await
}
