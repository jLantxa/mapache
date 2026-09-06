use std::io;

use clap::Parser;

use crate::{
    backend::new_backend_with_prompt,
    commands::{GlobalArgs, ToExitCode, cleanup::CleanupHandler, with_repository_lock},
    common::{ContentIdType, error::MapacheError},
    repository::repo::REPO_DROPPED_EXTENSION,
};

#[derive(Debug, thiserror::Error)]
pub enum RecallError {
    #[error("dropped snapshot not found: {0}")]
    SnapshotNotFound(String),
    #[error(transparent)]
    Repo(#[from] MapacheError),
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl ToExitCode for RecallError {
    fn to_exit_code(&self) -> i32 {
        match self {
            RecallError::SnapshotNotFound(_) => 20,
            RecallError::Repo(_) => 1,
            RecallError::Io(_) => 4,
        }
    }
}

#[derive(Parser, Debug, Clone)]
#[clap(about = "Recall forgotten snapshots")]
pub struct CmdArgs {
    #[arg(value_parser)]
    pub id: String,
}

pub async fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<(), RecallError> {
    with_repository_lock(
        global_args.auth_file.as_ref(),
        global_args.key.as_ref(),
        new_backend_with_prompt(global_args.backend_options(false))
            .await
            .map_err(RecallError::Repo)?,
        global_args.to_repo_config(),
        true,
        global_args.retry_lock_duration,
        global_args.no_lock,
        |repo, _, lock_handle| async move {
            let cleanup_handler = CleanupHandler::new();
            cleanup_handler.add_lock(lock_handle);

            tracing::info!(target: "recall", "Searching for dropped snapshot");
            let (id, _dropped_path) = repo
                .find_with_extension(
                    ContentIdType::Snapshot,
                    &args.id,
                    Some(REPO_DROPPED_EXTENSION),
                )
                .await
                .map_err(|e| {
                    RecallError::SnapshotNotFound(format!("{}: {}", args.id, e.inner()))
                })?;

            tracing::info!(target: "recall", "Recalling snapshot {}", id.to_short_hex(8));
            repo.recall_dropped_snapshot(&id).await?;

            Ok(())
        },
    )
    .await
}
