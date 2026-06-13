use anyhow::Result;
use clap::Parser;

use crate::{
    backend::new_backend_with_prompt,
    commands::{GlobalArgs, cleanup::CleanupHandler, with_repository_lock},
    mapache::ContentIdType,
    repository::repo::REPO_DROPPED_EXTENSION,
};

// Define argument groups for mutual exclusivity and multiple selection
#[derive(Parser, Debug, Clone)]
#[clap(about = "Recall forgotten snapshots")]
pub struct CmdArgs {
    #[arg(value_parser)]
    pub id: String,
}

pub async fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    with_repository_lock(
        global_args.auth_file.as_ref(),
        global_args.key.as_ref(),
        new_backend_with_prompt(global_args.backend_options(false)).await?,
        global_args.to_repo_config(),
        true,
        global_args.retry_lock_duration,
        global_args.no_lock,
        |repo, _, lock_handle| async move {
            let cleanup_handler = CleanupHandler::new()?;
            cleanup_handler.add_lock(lock_handle.clone());

            tracing::info!(target: "recall", "Searching for dropped snapshot");
            let (id, _dropped_path) = repo
                .find_with_extension(
                    ContentIdType::Snapshot,
                    &args.id,
                    Some(REPO_DROPPED_EXTENSION),
                )
                .await?;

            tracing::info!(target: "recall", "Recalling snapshot {}", id.to_short_hex(8));
            repo.recall_dropped_snapshot(&id).await?;

            Ok(())
        },
    )
    .await
}
