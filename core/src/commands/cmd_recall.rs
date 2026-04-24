use anyhow::Result;
use clap::Parser;

use crate::{
    backend::new_backend_with_prompt,
    commands::{GlobalArgs, cleanup::CleanupHandler, with_repository_lock},
    mapache::ContentIdType,
    repository::repo::REPO_DROPPED_EXTENSION,
};

// Define argument groups for mutual exclusivity and multiple selection
#[derive(Parser, Debug)]
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
        |repo, _, lock_handle| async move {
            let cleanup_handler = CleanupHandler::new()?;
            cleanup_handler.add_lock(lock_handle.clone());

            let (id, _dropped_path) = repo
                .find_with_extension(
                    ContentIdType::Snapshot,
                    &args.id,
                    Some(REPO_DROPPED_EXTENSION),
                )
                .await?;

            repo.recall_dropped_snapshot(&id).await?;

            Ok(())
        },
    )
    .await
}
