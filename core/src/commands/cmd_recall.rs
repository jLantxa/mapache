use anyhow::Result;
use clap::Parser;

use crate::{
    backend::new_backend_with_prompt,
    commands::{GlobalArgs, cleanup::CleanupHandler, open_repository_with_lock},
    mapache::ContentIdType,
    repository::repo::{REPO_DROPPED_EXTENSION, RepoConfig},
    utils::size,
};

// Define argument groups for mutual exclusivity and multiple selection
#[derive(Parser, Debug)]
#[clap(about = "Recall forgotten snapshots")]
pub struct CmdArgs {
    #[arg(value_parser)]
    pub id: String,
}

pub async fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let backend = new_backend_with_prompt(global_args.backend_options(false)).await?;

    let config = RepoConfig {
        pack_size: (global_args.pack_size_mib * size::MiB as f32) as u64,
        use_cache: !global_args.no_cache,
        compression: global_args.compression_level,
    };
    let (repo, _, mut lock_handle) = open_repository_with_lock(
        global_args.auth_file.as_ref(),
        global_args.key.as_ref(),
        backend,
        config,
        true,
        global_args.retry_lock_duration,
    )
    .await?;

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

    lock_handle.unlock().await;

    Ok(())
}
