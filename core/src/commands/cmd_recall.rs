use anyhow::Result;
use clap::Parser;

use crate::{
    backend::new_backend_with_prompt,
    commands::{GlobalArgs, cleanup::CleanupHandler},
    mapache::ContentIdType,
    repository::repo::{REPO_DROPPED_EXTENSION, RepoConfig, Repository},
    utils::{self, size},
};

// Define argument groups for mutual exclusivity and multiple selection
#[derive(Parser, Debug)]
#[clap(about = "Recall forgotten snapshots")]
pub struct CmdArgs {
    #[arg(value_parser)]
    pub id: String,
}

pub async fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let auth = utils::get_auth_from_file(&global_args.auth_file)?;
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
        true,
        global_args.retry_lock_duration,
    )
    .await?;

    let _cleanup_handler = CleanupHandler::new()?;

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
