use anyhow::Result;
use clap::Args;

use crate::{
    backend::new_backend_with_prompt,
    commands::open_repository,
    mapache::ContentIdType,
    repository::repo::RepoConfig,
    ui,
    utils::{self, size},
};

use super::GlobalArgs;

#[derive(Args, Debug)]
#[clap(about = "Remove existing locks")]
pub struct CmdArgs {
    #[clap(short, long, default_value_t = false)]
    pub force: bool,
}

pub async fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let backend = new_backend_with_prompt(global_args.backend_options(false)).await?;

    let config = RepoConfig {
        pack_size: (global_args.pack_size_mib * size::MiB as f32) as u64,
        use_cache: !global_args.no_cache,
        compression: global_args.compression_level,
    };

    let (repo, _) = open_repository(
        global_args.auth_file.as_ref(),
        global_args.key.as_ref(),
        backend,
        config,
    )
    .await?;

    let locks = repo.get_locks().await?;
    let mut num_deleted_locks = 0;
    for lock in locks {
        if args.force || lock.is_expired() {
            repo.delete_file(ContentIdType::Lock, lock.id(), None)
                .await?;
            num_deleted_locks += 1;
        }
    }

    ui::cli::log!(
        "Deleted {}",
        utils::format_count(num_deleted_locks, "lock", "locks")
    );

    Ok(())
}
