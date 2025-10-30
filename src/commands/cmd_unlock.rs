use anyhow::Result;
use clap::Args;

use crate::{
    backend::{BackendOptions, new_backend_with_prompt},
    mapache::ContentIdType,
    repository::repo::{RepoConfig, Repository},
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

pub fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let auth = utils::get_auth_from_file(&global_args.auth_file)?;
    let backend = new_backend_with_prompt(BackendOptions {
        repo_path: global_args.repo.clone(),
        ssh_pubkey: global_args.ssh_pubkey.clone(),
        ssh_privatekey: global_args.ssh_privatekey.clone(),
        dry_backend: false,
        cached: false,
    })?;

    let config = RepoConfig {
        pack_size: (global_args.pack_size_mib * size::MiB as f32) as u64,
        use_cache: false,
    };

    let (repo, _) =
        Repository::try_open_unlocked(auth.as_ref(), global_args.key.as_ref(), backend, config)?;

    let locks = repo.get_locks()?;
    let mut num_deleted_locks = 0;
    for lock in locks {
        if args.force || lock.is_expired() {
            repo.delete_file(ContentIdType::Lock, lock.id())?;
            num_deleted_locks += 1;
        }
    }

    ui::cli::log!(
        "Deleted {}",
        utils::format_count(num_deleted_locks, "lock", "locks")
    );

    Ok(())
}
