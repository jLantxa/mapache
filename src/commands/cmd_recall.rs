use anyhow::Result;
use clap::Parser;

use crate::{
    backend::{BackendOptions, new_backend_with_prompt},
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

pub fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let auth = utils::get_auth_from_file(&global_args.auth_file)?;
    let backend = new_backend_with_prompt(BackendOptions {
        repo_path: global_args.repo.clone(),
        ssh_pubkey: global_args.ssh_pubkey.clone(),
        ssh_privatekey: global_args.ssh_privatekey.clone(),
        dry_backend: false,
        cached: !global_args.no_cache,
    })?;

    let config = RepoConfig {
        pack_size: (global_args.pack_size_mib * size::MiB as f32) as u64,
        use_cache: !global_args.no_cache,
    };
    let (repo, _, lock_handle) = Repository::try_open_with_lock(
        auth.as_ref(),
        global_args.key.as_ref(),
        backend,
        config,
        true,
    )?;

    let lock_handle_clone = lock_handle.clone();
    let _cleanup_handler = CleanupHandler::new(move || {
        lock_handle_clone.write().unlock();
    })?;

    let (_id, dropped_path) = repo.find_with_extension(
        ContentIdType::Snapshot,
        &args.id,
        Some(REPO_DROPPED_EXTENSION),
    )?;

    // TODO: There should be a repo function for this.
    let dst_path = dropped_path.with_extension("");
    repo.backend().rename(&dropped_path, &dst_path)?;

    Ok(())
}
