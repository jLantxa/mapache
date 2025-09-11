// mapache is an incremental backup tool
// Copyright (C) 2025  Javier Lancha Vázquez <javier.lancha@gmail.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use std::time::Instant;

use anyhow::Result;
use clap::Args;

use crate::{
    backend::{BackendOptions, new_backend_with_prompt},
    commands::cleanup::CleanupHandler,
    repository::{
        repo::{RepoConfig, Repository},
        sync,
    },
    ui, utils,
};

use super::GlobalArgs;

// TODO: Add options for the target backend SSH keys
#[derive(Args, Debug)]
#[clap(about = "Synchronize a repository in a different location")]
pub struct CmdArgs {
    /// Destination path
    #[clap(long = "target", value_parser)]
    pub target: String,

    /// Delete unused files
    #[clap(long)]
    pub delete: bool,
}

pub fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let src_auth = utils::get_auth_from_file(&global_args.auth_file)?;
    let src_backend = new_backend_with_prompt(BackendOptions {
        repo_path: global_args.repo.clone(),
        ssh_pubkey: global_args.ssh_pubkey.clone(),
        ssh_privatekey: global_args.ssh_privatekey.clone(),
        dry_backend: false,
    })?;

    let (_repo, _, lock_handle) = Repository::try_open_with_lock(
        src_auth.as_ref(),
        global_args.key.as_ref(),
        src_backend.clone(),
        RepoConfig::default(),
        true,
    )?;

    let dst_backend = new_backend_with_prompt(BackendOptions {
        repo_path: args.target.clone(),
        ssh_pubkey: None,
        ssh_privatekey: None,
        dry_backend: false,
    })?;
    dst_backend.create()?; // Create the backend to create the directory if it doesn't exist.

    let lock_handle_clone = lock_handle.clone();
    let _cleanup_handler = CleanupHandler::new(move || {
        lock_handle_clone.write().unlock();
    })?;

    let start = Instant::now();

    ui::cli::log!("\nSynchronizing repository...");

    sync::sync_repository(src_backend.as_ref(), dst_backend.as_ref(), args.delete)?;

    ui::cli::log!(
        "\nFinished in {}",
        utils::pretty_print_duration(start.elapsed())
    );

    Ok(())
}
