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

use std::path::PathBuf;

use anyhow::{Result, bail};
use clap::Args;
use colored::Colorize;

use crate::{
    backend::{BackendUrl, new_backend_with_prompt},
    commands::{GlobalArgs, cleanup::CleanupHandler},
    fuse::fs::MapacheFS,
    repository::repo::{RepoConfig, Repository},
    ui,
    utils::{self, size},
};

#[derive(Args, Debug)]
#[clap(about = "Mount the repository as a file system")]
pub struct CmdArgs {
    /// Mount point
    #[arg(value_parser)]
    pub mountpoint: PathBuf,

    /// Mount point
    #[arg(long, value_parser, default_value_t = false)]
    pub allow_other: bool,
}

pub fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    // Don't allow mounting on the repo path
    let cannonical_mountpoint = std::fs::canonicalize(args.mountpoint.clone())?;
    if let BackendUrl::Local(repo_path) = BackendUrl::from(&global_args.repo)? {
        if cannonical_mountpoint == repo_path.canonicalize()? {
            bail!("Cannot mount the repository on itself");
        }
    }

    let pass = utils::get_password_from_file(&global_args.password_file)?;
    let backend = new_backend_with_prompt(global_args, true)?;

    let config = RepoConfig {
        pack_size: (global_args.pack_size_mib * size::MiB as f32) as u64,
    };
    let (repo, _, lock_handle) =
        Repository::try_open_with_lock(pass, global_args.key.as_ref(), backend, config, false)?;

    // Listen for CTRL + C to unmount.
    let mpoint = cannonical_mountpoint.clone();
    let lock_handle_clone = lock_handle.clone();
    let _cleanup_handler = CleanupHandler::new(move || {
        let _ = MapacheFS::unmount(&mpoint);
        let _ = lock_handle_clone.write().unlock();
    })?;

    ui::cli::log!("Mounting repository in {}", cannonical_mountpoint.display());
    ui::cli::log!(
        "Press {} to finish or unmount the filesystem manually.",
        "Ctrl+C".bold()
    );
    unsafe {
        MapacheFS::mount(repo, &cannonical_mountpoint, args.allow_other)?;
    }

    Ok(())
}
