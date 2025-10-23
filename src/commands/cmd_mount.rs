// mapache is a secure, de-duplicating, incremental backup tool.
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

use anyhow::{Context, Result, bail};
use clap::Args;
use colored::Colorize;

use crate::{
    backend::{BackendOptions, BackendUrl, new_backend_with_prompt},
    commands::{GlobalArgs, cleanup::CleanupHandler},
    fs,
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

    /// Create the mountpoint if it does not exist
    #[arg(short, long, value_parser, default_value_t = false)]
    pub create_mountpoint: bool,
}

pub fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    // Check that mountpoint exists and is a directory, or create it if requested
    let actual_mountpoint = args.mountpoint.clone();

    // The mountpoint was created by us and should be deleted when we finish.
    let mut created_mountpoint = false;

    if !fs::path_exists(&actual_mountpoint) {
        if args.create_mountpoint {
            std::fs::create_dir_all(&actual_mountpoint)
                .with_context(|| "Could not create mount point")?;
            created_mountpoint = true;
        } else {
            bail!("Mountpoint doesn't exist");
        }
    } else if !actual_mountpoint.is_dir() {
        bail!("Mountpoint must be a directory");
    }

    let cannonical_mountpoint = fs::get_absolute_normalized_path(&actual_mountpoint)?;

    // Don't allow mounting on the repo path
    if let BackendUrl::Local(repo_path) = BackendUrl::from(&global_args.repo)?
        && cannonical_mountpoint == fs::get_absolute_normalized_path(&repo_path)?
    {
        bail!("Cannot mount the repository on itself");
    }

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
        false,
    )?;

    // Listen for CTRL + C to unmount.
    let mpoint = cannonical_mountpoint.clone();
    let lock_handle_clone = lock_handle.clone();
    let _cleanup_handler = CleanupHandler::new(move || {
        let _ = MapacheFS::unmount(&mpoint);
        lock_handle_clone.write().unlock();

        if created_mountpoint {
            // Remove the mountpoint if it was created by us
            let _ = std::fs::remove_dir(&mpoint);
        }
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
