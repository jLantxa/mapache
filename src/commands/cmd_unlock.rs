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

use anyhow::Result;
use clap::Args;

use crate::{
    backend::{BackendOptions, new_backend_with_prompt},
    mapache::FileType,
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
        use_cache: !global_args.no_cache,
    };

    let (repo, _) =
        Repository::try_open_unlocked(auth.as_ref(), global_args.key.as_ref(), backend, config)?;

    let locks = repo.get_locks()?;
    let mut num_deleted_locks = 0;
    for lock in locks {
        if args.force || lock.is_expired() {
            repo.delete_file(FileType::Lock, lock.id())?;
            num_deleted_locks += 1;
        }
    }

    ui::cli::log!(
        "Deleted {}",
        utils::format_count(num_deleted_locks, "lock", "locks")
    );

    Ok(())
}
