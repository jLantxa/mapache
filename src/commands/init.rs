// [backup] is an incremental backup tool
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

use crate::backend::new_backend_with_prompt;
use crate::cli::{self, GlobalArgs};
use crate::repository;
use crate::repository::repository::{LATEST_REPOSITORY_VERSION, RepoVersion};

#[derive(Args, Debug)]
pub struct CmdArgs {
    /// Repository version
    #[clap(long, default_value_t = LATEST_REPOSITORY_VERSION)]
    pub repository_version: RepoVersion,
}

pub fn run(global: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let backend = new_backend_with_prompt(&global.repo)?;
    let repo_password = cli::request_new_password();

    cli::log!("Initializing a new repository in \'{}\'", &global.repo);

    repository::repository::init_repository_with_version(
        args.repository_version,
        backend,
        repo_password,
    )?;

    Ok(())
}
