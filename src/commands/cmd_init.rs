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

use anyhow::Result;
use clap::Args;
use colored::Colorize;

use crate::backend::new_backend_with_prompt;
use crate::repository::{LATEST_REPOSITORY_VERSION, RepoVersion};
use crate::ui;
use crate::{repository, utils};

use super::GlobalArgs;

#[derive(Args, Debug)]
#[clap(about = "Initialize a new repository")]
pub struct CmdArgs {
    /// Repository version
    #[clap(long, default_value_t = LATEST_REPOSITORY_VERSION)]
    pub repository_version: RepoVersion,
}

pub fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let pass = utils::get_password_from_file(&global_args.password_file)?;
    let backend = new_backend_with_prompt(global_args)?;

    ui::cli::log!("Initializing a new repository in \'{}\'", &global_args.repo);
    repository::init_repository_with_version(
        pass,
        global_args.key.as_ref(),
        args.repository_version,
        backend,
    )?;

    ui::cli::warning!(
        "{}\n{}",
        "This password is the key to your repository and the only way to access your data.",
        "Don't forget it.".bold().green()
    );

    Ok(())
}
