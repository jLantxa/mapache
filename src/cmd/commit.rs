/*
 * [backup] is an incremental backup tool
 * Copyright (C) 2025  Javier Lancha Vázquez <javier.lancha@gmail.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

use std::path::{Path, PathBuf};

use anyhow::Result;
use clap::Args;

use crate::{
    cli::{self, GlobalArgs},
    repository::repo::Repository,
};

#[derive(Args, Debug)]
pub struct CmdArgs {
    /// List of paths to commit
    #[clap(value_parser, required = true)]
    pub paths: Vec<PathBuf>,

    /// Force a complete analysis of all files and directories
    #[arg(long)]
    pub naive: bool,
}

pub fn run(global: &GlobalArgs, _args: &CmdArgs) -> Result<()> {
    let password = cli::request_password();
    let repo_path = Path::new(&global.repo);

    let mut _repo = Repository::open(repo_path, password)?;

    todo!()
}
