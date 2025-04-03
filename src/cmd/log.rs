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

use std::path::Path;

use anyhow::Result;
use clap::Args;

use crate::{
    cli::{self, GlobalArgs},
    repository::repo::Repository,
};

#[derive(Args, Debug)]
pub struct CmdArgs {}

pub fn run(global: &GlobalArgs, _args: &CmdArgs) -> Result<()> {
    let password = cli::request_password();
    let repo_path = Path::new(&global.repo);

    let repo = Repository::open(repo_path, password)?;

    let snapshots = repo
        .get_snapshots()?
        .sort_by_key(|(_, snapshot)| snapshot.timestamp); // Sort by timestamp

    dbg!(snapshots);

    Ok(())
}
