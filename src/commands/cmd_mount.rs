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

use anyhow::Result;
use clap::Args;

use crate::{
    backend::new_backend_with_prompt,
    commands::GlobalArgs,
    fuse::fs::MapacheFS,
    repository::{self},
    utils,
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
    let pass = utils::get_password_from_file(&global_args.password_file)?;
    let backend = new_backend_with_prompt(global_args, true)?;
    let (repo, _) = repository::try_open(pass, global_args.key.as_ref(), backend)?;

    unsafe {
        MapacheFS::mount(repo, &args.mountpoint, args.allow_other)?;
    }

    Ok(())
}
