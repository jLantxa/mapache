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

use std::path::PathBuf;

use anyhow::Result;
use clap::{ArgGroup, Args};

use crate::cli::GlobalArgs;

#[derive(Args, Debug)]
#[clap(group = ArgGroup::new("scan_mode").multiple(false))]
pub struct CmdArgs {
    /// List of paths to commit
    #[clap(value_parser, required = true)]
    pub paths: Vec<PathBuf>,

    /// Snapshot description
    #[clap(long, value_parser)]
    pub description: Option<String>,

    /// Force a complete analysis of all files and directories
    #[arg(long, group = "scan_mode")]
    pub naive: bool,

    /// Use a snapshot as parent. This snapshot will be the base when analyzing differences.
    #[arg(long, value_parser, group = "scan_mode")]
    pub parent: Option<String>,
}

pub fn run(_global: &GlobalArgs, _args: &CmdArgs) -> Result<()> {
    todo!()
}
