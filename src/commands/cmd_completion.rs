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

use anyhow::{Context, Result};
use clap::{Args, CommandFactory};
use clap_complete::Shell;

use crate::commands;

#[derive(Args, Debug)]
#[clap(about = "Generate autocompletion scripts")]
pub struct CmdArgs {
    /// Shell type (bash, zsh, fish, powershell, etc.)
    #[clap(long, value_parser)]
    pub shell: Shell,

    /// Output directory where the completion script will be written
    #[clap(long, value_parser)]
    pub path: PathBuf,
}

pub fn run(args: &CmdArgs) -> Result<()> {
    // Ensure directory exists
    std::fs::create_dir_all(&args.path)
        .with_context(|| format!("Failed to create directory: {}", args.path.display()))?;

    // Generate completion script
    let mut cmd = commands::Cli::command();
    let bin_name = &cmd.get_name().to_string();
    clap_complete::generate_to(args.shell, &mut cmd, bin_name, &args.path)
        .with_context(|| format!("Failed to generate completion for {:?}", args.shell))?;

    println!(
        "Completion script for {:?} written to {}",
        args.shell,
        args.path.display()
    );

    Ok(())
}
