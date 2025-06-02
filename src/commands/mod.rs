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

use std::{path::PathBuf, str::FromStr};

use anyhow::{Error, Result, anyhow};
use clap::{Parser, Subcommand};

pub mod cmd_cat;
pub mod cmd_forget;
pub mod cmd_init;
pub mod cmd_log;
pub mod cmd_ls;
pub mod cmd_restore;
pub mod cmd_snapshot;

// CLI arguments
#[derive(Parser, Debug)]
#[clap(
    version = env!("CARGO_PKG_VERSION"), // Version from crate metadata
    about = "[backup] is a de-duplicating, incremental backup tool",

)]
pub struct Cli {
    // Subcommand
    #[command(subcommand)]
    pub command: Command,

    // Global arguments
    #[clap(flatten)]
    pub global_args: GlobalArgs,
}

// List of commands
#[derive(Subcommand, Debug)]
pub enum Command {
    #[clap(about = "Initialize a new repository")]
    Init(cmd_init::CmdArgs),

    #[clap(about = "Create a new snapshot")]
    Snapshot(cmd_snapshot::CmdArgs),

    #[clap(about = "Restore a snapshot")]
    Restore(cmd_restore::CmdArgs),

    #[clap(about = "Show all snapshots present in the repository")]
    Log(cmd_log::CmdArgs),

    #[clap(about = "Remove snapshots from the repository")]
    Forget(cmd_forget::CmdArgs),

    #[clap(about = "List nodes in the repository")]
    Ls(cmd_ls::CmdArgs),

    #[clap(about = "Print repository objects")]
    Cat(cmd_cat::CmdArgs),
}

#[derive(Parser, Debug)]
pub struct GlobalArgs {
    /// Repository path
    #[clap(short, long, value_parser)]
    pub repo: String,

    /// Path to a KeyFile
    #[clap(long, value_parser)]
    pub key: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum UseSnapshot {
    Latest,
    SnapshotId(String),
}

impl FromStr for UseSnapshot {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "latest" => Ok(UseSnapshot::Latest),
            _ if !s.is_empty() => Ok(UseSnapshot::SnapshotId(s.to_string())),
            _ => Err(anyhow!(
                "Invalid snapshot value: must be 'latest' or a snapshot ID"
            )),
        }
    }
}

impl std::fmt::Display for UseSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UseSnapshot::Latest => write!(f, "latest"),
            UseSnapshot::SnapshotId(id) => write!(f, "{}", id),
        }
    }
}

pub fn run(args: &Cli) -> Result<()> {
    match &args.command {
        Command::Init(cmd_args) => cmd_init::run(&args.global_args, &cmd_args),
        Command::Snapshot(cmd_args) => cmd_snapshot::run(&args.global_args, &cmd_args),
        Command::Restore(cmd_args) => cmd_restore::run(&args.global_args, &cmd_args),
        Command::Forget(cmd_args) => cmd_forget::run(&args.global_args, &cmd_args),
        Command::Log(cmd_args) => cmd_log::run(&args.global_args, &cmd_args),
        Command::Ls(cmd_args) => cmd_ls::run(&args.global_args, &cmd_args),
        Command::Cat(cmd_args) => cmd_cat::run(&args.global_args, &cmd_args),
    }
}
