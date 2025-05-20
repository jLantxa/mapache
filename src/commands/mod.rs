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

use std::str::FromStr;

use anyhow::{Error, anyhow};
use clap::{Parser, Subcommand};

use crate::backup::SnapshotId;

pub mod cat;
pub mod init;
pub mod log;
pub mod restore;
pub mod snapshot;

// CLI arguments
#[derive(Parser, Debug)]
#[clap(
    version = env!("CARGO_PKG_VERSION"), // Version from crate metadata
    about = "Incremental backup tool"
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
    Init(init::CmdArgs),

    #[clap(about = "Show all snapshots present in the repository")]
    Log(log::CmdArgs),

    #[clap(about = "Create a new snapshot")]
    Snapshot(snapshot::CmdArgs),

    #[clap(about = "Restores a snapshot")]
    Restore(restore::CmdArgs),

    #[clap(about = "Prints repository objects")]
    Cat(cat::CmdArgs),
}

#[derive(Parser, Debug)]
pub struct GlobalArgs {
    /// Repository path
    #[clap(short, long, value_parser)]
    pub repo: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum UseSnapshot {
    Latest,
    Snapshot(SnapshotId),
}

impl FromStr for UseSnapshot {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "latest" => Ok(UseSnapshot::Latest),
            _ if !s.is_empty() => Ok(UseSnapshot::Snapshot(s.to_string())),
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
            UseSnapshot::Snapshot(id) => write!(f, "{}", id),
        }
    }
}
