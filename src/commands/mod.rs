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

use std::{collections::BTreeSet, path::PathBuf, str::FromStr, sync::Arc};

use anyhow::{Error, Result, anyhow, bail};
use clap::{ArgGroup, Parser, Subcommand};

use crate::{
    global::{FileType, ID},
    repository::{
        RepositoryBackend,
        snapshot::{Snapshot, SnapshotStreamer},
    },
};

pub mod cmd_amend;
pub mod cmd_cat;
pub mod cmd_clean;
pub mod cmd_diff;
pub mod cmd_forget;
pub mod cmd_init;
pub mod cmd_log;
pub mod cmd_ls;
pub mod cmd_restore;
pub mod cmd_snapshot;
pub mod cmd_verify;

#[cfg(unix)]
pub mod cmd_mount;

// CLI arguments
#[derive(Parser, Debug)]
#[clap(
    version = env!("CARGO_PKG_VERSION"), // Version from crate metadata
    about = "mapache backup tool",
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
    Init(cmd_init::CmdArgs),
    Snapshot(cmd_snapshot::CmdArgs),
    Restore(cmd_restore::CmdArgs),
    Log(cmd_log::CmdArgs),
    Diff(cmd_diff::CmdArgs),
    Forget(cmd_forget::CmdArgs),
    Clean(cmd_clean::CmdArgs),
    Amend(cmd_amend::CmdArgs),
    Ls(cmd_ls::CmdArgs),
    #[cfg(unix)]
    Mount(cmd_mount::CmdArgs),
    Cat(cmd_cat::CmdArgs),
    Verify(cmd_verify::CmdArgs),
}

#[derive(Parser, Debug)]
#[clap(group = ArgGroup::new("verbosity_group").multiple(true))]
pub struct GlobalArgs {
    /// Repository path
    #[clap(short = 'r', long = "repo", value_parser)]
    pub repo: String,

    /// SSH public key
    #[clap(long, value_parser)]
    pub ssh_pubkey: Option<PathBuf>,

    /// SSH private key
    #[clap(long, value_parser)]
    pub ssh_privatekey: Option<PathBuf>,

    /// Path to a file to read the repository password
    #[clap(short = 'p', long, value_parser)]
    pub password_file: Option<PathBuf>,

    /// Path to a KeyFile
    #[clap(short = 'k', long = "key-file", value_parser)]
    pub key: Option<PathBuf>,

    /// Disable logging (verbosity = 0)
    #[clap(long, value_parser, group = "verbosity_group")]
    pub quiet: bool,

    /// Set the verbosity level [0-3]
    #[clap(short = 'v', long, value_parser, group = "verbosity_group")]
    pub verbosity: Option<u32>,
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
            UseSnapshot::SnapshotId(id) => write!(f, "{id}"),
        }
    }
}

pub(crate) fn find_use_snapshot(
    repo: Arc<dyn RepositoryBackend>,
    use_snapshot: &UseSnapshot,
) -> Result<Option<(ID, Snapshot)>> {
    match use_snapshot {
        UseSnapshot::Latest => {
            let mut snapshots = SnapshotStreamer::new(repo.clone())?;
            Ok(snapshots.latest())
        }
        UseSnapshot::SnapshotId(prefix) => {
            let (id, _path) = repo.find(FileType::Snapshot, prefix)?;
            match &repo.load_snapshot(&id) {
                Ok(snap) => Ok(Some((id, snap.clone()))),
                Err(_) => bail!("Snapshot {:?} not found", id),
            }
        }
    }
}

/// A marker for an empty tag set
pub(crate) const EMPTY_TAG_MARK: &str = "[]";

pub(crate) fn parse_tags(s: Option<&str>) -> BTreeSet<String> {
    if s.is_none() {
        return BTreeSet::new();
    }

    let s = s.unwrap().trim();

    if s.is_empty() {
        BTreeSet::new()
    } else {
        s.split(",")
            .map(|tag| tag.trim().to_string())
            .filter(|tag| !tag.is_empty())
            .collect()
    }
}

pub fn run(args: &Cli) -> Result<()> {
    match &args.command {
        Command::Init(cmd_args) => cmd_init::run(&args.global_args, cmd_args),
        Command::Snapshot(cmd_args) => cmd_snapshot::run(&args.global_args, cmd_args),
        Command::Restore(cmd_args) => cmd_restore::run(&args.global_args, cmd_args),
        Command::Forget(cmd_args) => cmd_forget::run(&args.global_args, cmd_args),
        Command::Amend(cmd_args) => cmd_amend::run(&args.global_args, cmd_args),
        Command::Clean(cmd_args) => cmd_clean::run(&args.global_args, cmd_args),
        Command::Log(cmd_args) => cmd_log::run(&args.global_args, cmd_args),
        Command::Ls(cmd_args) => cmd_ls::run(&args.global_args, cmd_args),
        Command::Diff(cmd_args) => cmd_diff::run(&args.global_args, cmd_args),
        Command::Cat(cmd_args) => cmd_cat::run(&args.global_args, cmd_args),

        #[cfg(unix)]
        Command::Mount(cmd_args) => cmd_mount::run(&args.global_args, cmd_args),
        Command::Verify(cmd_args) => cmd_verify::run(&args.global_args, cmd_args),
    }
}
