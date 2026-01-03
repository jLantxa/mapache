use std::{collections::BTreeSet, path::PathBuf, str::FromStr, sync::Arc};

use anyhow::{Error, Result, anyhow, bail};
use clap::{ArgGroup, Parser, Subcommand};

use crate::{
    mapache::{
        ContentIdType, ID,
        defaults::{
            DEFAULT_DEFAULT_PACK_SIZE_MIB, MAX_CONFIGURABLE_PACK_SIZE_MIB,
            MIN_CONFIGURABLE_PACK_SIZE_MIB,
        },
        global::set_global_opts_with_args,
    },
    repository::{
        repo::Repository,
        snapshot::{Snapshot, SnapshotStream},
    },
};

// Subcommands
pub mod cmd_amend;
pub mod cmd_cache;
pub mod cmd_cat;
pub mod cmd_clean;
mod cmd_completion;
pub mod cmd_diff;
pub mod cmd_find;
pub mod cmd_forget;
pub mod cmd_init;
pub mod cmd_key;
pub mod cmd_log;
pub mod cmd_ls;
#[cfg(all(feature = "fuse", target_os = "linux"))]
pub mod cmd_mount;
pub mod cmd_recall;
pub mod cmd_rechunk;
pub mod cmd_restore;
pub mod cmd_snapshot;
pub mod cmd_stats;
pub mod cmd_sync;
pub mod cmd_unlock;
pub mod cmd_verify;

pub(crate) mod cleanup;

/// mapache CLI definition
#[derive(Parser, Debug)]
#[command(version, about = "mapache backup tool")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

/// Top-level commands (flat list)
#[derive(Subcommand, Debug)]
pub enum Command {
    Amend(WithGlobal<cmd_amend::CmdArgs>),
    Cache(cmd_cache::CmdArgs),
    Cat(WithGlobal<cmd_cat::CmdArgs>),
    Clean(WithGlobal<cmd_clean::CmdArgs>),
    Completion(cmd_completion::CmdArgs),
    Diff(WithGlobal<cmd_diff::CmdArgs>),
    Find(WithGlobal<cmd_find::CmdArgs>),
    Forget(WithGlobal<cmd_forget::CmdArgs>),
    Init(WithGlobal<cmd_init::CmdArgs>),
    Key(WithGlobal<cmd_key::CmdArgs>),
    Log(WithGlobal<cmd_log::CmdArgs>),
    Ls(WithGlobal<cmd_ls::CmdArgs>),
    #[cfg(all(feature = "fuse", target_os = "linux"))]
    Mount(WithGlobal<cmd_mount::CmdArgs>),
    Recall(WithGlobal<cmd_recall::CmdArgs>),
    Rechunk(WithGlobal<cmd_rechunk::CmdArgs>),
    Restore(WithGlobal<cmd_restore::CmdArgs>),
    Snapshot(WithGlobal<cmd_snapshot::CmdArgs>),
    Stats(WithGlobal<cmd_stats::CmdArgs>),
    Sync(WithGlobal<cmd_sync::CmdArgs>),
    Unlock(WithGlobal<cmd_unlock::CmdArgs>),
    Verify(WithGlobal<cmd_verify::CmdArgs>),
}

#[derive(Parser, Debug)]
pub struct WithGlobal<T: clap::Args> {
    #[clap(flatten)]
    pub global: GlobalArgs,

    #[clap(flatten)]
    pub args: T,
}

/// Global options
#[derive(Parser, Debug)]
#[clap(group = ArgGroup::new("verbosity_group").multiple(true))]
pub struct GlobalArgs {
    /// Repository path
    #[clap(short, long)]
    pub repo: String,

    /// Disable cache
    #[clap(long)]
    pub no_cache: bool,

    /// SSH public key
    #[clap(long)]
    pub ssh_pubkey: Option<PathBuf>,

    /// SSH private key
    #[clap(long)]
    pub ssh_privatekey: Option<PathBuf>,

    /// Path to a file to read repository authentication credentials
    #[clap(long)]
    pub auth_file: Option<PathBuf>,

    /// Pack target size in MiB
    #[clap(long = "pack-size", value_parser = pack_size_parser, default_value_t = DEFAULT_DEFAULT_PACK_SIZE_MIB)]
    pub pack_size_mib: f32,

    /// Path to a KeyFile
    #[clap(short = 'k', long = "key-file")]
    pub key: Option<PathBuf>,

    /// Disable logging (verbosity = 0)
    #[clap(long, group = "verbosity_group")]
    pub quiet: bool,

    /// Set the verbosity level [0-3]
    #[clap(short, long, group = "verbosity_group")]
    pub verbosity: Option<u32>,
}

fn pack_size_parser(s: &str) -> Result<f32> {
    let val = s.parse::<f32>()?;
    if !(MIN_CONFIGURABLE_PACK_SIZE_MIB..=MAX_CONFIGURABLE_PACK_SIZE_MIB).contains(&val) {
        bail!(
            "Pack size must be between {MIN_CONFIGURABLE_PACK_SIZE_MIB} and {MAX_CONFIGURABLE_PACK_SIZE_MIB} MiB"
        );
    }
    Ok(val)
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
            "latest" => Ok(Self::Latest),
            _ if !s.is_empty() => Ok(Self::SnapshotId(s.to_string())),
            _ => Err(anyhow!("Invalid snapshot: use 'latest' or a snapshot ID")),
        }
    }
}

impl std::fmt::Display for UseSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Latest => write!(f, "latest"),
            Self::SnapshotId(id) => write!(f, "{id}"),
        }
    }
}

fn find_use_snapshot(
    repo: Arc<Repository>,
    use_snapshot: &UseSnapshot,
) -> Result<Option<(ID, Snapshot)>> {
    match use_snapshot {
        UseSnapshot::Latest => Ok(SnapshotStream::new(repo.clone())?.latest()),
        UseSnapshot::SnapshotId(prefix) => {
            let (id, _) = repo.find(ContentIdType::Snapshot, prefix)?;
            let snap = repo.load_snapshot(&id, None)?;
            Ok(Some((id, snap)))
        }
    }
}

pub(crate) const EMPTY_TAG_MARK: &str = "[]";

fn parse_tags(s: Option<&str>) -> BTreeSet<String> {
    s.unwrap_or("")
        .split(',')
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .map(String::from)
        .collect()
}

/// CLI entry point
pub fn parse_and_run() -> Result<()> {
    let args = Cli::parse();

    if let Some(global) = extract_global(&args.command) {
        set_global_opts_with_args(global);
    }

    match args.command {
        Command::Amend(cmd) => cmd_amend::run(&cmd.global, &cmd.args),
        Command::Cat(cmd) => cmd_cat::run(&cmd.global, &cmd.args),
        Command::Cache(cmd) => cmd_cache::run(&cmd),
        Command::Completion(cmd) => cmd_completion::run(&cmd),
        Command::Clean(cmd) => cmd_clean::run(&cmd.global, &cmd.args),
        Command::Diff(cmd) => cmd_diff::run(&cmd.global, &cmd.args),
        Command::Find(cmd) => cmd_find::run(&cmd.global, &cmd.args),
        Command::Forget(cmd) => cmd_forget::run(&cmd.global, &cmd.args),
        Command::Init(cmd) => cmd_init::run(&cmd.global, &cmd.args),
        Command::Key(cmd) => cmd_key::run(&cmd.global, &cmd.args),
        Command::Log(cmd) => cmd_log::run(&cmd.global, &cmd.args),
        Command::Ls(cmd) => cmd_ls::run(&cmd.global, &cmd.args),
        #[cfg(all(feature = "fuse", target_os = "linux"))]
        Command::Mount(cmd) => cmd_mount::run(&cmd.global, &cmd.args),
        Command::Recall(cmd) => cmd_recall::run(&cmd.global, &cmd.args),
        Command::Rechunk(cmd) => cmd_rechunk::run(&cmd.global, &cmd.args),
        Command::Restore(cmd) => cmd_restore::run(&cmd.global, &cmd.args),
        Command::Snapshot(cmd) => cmd_snapshot::run(&cmd.global, &cmd.args),
        Command::Stats(cmd) => cmd_stats::run(&cmd.global, &cmd.args),
        Command::Sync(cmd) => cmd_sync::run(&cmd.global, &cmd.args),
        Command::Unlock(cmd) => cmd_unlock::run(&cmd.global, &cmd.args),
        Command::Verify(cmd) => cmd_verify::run(&cmd.global, &cmd.args),
    }
}

macro_rules! extract_global {
    ($cmd:expr, { $($(#[$meta:meta])* $variant:ident),* $(,)? }) => {
        match $cmd {
            $(
                $(#[$meta])*
                Command::$variant(inner) => Some(&inner.global),
            )*
            _=>None
        }
    };
}

/// Returns Some(&GlobalArgs) if command has them
fn extract_global(command: &Command) -> Option<&GlobalArgs> {
    extract_global!(command, {
        Amend,
        Cat,
        Clean,
        Diff,
        Find,
        Forget,
        Init,
        Key,
        Log,
        Ls,
        #[cfg(all(feature = "fuse", target_os = "linux"))]
        Mount,
        Recall,
        Rechunk,
        Restore,
        Snapshot,
        Stats,
        Sync,
        Unlock,
        Verify,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pack_size_parser() {
        assert!(pack_size_parser("0").is_err());
        assert!(matches!(pack_size_parser("1"), Ok(1.0)));
        assert!(matches!(pack_size_parser("2"), Ok(2.0)));
        assert!(matches!(pack_size_parser("16"), Ok(16.0)));
        assert!(matches!(pack_size_parser("32"), Ok(32.0)));
        assert!(matches!(pack_size_parser("4095"), Ok(4095.0)));
        assert!(pack_size_parser("4096").is_err());
        assert!(pack_size_parser("8000").is_err());
    }
}
