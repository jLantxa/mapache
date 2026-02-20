use std::{collections::BTreeSet, path::PathBuf, str::FromStr, sync::Arc};

use anyhow::{Error, Result, anyhow, bail};
use chrono::Duration;
use clap::{ArgGroup, Parser, Subcommand};
use serde::Serialize;

use crate::{
    backend::BackendOptions,
    mapache::{
        ContentIdType, ID,
        defaults::{
            DEFAULT_COMPRESSION, DEFAULT_DEFAULT_PACK_SIZE_MIB, MAX_CONFIGURABLE_PACK_SIZE_MIB,
            MIN_CONFIGURABLE_PACK_SIZE_MIB,
        },
        global::set_global_opts_with_args,
    },
    repository::{
        repo::Repository,
        snapshot::{Snapshot, SnapshotStream},
    },
    ui::{self},
    utils,
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
pub mod cmd_rebuild_index;
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
#[command(
    name = "mapache",
    version,
    about = "🦝 mapache backup program",
    long_about = "mapache is a fast, secure, efficient and deduplicated program \
        to make backup copies of your files."
)]
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
    RebuildIndex(WithGlobal<cmd_rebuild_index::CmdArgs>),
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

    /// Enable json output
    #[clap(long)]
    pub json: bool,

    /// Set the verbosity level [0-3]
    #[clap(short, long, group = "verbosity_group")]
    pub verbosity: Option<u32>,

    /// Compression level [fastest|fast|balanced|better|best|level:val]
    #[clap(long = "compression", value_parser = parse_compression_level,  default_value_t = DEFAULT_COMPRESSION)]
    pub compression_level: Compression,

    /// Retry acquiring a lock if the repository is already locked. Takes a duration
    /// string like 5m, 30s or 5m30s.
    #[clap(long = "retry-lock", value_parser = utils::parse_duration_string)]
    pub retry_lock_duration: Option<Duration>,
}

impl GlobalArgs {
    pub fn backend_options(&self, dry: bool) -> BackendOptions {
        BackendOptions {
            repo_path: self.repo.clone(),
            ssh_privatekey: self.ssh_privatekey.clone(),
            dry_backend: dry,
        }
    }
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

#[derive(Debug, Copy, Clone)]
pub enum Compression {
    Manual(i32),
    Fastest,
    Fast,
    Balanced,
    Better,
    Best,
}

impl Compression {
    pub fn to_level(&self) -> i32 {
        match self {
            Self::Manual(level) => *level,
            Self::Fastest => 1,
            Self::Fast => 3,
            Self::Balanced => 5,
            Self::Better => 10,
            Self::Best => 19,
        }
    }
}

impl FromStr for Compression {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let result = match s.to_lowercase().as_str() {
            "fastest" => Some(Self::Fastest),
            "fast" => Some(Self::Fast),
            "balanced" => Some(Self::Balanced),
            "better" => Some(Self::Better),
            "best" => Some(Self::Best),
            _ => None,
        };

        if let Some(variant) = result {
            return Ok(variant);
        }

        s.strip_prefix("level:")
            .ok_or_else(|| anyhow!("Invalid compression format: {s}"))
            .and_then(|val| {
                val.parse::<i32>()
                    .map(Self::Manual)
                    .map_err(|_| anyhow!("Invalid compression level: {val}"))
            })
    }
}

impl std::fmt::Display for Compression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fastest => write!(f, "fastest"),
            Self::Fast => write!(f, "fast"),
            Self::Balanced => write!(f, "balanced"),
            Self::Better => write!(f, "better"),
            Self::Best => write!(f, "best"),
            Self::Manual(level) => write!(f, "level:{level}"),
        }
    }
}

fn parse_compression_level(s: &str) -> Result<Compression> {
    Compression::from_str(s)
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

async fn find_use_snapshot(
    repo: Arc<Repository>,
    use_snapshot: &UseSnapshot,
) -> Result<Option<(ID, Snapshot)>> {
    match use_snapshot {
        UseSnapshot::Latest => Ok(SnapshotStream::new(repo.clone()).await?.latest().await),
        UseSnapshot::SnapshotId(prefix) => {
            let (id, _) = repo.find(ContentIdType::Snapshot, prefix).await?;
            let snap = repo.load_snapshot(&id, None).await?;
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
pub async fn parse_and_run() -> Result<()> {
    let args = Cli::parse();

    let json_enabled = extract_global(&args.command)
        .map(|g| g.json)
        .unwrap_or(false);

    if let Some(global_ref) = extract_global(&args.command) {
        set_global_opts_with_args(global_ref);
    }

    let result = match args.command {
        Command::Amend(cmd) => cmd_amend::run(&cmd.global, &cmd.args).await,
        Command::Cat(cmd) => cmd_cat::run(&cmd.global, &cmd.args).await,
        Command::Cache(cmd) => cmd_cache::run(&cmd),
        Command::Completion(cmd) => cmd_completion::run(&cmd),
        Command::Clean(cmd) => cmd_clean::run(&cmd.global, &cmd.args).await,
        Command::Diff(cmd) => cmd_diff::run(&cmd.global, &cmd.args).await,
        Command::Find(cmd) => cmd_find::run(&cmd.global, &cmd.args).await,
        Command::Forget(cmd) => cmd_forget::run(&cmd.global, &cmd.args).await,
        Command::Init(cmd) => cmd_init::run(&cmd.global, &cmd.args).await,
        Command::Key(cmd) => cmd_key::run(&cmd.global, &cmd.args).await,
        Command::Log(cmd) => cmd_log::run(&cmd.global, &cmd.args).await,
        Command::Ls(cmd) => cmd_ls::run(&cmd.global, &cmd.args).await,
        #[cfg(all(feature = "fuse", target_os = "linux"))]
        Command::Mount(cmd) => cmd_mount::run(&cmd.global, &cmd.args).await,
        Command::RebuildIndex(cmd) => cmd_rebuild_index::run(&cmd.global, &cmd.args).await,
        Command::Recall(cmd) => cmd_recall::run(&cmd.global, &cmd.args).await,
        Command::Rechunk(cmd) => cmd_rechunk::run(&cmd.global, &cmd.args).await,
        Command::Restore(cmd) => cmd_restore::run(&cmd.global, &cmd.args).await,
        Command::Snapshot(cmd) => cmd_snapshot::run(&cmd.global, &cmd.args).await,
        Command::Stats(cmd) => cmd_stats::run(&cmd.global, &cmd.args).await,
        Command::Sync(cmd) => cmd_sync::run(&cmd.global, &cmd.args).await,
        Command::Unlock(cmd) => cmd_unlock::run(&cmd.global, &cmd.args).await,
        Command::Verify(cmd) => cmd_verify::run(&cmd.global, &cmd.args).await,
    };

    if let Err(ref e) = result {
        if !json_enabled {
            ui::cli::error!("{}", e);
        } else {
            #[derive(Serialize)]
            struct ErrorMessage<'a> {
                msg: &'a str,
            }

            ui::json_reporter::emit_static(
                "exit_error",
                &ErrorMessage {
                    msg: &e.to_string(),
                },
            );
        }
    }

    result
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
        RebuildIndex,
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
