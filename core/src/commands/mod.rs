use std::{collections::BTreeSet, env, path::PathBuf, str::FromStr, sync::Arc};

use anyhow::{Error, Result, anyhow, bail};
use chrono::Duration;
use clap::{ArgGroup, Parser, Subcommand};
use colored::Colorize;
use conflate::Merge;
use serde::{Deserialize, Serialize};

use crate::{
    backend::{BackendOptions, StorageBackend},
    mapache::{
        ContentIdType, ID,
        config::{MapacheConfig, load_config},
        defaults::{
            DEFAULT_COMPRESSION, DEFAULT_PACK_SIZE_MIB, MAX_CONFIGURABLE_PACK_SIZE_MIB,
            MIN_CONFIGURABLE_PACK_SIZE_MIB, init_runtime_defaults,
        },
        global::{THIS_MAPACHE_VERSION, set_global_opts_with_args},
    },
    repository::{
        lock::LockHandle,
        repo::{RepoConfig, Repository},
        snapshot::{Snapshot, SnapshotStream},
        storage::SecureStorage,
    },
    ui::{self, cli},
    utils::{self, size},
};

// Subcommands
pub mod cmd_amend;
pub mod cmd_bundle;
pub mod cmd_cache;
pub mod cmd_cat;
pub mod cmd_clean;
pub mod cmd_completion;
mod cmd_diff;

pub mod cmd_find;
pub mod cmd_forget;
pub mod cmd_init;
pub mod cmd_key;
pub mod cmd_log;
pub mod cmd_ls;
#[cfg(all(feature = "fuse", unix))]
pub mod cmd_mount;
pub mod cmd_rebuild_index;
pub mod cmd_recall;
pub mod cmd_rechunk;
pub mod cmd_restore;
pub mod cmd_snapshot;
pub mod cmd_stats;
pub mod cmd_sync;
pub mod cmd_tui;
pub mod cmd_unlock;
pub mod cmd_verify;

pub mod cleanup;
pub mod error;

pub(crate) use error::{ToExitCode, fail};

/// mapache CLI definition
#[derive(Parser, Debug)]
#[command(
    name = "mapache",
    version = THIS_MAPACHE_VERSION,
    about = "🦝 mapache backup program",
    long_about = "mapache is a fast, secure, efficient and deduplicating program \
        to make backup copies of your files."
)]
pub struct Cli {
    /// Path to a TOML configuration file
    #[clap(long, global = true)]
    pub config: Option<PathBuf>,

    #[command(subcommand)]
    pub command: Command,
}

/// Top-level commands (flat list)
#[derive(Subcommand, Debug)]
pub enum Command {
    Amend(WithGlobal<cmd_amend::CmdArgs>),
    Bundle(cmd_bundle::CmdArgs),
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
    #[cfg(all(feature = "fuse", unix))]
    Mount(WithGlobal<cmd_mount::CmdArgs>),
    RebuildIndex(WithGlobal<cmd_rebuild_index::CmdArgs>),
    Recall(WithGlobal<cmd_recall::CmdArgs>),
    Rechunk(WithGlobal<cmd_rechunk::CmdArgs>),
    Restore(WithGlobal<cmd_restore::CmdArgs>),
    Snapshot(WithGlobal<cmd_snapshot::CmdArgs>),
    Stats(WithGlobal<cmd_stats::CmdArgs>),
    Sync(WithGlobal<cmd_sync::CmdArgs>),
    Tui(WithGlobal<cmd_tui::CmdArgs>),
    Unlock(WithGlobal<cmd_unlock::CmdArgs>),
    Verify(WithGlobal<cmd_verify::CmdArgs>),
}

#[derive(Parser, Debug)]
pub struct WithGlobal<T: clap::Args> {
    #[clap(flatten)]
    pub global: CliGlobalArgs,

    #[clap(flatten)]
    pub args: T,
}

/// CLI-parsable global options (all configurable fields are Option<T>)
#[derive(Parser, Debug, Clone, Merge, Deserialize, Default)]
#[clap(group = ArgGroup::new("verbosity_group").multiple(true))]
#[serde(default, rename_all = "kebab-case")]
pub struct CliGlobalArgs {
    /// Repository path
    #[clap(short, long)]
    #[merge(strategy = conflate::option::overwrite_none)]
    pub repo: Option<String>,

    /// Disable cache
    #[clap(long, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
    #[merge(strategy = conflate::option::overwrite_none)]
    pub no_cache: Option<bool>,

    /// SSH private key
    #[clap(long)]
    #[merge(strategy = conflate::option::overwrite_none)]
    #[serde(deserialize_with = "crate::mapache::config::deserialize_config_path_opt")]
    pub ssh_privatekey: Option<PathBuf>,

    /// Path to a file to read repository authentication credentials
    #[clap(long)]
    #[merge(strategy = conflate::option::overwrite_none)]
    #[serde(deserialize_with = "crate::mapache::config::deserialize_config_path_opt")]
    pub auth_file: Option<PathBuf>,

    /// Pack target size in MiB
    #[clap(long = "pack-size", value_parser = pack_size_parser)]
    #[merge(strategy = conflate::option::overwrite_none)]
    pub pack_size_mib: Option<f32>,

    /// Path to a KeyFile
    #[clap(short = 'k', long = "key-file")]
    #[merge(strategy = conflate::option::overwrite_none)]
    #[serde(
        rename = "key-file",
        deserialize_with = "crate::mapache::config::deserialize_config_path_opt"
    )]
    pub key: Option<PathBuf>,

    /// Disable logging (verbosity = 0)
    #[clap(long, group = "verbosity_group", action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
    #[merge(strategy = conflate::option::overwrite_none)]
    pub quiet: Option<bool>,

    /// Enable json output
    #[clap(long, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
    #[merge(strategy = conflate::option::overwrite_none)]
    pub json: Option<bool>,

    /// Set the verbosity level [0-3]
    #[clap(short, long, group = "verbosity_group")]
    #[merge(strategy = conflate::option::overwrite_none)]
    pub verbosity: Option<u32>,

    /// Compression level [fastest|fast|balanced|better|best|level:val]
    #[clap(long = "compression", value_parser = parse_compression_level)]
    #[merge(strategy = conflate::option::overwrite_none)]
    #[serde(deserialize_with = "deserialize_compression_opt")]
    pub compression_level: Option<Compression>,

    /// Retry acquiring a lock if the repository is already locked. Takes a duration
    /// string like 5m, 30s or 5m30s.
    #[clap(long = "retry-lock", value_parser = utils::parse_duration_string)]
    #[merge(strategy = conflate::option::overwrite_none)]
    #[serde(rename = "retry-lock", deserialize_with = "deserialize_duration_opt")]
    pub retry_lock_duration: Option<Duration>,

    /// Limit upload speed (e.g. 10MB/s, 500KB/s)
    #[clap(long = "limit-upload", value_parser = parse_bandwidth)]
    #[merge(strategy = conflate::option::overwrite_none)]
    #[serde(
        rename = "limit-upload",
        deserialize_with = "deserialize_bandwidth_opt"
    )]
    pub limit_upload: Option<u64>,

    /// Limit download speed (e.g. 10MB/s, 500KB/s)
    #[clap(long = "limit-download", value_parser = parse_bandwidth)]
    #[merge(strategy = conflate::option::overwrite_none)]
    #[serde(
        rename = "limit-download",
        deserialize_with = "deserialize_bandwidth_opt"
    )]
    pub limit_download: Option<u64>,
}

fn deserialize_compression_opt<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<Compression>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt = Option::<String>::deserialize(deserializer)?;
    opt.map(|s| Compression::from_str(&s).map_err(serde::de::Error::custom))
        .transpose()
}

fn deserialize_duration_opt<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<Duration>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt = Option::<String>::deserialize(deserializer)?;
    opt.map(|s| utils::parse_duration_string(&s).map_err(serde::de::Error::custom))
        .transpose()
}

fn deserialize_bandwidth_opt<'de, D>(deserializer: D) -> std::result::Result<Option<u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt = Option::<String>::deserialize(deserializer)?;
    opt.map(|s| utils::parse_bandwidth(&s).map_err(serde::de::Error::custom))
        .transpose()
}

/// Resolved global options (concrete values after merging CLI + config + env + defaults)
#[derive(Debug, Clone)]
pub struct GlobalArgs {
    pub repo: String,
    pub no_cache: bool,
    pub ssh_privatekey: Option<PathBuf>,
    pub auth_file: Option<PathBuf>,
    pub pack_size_mib: f32,
    pub key: Option<PathBuf>,
    pub quiet: bool,
    pub json: bool,
    pub verbosity: Option<u32>,
    pub compression_level: Compression,
    pub retry_lock_duration: Option<Duration>,
    pub limit_upload: Option<u64>,
    pub limit_download: Option<u64>,
}

impl GlobalArgs {
    pub fn backend_options(&self, dry: bool) -> BackendOptions {
        BackendOptions {
            repo_path: self.repo.clone(),
            ssh_privatekey: self.ssh_privatekey.clone(),
            dry_backend: dry,
            limit_upload: self.limit_upload,
            limit_download: self.limit_download,
        }
    }

    pub fn to_repo_config(&self) -> RepoConfig {
        RepoConfig {
            pack_size: (self.pack_size_mib * size::MiB as f32) as u64,
            use_cache: !self.no_cache,
            compression: self.compression_level,
        }
    }
}

/// Converts merged CLI+config global options into concrete GlobalArgs.
/// Applies defaults for any remaining None values.
fn cli_to_global_args(cli: &CliGlobalArgs) -> Result<GlobalArgs> {
    let repo = cli
        .repo
        .clone()
        .or_else(|| env::var("MAPACHE_REPOSITORY").ok())
        .ok_or_else(|| anyhow!("Repository path is required. Use --repo, set MAPACHE_REPOSITORY, or add it to config file."))?;

    let pack_size_mib = cli.pack_size_mib.unwrap_or(DEFAULT_PACK_SIZE_MIB);
    let compression_level = cli.compression_level.unwrap_or(DEFAULT_COMPRESSION);

    Ok(GlobalArgs {
        repo,
        no_cache: cli.no_cache.unwrap_or(false),
        ssh_privatekey: cli.ssh_privatekey.clone(),
        auth_file: cli.auth_file.clone(),
        pack_size_mib,
        key: cli.key.clone(),
        quiet: cli.quiet.unwrap_or(false),
        json: cli.json.unwrap_or(false),
        verbosity: cli.verbosity,
        compression_level,
        retry_lock_duration: cli.retry_lock_duration,
        limit_upload: cli.limit_upload,
        limit_download: cli.limit_download,
    })
}

fn parse_bandwidth(s: &str) -> Result<u64, String> {
    utils::parse_bandwidth(s).map_err(|e| e.to_string())
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

#[derive(Debug, Clone, PartialEq, Deserialize, Default)]
pub enum UseSnapshot {
    #[default]
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
        UseSnapshot::Latest => SnapshotStream::new(repo.clone()).await?.latest().await,
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
pub async fn parse_and_run() -> i32 {
    let args = Cli::parse();

    // Load config if --config provided
    let config = match &args.config {
        Some(path) => match load_config(path) {
            Ok(c) => c,
            Err(e) => {
                ui::cli::error!("{e}");
                return 1;
            }
        },
        None => MapacheConfig::default(),
    };

    // Initialize runtime defaults from config
    init_runtime_defaults(config.runtime.as_ref());

    let (global_result, command_result) = match args.command {
        Command::Amend(cmd) => {
            let mut g = cmd.global.clone();
            if let Some(cfg) = &config.global {
                g.merge(cfg.clone());
            }
            let global = cli_to_global_args(&g);
            (global, Command::Amend(cmd))
        }
        Command::Bundle(cmd) => (Ok(GlobalArgs::default_for_bundle()), Command::Bundle(cmd)),
        Command::Cache(cmd) => (Ok(GlobalArgs::default_for_cache()), Command::Cache(cmd)),
        Command::Cat(cmd) => {
            let mut g = cmd.global.clone();
            if let Some(cfg) = &config.global {
                g.merge(cfg.clone());
            }
            (cli_to_global_args(&g), Command::Cat(cmd))
        }
        Command::Clean(cmd) => {
            let mut g = cmd.global.clone();
            if let Some(cfg) = &config.global {
                g.merge(cfg.clone());
            }
            (cli_to_global_args(&g), Command::Clean(cmd))
        }
        Command::Completion(cmd) => (
            Ok(GlobalArgs::default_for_completion()),
            Command::Completion(cmd),
        ),
        Command::Diff(cmd) => {
            let mut g = cmd.global.clone();
            if let Some(cfg) = &config.global {
                g.merge(cfg.clone());
            }
            (cli_to_global_args(&g), Command::Diff(cmd))
        }
        Command::Find(cmd) => {
            let mut g = cmd.global.clone();
            if let Some(cfg) = &config.global {
                g.merge(cfg.clone());
            }
            (cli_to_global_args(&g), Command::Find(cmd))
        }
        Command::Forget(cmd) => {
            let mut g = cmd.global.clone();
            if let Some(cfg) = &config.global {
                g.merge(cfg.clone());
            }
            let global = cli_to_global_args(&g);
            let mut cmd = cmd;
            if let Some(cfg) = &config.forget {
                cmd.args.merge(cfg.clone());
            }
            (global, Command::Forget(cmd))
        }
        Command::Init(cmd) => {
            let mut g = cmd.global.clone();
            if let Some(cfg) = &config.global {
                g.merge(cfg.clone());
            }
            (cli_to_global_args(&g), Command::Init(cmd))
        }
        Command::Key(cmd) => {
            let mut g = cmd.global.clone();
            if let Some(cfg) = &config.global {
                g.merge(cfg.clone());
            }
            (cli_to_global_args(&g), Command::Key(cmd))
        }
        Command::Log(cmd) => {
            let mut g = cmd.global.clone();
            if let Some(cfg) = &config.global {
                g.merge(cfg.clone());
            }
            (cli_to_global_args(&g), Command::Log(cmd))
        }
        Command::Ls(cmd) => {
            let mut g = cmd.global.clone();
            if let Some(cfg) = &config.global {
                g.merge(cfg.clone());
            }
            (cli_to_global_args(&g), Command::Ls(cmd))
        }
        #[cfg(all(feature = "fuse", unix))]
        Command::Mount(cmd) => {
            let mut g = cmd.global.clone();
            if let Some(cfg) = &config.global {
                g.merge(cfg.clone());
            }
            (cli_to_global_args(&g), Command::Mount(cmd))
        }
        Command::RebuildIndex(cmd) => {
            let mut g = cmd.global.clone();
            if let Some(cfg) = &config.global {
                g.merge(cfg.clone());
            }
            (cli_to_global_args(&g), Command::RebuildIndex(cmd))
        }
        Command::Recall(cmd) => {
            let mut g = cmd.global.clone();
            if let Some(cfg) = &config.global {
                g.merge(cfg.clone());
            }
            (cli_to_global_args(&g), Command::Recall(cmd))
        }
        Command::Rechunk(cmd) => {
            let mut g = cmd.global.clone();
            if let Some(cfg) = &config.global {
                g.merge(cfg.clone());
            }
            (cli_to_global_args(&g), Command::Rechunk(cmd))
        }
        Command::Restore(cmd) => {
            let mut g = cmd.global.clone();
            if let Some(cfg) = &config.global {
                g.merge(cfg.clone());
            }
            let global = cli_to_global_args(&g);
            let mut cmd = cmd;
            if let Some(cfg) = &config.restore {
                cmd.args.merge(cfg.clone());
            }
            (global, Command::Restore(cmd))
        }
        Command::Snapshot(cmd) => {
            let mut g = cmd.global.clone();
            if let Some(cfg) = &config.global {
                g.merge(cfg.clone());
            }
            let global = cli_to_global_args(&g);
            let mut cmd = cmd;
            if let Some(cfg) = &config.snapshot {
                cmd.args.merge(cfg.clone());
            }
            (global, Command::Snapshot(cmd))
        }
        Command::Stats(cmd) => {
            let mut g = cmd.global.clone();
            if let Some(cfg) = &config.global {
                g.merge(cfg.clone());
            }
            (cli_to_global_args(&g), Command::Stats(cmd))
        }
        Command::Sync(cmd) => {
            let mut g = cmd.global.clone();
            if let Some(cfg) = &config.global {
                g.merge(cfg.clone());
            }
            (cli_to_global_args(&g), Command::Sync(cmd))
        }
        Command::Unlock(cmd) => {
            let mut g = cmd.global.clone();
            if let Some(cfg) = &config.global {
                g.merge(cfg.clone());
            }
            (cli_to_global_args(&g), Command::Unlock(cmd))
        }
        Command::Verify(cmd) => {
            let mut g = cmd.global.clone();
            if let Some(cfg) = &config.global {
                g.merge(cfg.clone());
            }
            (cli_to_global_args(&g), Command::Verify(cmd))
        }
        Command::Tui(cmd) => {
            let mut g = cmd.global.clone();
            if let Some(cfg) = &config.global {
                g.merge(cfg.clone());
            }
            (cli_to_global_args(&g), Command::Tui(cmd))
        }
    };

    let global = match global_result {
        Ok(g) => g,
        Err(e) => {
            ui::cli::error!("{e}");
            return 1;
        }
    };

    let json_enabled = global.json;

    set_global_opts_with_args(&global);

    ui::debug::init_debugger();

    tracing::info!(target: "mapache", "called with args: {}", std::env::args().collect::<Vec<_>>().join(" "));

    let result = match command_result {
        Command::Amend(cmd) => cmd_amend::run(&global, &cmd.args).await,
        Command::Bundle(cmd) => cmd_bundle::run(&cmd).await,
        Command::Cat(cmd) => cmd_cat::run(&global, &cmd.args).await,
        Command::Cache(cmd) => cmd_cache::run(&cmd),
        Command::Completion(cmd) => cmd_completion::run(&cmd),
        Command::Clean(cmd) => cmd_clean::run(&global, &cmd.args).await,
        Command::Diff(cmd) => cmd_diff::run(&global, &cmd.args).await,
        Command::Find(cmd) => cmd_find::run(&global, &cmd.args).await,
        Command::Forget(cmd) => cmd_forget::run(&global, &cmd.args).await,
        Command::Init(cmd) => cmd_init::run(&global, &cmd.args).await,
        Command::Key(cmd) => cmd_key::run(&global, &cmd.args).await,
        Command::Log(cmd) => cmd_log::run(&global, &cmd.args).await,
        Command::Ls(cmd) => cmd_ls::run(&global, &cmd.args).await,
        #[cfg(all(feature = "fuse", unix))]
        Command::Mount(cmd) => cmd_mount::run(&global, &cmd.args).await,
        Command::RebuildIndex(cmd) => cmd_rebuild_index::run(&global, &cmd.args).await,
        Command::Recall(cmd) => cmd_recall::run(&global, &cmd.args).await,
        Command::Rechunk(cmd) => cmd_rechunk::run(&global, &cmd.args).await,
        Command::Restore(cmd) => cmd_restore::run(&global, &cmd.args).await,
        Command::Snapshot(cmd) => cmd_snapshot::run(&global, &cmd.args).await,
        Command::Stats(cmd) => cmd_stats::run(&global, &cmd.args).await,
        Command::Sync(cmd) => cmd_sync::run(&global, &cmd.args).await,
        Command::Tui(cmd) => cmd_tui::run(&global, &cmd.args).await,
        Command::Unlock(cmd) => cmd_unlock::run(&global, &cmd.args).await,
        Command::Verify(cmd) => cmd_verify::run(&global, &cmd.args).await,
    };

    if let Err(ref e) = result {
        let exit_code = e
            .downcast_ref::<error::MapacheError>()
            .map(|me| me.exit_code)
            .unwrap_or(error::GENERIC_ERROR_CODE);

        tracing::error!(target: "mapache", "return exit code {}: {}", exit_code, e);

        if !json_enabled {
            ui::cli::error!("{}", e);
        } else {
            #[derive(Serialize)]
            struct ErrorMessage<'a> {
                msg: &'a str,
                exit_code: i32,
            }

            ui::json::emit_static(
                "exit_error",
                &ErrorMessage {
                    msg: &e.to_string(),
                    exit_code,
                },
            );
        }
        return exit_code;
    }

    tracing::info!(target: "mapache", "return with code 0");
    0
}

impl GlobalArgs {
    fn default_for_bundle() -> Self {
        Self {
            repo: String::new(),
            no_cache: false,
            ssh_privatekey: None,
            auth_file: None,
            pack_size_mib: DEFAULT_PACK_SIZE_MIB,
            key: None,
            quiet: false,
            json: false,
            verbosity: None,
            compression_level: DEFAULT_COMPRESSION,
            retry_lock_duration: None,
            limit_upload: None,
            limit_download: None,
        }
    }

    fn default_for_cache() -> Self {
        Self {
            repo: String::new(),
            no_cache: false,
            ssh_privatekey: None,
            auth_file: None,
            pack_size_mib: DEFAULT_PACK_SIZE_MIB,
            key: None,
            quiet: false,
            json: false,
            verbosity: None,
            compression_level: DEFAULT_COMPRESSION,
            retry_lock_duration: None,
            limit_upload: None,
            limit_download: None,
        }
    }

    fn default_for_completion() -> Self {
        Self {
            repo: String::new(),
            no_cache: false,
            ssh_privatekey: None,
            auth_file: None,
            pack_size_mib: DEFAULT_PACK_SIZE_MIB,
            key: None,
            quiet: false,
            json: false,
            verbosity: None,
            compression_level: DEFAULT_COMPRESSION,
            retry_lock_duration: None,
            limit_upload: None,
            limit_download: None,
        }
    }
}

/// Helper to open a repository with interactive authentication if needed.
pub async fn open_repository(
    auth_file: Option<&PathBuf>,
    key_file_path: Option<&PathBuf>,
    backend: Arc<dyn StorageBackend>,
    config: RepoConfig,
) -> Result<(Arc<Repository>, Arc<SecureStorage>)> {
    let mut auth = utils::get_auth(&auth_file.cloned())?;

    // If auth is provided (from file or env), try it once.
    if let Some(a) = auth.take() {
        return Repository::try_open_unlocked(&a, key_file_path, backend, config).await;
    }

    // Otherwise, loop with prompts
    const MAX_PASSWORD_RETRIES: u32 = 3;
    let mut password_try_count = 0;

    loop {
        let current_auth = cli::request_auth()?;

        match Repository::try_open_unlocked(&current_auth, key_file_path, backend.clone(), config)
            .await
        {
            Ok(val) => return Ok(val),
            Err(e) => {
                let is_retryable = e
                    .chain()
                    .find_map(|err| err.downcast_ref::<crate::repository::keys::KeyManagerError>())
                    .map(|key_err| !key_err.is_fatal())
                    .unwrap_or(false);

                if is_retryable {
                    password_try_count += 1;
                    if password_try_count < MAX_PASSWORD_RETRIES {
                        cli::log!("Incorrect username or password. Try again.");
                        continue;
                    }
                }
                return Err(e);
            }
        }
    }
}

/// Helper to open a repository with a lock and interactive authentication if needed,
/// ensuring the lock is released when the provided closure finishes.
pub async fn with_repository_lock<F, Fut, T>(
    auth_file: Option<&PathBuf>,
    key_file_path: Option<&PathBuf>,
    backend: Arc<dyn StorageBackend>,
    config: RepoConfig,
    exclusive_lock: bool,
    retry_duration: Option<Duration>,
    f: F,
) -> Result<T>
where
    F: FnOnce(Arc<Repository>, Arc<SecureStorage>, LockHandle) -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let (repo, storage, lock) = open_repository_with_lock(
        auth_file,
        key_file_path,
        backend,
        config,
        exclusive_lock,
        retry_duration,
    )
    .await?;

    let res = f(repo, storage, lock.clone()).await;
    lock.unlock().await;
    res
}

/// Helper to open a repository with a lock and interactive authentication if needed.
pub async fn open_repository_with_lock(
    auth_file: Option<&PathBuf>,
    key_file_path: Option<&PathBuf>,
    backend: Arc<dyn StorageBackend>,
    config: RepoConfig,
    exclusive_lock: bool,
    retry_duration: Option<Duration>,
) -> Result<(Arc<Repository>, Arc<SecureStorage>, LockHandle)> {
    let mut auth = utils::get_auth(&auth_file.cloned())?;

    // If auth is provided (from file or env), try it once.
    if let Some(a) = auth.take() {
        return Repository::try_open_with_lock(
            &a,
            key_file_path,
            backend,
            config,
            exclusive_lock,
            retry_duration,
        )
        .await;
    }

    // Otherwise, loop with prompts
    const MAX_PASSWORD_RETRIES: u32 = 3;
    let mut password_try_count = 0;

    loop {
        let current_auth = cli::request_auth()?;

        match Repository::try_open_with_lock(
            &current_auth,
            key_file_path,
            backend.clone(),
            config,
            exclusive_lock,
            retry_duration,
        )
        .await
        {
            Ok(val) => return Ok(val),
            Err(e) => {
                let is_retryable = e
                    .chain()
                    .find_map(|err| err.downcast_ref::<crate::repository::keys::KeyManagerError>())
                    .map(|key_err| !key_err.is_fatal())
                    .unwrap_or(false);

                if is_retryable {
                    password_try_count += 1;
                    if password_try_count < MAX_PASSWORD_RETRIES {
                        crate::log!("Incorrect username or password. Try again.");
                        continue;
                    }
                }
                return Err(e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_bandwidth() {
        assert_eq!(parse_bandwidth("1000").unwrap(), 1000);
        assert_eq!(parse_bandwidth("1K").unwrap(), 1024);
        assert_eq!(parse_bandwidth("1KB").unwrap(), 1000);
        assert_eq!(parse_bandwidth("1M").unwrap(), 1024 * 1024);
        assert_eq!(parse_bandwidth("1MB").unwrap(), 1000 * 1000);
        assert_eq!(parse_bandwidth("1G").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_bandwidth("1GB").unwrap(), 1000 * 1000 * 1000);
        assert_eq!(
            parse_bandwidth("1.5M").unwrap(),
            (1.5 * 1024.0 * 1024.0) as u64
        );
        assert_eq!(parse_bandwidth("10MiB/s").unwrap(), 10 * 1024 * 1024);
    }

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
