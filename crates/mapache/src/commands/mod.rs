// Subcommands
pub mod cmd_amend;
pub mod cmd_bundle;
pub mod cmd_cache;
pub mod cmd_cat;
pub mod cmd_clean;
pub mod cmd_completion;
pub mod cmd_config;
pub mod cmd_copy;
mod cmd_diff;
pub mod cmd_dump;
pub mod cmd_find;
pub mod cmd_forget;
pub mod cmd_init;
pub mod cmd_key;
pub mod cmd_log;
pub mod cmd_ls;
#[cfg(all(feature = "mount", unix))]
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

pub(crate) use error::ToExitCode;

use std::{collections::BTreeSet, env, path::PathBuf, str::FromStr, sync::Arc};

use chrono::Duration;
use clap::{ArgGroup, Args, CommandFactory, FromArgMatches, Parser, Subcommand};
use serde::{Deserialize, Serialize, Serializer};
use zeroize::Zeroizing;

use crate::{
    backend::{BackendOptions, StorageBackend},
    commands::error::CmdError,
    common::{
        ContentIdType, ID,
        config::{MapacheConfig, load_config},
        defaults::{
            DEFAULT_COMPRESSION, DEFAULT_PACK_SIZE_MIB, DEFAULT_VERBOSITY,
            MAX_CONFIGURABLE_PACK_SIZE_MIB, MIN_CONFIGURABLE_PACK_SIZE_MIB, init_runtime_defaults,
        },
        error::{MapacheError, Result},
        global::{MAPACHE_VERSION_INFO, THIS_MAPACHE_VERSION, set_global_opts_with_args},
        hooks,
    },
    repository::{
        lock::LockHandle,
        repo::{Auth, RepoConfig, Repository},
        snapshot::{Snapshot, SnapshotStream},
        storage::SecureStorage,
    },
    ui::{self, cli},
    utils::{self, size},
};

/// mapache CLI definition
#[derive(Parser, Debug)]
#[command(
    name = "mapache",
    version = THIS_MAPACHE_VERSION,
    about = "🦝 mapache backup program",
    long_about = "🦝 mapache is a fast, secure, efficient and deduplicating program \
        to make backup copies of your files."
)]
pub struct Cli {
    /// Path to a TOML configuration file
    #[clap(long = "with-config", global = true)]
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
    Copy(WithGlobal<cmd_copy::CmdArgs>),
    Completion(cmd_completion::CmdArgs),
    Config(cmd_config::CmdArgs),
    Diff(WithGlobal<cmd_diff::CmdArgs>),
    Dump(WithGlobal<cmd_dump::CmdArgs>),
    Find(WithGlobal<cmd_find::CmdArgs>),
    Forget(WithGlobal<cmd_forget::CmdArgs>),
    Init(WithGlobal<cmd_init::CmdArgs>),
    Key(WithGlobal<cmd_key::CmdArgs>),
    Log(WithGlobal<cmd_log::CmdArgs>),
    Ls(WithGlobal<cmd_ls::CmdArgs>),
    #[cfg(all(feature = "mount", unix))]
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

/// Merge CLI arguments with configuration file values.
///
/// `other` (typically from config) is merged into `self` (typically from CLI),
/// so CLI flags take precedence: `self` only accepts a value from `other` when
/// the corresponding field in `self` is still `None`/empty (i.e. not provided on
/// the command line).
pub trait Merge {
    fn merge(&mut self, other: Self);
}

fn merge_opt<T>(dst: &mut Option<T>, src: Option<T>) {
    if src.is_some() {
        *dst = src;
    }
}

#[derive(Parser, Debug)]
pub struct WithGlobal<T: clap::Args> {
    #[clap(flatten)]
    pub global: CliGlobalArgs,

    #[clap(flatten)]
    pub args: T,
}

#[derive(Parser, Debug, Clone, Serialize, Deserialize, Default)]
#[clap(group = ArgGroup::new("verbosity_group").multiple(true))]
#[serde(default, rename_all = "kebab-case")]
pub struct CliGlobalArgs {
    /// Repository path
    #[clap(short, long)]
    pub repo: Option<String>,

    /// Disable cache
    #[clap(long, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
    pub no_cache: Option<bool>,

    /// SSH private key
    #[clap(long)]
    #[serde(deserialize_with = "crate::common::config::deserialize_config_path_opt")]
    pub ssh_privatekey: Option<PathBuf>,

    /// SSH known_hosts file
    #[clap(long)]
    #[serde(deserialize_with = "crate::common::config::deserialize_config_path_opt")]
    pub ssh_known_hosts: Option<PathBuf>,

    /// Path to a file to read repository authentication credentials
    #[clap(long)]
    #[serde(deserialize_with = "crate::common::config::deserialize_config_path_opt")]
    pub auth_file: Option<PathBuf>,

    /// Pack target size in MiB
    #[clap(long = "pack-size", value_parser = pack_size_parser)]
    pub pack_size_mib: Option<f32>,

    /// Path to a KeyFile
    #[clap(short = 'k', long = "key-file")]
    #[serde(
        rename = "key-file",
        deserialize_with = "crate::common::config::deserialize_config_path_opt"
    )]
    pub key: Option<PathBuf>,

    /// Disable logging (verbosity = 0)
    #[clap(long, group = "verbosity_group", action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
    pub quiet: Option<bool>,

    /// Enable json output
    #[clap(long, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
    pub json: Option<bool>,

    /// Set the verbosity level [0-3]
    #[clap(short, long, group = "verbosity_group")]
    pub verbosity: Option<u32>,

    /// Compression level [fastest|fast|balanced|better|best|level:val]
    #[clap(long = "compression", value_parser = parse_compression_level)]
    #[serde(deserialize_with = "deserialize_compression_opt")]
    pub compression_level: Option<Compression>,

    /// Retry acquiring a lock if the repository is already locked. Takes a duration
    /// string like 5m, 30s or 5m30s.
    #[clap(long = "retry-lock", value_parser = utils::parse_duration_string)]
    #[serde(
        rename = "retry-lock",
        deserialize_with = "deserialize_duration_opt",
        serialize_with = "serialize_duration_opt"
    )]
    pub retry_lock_duration: Option<Duration>,

    /// Limit upload speed (e.g. 10MB/s, 500KB/s)
    #[clap(long = "limit-upload", value_parser = parse_bandwidth)]
    #[serde(
        rename = "limit-upload",
        deserialize_with = "deserialize_bandwidth_opt",
        serialize_with = "serialize_bandwidth_opt"
    )]
    pub limit_upload: Option<u64>,

    /// Limit download speed (e.g. 10MB/s, 500KB/s)
    #[clap(long = "limit-download", value_parser = parse_bandwidth)]
    #[serde(
        rename = "limit-download",
        deserialize_with = "deserialize_bandwidth_opt",
        serialize_with = "serialize_bandwidth_opt"
    )]
    pub limit_download: Option<u64>,

    /// Disable repository locking (read-only operations)
    #[clap(long, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
    pub no_lock: Option<bool>,
}

impl CliGlobalArgs {
    pub(crate) fn template() -> Self {
        Self {
            repo: Some("/path/to/repo".to_string()),
            no_cache: Some(false),
            ssh_privatekey: Some(PathBuf::from("~/.ssh/id_ed25519")),
            ssh_known_hosts: Some(PathBuf::from("~/.ssh/known_hosts")),
            auth_file: Some(PathBuf::from("~/.mapache/auth")),
            pack_size_mib: Some(DEFAULT_PACK_SIZE_MIB),
            key: Some(PathBuf::from("~/.mapache/repo.key")),
            quiet: Some(false),
            json: Some(false),
            verbosity: Some(DEFAULT_VERBOSITY),
            compression_level: Some(DEFAULT_COMPRESSION),
            retry_lock_duration: Some(Duration::minutes(5)),
            limit_upload: Some(10 * 1024 * 1024),
            limit_download: Some(50 * 1024 * 1024),
            no_lock: Some(false),
        }
    }
}

/// CLI flags for overriding hooks. Flattened into each command's CmdArgs.
#[derive(Args, Debug, Clone, Default, Serialize, Deserialize)]
pub struct HookArgs {
    /// Shell command to run before the main command (overrides TOML pre-hook)
    #[clap(long = "pre-hook")]
    #[serde(skip)]
    pub pre_hook: Option<String>,

    /// Shell command to run after the main command (overrides TOML post-hook)
    #[clap(long = "post-hook")]
    #[serde(skip)]
    pub post_hook: Option<String>,
}

impl Merge for CliGlobalArgs {
    fn merge(&mut self, other: Self) {
        merge_opt(&mut self.repo, other.repo);
        merge_opt(&mut self.no_cache, other.no_cache);
        merge_opt(&mut self.ssh_privatekey, other.ssh_privatekey);
        merge_opt(&mut self.ssh_known_hosts, other.ssh_known_hosts);
        merge_opt(&mut self.auth_file, other.auth_file);
        merge_opt(&mut self.pack_size_mib, other.pack_size_mib);
        merge_opt(&mut self.key, other.key);
        merge_opt(&mut self.quiet, other.quiet);
        merge_opt(&mut self.json, other.json);
        merge_opt(&mut self.verbosity, other.verbosity);
        merge_opt(&mut self.compression_level, other.compression_level);
        merge_opt(&mut self.retry_lock_duration, other.retry_lock_duration);
        merge_opt(&mut self.limit_upload, other.limit_upload);
        merge_opt(&mut self.limit_download, other.limit_download);
        merge_opt(&mut self.no_lock, other.no_lock);
    }
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

fn serialize_duration_opt<S>(
    val: &Option<Duration>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match val {
        Some(d) => {
            let std_dur = std::time::Duration::from_millis(d.num_milliseconds().unsigned_abs());
            serializer.serialize_some(&utils::pretty_print_duration(std_dur).replace(' ', ""))
        }
        None => serializer.serialize_none(),
    }
}

fn serialize_bandwidth_opt<S>(
    val: &Option<u64>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match val {
        Some(b) => {
            let s = if *b >= utils::size::GiB {
                format!("{} GiB/s", b / utils::size::GiB)
            } else if *b >= utils::size::MiB {
                format!("{} MiB/s", b / utils::size::MiB)
            } else if *b >= utils::size::KiB {
                format!("{} KiB/s", b / utils::size::KiB)
            } else {
                format!("{b} B/s")
            };
            serializer.serialize_some(&s)
        }
        None => serializer.serialize_none(),
    }
}

/// Resolved global options (concrete values after merging CLI + config + env + defaults)
#[derive(Debug, Clone)]
pub struct GlobalArgs {
    pub repo: String,
    pub no_cache: bool,
    pub ssh_privatekey: Option<PathBuf>,
    pub ssh_known_hosts: Option<PathBuf>,
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
    pub no_lock: bool,
}

impl GlobalArgs {
    pub fn backend_options(&self, dry: bool) -> BackendOptions {
        BackendOptions {
            repo_path: self.repo.clone(),
            ssh_privatekey: self.ssh_privatekey.clone(),
            ssh_known_hosts: self.ssh_known_hosts.clone(),
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
fn cli_to_global_args(cli: &CliGlobalArgs) -> std::result::Result<GlobalArgs, String> {
    let repo = cli
        .repo
        .clone()
        .or_else(|| env::var("MAPACHE_REPOSITORY").ok())
        .ok_or_else(|| "Repository path is required. Use --repo, set MAPACHE_REPOSITORY, or add it to config file.".to_string())?;

    let pack_size_mib = cli.pack_size_mib.unwrap_or(DEFAULT_PACK_SIZE_MIB);
    let compression_level = cli.compression_level.unwrap_or(DEFAULT_COMPRESSION);

    Ok(GlobalArgs {
        repo,
        no_cache: cli.no_cache.unwrap_or(false),
        ssh_privatekey: cli.ssh_privatekey.clone(),
        ssh_known_hosts: cli.ssh_known_hosts.clone(),
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
        no_lock: cli.no_lock.unwrap_or(false),
    })
}

fn parse_bandwidth(s: &str) -> std::result::Result<u64, String> {
    utils::parse_bandwidth(s).map_err(|e| e.to_string())
}

fn pack_size_parser(s: &str) -> std::result::Result<f32, String> {
    let val = s
        .parse::<f32>()
        .map_err(|e| format!("Invalid pack size: {e}"))?;
    if !(MIN_CONFIGURABLE_PACK_SIZE_MIB..=MAX_CONFIGURABLE_PACK_SIZE_MIB).contains(&val) {
        return Err(format!(
            "Pack size must be between {MIN_CONFIGURABLE_PACK_SIZE_MIB} and {MAX_CONFIGURABLE_PACK_SIZE_MIB} MiB"
        ));
    }
    Ok(val)
}

#[derive(Debug, Copy, Clone, Serialize)]
#[serde(rename_all = "lowercase")]
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
    type Err = String;

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

        match s.strip_prefix("level:") {
            Some(val) => val
                .parse::<i32>()
                .map(Self::Manual)
                .map_err(|_| format!("Invalid compression level: {val}")),
            None => Err(format!("Invalid compression format: {s}")),
        }
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

fn parse_compression_level(s: &str) -> std::result::Result<Compression, String> {
    Compression::from_str(s)
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum UseSnapshot {
    #[default]
    Latest,
    SnapshotId(String),
}

impl FromStr for UseSnapshot {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "latest" => Ok(Self::Latest),
            _ if !s.is_empty() => Ok(Self::SnapshotId(s.to_string())),
            _ => Err("invalid snapshot: use 'latest' or a snapshot ID".to_string()),
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

pub(crate) async fn find_use_snapshot(
    repo: Arc<Repository>,
    use_snapshot: &UseSnapshot,
) -> Result<Option<(ID, Snapshot)>> {
    match use_snapshot {
        UseSnapshot::Latest => Ok(SnapshotStream::new(repo.clone()).await?.latest().await?),
        UseSnapshot::SnapshotId(prefix) => {
            let (id, _) = repo.find(ContentIdType::Snapshot, prefix).await?;
            let snap = repo.load_snapshot(&id, None).await?;
            Ok(Some((id, snap)))
        }
    }
}

pub(crate) const EMPTY_TAG_MARK: &str = "[]";

pub(crate) fn parse_tags(s: Option<&str>) -> BTreeSet<String> {
    s.unwrap_or("")
        .split(',')
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .map(String::from)
        .collect()
}

/// Resolve global args with config merging.
fn resolve_global(
    cli: &CliGlobalArgs,
    config: &MapacheConfig,
) -> std::result::Result<GlobalArgs, String> {
    let mut g = cli.clone();
    if let Some(cfg) = &config.global {
        g.merge(cfg.clone());
    }
    cli_to_global_args(&g)
}

/// Resolve global args and merge optional command-specific config.
fn resolve_global_with_extra<T: Merge + Clone + clap::Args>(
    mut cmd: WithGlobal<T>,
    config: &MapacheConfig,
    extra: &Option<T>,
) -> (std::result::Result<GlobalArgs, String>, WithGlobal<T>) {
    if let Some(cfg) = extra {
        cmd.args.merge(cfg.clone());
    }
    (resolve_global(&cmd.global, config), cmd)
}

/// CLI entry point
pub async fn parse_and_run() -> i32 {
    cli::color::init_console();
    let args = Cli::from_arg_matches(
        &Cli::command()
            .version(MAPACHE_VERSION_INFO.as_str())
            .get_matches_from(std::env::args_os()),
    )
    .unwrap_or_else(|e| e.exit());

    // Load config if --with-config provided
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

    // Initialize hooks for the current command only
    let command_name = match &args.command {
        Command::Snapshot(_) => Some("snapshot"),
        Command::Restore(_) => Some("restore"),
        Command::Forget(_) => Some("forget"),
        Command::Clean(_) => Some("clean"),
        Command::Verify(_) => Some("verify"),
        _ => None,
    };
    let cmd_hooks =
        command_name.and_then(|name| config.hooks.as_ref().and_then(|h| h.get_command(name)));
    hooks::init(cmd_hooks);

    let (global_result, command_result) = match args.command {
        // Commands without repository URL
        n @ (Command::Bundle(_)
        | Command::Cache(_)
        | Command::Completion(_)
        | Command::Config(_)) => (Ok(GlobalArgs::default_no_repo()), n),
        // Commands with command-specific config merged into args
        Command::Forget(cmd) => {
            let (g, cmd) = resolve_global_with_extra(cmd, &config, &config.forget);
            (g, Command::Forget(cmd))
        }
        Command::Restore(cmd) => {
            let (g, cmd) = resolve_global_with_extra(cmd, &config, &config.restore);
            (g, Command::Restore(cmd))
        }
        Command::Snapshot(cmd) => {
            let (g, cmd) = resolve_global_with_extra(cmd, &config, &config.snapshot);
            (g, Command::Snapshot(cmd))
        }
        // Standard commands — merge global args only
        Command::Amend(cmd) => (resolve_global(&cmd.global, &config), Command::Amend(cmd)),
        Command::Cat(cmd) => (resolve_global(&cmd.global, &config), Command::Cat(cmd)),
        Command::Clean(cmd) => (resolve_global(&cmd.global, &config), Command::Clean(cmd)),
        Command::Copy(cmd) => (resolve_global(&cmd.global, &config), Command::Copy(cmd)),
        Command::Diff(cmd) => (resolve_global(&cmd.global, &config), Command::Diff(cmd)),
        Command::Dump(cmd) => (resolve_global(&cmd.global, &config), Command::Dump(cmd)),
        Command::Find(cmd) => (resolve_global(&cmd.global, &config), Command::Find(cmd)),
        Command::Init(cmd) => (resolve_global(&cmd.global, &config), Command::Init(cmd)),
        Command::Key(cmd) => (resolve_global(&cmd.global, &config), Command::Key(cmd)),
        Command::Log(cmd) => (resolve_global(&cmd.global, &config), Command::Log(cmd)),
        Command::Ls(cmd) => (resolve_global(&cmd.global, &config), Command::Ls(cmd)),
        #[cfg(all(feature = "mount", unix))]
        Command::Mount(cmd) => (resolve_global(&cmd.global, &config), Command::Mount(cmd)),
        Command::RebuildIndex(cmd) => (
            resolve_global(&cmd.global, &config),
            Command::RebuildIndex(cmd),
        ),
        Command::Recall(cmd) => (resolve_global(&cmd.global, &config), Command::Recall(cmd)),
        Command::Rechunk(cmd) => (resolve_global(&cmd.global, &config), Command::Rechunk(cmd)),
        Command::Stats(cmd) => (resolve_global(&cmd.global, &config), Command::Stats(cmd)),
        Command::Sync(cmd) => (resolve_global(&cmd.global, &config), Command::Sync(cmd)),
        Command::Tui(cmd) => (resolve_global(&cmd.global, &config), Command::Tui(cmd)),
        Command::Unlock(cmd) => (resolve_global(&cmd.global, &config), Command::Unlock(cmd)),
        Command::Verify(cmd) => (resolve_global(&cmd.global, &config), Command::Verify(cmd)),
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

    let result: std::result::Result<(), error::CmdError> = match command_result {
        // Commands without global args
        Command::Bundle(cmd) => cmd_bundle::run(&cmd).await.map_err(CmdError::new),
        Command::Cache(cmd) => cmd_cache::run(&cmd).map_err(CmdError::new),
        Command::Completion(cmd) => cmd_completion::run(&cmd).map_err(CmdError::new),
        Command::Config(cmd) => cmd_config::run(&cmd).await.map_err(CmdError::new),
        // Tui needs extra config
        Command::Tui(cmd) => {
            let snapshot_cfg = config.snapshot.clone();
            let forget_cfg = config.forget.clone();
            cmd_tui::run(&global, &cmd.args, snapshot_cfg, forget_cfg)
                .await
                .map_err(CmdError::new)
        }
        // Standard async commands
        Command::Amend(cmd) => cmd_amend::run(&global, &cmd.args)
            .await
            .map_err(CmdError::new),
        Command::Cat(cmd) => cmd_cat::run(&global, &cmd.args)
            .await
            .map_err(CmdError::new),
        Command::Clean(cmd) => cmd_clean::run(&global, &cmd.args)
            .await
            .map_err(CmdError::new),
        Command::Copy(cmd) => cmd_copy::run(&global, &cmd.args)
            .await
            .map_err(CmdError::new),
        Command::Diff(cmd) => cmd_diff::run(&global, &cmd.args)
            .await
            .map_err(CmdError::new),
        Command::Dump(cmd) => cmd_dump::run(&global, &cmd.args)
            .await
            .map_err(CmdError::new),
        Command::Find(cmd) => cmd_find::run(&global, &cmd.args)
            .await
            .map_err(CmdError::new),
        Command::Forget(cmd) => cmd_forget::run(&global, &cmd.args)
            .await
            .map_err(CmdError::new),
        Command::Init(cmd) => cmd_init::run(&global, &cmd.args)
            .await
            .map_err(CmdError::new),
        Command::Key(cmd) => cmd_key::run(&global, &cmd.args)
            .await
            .map_err(CmdError::new),
        Command::Log(cmd) => cmd_log::run(&global, &cmd.args)
            .await
            .map_err(CmdError::new),
        Command::Ls(cmd) => cmd_ls::run(&global, &cmd.args).await.map_err(CmdError::new),
        #[cfg(all(feature = "mount", unix))]
        Command::Mount(cmd) => cmd_mount::run(&global, &cmd.args)
            .await
            .map_err(CmdError::new),
        Command::RebuildIndex(cmd) => cmd_rebuild_index::run(&global, &cmd.args)
            .await
            .map_err(CmdError::new),
        Command::Recall(cmd) => cmd_recall::run(&global, &cmd.args)
            .await
            .map_err(CmdError::new),
        Command::Rechunk(cmd) => cmd_rechunk::run(&global, &cmd.args)
            .await
            .map_err(CmdError::new),
        Command::Restore(cmd) => cmd_restore::run(&global, &cmd.args)
            .await
            .map_err(CmdError::new),
        Command::Snapshot(cmd) => cmd_snapshot::run(&global, &cmd.args)
            .await
            .map_err(CmdError::new),
        Command::Stats(cmd) => cmd_stats::run(&global, &cmd.args)
            .await
            .map_err(CmdError::new),
        Command::Sync(cmd) => cmd_sync::run(&global, &cmd.args)
            .await
            .map_err(CmdError::new),
        Command::Unlock(cmd) => cmd_unlock::run(&global, &cmd.args)
            .await
            .map_err(CmdError::new),
        Command::Verify(cmd) => cmd_verify::run(&global, &cmd.args)
            .await
            .map_err(CmdError::new),
    };

    if let Err(ref e) = result {
        let exit_code = e.exit_code();

        tracing::error!(target: "mapache", "return exit code {}: {}", exit_code, e);

        if !json_enabled {
            ui::cli::error!("{:#}", e);
        } else {
            #[derive(Serialize)]
            struct ErrorMessage {
                msg: String,
                exit_code: i32,
            }

            ui::json::emit_static(
                "exit_error",
                &ErrorMessage {
                    msg: e.to_string(),
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
    fn default_no_repo() -> Self {
        Self {
            repo: String::new(),
            no_cache: false,
            ssh_privatekey: None,
            ssh_known_hosts: None,
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
            no_lock: false,
        }
    }
}

/// Resolves authentication from file/env or falls back to interactive prompt,
/// then retries the open operation on auth errors.
async fn open_with_retry<T, F, Fut>(auth_file: Option<&PathBuf>, open_fn: F) -> Result<T>
where
    F: Fn(Zeroizing<String>, String) -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let auth = match utils::get_auth(&auth_file.cloned()) {
        Ok(a) => a,
        Err(e) => {
            tracing::warn!("{e:#} — falling back to interactive prompt");
            None
        }
    };

    // If auth is provided (from file or env), try it once.
    if let Some(a) = auth {
        return open_fn(a.password, a.username).await;
    }

    // Otherwise, loop with prompts
    const MAX_PASSWORD_RETRIES: u32 = 3;
    let mut password_try_count = 0;

    loop {
        let current_auth = cli::request_auth().map_err(|e| MapacheError::Auth(e.to_string()))?;

        match open_fn(current_auth.password, current_auth.username).await {
            Ok(val) => return Ok(val),
            Err(e) => {
                let is_retryable = matches!(e, MapacheError::Auth(_));

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

/// Helper to open a repository with interactive authentication if needed.
pub async fn open_repository(
    auth_file: Option<&PathBuf>,
    key_file_path: Option<&PathBuf>,
    backend: Arc<dyn StorageBackend>,
    config: RepoConfig,
) -> Result<(Arc<Repository>, Arc<SecureStorage>)> {
    open_with_retry(auth_file, |password, username| {
        let backend = backend.clone();
        async move {
            let auth = Auth { username, password };
            Repository::try_open_unlocked(&auth, key_file_path, backend, config).await
        }
    })
    .await
}

/// Helper to open a repository with a lock and interactive authentication if needed,
/// ensuring the lock is released when the provided closure finishes.
///
/// When `no_lock` is true, the repository is opened without acquiring a lock and a
/// no-op lock handle is returned.
#[allow(clippy::too_many_arguments)]
pub async fn with_repository_lock<F, Fut, T, E>(
    auth_file: Option<&PathBuf>,
    key_file_path: Option<&PathBuf>,
    backend: Arc<dyn StorageBackend>,
    config: RepoConfig,
    exclusive_lock: bool,
    retry_duration: Option<Duration>,
    no_lock: bool,
    f: F,
) -> std::result::Result<T, E>
where
    F: FnOnce(Arc<Repository>, Arc<SecureStorage>, Option<LockHandle>) -> Fut,
    Fut: std::future::Future<Output = std::result::Result<T, E>>,
    E: From<MapacheError>,
{
    let (repo, storage, lock) = if no_lock {
        let (repo, storage) = open_repository(auth_file, key_file_path, backend, config).await?;
        (repo, storage, None)
    } else {
        let (repo, storage, lock) = open_repository_with_lock(
            auth_file,
            key_file_path,
            backend,
            config,
            exclusive_lock,
            retry_duration,
        )
        .await?;
        (repo, storage, Some(lock))
    };

    let res = if let Some(ref lock) = lock {
        f(repo, storage, Some(lock.clone())).await
    } else {
        f(repo, storage, None).await
    };
    if let Some(ref lock) = lock {
        lock.unlock().await;
    }
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
    open_with_retry(auth_file, |password, username| {
        let backend = backend.clone();
        async move {
            let auth = Auth { username, password };
            Repository::try_open_with_lock(
                &auth,
                key_file_path,
                backend,
                config,
                exclusive_lock,
                retry_duration,
            )
            .await
        }
    })
    .await
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
