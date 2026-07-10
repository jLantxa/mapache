use std::{
    io,
    path::PathBuf,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use clap::Args;
use serde::{Deserialize, Serialize};

use crate::{
    backend::new_backend_with_prompt,
    commands::{
        GlobalArgs, Merge, ToExitCode, UseSnapshot, cleanup::CleanupHandler, find_use_snapshot,
        merge_opt, with_repository_lock,
    },
    common::{defaults::SHORT_SNAPSHOT_ID_LEN, error::MapacheError, hooks},
    fs::{
        calculate_lcp,
        filter::{
            expand_include_paths, merge_filtered_paths, parse_relative_filter_paths,
            read_filtered_paths_from_file,
        },
        get_absolute_normalized_path,
    },
    log, log_always,
    repository::{lock::LockHandle, repo::Repository, snapshot::SnapshotPair},
    restorer::{self, RestoreOptions, Strategy},
    ui::{
        self,
        cli::{color::Colorize, restore},
        events::{Event, EventSender, RestoreEvent, emit_event},
        json::restore as json_restore,
    },
    utils,
};

#[derive(Debug, thiserror::Error)]
pub enum RestoreError {
    #[error("failed to open repository: {0}")]
    RepoOpenFail(String),
    #[error("snapshot not found: {0}")]
    SnapshotNotFound(String),
    #[error("target path error: {0}")]
    TargetError(String),
    #[error("restore failed: {0}")]
    RestoreFailed(String),
    #[error("restore interrupted by user")]
    Interrupted,
    #[error(transparent)]
    Repo(#[from] MapacheError),
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl ToExitCode for RestoreError {
    fn to_exit_code(&self) -> i32 {
        match self {
            RestoreError::RepoOpenFail(_) => 10,
            RestoreError::SnapshotNotFound(_) => 20,
            RestoreError::TargetError(_) => 21,
            RestoreError::RestoreFailed(_) => 30,
            RestoreError::Interrupted => 130,
            RestoreError::Repo(_) => 1,
            RestoreError::Io(_) => 1,
        }
    }
}

pub(crate) struct RestoreCounters {
    pub(crate) event_sender: EventSender,
    pub(crate) error_count: Arc<AtomicU64>,
    pub(crate) warning_count: Arc<AtomicU64>,
    pub(crate) total_items: Arc<AtomicU64>,
    pub(crate) total_bytes: Arc<AtomicU64>,
}

impl std::fmt::Display for Strategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Strategy::Overwrite => write!(f, "overwrite"),
            Strategy::Skip => write!(f, "skip"),
            Strategy::Newer => write!(f, "newer"),
            Strategy::Fail => write!(f, "fail"),
        }
    }
}

#[derive(Args, Debug, Clone, Serialize, Deserialize, Default)]
#[clap(
    about = "Restore a snapshot in a target path",
    long_about = "Restore a snapshot in a target path. Running this command in \
    --dry-run mode simulates the restoration of a snapshot, and can be used to \
    detect errors before running the actual restore."
)]
#[serde(default, rename_all = "kebab-case")]
pub struct CmdArgs {
    /// The ID of the snapshot to restore, or 'latest' to restore the most recent snapshot saved.
    #[arg(value_parser = clap::value_parser!(UseSnapshot), default_value_t=UseSnapshot::Latest)]
    #[serde(default)]
    pub snapshot: UseSnapshot,

    /// A path where the files will be restored.
    #[clap(long)]
    #[serde(deserialize_with = "crate::common::config::deserialize_config_path_opt")]
    pub target: Option<PathBuf>,

    /// A list of paths to restore: path[,path,...]. Can be used multiple times.
    #[clap(long, value_parser, value_delimiter = ',', num_args = 1..)]
    pub include: Option<Vec<String>>,

    /// A file containing a list of paths to include, one per line.
    #[clap(long, value_parser)]
    #[serde(deserialize_with = "crate::common::config::deserialize_config_path_opt")]
    pub include_file: Option<PathBuf>,

    /// A list of paths to exclude: path[,path,...]. Can be used multiple times.
    #[clap(long, value_parser, value_delimiter = ',', num_args = 1..)]
    pub exclude: Option<Vec<String>>,

    /// A file containing a list of paths to exclude, one per line.
    #[clap(long, value_parser)]
    #[serde(deserialize_with = "crate::common::config::deserialize_config_path_opt")]
    pub exclude_file: Option<PathBuf>,

    /// Strip the longest common prefix from all restored routes.
    #[clap(long, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
    pub strip_prefix: Option<bool>,

    /// Method for conflict resolution in case a file or directory already exists in the target location.
    ///
    /// fail: Terminates the command with an error.
    /// skip: Skips restoring and keeps the local item.
    /// overwrite: Overwrites the item in the target location with the node from the snapshot.
    /// newer: Keeps the item with the more recent modified time.
    #[clap(long = "strategy")]
    #[serde(deserialize_with = "deserialize_strategy_opt")]
    pub strategy: Option<Strategy>,

    /// Delete files in the target directory that are not present in the snapshot.
    /// Use with caution.
    #[clap(long, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
    pub delete: Option<bool>,

    #[clap(long, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true", requires = "delete")]
    pub no_preserve_root: Option<bool>,

    /// Quit immediately if a restore error occurs
    #[clap(long, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
    pub quit_on_error: Option<bool>,

    /// Create sparse files instead of preallocating them.
    /// This is faster on some filesystems but may cause higher disk fragmentation.
    #[clap(long, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
    pub sparse: Option<bool>,

    /// Force verification of existing files by content (hashing) even if mtime matches.
    #[clap(long, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
    pub verify: Option<bool>,

    /// Maximum number of files per batch. Limits peak memory when restoring many
    /// small files. Default: no limit (all files processed in a single batch).
    #[clap(long)]
    pub batch_size: Option<usize>,

    /// Dry run
    #[clap(long)]
    pub dry_run: bool,
}

impl CmdArgs {
    pub(crate) fn template() -> Self {
        Self {
            snapshot: UseSnapshot::Latest,
            target: Some(PathBuf::from("/tmp/restore")),
            include: None,
            include_file: None,
            exclude: Some(vec!["**/node_modules".to_string()]),
            exclude_file: None,
            strip_prefix: Some(true),
            strategy: Some(Strategy::Fail),
            delete: Some(false),
            batch_size: None,
            dry_run: false,
            no_preserve_root: Some(false),
            quit_on_error: Some(false),
            sparse: Some(false),
            verify: Some(true),
        }
    }
}

impl Merge for CmdArgs {
    fn merge(&mut self, other: Self) {
        merge_opt(&mut self.target, other.target);
        merge_opt(&mut self.include, other.include);
        merge_opt(&mut self.include_file, other.include_file);
        merge_opt(&mut self.exclude, other.exclude);
        merge_opt(&mut self.exclude_file, other.exclude_file);
        merge_opt(&mut self.strip_prefix, other.strip_prefix);
        merge_opt(&mut self.strategy, other.strategy);
        merge_opt(&mut self.delete, other.delete);
        merge_opt(&mut self.no_preserve_root, other.no_preserve_root);
        merge_opt(&mut self.quit_on_error, other.quit_on_error);
        merge_opt(&mut self.sparse, other.sparse);
        merge_opt(&mut self.verify, other.verify);
        merge_opt(&mut self.batch_size, other.batch_size);
    }
}

fn deserialize_strategy_opt<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<Strategy>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt = Option::<String>::deserialize(deserializer)?;
    opt.map(|s| Strategy::from_str(&s).map_err(serde::de::Error::custom))
        .transpose()
}

pub async fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<(), RestoreError> {
    tracing::info!(target: "restore", "Starting restore command");

    let json_output = global_args.json;
    let dry_run = args.dry_run;

    let repo_result = with_repository_lock(
        global_args.auth_file.as_ref(),
        global_args.key.as_ref(),
        new_backend_with_prompt(global_args.backend_options(false)).await?,
        global_args.to_repo_config(),
        false,
        global_args.retry_lock_duration,
        global_args.no_lock,
        |repo, _secure_storage, lock_handle| async move {
            repo.reload_master_index().await?;

            let (id, snap) = find_use_snapshot(repo.clone(), &args.snapshot)
                .await
                .map_err(|e| RestoreError::SnapshotNotFound(e.inner()))?
                .ok_or_else(|| {
                    RestoreError::SnapshotNotFound(
                        "no snapshot matches the given identifier".to_string(),
                    )
                })?;
            let snapshot_pair = SnapshotPair { id, snapshot: snap };

            let target = args.target.as_ref().ok_or_else(|| {
                RestoreError::TargetError(
                    "target path is required. use --target or set it in config file.".to_string(),
                )
            })?;

            if json_output {
                #[derive(Serialize)]
                struct RestoreStartMsg {
                    snapshot: String,
                    target: String,
                    dry_run: bool,
                    strategy: String,
                }

                ui::json::emit_static(
                    "restore_start",
                    &RestoreStartMsg {
                        snapshot: snapshot_pair.id.to_short_hex(SHORT_SNAPSHOT_ID_LEN),
                        target: target.display().to_string(),
                        dry_run: args.dry_run,
                        strategy: args.strategy.clone().unwrap_or(Strategy::Fail).to_string(),
                    },
                );
            } else {
                if args.dry_run {
                    log!("{}", "[DRY RUN]".bold().purple());
                }
                log_always!(
                    "Restoring snapshot {}",
                    snapshot_pair
                        .id
                        .to_short_hex(SHORT_SNAPSHOT_ID_LEN)
                        .bold()
                        .yellow()
                );
            }

            let (event_sender, error_count, warning_count, total_items, total_bytes) =
                if json_output {
                    json_restore::make_event_sender(None, None)
                } else {
                    restore::make_event_sender(None, None)
                };

            let counters = RestoreCounters {
                event_sender,
                error_count,
                warning_count,
                total_items,
                total_bytes,
            };

            let start = Instant::now();

            if !dry_run {
                // Run pre-hook: abort command if it fails
                hooks::run_pre(hooks::restore(), "restore", &global_args.repo).await?;
            }

            run_with_repo(
                repo,
                lock_handle,
                args,
                counters,
                snapshot_pair,
                start,
                json_output,
            )
            .await?;

            Ok(())
        },
    )
    .await;

    let result_str = match &repo_result {
        Ok(_) => "success".to_string(),
        Err(e) => format!("{e}"),
    };
    if !dry_run {
        // Run post-hook: warning on failure, always continues
        hooks::run_post(hooks::restore(), "restore", &global_args.repo, &result_str).await;
    }

    repo_result
}

pub(crate) async fn run_with_repo(
    repo: Arc<Repository>,
    lock_handle: Option<LockHandle>,
    args: &CmdArgs,
    counters: RestoreCounters,
    snapshot_pair: SnapshotPair,
    start: Instant,
    json_output: bool,
) -> Result<(), RestoreError> {
    let target = args.target.as_ref().ok_or_else(|| {
        RestoreError::TargetError(
            "target path is required. use --target or set it in config file.".to_string(),
        )
    })?;

    let strategy = args.strategy.clone().unwrap_or(Strategy::Fail);
    let dry_run = args.dry_run;
    let delete = args.delete.unwrap_or(false);
    let no_preserve_root = args.no_preserve_root.unwrap_or(false);
    let quit_on_error = args.quit_on_error.unwrap_or(false);
    let sparse = args.sparse.unwrap_or(false);
    let verify = args.verify.unwrap_or(false);
    let batch_size = args.batch_size;
    let strip_prefix = args.strip_prefix.unwrap_or(false);

    // Read include and exclude paths from files if provided.
    let excludes_from_file = match &args.exclude_file {
        Some(path) => Some(read_filtered_paths_from_file(path)?),
        None => None,
    };
    let all_excludes = merge_filtered_paths(args.exclude.as_ref(), excludes_from_file.as_ref());
    let includes_from_file = match &args.include_file {
        Some(path) => Some(read_filtered_paths_from_file(path)?),
        None => None,
    };
    let all_includes = merge_filtered_paths(args.include.as_ref(), includes_from_file.as_ref());

    let parsed_excludes = parse_relative_filter_paths(all_excludes.as_ref());
    let parsed_includes = expand_include_paths(
        repo.clone(),
        &snapshot_pair.id,
        all_includes.as_deref(),
        parsed_excludes.clone(),
    )
    .await?;

    let common_prefix: Option<PathBuf> = if strip_prefix {
        parsed_includes
            .as_ref()
            .map(|includes| calculate_lcp(includes, false))
    } else {
        None
    };

    let abs_normalized_target = get_absolute_normalized_path(target)
        .map_err(|e| RestoreError::TargetError(format!("invalid target path: {}", e.inner())))?;

    tracing::info!(target: "restore", "Restoring snapshot {} (root={:?}) to {:?}", snapshot_pair.id, snapshot_pair.snapshot.root, abs_normalized_target);

    let event_sender_for_cleanup = counters.event_sender.clone();
    let cleanup_handler = CleanupHandler::new_with_callback(move || {
        emit_event(
            &event_sender_for_cleanup,
            Event::Restore(RestoreEvent::Finished),
        );
    });
    cleanup_handler.add_lock(lock_handle);

    let restore_result = restorer::restore(
        repo.clone(),
        &snapshot_pair.snapshot,
        &abs_normalized_target,
        RestoreOptions {
            dry_run,
            strategy,
            quit_on_error,
            strip_prefix: common_prefix,
            preallocate: !sparse,
            verify,
            include: parsed_includes.clone(),
            exclude: parsed_excludes.clone(),
            batch_size,
        },
        counters.event_sender.clone(),
        cleanup_handler.interrupted.clone(),
    )
    .await;

    if restore_result.is_err() && cleanup_handler.is_interrupted() {
        tracing::info!(target: "restore", "Restore interrupted by user");
        emit_event(
            &counters.event_sender,
            Event::Restore(RestoreEvent::Finished),
        );
        return Err(RestoreError::Interrupted);
    }
    restore_result.map_err(|e| RestoreError::RestoreFailed(e.inner()))?;

    emit_event(
        &counters.event_sender,
        Event::Restore(RestoreEvent::Finished),
    );

    if delete {
        tracing::info!(target: "restore", "Starting post-restore cleanup (delete)");
        let delete_result = restorer::delete_nodes(
            repo,
            abs_normalized_target.clone(),
            &snapshot_pair.snapshot.tree,
            restorer::SyncOpts {
                include: parsed_includes,
                exclude: parsed_excludes,
                dry_run,
                no_preserve_root,
                shutdown_signal: cleanup_handler.interrupted.clone(),
                event_sender: counters.event_sender.clone(),
            },
        )
        .await;

        if delete_result.is_err() && cleanup_handler.is_interrupted() {
            tracing::info!(target: "restore", "Post-restore cleanup interrupted by user");
            return Err(RestoreError::Interrupted);
        }
        delete_result
            .map_err(|e| RestoreError::RestoreFailed(format!("delete failed: {}", e.inner())))?;
    }

    tracing::info!(target: "restore", "Restore command completed");
    let errs = counters.error_count.load(Ordering::Relaxed);
    let warns = counters.warning_count.load(Ordering::Relaxed);
    let items = counters.total_items.load(Ordering::Relaxed);
    let bytes = counters.total_bytes.load(Ordering::Relaxed);
    if json_output {
        #[derive(Serialize)]
        struct RestoreCompleteMsg {
            duration_seconds: f64,
            errors: u64,
            warnings: u64,
            total_items: u64,
            total_bytes: u64,
            dry_run: bool,
        }

        ui::json::emit_static(
            "restore_complete",
            &RestoreCompleteMsg {
                duration_seconds: start.elapsed().as_secs_f64(),
                errors: errs,
                warnings: warns,
                total_items: items,
                total_bytes: bytes,
                dry_run,
            },
        );
    } else {
        let prefix = if dry_run {
            format!("{} ", "[DRY RUN]".bold().purple())
        } else {
            String::new()
        };
        ui::cli::log!(
            "{}Restored {} ({}) in {} with {} and {}",
            prefix,
            utils::format_count(items, "item", "items"),
            utils::format_size_binary(bytes, 3),
            utils::pretty_print_duration(start.elapsed()),
            utils::format_count(errs, "error", "errors"),
            utils::format_count(warns, "warning", "warnings"),
        );
    }
    Ok(())
}
