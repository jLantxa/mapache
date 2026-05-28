use std::{path::PathBuf, str::FromStr, sync::Arc, time::Instant};

use anyhow::Result;
use clap::Args;
use colored::Colorize;
use conflate::Merge;
use serde::{Deserialize, Serialize};

use crate::{
    backend::new_backend_with_prompt,
    commands::{
        GlobalArgs, ToExitCode, UseSnapshot, cleanup::CleanupHandler, fail, find_use_snapshot,
        with_repository_lock,
    },
    fs::{
        calculate_lcp,
        filter::{
            expand_include_paths, merge_filtered_paths, parse_relative_filter_paths,
            read_filtered_paths_from_file,
        },
        get_absolute_normalized_path,
    },
    mapache::defaults::SHORT_SNAPSHOT_ID_LEN,
    repository::{repo::Repository, snapshot::SnapshotPair},
    restorer::{self, RestoreOptions, Strategy},
    ui::{
        self, RestoreProgressReporter, cli::restore::CliRestoreProgressReporter,
        json::restore::JsonRestoreProgressReporter,
    },
    utils,
};

use crate::log;
use crate::log_always;

#[derive(Debug, Clone, Copy)]
pub enum RestoreError {
    RepoOpenFail = 10,
    SnapshotNotFound = 20,
    TargetError = 21,
    RestoreFailed = 30,
}

impl ToExitCode for RestoreError {
    fn to_exit_code(&self) -> i32 {
        *self as i32
    }
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

#[derive(Args, Debug, Clone, Merge, Deserialize, Default)]
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
    #[merge(skip)]
    #[serde(default)]
    pub snapshot: UseSnapshot,

    /// A path where the files will be restored.
    #[clap(long)]
    #[merge(strategy = conflate::option::overwrite_none)]
    #[serde(deserialize_with = "crate::mapache::config::deserialize_config_path_opt")]
    pub target: Option<PathBuf>,

    /// A list of paths to restore: path[,path,...]. Can be used multiple times.
    #[clap(long, value_parser, value_delimiter = ',', num_args = 1..)]
    #[merge(strategy = conflate::option::overwrite_none)]
    pub include: Option<Vec<String>>,

    /// A file containing a list of paths to include, one per line.
    #[clap(long, value_parser)]
    #[merge(strategy = conflate::option::overwrite_none)]
    #[serde(deserialize_with = "crate::mapache::config::deserialize_config_path_opt")]
    pub include_file: Option<PathBuf>,

    /// A list of paths to exclude: path[,path,...]. Can be used multiple times.
    #[clap(long, value_parser, value_delimiter = ',', num_args = 1..)]
    #[merge(strategy = conflate::option::overwrite_none)]
    pub exclude: Option<Vec<String>>,

    /// A file containing a list of paths to exclude, one per line.
    #[clap(long, value_parser)]
    #[merge(strategy = conflate::option::overwrite_none)]
    #[serde(deserialize_with = "crate::mapache::config::deserialize_config_path_opt")]
    pub exclude_file: Option<PathBuf>,

    /// Strip the longest common prefix from all restored routes.
    #[clap(long, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
    #[merge(strategy = conflate::option::overwrite_none)]
    pub strip_prefix: Option<bool>,

    /// Method for conflict resolution in case a file or directory already exists in the target location.
    ///
    /// fail: Terminates the command with an error.
    /// skip: Skips restoring and keeps the local item.
    /// overwrite: Overwrites the item in the target location with the node from the snapshot.
    /// newer: Keeps the item with the more recent modified time.
    #[clap(long = "strategy")]
    #[merge(strategy = conflate::option::overwrite_none)]
    #[serde(deserialize_with = "deserialize_strategy_opt")]
    pub strategy: Option<Strategy>,

    /// Delete files in the target directory that are not present in the snapshot.
    /// Use with caution.
    #[clap(long, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
    #[merge(strategy = conflate::option::overwrite_none)]
    pub delete: Option<bool>,

    /// When used with --delete, also delete nodes in the same level as the root.
    #[clap(long, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true", requires = "delete")]
    #[merge(strategy = conflate::option::overwrite_none)]
    pub no_preserve_root: Option<bool>,

    /// Quit immediately if a restore error occurs
    #[clap(long, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
    #[merge(strategy = conflate::option::overwrite_none)]
    pub quit_on_error: Option<bool>,

    /// Create sparse files instead of preallocating them.
    /// This is faster on some filesystems but may cause higher disk fragmentation.
    #[clap(long, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
    #[merge(strategy = conflate::option::overwrite_none)]
    pub sparse: Option<bool>,

    /// Force verification of existing files by content (hashing) even if mtime matches.
    #[clap(long, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
    #[merge(strategy = conflate::option::overwrite_none)]
    pub verify: Option<bool>,

    /// Dry run
    #[clap(long)]
    #[merge(skip)]
    pub dry_run: bool,
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

pub async fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    tracing::info!(target: "restore", "Starting restore command");

    let json_output = global_args.json;

    with_repository_lock(
        global_args.auth_file.as_ref(),
        global_args.key.as_ref(),
        new_backend_with_prompt(global_args.backend_options(false))
            .await
            .map_err(|e| {
                fail(
                    format!("Failed to initialize backend: {}", e),
                    RestoreError::RestoreFailed,
                )
            })?,
        global_args.to_repo_config(),
        false,
        global_args.retry_lock_duration,
        |repo, _secure_storage, lock_handle| async move {
            repo.reload_master_index().await?;

            let (id, snap) = find_use_snapshot(repo.clone(), &args.snapshot)
                .await
                .map_err(|_| fail("Snapshot not found", RestoreError::SnapshotNotFound))?
                .ok_or_else(|| fail("Snapshot not found", RestoreError::SnapshotNotFound))?;
            let pair = SnapshotPair { id, snapshot: snap };

            let target = args.target.as_ref().ok_or_else(|| {
                fail(
                    "Target path is required. Use --target or set it in config file.",
                    RestoreError::TargetError,
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
                        snapshot: pair.id.to_short_hex(SHORT_SNAPSHOT_ID_LEN),
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
                    pair.id.to_short_hex(SHORT_SNAPSHOT_ID_LEN).bold().yellow()
                );
            }

            let progress_reporter: Arc<dyn RestoreProgressReporter> = if json_output {
                Arc::new(JsonRestoreProgressReporter::new(None, None))
            } else {
                Arc::new(CliRestoreProgressReporter::new(None, None))
            };

            let start = Instant::now();

            run_with_repo(
                repo,
                lock_handle,
                args,
                progress_reporter.clone(),
                pair,
                start,
                json_output,
            )
            .await?;

            Ok(())
        },
    )
    .await
    .map_err(|e| {
        if e.is::<crate::commands::error::MapacheError>() {
            e
        } else {
            fail(
                format!("Failed to open repository: {}", e),
                RestoreError::RepoOpenFail,
            )
        }
    })
}

pub async fn run_with_repo(
    repo: Arc<Repository>,
    lock_handle: crate::repository::lock::LockHandle,
    args: &CmdArgs,
    progress_reporter: Arc<dyn RestoreProgressReporter>,
    pair: SnapshotPair,
    start: std::time::Instant,
    json_output: bool,
) -> Result<()> {
    let target = args.target.as_ref().ok_or_else(|| {
        fail(
            "Target path is required. Use --target or set it in config file.",
            RestoreError::TargetError,
        )
    })?;

    let strategy = args.strategy.clone().unwrap_or(Strategy::Fail);
    let dry_run = args.dry_run;
    let delete = args.delete.unwrap_or(false);
    let no_preserve_root = args.no_preserve_root.unwrap_or(false);
    let quit_on_error = args.quit_on_error.unwrap_or(false);
    let sparse = args.sparse.unwrap_or(false);
    let verify = args.verify.unwrap_or(false);
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
        &pair.id,
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

    let abs_normalized_target = get_absolute_normalized_path(target).map_err(|e| {
        fail(
            format!("Invalid target path: {}", e),
            RestoreError::TargetError,
        )
    })?;

    tracing::info!(target: "restore", "Restoring snapshot {} (root={:?}) to {:?}", pair.id, pair.snapshot.root, abs_normalized_target);

    let reporter_clone = progress_reporter.clone();
    let cleanup_handler = CleanupHandler::new_with_callback(move || {
        reporter_clone.finalize();
    })?;
    cleanup_handler.add_lock(lock_handle.clone());

    let restore_result = restorer::restore(
        repo.clone(),
        &pair.snapshot,
        &abs_normalized_target,
        parsed_includes.clone(),
        parsed_excludes.clone(),
        RestoreOptions {
            dry_run,
            strategy,
            quit_on_error,
            strip_prefix: common_prefix,
            preallocate: !sparse,
            verify,
        },
        progress_reporter.clone(),
        cleanup_handler.interrupted.clone(),
    )
    .await;

    if restore_result.is_err() && cleanup_handler.is_interrupted() {
        tracing::info!(target: "restore", "Restore interrupted by user");
        if !json_output {
            ui::cli::log!("Restore interrupted by user.");
        }
        progress_reporter.finalize();
        return Ok(());
    }
    restore_result?;

    progress_reporter.finalize();

    if delete {
        tracing::info!(target: "restore", "Starting post-restore cleanup (delete)");
        let delete_result = restorer::delete_nodes(
            repo,
            abs_normalized_target.clone(),
            &pair.snapshot.tree,
            parsed_includes,
            parsed_excludes,
            dry_run,
            no_preserve_root,
            cleanup_handler.interrupted.clone(),
            progress_reporter.clone(),
        )
        .await;

        if delete_result.is_err() && cleanup_handler.is_interrupted() {
            tracing::info!(target: "restore", "Post-restore cleanup interrupted by user");
            if !json_output {
                ui::cli::log!("Post-restore cleanup interrupted by user.");
            }
            return Ok(());
        }
        delete_result?;
    }

    tracing::info!(target: "restore", "Restore command completed");
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
                errors: progress_reporter.error_count(),
                warnings: progress_reporter.warning_count(),
                total_items: progress_reporter.total_items(),
                total_bytes: progress_reporter.total_bytes(),
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
            "{}Finished in {} with {} and {}",
            prefix,
            utils::pretty_print_duration(start.elapsed()),
            utils::format_count(progress_reporter.error_count(), "error", "errors"),
            utils::format_count(progress_reporter.warning_count(), "warning", "warnings"),
        );
    }
    Ok(())
}
