use std::{
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::Instant,
};

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
    mapache::{ID, defaults::SHORT_SNAPSHOT_ID_LEN},
    restorer::{self, RestoreOptions, Strategy},
    ui::{
        self,
        restore::{
            CliRestoreProgressReporter, JsonRestoreProgressReporter, RestoreProgressReporter,
        },
    },
    utils::{self},
};

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

    with_repository_lock(
        global_args.auth_file.as_ref(),
        global_args.key.as_ref(),
        new_backend_with_prompt(global_args.backend_options(dry_run))
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
            let start = Instant::now();

            tracing::info!(target: "restore", "Reloading master index");
            repo.reload_master_index().await?;

            tracing::info!(target: "restore", "Finding snapshot to restore (snapshot={:?})", args.snapshot);
            let (snapshot_id, snapshot) =
                match find_use_snapshot(repo.clone(), &args.snapshot).await {
                    Ok(Some((id, snap))) => (id, snap),
                    Ok(None) | Err(_) => {
                        return Err(fail("Snapshot not found", RestoreError::SnapshotNotFound));
                    }
                };

            // Read include and exclude paths from files if provided.
            let excludes_from_file = match &args.exclude_file {
                Some(path) => Some(read_filtered_paths_from_file(path)?),
                None => None,
            };
            let all_excludes =
                merge_filtered_paths(args.exclude.as_ref(), excludes_from_file.as_ref());
            let includes_from_file = match &args.include_file {
                Some(path) => Some(read_filtered_paths_from_file(path)?),
                None => None,
            };
            let all_includes =
                merge_filtered_paths(args.include.as_ref(), includes_from_file.as_ref());

            let parsed_excludes = parse_relative_filter_paths(all_excludes.as_ref());
            let parsed_includes = expand_include_paths(
                repo.clone(),
                &snapshot_id,
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

            let abs_normalized_target =
                get_absolute_normalized_path(target).map_err(|e| {
                    fail(
                        format!("Invalid target path: {}", e),
                        RestoreError::TargetError,
                    )
                })?;

            tracing::info!(target: "restore", "Restoring snapshot {snapshot_id} (root={:?}) to {:?}", snapshot.root, abs_normalized_target);

            emit_restore_start(global_args.json, &snapshot_id, &abs_normalized_target, dry_run, &strategy);

            // We initialize the reporter with 0 totals. The Restorer will resize_workload it
            // with actual totals once it finishes planning (which is now the same as scanning).
            let progress_reporter = make_restore_progress_reporter(global_args.json, None, None);

            let json_mode = global_args.json;
            let reporter_clone = progress_reporter.clone();
            let cleanup_handler = CleanupHandler::new_with_callback(move || {
                reporter_clone.finalize();
                if !json_mode {
                    ui::cli::log!(
                        "\n{}",
                        "Process interrupted. Cleaning up...".bold().yellow()
                    );
                }
            })?;
            cleanup_handler.add_lock(lock_handle.clone());

            restorer::restore(
                repo.clone(),
                &snapshot,
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
            .await?;

            progress_reporter.finalize();
            let warning_count = progress_reporter.warning_count();
            let error_count = progress_reporter.error_count();

            if delete {
                // Delete local nodes not present in the snapshot tree
                tracing::info!(target: "restore", "Starting post-restore cleanup (delete)");
                restorer::sync::delete_nodes(
                    repo,
                    abs_normalized_target.clone(),
                    &snapshot.tree,
                    parsed_includes,
                    parsed_excludes,
                    dry_run,
                    no_preserve_root,
                    cleanup_handler.interrupted.clone(),
                )
                .await?;

                if !global_args.json {
                    ui::cli::log!();
                }
            }

            emit_restore_complete(
                global_args.json,
                start.elapsed().as_secs_f64(),
                error_count,
                warning_count,
                dry_run,
            );
            tracing::info!(target: "restore", "Restore command completed in {:?}", start.elapsed());

            Ok(())
        },
    )
    .await
    .map_err(|e| {
        // If it's already a MapacheError (e.g. from fail calls inside), keep it.
        // Otherwise, it might be an open error.
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

fn emit_restore_start(
    json: bool,
    snapshot_id: &ID,
    target: &Path,
    dry_run: bool,
    strategy: &Strategy,
) {
    if json {
        #[derive(Serialize)]
        struct RestoreStartMsg {
            snapshot: String,
            target: String,
            dry_run: bool,
            strategy: String,
        }

        ui::json_reporter::emit_static(
            "restore_start",
            &RestoreStartMsg {
                snapshot: snapshot_id.to_short_hex(SHORT_SNAPSHOT_ID_LEN),
                target: target.display().to_string(),
                dry_run,
                strategy: strategy.to_string(),
            },
        );
    } else {
        if dry_run {
            ui::cli::log!("{}", "[DRY RUN]".bold().purple());
        }

        ui::cli::log!(
            "Restoring snapshot {}",
            snapshot_id
                .to_short_hex(SHORT_SNAPSHOT_ID_LEN)
                .bold()
                .yellow()
        );
    }
}

fn make_restore_progress_reporter(
    json: bool,
    num_expected_items: Option<u64>,
    total_bytes: Option<u64>,
) -> Arc<dyn RestoreProgressReporter> {
    if json {
        Arc::new(JsonRestoreProgressReporter::new(
            num_expected_items,
            total_bytes,
        ))
    } else {
        Arc::new(CliRestoreProgressReporter::new(
            num_expected_items,
            total_bytes,
        ))
    }
}

fn emit_restore_complete(
    json: bool,
    duration_seconds: f64,
    errors: u64,
    warnings: u64,
    dry_run: bool,
) {
    if json {
        #[derive(Serialize)]
        struct RestoreCompleteMsg {
            duration_seconds: f64,
            errors: u64,
            warnings: u64,
            dry_run: bool,
        }

        ui::json_reporter::emit_static(
            "restore_complete",
            &RestoreCompleteMsg {
                duration_seconds,
                errors,
                warnings,
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
            utils::pretty_print_duration(std::time::Duration::from_secs_f64(duration_seconds)),
            utils::format_count(errors, "error", "errors"),
            utils::format_count(warnings, "warning", "warnings")
        );
    }
}
