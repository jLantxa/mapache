use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use anyhow::{Result, bail};
use clap::Args;
use colored::Colorize;
use futures::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use serde::Serialize;

use crate::{
    backend::new_backend_with_prompt,
    commands::{GlobalArgs, UseSnapshot, cleanup::CleanupHandler, find_use_snapshot},
    fs::{
        calculate_lcp,
        filter::{expand_include_paths, parse_relative_filter_paths},
        get_absolute_normalized_path,
        tree::SerializedNodeStream,
    },
    mapache::{defaults::SHORT_SNAPSHOT_ID_LEN, global::GlobalOpts},
    repository::repo::{RepoConfig, Repository},
    restorer::{self, RestoreOptions, Strategy},
    ui::{
        self, SPINNER_TICK_CHARS, default_bar_draw_target,
        restore::{
            CliRestoreProgressReporter, JsonRestoreProgressReporter, RestoreProgressReporter,
        },
    },
    utils::{self, format_size_binary, size},
};

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

#[derive(Args, Debug)]
#[clap(
    about = "Restore a snapshot in a target path",
    long_about = "Restore a snapshot in a target path. Running this command in \
    --dry-run mode simulates the restoration of a snapshot, and can be used to \
    detect errors before running the actual restore."
)]
pub struct CmdArgs {
    /// The ID of the snapshot to restore, or 'latest' to restore the most recent snapshot saved.
    #[arg(value_parser = clap::value_parser!(UseSnapshot), default_value_t=UseSnapshot::Latest)]
    pub snapshot: UseSnapshot,

    /// A path where the files will be restored.
    #[clap(long, required = true)]
    pub target: PathBuf,

    /// A list of paths to restore: path[,path,...]. Can be used multiple times.
    #[clap(long, value_parser, required = false, value_delimiter = ',', num_args = 1..)]
    pub include: Option<Vec<String>>,

    /// A list of paths to exclude: path[,path,...]. Can be used multiple times.
    #[clap(long, value_parser, required = false, value_delimiter = ',', num_args = 1..)]
    pub exclude: Option<Vec<String>>,

    /// Strip the longest common prefix from all restored routes.
    #[clap(long, value_parser, default_value_t = false)]
    pub strip_prefix: bool,

    /// Method for conflict resolution in case a file or directory already exists in the target location.
    ///
    /// fail: Terminates the command with an error.
    /// skip: Skips restoring and keeps the local item.
    /// overwrite: Overwrites the item in the target location with the node from the snapshot.
    /// newer: Keeps the item with the more recent modified time.
    #[clap(long = "strategy", default_value_t=Strategy::Fail)]
    pub strategy: Strategy,

    /// Delete files in the target directory that are not present in the snapshot.
    /// Use with caution.
    #[clap(long, value_parser, default_value_t = false)]
    pub delete: bool,

    /// When used with --delete, also delete nodes in the same level as the root.
    #[clap(long, value_parser, default_value_t = false, requires = "delete")]
    pub no_preserve_root: bool,

    /// Quit immediately if a restore error occurs
    #[clap(long, default_value_t = false)]
    pub quit_on_error: bool,

    /// Eagerly preallocate files before restoring contents.
    /// This may avoid failures on nearly full disks, but can be slower.
    #[clap(long, default_value_t = false)]
    pub preallocate: bool,

    /// Dry run
    #[clap(long, default_value_t = false)]
    pub dry_run: bool,
}

pub async fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let auth = utils::get_auth_from_file(&global_args.auth_file)?;
    let backend = new_backend_with_prompt(global_args.backend_options(args.dry_run)).await?;

    let config = RepoConfig {
        pack_size: (global_args.pack_size_mib * size::MiB as f32) as u64,
        use_cache: !global_args.no_cache,
        compression: global_args.compression_level,
    };
    let (repo, _, mut lock_handle) = Repository::try_open_with_lock(
        auth.as_ref(),
        global_args.key.as_ref(),
        backend,
        config,
        false,
        global_args.retry_lock_duration,
    )
    .await?;

    let start = Instant::now();

    repo.reload_master_index().await?;

    let (snapshot_id, snapshot) = match find_use_snapshot(repo.clone(), &args.snapshot).await {
        Ok(Some((id, snap))) => (id, snap),
        Ok(None) | Err(_) => bail!("Snapshot not found"),
    };

    let parsed_excludes = parse_relative_filter_paths(args.exclude.as_ref());
    let parsed_includes = expand_include_paths(
        repo.clone(),
        &snapshot_id,
        args.include.as_deref(),
        parsed_excludes.clone(),
    )
    .await?;

    let common_prefix: Option<PathBuf> = if args.strip_prefix {
        parsed_includes
            .as_ref()
            .map(|includes| calculate_lcp(includes, false))
    } else {
        None
    };

    let abs_normalized_target = get_absolute_normalized_path(&args.target)?;

    emit_restore_start(global_args.json, args, &snapshot_id, &abs_normalized_target);

    let (num_files, num_dirs, num_expected_items, total_bytes) = scan_restore_plan(
        repo.clone(),
        snapshot.tree,
        parsed_includes.clone(),
        parsed_excludes.clone(),
    )
    .await?;

    emit_restore_plan(
        global_args.json,
        num_files,
        num_dirs,
        num_expected_items,
        total_bytes,
    );

    const NUM_SHOWN_PROCESSING_ITEMS: usize = 1;
    let progress_reporter = make_restore_progress_reporter(
        global_args.json,
        num_expected_items,
        total_bytes,
        NUM_SHOWN_PROCESSING_ITEMS,
    );

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
            dry_run: args.dry_run,
            strategy: args.strategy.clone(),
            quit_on_error: args.quit_on_error,
            strip_prefix: common_prefix,
            preallocate: args.preallocate,
        },
        progress_reporter.clone(),
        cleanup_handler.interrupted.clone(),
    )
    .await?;

    progress_reporter.finalize();
    let warning_count = progress_reporter.warning_count();
    let error_count = progress_reporter.error_count();

    if args.delete {
        // Delete local nodes not present in the snapshot tree
        restorer::sync::delete_nodes(
            repo,
            abs_normalized_target.clone(),
            &snapshot.tree,
            parsed_includes,
            parsed_excludes,
            args.dry_run,
            args.no_preserve_root,
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
        args.dry_run,
    );

    lock_handle.unlock().await;

    Ok(())
}

fn emit_restore_start(json: bool, args: &CmdArgs, snapshot_id: &crate::mapache::ID, target: &Path) {
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
                dry_run: args.dry_run,
                strategy: args.strategy.to_string(),
            },
        );
    } else {
        if args.dry_run {
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

fn emit_restore_plan(json: bool, files: u64, directories: u64, total_items: u64, total_bytes: u64) {
    if json {
        #[derive(Serialize)]
        struct RestorePlanMsg {
            files: u64,
            directories: u64,
            total_items: u64,
            total_bytes: u64,
        }

        ui::json_reporter::emit_static(
            "restore_plan",
            &RestorePlanMsg {
                files,
                directories,
                total_items,
                total_bytes,
            },
        );
    } else {
        ui::cli::log!(
            "{} {} files, {} directories, {}\n",
            "To restore:".bold().cyan(),
            files,
            directories,
            utils::format_size_binary(total_bytes, 3),
        );
    }
}

fn make_restore_progress_reporter(
    json: bool,
    num_expected_items: u64,
    total_bytes: u64,
    num_display_items: usize,
) -> Arc<dyn RestoreProgressReporter> {
    if json {
        Arc::new(JsonRestoreProgressReporter::new(
            num_expected_items,
            total_bytes,
            num_display_items,
        ))
    } else {
        Arc::new(CliRestoreProgressReporter::new(
            num_expected_items,
            total_bytes,
            num_display_items,
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

async fn scan_restore_plan(
    repo: Arc<Repository>,
    tree_id: crate::mapache::ID,
    include: Option<Vec<PathBuf>>,
    exclude: Option<Vec<PathBuf>>,
) -> Result<(u64, u64, u64, u64)> {
    let mut node_stream =
        SerializedNodeStream::new(repo, Some(tree_id), PathBuf::new(), include, exclude).await?;
    let mut total_bytes = 0;
    let mut num_files = 0;
    let mut num_dirs = 0;
    let mut num_expected_items = 0;

    let spinner = ProgressBar::new_spinner();
    spinner.set_draw_target(default_bar_draw_target());
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.cyan} Scanning snapshot tree ({msg})")
            .unwrap()
            .tick_chars(SPINNER_TICK_CHARS),
    );
    spinner.enable_steady_tick(GlobalOpts::progress_refresh_interval());

    while let Some(res) = node_stream.next().await {
        let (_path, stream_node_res) = res?;
        let stream_node = stream_node_res?;

        let node = stream_node.node;
        num_expected_items += 1;

        if node.is_dir() {
            num_dirs += 1;
        } else if node.is_file() {
            num_files += 1;
            total_bytes += node.metadata.size;
        }

        spinner.set_message(format!(
            "{} files, {} dirs, {}",
            num_files,
            num_dirs,
            format_size_binary(total_bytes, 3)
        ));
    }

    spinner.finish_and_clear();
    Ok((num_files, num_dirs, num_expected_items, total_bytes))
}
