use std::{collections::BTreeSet, path::PathBuf, str::FromStr, sync::Arc, time::Instant};

use anyhow::{Result, bail};
use clap::{ArgGroup, Args};
use colored::Colorize;
use conflate::Merge;
use serde::Deserialize;

use crate::{
    archiver::{self, SnapshotOptions, progress::SnapshotProgress},
    backend::{StorageHint, new_backend_with_prompt},
    commands::{
        EMPTY_TAG_MARK, GlobalArgs, ToExitCode, UseSnapshot, cleanup::CleanupHandler, fail,
        find_use_snapshot, parse_tags, with_repository_lock,
    },
    fs::{
        self, calculate_lcp,
        filter::{
            PathFilter, merge_filtered_paths, normalized_exclude_paths,
            read_filtered_paths_from_file,
        },
    },
    mapache::{
        self, ContentIdType, ID,
        defaults::{DEFAULT_SNAPSHOT_READERS, SHORT_SNAPSHOT_ID_LEN},
    },
    repository::snapshot::{SnapshotPair, SnapshotSummary},
    repository::{lock::LockHandle, repo::Repository},
    ui::{
        self, SnapshotProgressReporter,
        cli::{
            snapshot::CliSnapshotProgressReporter,
            table::{Alignment, Table},
        },
        json::snapshot::JsonSnapshotProgressReporter,
    },
    utils,
};

#[derive(Debug, Clone, Copy)]
pub enum SnapshotError {
    BackendError = 11,
    RepoOpenFail = 12,
    SourcePathError = 20,
    ParentNotFound = 21,
    SnapshotFailed = 30,
}

impl ToExitCode for SnapshotError {
    fn to_exit_code(&self) -> i32 {
        *self as i32
    }
}

#[derive(Args, Debug, Clone, Merge, Deserialize, Default)]
#[clap(group = ArgGroup::new("scan_mode").multiple(false))]
#[clap(about = "Create a new snapshot")]
#[serde(default, rename_all = "kebab-case")]
pub struct CmdArgs {
    /// List of paths to backup
    #[clap(value_parser)]
    #[merge(strategy = conflate::vec::overwrite_empty)]
    #[serde(deserialize_with = "crate::mapache::config::deserialize_config_paths_vec")]
    pub paths: Vec<PathBuf>,

    /// Use a single directory path as the snapshot root
    #[clap(long = "as-root", value_parser, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
    #[merge(strategy = conflate::option::overwrite_none)]
    pub as_root: Option<bool>,

    /// A list of paths to exclude: path[,path,...]. Can be used multiple times.
    #[clap(long, value_parser, value_delimiter = ',', num_args = 1..)]
    #[merge(strategy = crate::mapache::config::merge_option_vec)]
    #[serde(deserialize_with = "crate::mapache::config::deserialize_config_string_vec_opt")]
    pub exclude: Option<Vec<String>>,

    /// A file containing a list of paths to exclude, one per line.
    #[clap(long, value_parser)]
    #[merge(strategy = conflate::option::overwrite_none)]
    #[serde(deserialize_with = "crate::mapache::config::deserialize_config_path_opt")]
    pub exclude_file: Option<PathBuf>,

    /// Tags
    #[clap(long = "tags", value_parser)]
    #[merge(strategy = conflate::option::overwrite_none)]
    pub tags_str: Option<String>,

    /// Snapshot description
    #[clap(long, value_parser)]
    #[merge(strategy = conflate::option::overwrite_none)]
    pub description: Option<String>,

    /// Force a complete analysis of all files and directories
    #[clap(long, group = "scan_mode")]
    #[merge(skip)]
    pub no_parent: bool,

    /// Don't scan the file system
    #[clap(long, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
    #[merge(strategy = conflate::option::overwrite_none)]
    pub no_scan: Option<bool>,

    /// Don't create a snapshot if there are no changes since the parent snapshot
    #[clap(long, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
    #[merge(strategy = conflate::option::overwrite_none)]
    pub skip_if_unchanged: Option<bool>,

    /// Use a snapshot as parent (ID or 'latest'). This snapshot will be the base when analyzing differences.
    #[clap(long, group = "scan_mode", value_parser = clap::value_parser!(UseSnapshot))]
    #[merge(strategy = conflate::option::overwrite_none)]
    #[serde(deserialize_with = "deserialize_use_snapshot_opt")]
    pub parent: Option<UseSnapshot>,

    /// Number of files to process in parallel.
    #[clap(long = "readers")]
    #[merge(strategy = conflate::option::overwrite_none)]
    pub num_readers: Option<usize>,

    /// Number of writer threads.
    #[clap(long = "packers")]
    #[merge(strategy = conflate::option::overwrite_none)]
    pub num_packers: Option<usize>,

    /// Dry run
    #[clap(long)]
    #[merge(skip)]
    pub dry_run: bool,

    /// Store the access time for all files and directories.
    /// Enabling this may result in significantly more metadata, so it's off by default.
    #[clap(long, action = clap::ArgAction::Set, num_args = 0..=1, default_missing_value = "true")]
    #[merge(strategy = conflate::option::overwrite_none)]
    pub with_atime: Option<bool>,
}

fn deserialize_use_snapshot_opt<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<UseSnapshot>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt = Option::<String>::deserialize(deserializer)?;
    opt.map(|s| UseSnapshot::from_str(&s).map_err(serde::de::Error::custom))
        .transpose()
}

pub async fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    tracing::info!(target: "snapshot", "Starting snapshot command");
    if args.paths.is_empty() {
        return Err(fail(
            "No source paths provided.",
            SnapshotError::SourcePathError,
        ));
    };

    let dry_run = args.dry_run;
    let num_readers = args.num_readers.unwrap_or(DEFAULT_SNAPSHOT_READERS);

    with_repository_lock(
        global_args.auth_file.as_ref(),
        global_args.key.as_ref(),
        new_backend_with_prompt(global_args.backend_options(dry_run))
            .await
            .map_err(|e| {
                fail(
                    format!("Failed to initialize backend: {}", e),
                    SnapshotError::SnapshotFailed,
                )
            })?,
        global_args.to_repo_config(),
        false,
        global_args.retry_lock_duration,
        |repo, _, lock_handle| async move {
            let progress_reporter: Arc<dyn SnapshotProgressReporter> = if global_args.json {
                Arc::new(JsonSnapshotProgressReporter::new(None, None))
            } else {
                Arc::new(CliSnapshotProgressReporter::new(None, None, num_readers))
            };

            execute(repo, lock_handle, args, progress_reporter, global_args.json).await
        },
    )
    .await
}

pub async fn execute(
    repo: Arc<Repository>,
    lock_handle: LockHandle,
    args: &CmdArgs,
    progress_reporter: Arc<dyn SnapshotProgressReporter>,
    json_output: bool,
) -> Result<()> {
    let as_root = args.as_root.unwrap_or(false);
    let no_scan = args.no_scan.unwrap_or(false);
    let skip_if_unchanged = args.skip_if_unchanged.unwrap_or(false);
    let with_atime = args.with_atime.unwrap_or(false);
    let parent = args.parent.clone().unwrap_or(UseSnapshot::Latest);
    let no_parent = args.no_parent;
    let num_readers = args.num_readers.unwrap_or(DEFAULT_SNAPSHOT_READERS);
    let num_packers = args
        .num_packers
        .unwrap_or(mapache::defaults::DEFAULT_SNAPSHOT_PACKERS);
    let dry_run = args.dry_run;
    let tags_str = args
        .tags_str
        .clone()
        .unwrap_or_else(|| EMPTY_TAG_MARK.to_string());

    let start = Instant::now();

    tracing::info!(target: "snapshot", "Reloading master index");
    repo.reload_master_index().await?;

    // Get source paths from arguments or readdir root path
    tracing::info!(target: "snapshot", "Processing source paths: {:?}", args.paths);
    let source_paths = if !as_root {
        args.paths.clone()
    } else {
        // Use path as root and readdir
        if args.paths.len() != 1 {
            bail!("Only one path can be the snapshot root");
        } else {
            let root = args.paths.last().unwrap();
            if !root.is_dir() {
                bail!("The snapshot root must be a directory");
            }

            let mut dir = tokio::fs::read_dir(root).await?;
            let mut paths = Vec::new();
            while let Some(entry) = dir.next_entry().await? {
                paths.push(entry.path());
            }
            paths
        }
    };

    let mut tags: BTreeSet<String> = parse_tags(Some(&tags_str));
    tags.retain(|tag| tag != EMPTY_TAG_MARK);

    // Canonicalize and deduplicate source paths
    let mut absolute_source_paths = BTreeSet::new();
    for path in &source_paths {
        match fs::get_absolute_normalized_path(path) {
            Ok(absolute_path) => {
                let _ = absolute_source_paths.insert(absolute_path);
            }
            Err(e) => {
                return Err(fail(
                    format!("Error processing path {:?}: {}", path, e),
                    SnapshotError::SourcePathError,
                ));
            }
        }
    }

    // Read exclude paths from file if provided.
    let excludes_from_file = match &args.exclude_file {
        Some(path) => Some(read_filtered_paths_from_file(path).map_err(|e| {
            fail(
                format!("Error reading exclude file: {}", e),
                SnapshotError::SourcePathError,
            )
        })?),
        None => None,
    };

    let all_excludes = merge_filtered_paths(args.exclude.as_ref(), excludes_from_file.as_ref());

    // Normalize the exclude paths and filter the source paths using the excludes
    let normalized_excludes: Option<Vec<PathBuf>> =
        normalized_exclude_paths(all_excludes.as_ref())?;
    let path_filter = PathFilter::new(None, normalized_excludes.clone());

    absolute_source_paths.retain(|p| path_filter.allow(p));
    let absolute_source_paths: Vec<PathBuf> = absolute_source_paths.into_iter().collect();

    // Extract the snapshot root path
    let snapshot_root_path = calculate_lcp(&absolute_source_paths, false);

    progress_reporter.log(String::new());
    if dry_run {
        progress_reporter.log(format!("{}", "[DRY RUN]".bold().purple()));
        tracing::info!(target: "snapshot", "Dry run enabled");
    }

    tracing::info!(target: "snapshot", "Finding parent snapshot (parent={:?})", parent);
    let parent_snapshot_pair: Option<SnapshotPair> = match no_parent {
        true => {
            progress_reporter.log("Full scan".to_string());
            tracing::info!(target: "snapshot", "Full scan requested (no parent)");
            None
        }
        false => match find_use_snapshot(repo.clone(), &parent).await {
            Ok(Some((id, snapshot))) => {
                progress_reporter.log(format!(
                    "Using snapshot {} as parent",
                    id.to_short_hex(SHORT_SNAPSHOT_ID_LEN).bold().yellow()
                ));
                tracing::info!(target: "snapshot", "Parent snapshot found: {id} (root={:?})", snapshot.root);
                Some(SnapshotPair { id, snapshot })
            }
            Ok(None) => {
                progress_reporter.log(format!(
                    "{} This is the first snapshot.",
                    "[!]".bold().cyan()
                ));
                tracing::info!(target: "snapshot", "No parent snapshot found (first snapshot)");
                None
            }
            Err(e) => {
                return Err(fail(
                    format!("Parent snapshot not found: {}", e),
                    SnapshotError::ParentNotFound,
                ));
            }
        },
    };

    // Run Archiver
    tracing::info!(target: "snapshot", "Initializing archiver");
    let progress = Arc::new(SnapshotProgress::new());

    // Init cleanup handler
    // We pass the reporter to the handler so it can clear the bars and print the message
    // immediately upon signal reception, instead of waiting for the main thread loop.
    let reporter_clone = progress_reporter.clone();
    let cleanup_handler = CleanupHandler::new_with_callback(move || {
        reporter_clone.finalize();
        reporter_clone.log(format!(
            "\n{}",
            "Process interrupted. Cleaning up...".bold().yellow()
        ));
    })?;
    cleanup_handler.add_lock(lock_handle.clone());

    repo.init_pack_saver(num_packers).map_err(|e| {
        fail(
            format!("Failed to initialize pack saver: {}", e),
            SnapshotError::SnapshotFailed,
        )
    })?;

    // Process and save new snapshot
    tracing::info!(target: "snapshot", "Starting archival process");
    let snapshot_result = archiver::snapshot(
        repo.clone(),
        SnapshotOptions {
            absolute_source_paths,
            snapshot_root_path,
            exclude_paths: normalized_excludes.unwrap_or_default(),
            parent_snapshot: parent_snapshot_pair.as_ref(),
            tags,
            description: args.description.clone(),
            no_scan,
            with_atime,
        },
        num_readers,
        progress.clone(),
        progress_reporter.clone(),
        cleanup_handler.interrupted.clone(),
    )
    .await;

    // Flush repo and finalize pack saver
    tracing::info!(target: "snapshot", "Flushing and finalizing repository");
    let repo_stats = repo.flush_and_finalize_pack_saver().await.map_err(|e| {
        fail(
            format!("Failed to finalize snapshot: {}", e),
            SnapshotError::SnapshotFailed,
        )
    })?;

    // Handle potential interruption or error
    let mut new_snapshot = match snapshot_result {
        Ok(s) => s,
        Err(e) => {
            if cleanup_handler.is_interrupted() {
                tracing::info!(target: "snapshot", "Snapshot interrupted by user");
                return Ok(());
            }

            progress_reporter.finalize();
            tracing::error!(target: "snapshot", "Snapshot archival failed: {e}");
            return Err(fail(
                format!("Snapshot failed: {}", e),
                SnapshotError::SnapshotFailed,
            ));
        }
    };

    let snapshot_report_summary = progress.summary();
    let snapshot_report_summary_clone = snapshot_report_summary.clone();

    // Fill snapshot summary
    new_snapshot.summary.processed_items_count = snapshot_report_summary.processed_items_count;
    new_snapshot.summary.processed_bytes = snapshot_report_summary.processed_bytes;
    new_snapshot.summary.diff_counts = snapshot_report_summary.diff_counts;
    new_snapshot.summary.raw_bytes = repo_stats.data.raw;
    new_snapshot.summary.encoded_bytes = repo_stats.data.encoded;
    new_snapshot.summary.meta_raw_bytes = repo_stats.meta.raw;
    new_snapshot.summary.meta_encoded_bytes = repo_stats.meta.encoded;
    new_snapshot.summary.total_raw_bytes =
        new_snapshot.summary.raw_bytes + new_snapshot.summary.meta_raw_bytes;
    new_snapshot.summary.total_encoded_bytes =
        new_snapshot.summary.encoded_bytes + new_snapshot.summary.meta_encoded_bytes;
    new_snapshot.summary.data_blobs = repo_stats.blobs;
    new_snapshot.summary.meta_blobs = repo_stats.meta_blobs;

    let should_save_snapshot = !skip_if_unchanged
        || parent_snapshot_pair.is_none()
        || (parent_snapshot_pair.unwrap().snapshot.tree != new_snapshot.tree);

    if should_save_snapshot {
        tracing::info!(target: "snapshot", "Saving new snapshot");
        let (new_snapshot_id, new_snapshot_size) = repo
            .save_file(
                &mapache::SaveID::CalculateID,
                serde_json::to_string(&new_snapshot)?.as_bytes(),
                StorageHint {
                    is_metadata: true,
                    file_type: ContentIdType::Snapshot,
                },
                None,
            )
            .await?;
        progress_reporter.finalize();
        tracing::info!(target: "snapshot", "Snapshot saved: {new_snapshot_id}");

        // Add the size of the snapshot file and index for display
        new_snapshot.summary.meta_raw_bytes += new_snapshot_size.raw + repo_stats.index.raw;
        new_snapshot.summary.meta_encoded_bytes +=
            new_snapshot_size.encoded + repo_stats.index.encoded;

        if json_output {
            #[derive(serde::Serialize)]
            struct SnapshotCompleteMsg {
                summary: crate::ui::SnapshotProcessSummary,
                raw_bytes_data: u64,
                compressed_bytes_data: u64,
                raw_bytes_meta: u64,
                compressed_bytes_meta: u64,
                time_taken_seconds: f64,
            }

            ui::json::emit_static(
                "snapshot_complete",
                &SnapshotCompleteMsg {
                    summary: snapshot_report_summary_clone,
                    raw_bytes_data: repo_stats.data.raw,
                    compressed_bytes_data: repo_stats.data.encoded,
                    raw_bytes_meta: repo_stats.meta.raw + new_snapshot_size.raw,
                    compressed_bytes_meta: repo_stats.meta.encoded + new_snapshot_size.encoded,
                    time_taken_seconds: start.elapsed().as_secs_f64(),
                },
            );
        }

        progress_reporter.log(String::new());
        show_final_report(
            &new_snapshot_id,
            &new_snapshot.summary,
            args,
            progress_reporter.clone(),
        );
    } else {
        progress_reporter.finalize();
        progress_reporter.log("No changes detected since parent. Skipping snapshot.".to_string());
        tracing::info!(target: "snapshot", "No changes detected. Snapshot skipped.");
    }

    let prefix = if args.dry_run {
        format!("{} ", "[DRY RUN]".bold().purple())
    } else {
        String::new()
    };

    progress_reporter.log(format!(
        "{}Processed {} in {}",
        prefix,
        utils::format_size_binary(new_snapshot.summary.processed_bytes, 3).cyan(),
        utils::pretty_print_duration(start.elapsed()).cyan()
    ));
    tracing::info!(target: "snapshot", "Snapshot command completed in {:?}", start.elapsed());
    Ok(())
}

fn show_final_report(
    snapshot_id: &ID,
    summary: &SnapshotSummary,
    args: &CmdArgs,
    progress_reporter: Arc<dyn SnapshotProgressReporter>,
) {
    progress_reporter.log(format!("{}", "Changes since parent snapshot:".bold()));

    let mut table = Table::new_with_alignments(vec![
        Alignment::Left,
        Alignment::Right,
        Alignment::Right,
        Alignment::Right,
        Alignment::Right,
    ]);
    table.set_headers(vec![
        "".to_string(),
        "new".bold().green().to_string(),
        "changed".bold().yellow().to_string(),
        "deleted".bold().red().to_string(),
        "unchanged".bold().to_string(),
    ]);

    table.add_row(vec![
        "Files".bold().to_string(),
        summary.diff_counts.new_files.to_string(),
        summary.diff_counts.changed_files.to_string(),
        summary.diff_counts.deleted_files.to_string(),
        summary.diff_counts.unchanged_files.to_string(),
    ]);
    table.add_row(vec![
        "Dirs".bold().to_string(),
        summary.diff_counts.new_dirs.to_string(),
        summary.diff_counts.changed_dirs.to_string(),
        summary.diff_counts.deleted_dirs.to_string(),
        summary.diff_counts.unchanged_dirs.to_string(),
    ]);
    progress_reporter.log(table.render());

    if !args.dry_run {
        progress_reporter.log(format!(
            "Created snapshot {}",
            snapshot_id
                .to_short_hex(mapache::defaults::SHORT_SNAPSHOT_ID_LEN)
                .to_string()
                .bold()
                .green()
        ));
        progress_reporter.log("This snapshot added:".to_string());
    } else {
        progress_reporter.log("This snapshot would add:".to_string());
    }

    let mut data_table =
        Table::new_with_alignments(vec![Alignment::Left, Alignment::Right, Alignment::Right]);
    data_table.set_headers(vec![
        "".to_string(),
        "Raw".bold().yellow().to_string(),
        "Compressed".bold().green().to_string(),
    ]);
    data_table.add_row(vec![
        "Data".bold().to_string(),
        utils::format_size_binary(summary.raw_bytes, 3)
            .yellow()
            .to_string(),
        utils::format_size_binary(summary.encoded_bytes, 3)
            .green()
            .to_string(),
    ]);
    data_table.add_row(vec![
        "Metadata".bold().to_string(),
        utils::format_size_binary(summary.meta_raw_bytes, 3)
            .yellow()
            .to_string(),
        utils::format_size_binary(summary.meta_encoded_bytes, 3)
            .green()
            .to_string(),
    ]);
    data_table.add_separator();
    data_table.add_row(vec![
        "Total".bold().to_string(),
        utils::format_size_binary(summary.total_raw_bytes, 3)
            .bold()
            .yellow()
            .to_string(),
        utils::format_size_binary(summary.total_encoded_bytes, 3)
            .bold()
            .green()
            .to_string(),
    ]);
    progress_reporter.log(data_table.render());
}
