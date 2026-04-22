use std::{collections::BTreeSet, path::PathBuf, sync::Arc, time::Instant};

use anyhow::{Result, bail};
use clap::{ArgGroup, Args};
use colored::Colorize;

use crate::archiver::{self, SnapshotOptions, progress::SnapshotProgress};
use crate::backend::{StorageHint, new_backend_with_prompt};
use crate::commands::{EMPTY_TAG_MARK, cleanup::CleanupHandler, find_use_snapshot, parse_tags};
use crate::commands::{ToExitCode, fail, open_repository_with_lock};
use crate::fs::filter::{merge_filtered_paths, read_filtered_paths_from_file};
use crate::fs::{
    self, calculate_lcp,
    filter::{PathFilter, normalized_exclude_paths},
};
use crate::mapache::defaults::DEFAULT_SNAPSHOT_READERS;
use crate::mapache::{self, ContentIdType, ID, defaults::SHORT_SNAPSHOT_ID_LEN};
use crate::repository::{
    repo::RepoConfig,
    snapshot::{SnapshotPair, SnapshotSummary},
};
use crate::ui::{
    self,
    snapshot::{
        SnapshotProgressReporter, cli::CliSnapshotProgressReporter,
        json::JsonSnapshotProgressReporter,
    },
    table::{Alignment, Table},
};
use crate::utils::{self, size};

use super::{GlobalArgs, UseSnapshot};

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

#[derive(Args, Debug)]
#[clap(group = ArgGroup::new("scan_mode").multiple(false))]
#[clap(about = "Create a new snapshot")]
pub struct CmdArgs {
    /// List of paths to backup
    #[clap(value_parser, required = true)]
    pub paths: Vec<PathBuf>,

    /// Use a single directory path as the snapshot root
    #[clap(long = "as-root", value_parser, default_value_t = false)]
    pub as_root: bool,

    /// A list of paths to exclude: path[,path,...]. Can be used multiple times.
    #[clap(long, value_parser, value_delimiter = ',', num_args = 1..)]
    pub exclude: Option<Vec<String>>,

    /// A file containing a list of paths to exclude, one per line.
    #[clap(long, value_parser)]
    pub exclude_file: Option<PathBuf>,

    /// Tags
    #[clap(long = "tags", value_parser, default_value_t = EMPTY_TAG_MARK.to_string())]
    pub tags_str: String,

    /// Snapshot description
    #[clap(long, value_parser)]
    pub description: Option<String>,

    /// Force a complete analysis of all files and directories
    #[clap(long, group = "scan_mode")]
    pub no_parent: bool,

    /// Don't scan the file system
    #[clap(long, value_parser, default_value_t = false)]
    pub no_scan: bool,

    /// Don't create a snapshot if there are no changes since the parent snapshot
    #[clap(long, default_value_t = false)]
    pub skip_if_unchanged: bool,

    /// Use a snapshot as parent (ID or 'latest'). This snapshot will be the base when analyzing differences.
    #[clap(long, group = "scan_mode", value_parser = clap::value_parser!(UseSnapshot),
           default_value_t = UseSnapshot::Latest )]
    pub parent: UseSnapshot,

    /// Number of files to process in parallel.
    #[clap(long = "readers", default_value_t = DEFAULT_SNAPSHOT_READERS)]
    pub num_readers: usize,

    /// Number of writer threads.
    #[clap(long = "packers", default_value_t = mapache::defaults::DEFAULT_SNAPSHOT_PACKERS)]
    pub num_packers: usize,

    /// Dry run
    #[clap(long, default_value_t = false)]
    pub dry_run: bool,
}

pub async fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    if args.paths.is_empty() {
        return Err(fail(
            "No source paths provided.",
            SnapshotError::SourcePathError,
        ));
    };

    let backend = new_backend_with_prompt(global_args.backend_options(args.dry_run))
        .await
        .map_err(|e| {
            fail(
                format!("Failed to initialize backend: {}", e),
                SnapshotError::SnapshotFailed,
            )
        })?;

    let config = RepoConfig {
        pack_size: (global_args.pack_size_mib * size::MiB as f32) as u64,
        use_cache: !global_args.no_cache,
        compression: global_args.compression_level,
    };
    let (repo, _, mut lock_handle) = open_repository_with_lock(
        global_args.auth_file.as_ref(),
        global_args.key.as_ref(),
        backend,
        config,
        false,
        global_args.retry_lock_duration,
    )
    .await
    .map_err(|e| {
        fail(
            format!("Failed to open repository: {}", e),
            SnapshotError::RepoOpenFail,
        )
    })?;

    let start = Instant::now();

    repo.reload_master_index().await?;

    // Get source paths from arguments or readdir root path
    let source_paths = if !args.as_root {
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

    let mut tags: BTreeSet<String> = parse_tags(Some(&args.tags_str));
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

    ui::cli::log!();
    if args.dry_run {
        ui::cli::log!("{}", "[DRY RUN]".bold().purple());
    }

    let parent_snapshot_pair: Option<SnapshotPair> = match args.no_parent {
        true => {
            ui::cli::log!("Full scan");
            None
        }
        false => match find_use_snapshot(repo.clone(), &args.parent).await {
            Ok(Some((id, snapshot))) => {
                ui::cli::log!(
                    "Using snapshot {} as parent",
                    id.to_short_hex(SHORT_SNAPSHOT_ID_LEN).bold().yellow()
                );
                Some(SnapshotPair { id, snapshot })
            }
            Ok(None) => {
                ui::cli::log!("{} This is the first snapshot.", "[!]".bold().cyan());
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
    let progress = Arc::new(SnapshotProgress::new());
    let progress_reporter: Arc<dyn SnapshotProgressReporter> = if global_args.json {
        Arc::new(JsonSnapshotProgressReporter::new(None, None))
    } else {
        Arc::new(CliSnapshotProgressReporter::new(
            None,
            None,
            args.num_readers,
        ))
    };

    // Init cleanup handler
    // We pass the reporter to the handler so it can clear the bars and print the message
    // immediately upon signal reception, instead of waiting for the main thread loop.
    let reporter_clone = progress_reporter.clone();
    let cleanup_handler = CleanupHandler::new_with_callback(move || {
        reporter_clone.finalize();
        ui::cli::log!(
            "\n{}",
            "Process interrupted. Cleaning up...".bold().yellow()
        );
    })?;
    cleanup_handler.add_lock(lock_handle.clone());

    repo.init_pack_saver(args.num_packers).map_err(|e| {
        fail(
            format!("Failed to initialize pack saver: {}", e),
            SnapshotError::SnapshotFailed,
        )
    })?;

    // Process and save new snapshot
    let snapshot_result = archiver::snapshot(
        repo.clone(),
        SnapshotOptions {
            absolute_source_paths,
            snapshot_root_path,
            exclude_paths: normalized_excludes.unwrap_or_default(),
            parent_snapshot: parent_snapshot_pair.as_ref(),
            tags,
            description: args.description.clone(),
            no_scan: args.no_scan,
        },
        args.num_readers,
        progress.clone(),
        progress_reporter.clone(),
        cleanup_handler.interrupted.clone(),
    )
    .await;

    // Flush repo and finalize pack saver
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
                return Ok(());
            }

            progress_reporter.finalize();
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

    let should_save_snapshot = !args.skip_if_unchanged
        || parent_snapshot_pair.is_none()
        || (parent_snapshot_pair.unwrap().snapshot.tree != new_snapshot.tree);

    if should_save_snapshot {
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

        // Add the size of the snapshot file and index for display
        new_snapshot.summary.meta_raw_bytes += new_snapshot_size.raw + repo_stats.index.raw;
        new_snapshot.summary.meta_encoded_bytes +=
            new_snapshot_size.encoded + repo_stats.index.encoded;

        if global_args.json {
            #[derive(serde::Serialize)]
            struct SnapshotCompleteMsg {
                summary: crate::ui::snapshot::SnapshotProcessSummary,
                raw_bytes_data: u64,
                compressed_bytes_data: u64,
                raw_bytes_meta: u64,
                compressed_bytes_meta: u64,
                time_taken_seconds: f64,
            }

            ui::json_reporter::emit_static(
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

        ui::cli::log!();
        show_final_report(&new_snapshot_id, &new_snapshot.summary, args);
    } else {
        progress_reporter.finalize();
        ui::cli::log!("No changes detected since parent. Skipping snapshot.");
    }

    let prefix = if args.dry_run {
        format!("{} ", "[DRY RUN]".bold().purple())
    } else {
        String::new()
    };

    ui::cli::log!(
        "{}Processed {} in {}",
        prefix,
        utils::format_size_binary(new_snapshot.summary.processed_bytes, 3).cyan(),
        utils::pretty_print_duration(start.elapsed()).cyan()
    );

    lock_handle.unlock().await;

    Ok(())
}

fn show_final_report(snapshot_id: &ID, summary: &SnapshotSummary, args: &CmdArgs) {
    ui::cli::log!("{}", "Changes since parent snapshot:".bold());

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
    ui::cli::log!("{}", table.render());

    if !args.dry_run {
        ui::cli::log!(
            "Created snapshot {}",
            snapshot_id
                .to_short_hex(mapache::defaults::SHORT_SNAPSHOT_ID_LEN)
                .to_string()
                .bold()
                .green()
        );
        ui::cli::log!("This snapshot added:");
    } else {
        ui::cli::log!("This snapshot would add:");
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
    ui::cli::log!("{}", data_table.render());
}
