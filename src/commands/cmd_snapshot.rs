use std::{collections::BTreeSet, path::PathBuf, sync::Arc, time::Instant};

use anyhow::{Result, bail};
use clap::{ArgGroup, Args};
use colored::Colorize;

use crate::{
    archiver::{Archiver, SnapshotOptions},
    backend::{BackendOptions, StorageHint, new_backend_with_prompt},
    commands::{EMPTY_TAG_MARK, cleanup::CleanupHandler, find_use_snapshot, parse_tags},
    fs::{self},
    mapache::{self, ContentIdType, ID, defaults::SHORT_SNAPSHOT_ID_LEN},
    repository::{
        repo::{RepoConfig, Repository},
        snapshot::{SnapshotSummary, SnapshotTuple},
    },
    ui::{
        self,
        snapshot_progress::SnapshotProgressReporter,
        table::{Alignment, Table},
    },
    utils::{self, size},
};

use super::{GlobalArgs, UseSnapshot};

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
    #[clap(long, value_parser, value_delimiter = ',', required = false)]
    pub exclude: Option<Vec<PathBuf>>,

    /// Tags
    #[clap(long = "tags", value_parser, default_value_t = EMPTY_TAG_MARK.to_string())]
    pub tags_str: String,

    /// Snapshot description
    #[clap(long, value_parser)]
    pub description: Option<String>,

    /// Force a complete analysis of all files and directories
    #[clap(long = "no-parent", group = "scan_mode")]
    pub rescan: bool,

    /// Use a snapshot as parent (ID or 'latest'). This snapshot will be the base when analyzing differences.
    #[clap(long, group = "scan_mode", value_parser = clap::value_parser!(UseSnapshot),
           default_value_t = UseSnapshot::Latest )]
    pub parent: UseSnapshot,

    /// Number of files to process in parallel.
    #[clap(long, default_value_t = mapache::defaults::DEFAULT_READ_CONCURRENCY)]
    pub read_concurrency: usize,

    /// Number of writer threads.
    #[clap(long, default_value_t = mapache::defaults::DEFAULT_WRITE_CONCURRENCY)]
    pub write_concurrency: usize,

    /// Dry run
    #[clap(long, default_value_t = false)]
    pub dry_run: bool,
}

pub fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    if args.paths.is_empty() {
        bail!("No source paths provided.");
    };

    let auth = utils::get_auth_from_file(&global_args.auth_file)?;
    let backend = new_backend_with_prompt(BackendOptions {
        repo_path: global_args.repo.clone(),
        ssh_pubkey: global_args.ssh_pubkey.clone(),
        ssh_privatekey: global_args.ssh_privatekey.clone(),
        dry_backend: args.dry_run,
        cached: !global_args.no_cache,
    })?;

    let config = RepoConfig {
        pack_size: (global_args.pack_size_mib * size::MiB as f32) as u64,
        use_cache: !global_args.no_cache,
    };
    let (repo, _, lock_handle) = Repository::try_open_with_lock(
        auth.as_ref(),
        global_args.key.as_ref(),
        backend,
        config,
        false,
    )?;

    let start = Instant::now();

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

            std::fs::read_dir(root)?
                .filter_map(|entry| entry.ok())
                .map(|entry| entry.path())
                .collect()
        }
    };

    let mut tags: BTreeSet<String> = parse_tags(Some(&args.tags_str));
    tags.retain(|tag| tag != EMPTY_TAG_MARK);

    // Cannonicalize and deduplicate source paths
    // Use a BTreeSet to remove duplicate paths and sort them alphabetically.
    let mut absolute_source_paths = BTreeSet::new();
    for path in &source_paths {
        match fs::get_absolute_normalized_path(path) {
            Ok(absolute_path) => {
                let _ = absolute_source_paths.insert(absolute_path);
            }
            Err(e) => bail!("{path:?}: {e}"),
        }
    }

    // Normalize the exclude paths and filter the source paths using the excludes
    let normalized_excludes: Option<Vec<PathBuf>> = if let Some(exclude_paths) = &args.exclude {
        let mut normalized_vec = Vec::new();
        for path in exclude_paths {
            match fs::get_absolute_normalized_path(path) {
                Ok(normalized_path) => normalized_vec.push(normalized_path),
                Err(e) => bail!("{path:?}: {e}"),
            };
        }
        Some(normalized_vec)
    } else {
        None
    };

    absolute_source_paths.retain(|p| utils::filter_path(p, None, normalized_excludes.as_ref()));
    let absolute_source_paths: Vec<PathBuf> = absolute_source_paths.into_iter().collect();

    // Extract the snapshot root path
    let snapshot_root_path = utils::calculate_lcp(&absolute_source_paths, false);

    ui::cli::log!();
    let parent_snapshot_tuple: Option<SnapshotTuple> = match args.rescan {
        true => {
            ui::cli::log!("Full scan");
            None
        }
        false => match find_use_snapshot(repo.clone(), &args.parent) {
            Ok(Some((id, snap))) => {
                ui::cli::log!(
                    "Using snapshot {} as parent",
                    id.to_short_hex(SHORT_SNAPSHOT_ID_LEN).bold().yellow()
                );
                Some((id, snap))
            }
            Ok(None) => {
                ui::cli::log!("{} This is the first snapshot.", "[!]".bold().cyan());
                None
            }
            Err(_) => bail!("Parent snapshot not found"),
        },
    };

    // Run Archiver
    let progress_reporter = Arc::new(SnapshotProgressReporter::new(
        None,
        None,
        args.read_concurrency,
    ));

    // Init cleanup handlerf
    let repo_clone = repo.clone();
    let reporter_clone = progress_reporter.clone();
    let lock_handle_clone = lock_handle.clone();
    let _cleanup_handler = CleanupHandler::new(move || {
        reporter_clone.finalize();
        ui::cli::log!("Process interrupted. Cleaning up...");

        let _ = repo_clone.flush();
        repo_clone.finalize_pack_saver();
        lock_handle_clone.write().unlock();
    })?;

    // Process and save new snapshot
    let archiver = Archiver::new(
        repo.clone(),
        SnapshotOptions {
            absolute_source_paths,
            snapshot_root_path,
            exclude_paths: normalized_excludes.unwrap_or_default(),
            parent_snapshot: parent_snapshot_tuple,
            tags,
            description: args.description.clone(),
        },
        (args.read_concurrency, args.write_concurrency),
        progress_reporter.clone(),
    );
    let new_snapshot = archiver.snapshot()?;

    let (snapshot_id, snapshot_raw_size, snapshot_encoded_size) = repo.save_file(
        &mapache::SaveID::CalculateID,
        serde_json::to_string(&new_snapshot)?.as_bytes(),
        StorageHint {
            is_metadata: true,
            file_type: ContentIdType::Snapshot,
        },
        None,
    )?;

    progress_reporter.written_meta_bytes(snapshot_raw_size, snapshot_encoded_size);

    // Finalize reporter. This removes the progress bars.
    progress_reporter.finalize();

    // Final report
    show_final_report(&snapshot_id, &progress_reporter.get_summary(), args);

    ui::cli::log!(
        "Finished in {}",
        utils::pretty_print_duration(start.elapsed())
    );

    Ok(())
}

fn show_final_report(snapshot_id: &ID, summary: &SnapshotSummary, args: &CmdArgs) {
    ui::cli::log!("{}", "Changes since parent snapshot".bold());
    ui::cli::log!();

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
        "unmodiffied".bold().to_string(),
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
            "New snapshot created: {}",
            snapshot_id
                .to_short_hex(mapache::defaults::SHORT_SNAPSHOT_ID_LEN)
                .to_string()
                .bold()
                .green()
        );
        ui::cli::log!("This snapshot added:\n");
    } else {
        ui::cli::log!("This snapshot would add:\n");
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
        utils::format_size(summary.raw_bytes, 3)
            .yellow()
            .to_string(),
        utils::format_size(summary.encoded_bytes, 3)
            .green()
            .to_string(),
    ]);
    data_table.add_row(vec![
        "Metadata".bold().to_string(),
        utils::format_size(summary.meta_raw_bytes, 3)
            .yellow()
            .to_string(),
        utils::format_size(summary.meta_encoded_bytes, 3)
            .green()
            .to_string(),
    ]);
    data_table.add_separator();
    data_table.add_row(vec![
        "Total".bold().to_string(),
        utils::format_size(summary.total_raw_bytes, 3)
            .bold()
            .yellow()
            .to_string(),
        utils::format_size(summary.total_encoded_bytes, 3)
            .bold()
            .green()
            .to_string(),
    ]);
    ui::cli::log!("{}", data_table.render());
}
