// [backup] is an incremental backup tool
// Copyright (C) 2025  Javier Lancha Vázquez <javier.lancha@gmail.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Result, bail};
use clap::{ArgGroup, Args};
use colored::Colorize;
use indicatif::{ProgressBar, ProgressStyle};

use crate::{
    archiver::Archiver,
    backend::{make_dry_backend, new_backend_with_prompt},
    global::{self, ID},
    repository::{
        self,
        snapshot::{Snapshot, SnapshotStreamer},
        streamers::FSNodeStreamer,
    },
    ui::{
        self,
        snapshot_progress::SnapshotProgressReporter,
        table::{Alignment, Table},
    },
    utils,
};

use super::{GlobalArgs, UseSnapshot};

#[derive(Args, Debug)]
#[clap(group = ArgGroup::new("scan_mode").multiple(false))]
pub struct CmdArgs {
    /// List of paths to backup
    #[clap(value_parser, required = true)]
    pub paths: Vec<PathBuf>,

    /// List of paths to exclude from the backup
    #[clap(long, value_parser, required = false)]
    pub exclude: Vec<PathBuf>,

    /// Snapshot description
    #[clap(long, value_parser)]
    pub description: Option<String>,

    /// Force a complete analysis of all files and directories
    #[clap(long, group = "scan_mode")]
    pub full_scan: bool,

    /// Use a snapshot as parent. This snapshot will be the base when analyzing differences.
    #[clap(long, group = "scan_mode",value_parser = clap::value_parser!(UseSnapshot), default_value_t=UseSnapshot::Latest )]
    pub parent: UseSnapshot,

    /// Number of files to process in parallel.
    #[clap(long, default_value_t = 2)]
    pub read_concurrency: usize,

    /// Number of writer threads.
    #[clap(long, default_value_t = 5)]
    pub write_concurrency: usize,

    /// Dry run
    #[clap(long, default_value_t = false)]
    pub dry_run: bool,
}

pub fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let backend = new_backend_with_prompt(&global_args.repo)?;
    let repo_password = ui::cli::request_repo_password();

    // If dry-run, wrap the backend inside the DryBackend
    let backend = make_dry_backend(backend, args.dry_run);

    let repo = repository::try_open(repo_password, global_args.key.as_ref(), backend)?;

    // Cannonicalize source paths
    let source_paths = &args.paths;
    let mut absolute_source_paths = Vec::new();
    for path in source_paths {
        match std::fs::canonicalize(&path) {
            Ok(absolute_path) => absolute_source_paths.push(absolute_path),
            Err(e) => bail!(e),
        }
    }

    // Cannonicalize the exclude paths
    let mut cannonical_excludes = Vec::new();
    for path in &args.exclude {
        match std::fs::canonicalize(path) {
            Ok(absolute_path) => cannonical_excludes.push(absolute_path),
            Err(e) => bail!(e),
        }
    }

    absolute_source_paths.retain(|p| utils::filter_path(p, None, Some(&cannonical_excludes)));

    // Extract the snapshot root path
    let snapshot_root_path = if absolute_source_paths.is_empty() {
        ui::cli::log_warning("No source paths provided. Creating empty snapshot.");
        PathBuf::new()
    } else if absolute_source_paths.len() == 1 {
        let single_source = absolute_source_paths.first().unwrap();
        utils::extract_parent(single_source).unwrap_or(PathBuf::new())
    } else {
        utils::calculate_lcp(&absolute_source_paths)
    };

    ui::cli::log!();
    let parent_snapshot = match args.full_scan {
        true => {
            ui::cli::log!("Full scan");
            None
        }
        false => match &args.parent {
            UseSnapshot::Latest => {
                let mut snapshots = SnapshotStreamer::new(repo.clone())?;
                let s = snapshots.latest();
                match &s {
                    Some((id, snap)) => {
                        ui::cli::log!(
                            "Using last snapshot {} as parent",
                            &id.to_short_hex(global::defaults::SHORT_SNAPSHOT_ID_LEN)
                                .bold()
                                .yellow()
                        );
                        Some(snap.clone())
                    }
                    None => {
                        ui::cli::log_warning("No previous snapshots found. Doing full scan.");
                        None
                    }
                }
            }
            UseSnapshot::SnapshotId(id_hex) => {
                let id = ID::from_hex(&id_hex)?;
                match &repo.load_snapshot(&id) {
                    Ok(snap) => {
                        ui::cli::log!("Using snapshot {:?} as parent", id);
                        Some(snap.clone())
                    }
                    Err(_) => bail!("Snapshot {:?} not found", id),
                }
            }
        },
    };

    let start = Instant::now();

    // Scan filesystem
    let spinner = ProgressBar::new_spinner();
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.cyan} {msg}")
            .unwrap()
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"),
    );
    spinner.set_message("Scanning filesystem...");
    spinner.enable_steady_tick(Duration::from_millis(100));

    // Scan the filesystem to collect stats about the targets
    let mut num_files = 0;
    let mut num_dirs = 0;
    let mut total_bytes = 0;
    let scan_streamer =
        FSNodeStreamer::from_paths(absolute_source_paths.clone(), cannonical_excludes.clone())?;
    for stream_node_result in scan_streamer {
        let (_path, stream_node) = stream_node_result?;
        let node = stream_node.node;

        if node.is_dir() {
            num_dirs += 1;
        } else if node.is_file() {
            num_files += 1;
            total_bytes += node.metadata.size;
        }
    }

    spinner.finish_and_clear();
    ui::cli::log!(
        "{} {} files, {} directories, {}",
        "To commit:".bold().cyan(),
        num_files,
        num_dirs,
        utils::format_size(total_bytes),
    );
    ui::cli::log!();

    // Run Archiver
    let expected_items = num_files + num_dirs;
    let progress_reporter = Arc::new(SnapshotProgressReporter::new(
        expected_items,
        total_bytes,
        args.read_concurrency,
    ));

    // Process and save new snapshot
    let archiver = Archiver::new(
        repo.clone(),
        absolute_source_paths,
        snapshot_root_path,
        cannonical_excludes.clone(),
        parent_snapshot,
        (args.read_concurrency, args.write_concurrency),
        progress_reporter.clone(),
    );
    let mut new_snapshot = archiver.snapshot()?;

    if let Some(description) = args.description.as_ref() {
        new_snapshot.description = Some(description.clone());
    }
    let (snapshot_id, _snapshot_raw_size, _snapshot_encoded_size) =
        repo.save_snapshot(&new_snapshot)?;

    // Finalize reporter. This removes the progress bars.
    progress_reporter.finalize();

    // Final report
    show_final_report(&snapshot_id, &new_snapshot, args);

    ui::cli::log!(
        "Finished in {}",
        utils::pretty_print_duration(start.elapsed())
    );

    Ok(())
}

fn show_final_report(
    snapshot_id: &ID,
    // The type changes here!
    snapshot: &Snapshot,
    args: &CmdArgs,
) {
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

    let summary = &snapshot.summary;

    table.add_row(vec![
        "Files".bold().to_string(),
        summary.new_files.to_string(),
        summary.changed_files.to_string(),
        summary.deleted_files.to_string(),
        summary.unchanged_files.to_string(),
    ]);
    table.add_row(vec![
        "Dirs".bold().to_string(),
        summary.new_dirs.to_string(),
        summary.changed_dirs.to_string(),
        summary.deleted_dirs.to_string(),
        summary.unchanged_dirs.to_string(),
    ]);
    table.print();

    ui::cli::log!();
    if !args.dry_run {
        ui::cli::log!(
            "New snapshot created {}",
            format!(
                "{}",
                &snapshot_id.to_short_hex(global::defaults::SHORT_SNAPSHOT_ID_LEN)
            )
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
        utils::format_size(summary.raw_bytes).yellow().to_string(),
        utils::format_size(summary.encoded_bytes)
            .green()
            .to_string(),
    ]);
    data_table.add_row(vec![
        "Metadata".bold().to_string(),
        utils::format_size(summary.meta_raw_bytes)
            .yellow()
            .to_string(),
        utils::format_size(summary.meta_encoded_bytes)
            .green()
            .to_string(),
    ]);
    data_table.add_row(vec![
        "Total".bold().cyan().to_string(),
        utils::format_size(summary.total_raw_bytes)
            .yellow()
            .to_string(),
        utils::format_size(summary.total_encoded_bytes)
            .green()
            .to_string(),
    ]);
    data_table.print();

    ui::cli::log!();
}
