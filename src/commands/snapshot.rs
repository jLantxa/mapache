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
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use anyhow::{Result, bail};
use clap::{ArgGroup, Args};
use colored::Colorize;

use crate::{
    archiver::{Archiver, CommitProgressReporter},
    backend::{make_dry_backend, new_backend_with_prompt},
    cli,
    repository::{self, RepositoryBackend, storage::SecureStorage, tree::FSNodeStreamer},
    utils,
};

use super::{GlobalArgs, UseSnapshot};

const SHORT_ID_LEN: usize = 8;

#[derive(Args, Debug)]
#[clap(group = ArgGroup::new("scan_mode").multiple(false))]
pub struct CmdArgs {
    /// List of paths to commit
    #[clap(value_parser, required = true)]
    pub paths: Vec<PathBuf>,

    /// Snapshot description
    #[clap(long, value_parser)]
    pub description: Option<String>,

    /// Force a complete analysis of all files and directories
    #[arg(long, group = "scan_mode")]
    pub full_scan: bool,

    /// Use a snapshot as parent. This snapshot will be the base when analyzing differences.
    #[arg(long, group = "scan_mode",value_parser = clap::value_parser!(UseSnapshot), default_value_t=UseSnapshot::Latest )]
    pub parent: UseSnapshot,

    /// Dry run
    #[arg(long, default_value_t = false)]
    pub dry_run: bool,
}

pub fn run(global: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let backend = new_backend_with_prompt(&global.repo)?;
    let repo_password = cli::request_repo_password();

    // If dry-run, wrap the backend inside the DryBackend
    let backend = make_dry_backend(backend, args.dry_run);

    let key = repository::retrieve_key(repo_password, backend.clone())?;
    let secure_storage = Arc::new(
        SecureStorage::build()
            .with_key(key)
            .with_compression(zstd::DEFAULT_COMPRESSION_LEVEL),
    );

    let repo: Arc<dyn RepositoryBackend> =
        Arc::from(repository::open(backend, secure_storage.clone())?);

    let source_paths = &args.paths;
    let mut absolute_source_paths = Vec::new();
    for path in source_paths {
        match std::fs::canonicalize(&path) {
            Ok(absolute_path) => absolute_source_paths.push(absolute_path),
            Err(e) => bail!(e),
        }
    }

    // Extract the commit root path
    let commit_root_path = if absolute_source_paths.is_empty() {
        cli::log_warning("No source paths provided. Creating empty commit.");
        PathBuf::new()
    } else if absolute_source_paths.len() == 1 {
        let single_source = absolute_source_paths.first().unwrap();
        if single_source == Path::new("/") {
            PathBuf::new()
        } else {
            single_source
                .parent()
                .map(|p| p.to_path_buf())
                .unwrap_or_else(|| PathBuf::new())
        }
    } else {
        utils::calculate_lcp(&absolute_source_paths)
    };

    cli::log!();
    let parent_snapshot = match args.full_scan {
        true => {
            cli::log!("Full scan");
            None
        }
        false => match &args.parent {
            UseSnapshot::Latest => {
                let snapshots_sorted = repo.load_all_snapshots_sorted()?;
                let s = snapshots_sorted.last().cloned();
                match &s {
                    Some((id, snap)) => {
                        cli::log!(
                            "Using last snapshot {} as parent",
                            &id[0..SHORT_ID_LEN].bold().yellow()
                        );
                        Some(snap.clone())
                    }
                    None => {
                        cli::log_warning("No previous snapshots found. Doing full scan.");
                        None
                    }
                }
            }
            UseSnapshot::Snapshot(id) => match &repo.load_snapshot(id) {
                Ok(snap) => {
                    cli::log!("Using snapshot {:?} as parent", id);
                    Some(snap.clone())
                }
                Err(_) => bail!("Snapshot {:?} not found", id),
            },
        },
    };

    let progress_reporter = Arc::new(Mutex::new(ProgressReporter::new()));

    // Scan the filesystem to collect stats about the targets
    let mut num_files = 0;
    let mut num_dirs = 0;
    let mut total_bytes = 0;
    let scan_streamer = FSNodeStreamer::from_paths(source_paths)?;
    for stream_node_result in scan_streamer {
        let (_, stream_node) = stream_node_result?;
        let node = stream_node.node;

        if node.is_dir() {
            num_dirs += 1;
        } else if node.is_file() {
            num_files += 1;
            total_bytes += node.metadata.size;
        }
    }

    cli::log!(
        "{} {} files, {} directories, {}",
        "To commit:".bold().cyan(),
        num_files,
        num_dirs,
        utils::format_size(total_bytes)
    );

    let mut new_snapshot = Archiver::snapshot(
        repo.clone(),
        absolute_source_paths,
        commit_root_path,
        parent_snapshot,
        Some(progress_reporter.clone()),
    )?;

    if let Some(description) = args.description.as_ref() {
        new_snapshot.description = Some(description.clone());
    }

    let (snapshot_id, _snapshot_uncompressed_snapshot_size, _snapshot_compressed_snapshot_size) =
        repo.save_snapshot(&new_snapshot)?;

    // Final report
    let pr = progress_reporter.lock().unwrap();
    cli::log!();
    cli::log!("{}", "Changes since parent snapshot".bold());
    cli::log!();

    let type_len = std::cmp::max("Files:".len(), "Dirs:".len());
    let new_len = std::cmp::max(8, "new".len());
    let changed_len = std::cmp::max(8, "changed".len());
    let del_len = std::cmp::max(8, "deleted".len());
    let unmod_len = std::cmp::max(8, "unmodiffied".len());

    let file_summary_line = format!(
        "{0: <type_len$} {1: >new_len$}  {2: >changed_len$}  {3: >del_len$}  {4: >unmod_len$}",
        "Files:".bold(),
        pr.new_files,
        pr.changed_files,
        pr.deleted_files,
        pr.unchanged_files,
    );
    let dir_summary_line = format!(
        "{0: <type_len$} {1: >new_len$}  {2: >changed_len$}  {3: >del_len$}  {4: >unmod_len$}",
        "Dirs:".bold(),
        pr.new_dirs,
        pr.changed_dirs,
        pr.deleted_dirs,
        pr.unchanged_dirs,
    );

    cli::log!(
        "{0: <type_len$} {1: >new_len$}  {2: >changed_len$}  {3: >del_len$}  {4: >unmod_len$}",
        "",
        "new".bold().green(),
        "changed".bold().yellow(),
        "deleted".bold().red(),
        "unmodiffied".bold(),
    );
    cli::print_separator('-', file_summary_line.chars().count());
    cli::log!("{}", file_summary_line);
    cli::log!("{}", dir_summary_line);

    cli::log!();
    if !args.dry_run {
        cli::log!(
            "New snapshot created {}",
            format!("{}", &snapshot_id[0..SHORT_ID_LEN]).bold().green()
        );
        cli::log!(
            "This snapshot added {} {}",
            utils::format_size(pr.raw_bytes).yellow(),
            format!("({} compressed)", utils::format_size(pr.encoded_bytes))
                .bold()
                .green()
        );
    } else {
        cli::log!(
            "This snapshot would add {} {}",
            utils::format_size(pr.raw_bytes).yellow(),
            format!("({} compressed)", utils::format_size(pr.encoded_bytes))
                .bold()
                .green()
        );
        cli::log!(
            "{} This was a dry run. Nothing was written.",
            "[!]".bold().yellow()
        );
    }

    cli::log!();

    Ok(())
}

pub struct ProgressReporter {
    pub processed_bytes: u64,
    pub encoded_bytes: u64,
    pub raw_bytes: u64,

    pub new_files: u32,
    pub changed_files: u32,
    pub unchanged_files: u32,
    pub deleted_files: u32,
    pub new_dirs: u32,
    pub changed_dirs: u32,
    pub unchanged_dirs: u32,
    pub deleted_dirs: u32,
}

impl ProgressReporter {
    pub fn new() -> Self {
        Self {
            processed_bytes: 0,
            encoded_bytes: 0,
            raw_bytes: 0,
            new_files: 0,
            changed_files: 0,
            unchanged_files: 0,
            deleted_files: 0,
            new_dirs: 0,
            changed_dirs: 0,
            unchanged_dirs: 0,
            deleted_dirs: 0,
        }
    }
}

impl CommitProgressReporter for ProgressReporter {
    fn processing_file(&mut self, _path: PathBuf) {
        // Do nothing yet
    }

    fn processed_bytes(&mut self, bytes: u64) {
        self.processed_bytes += bytes;
    }

    fn raw_bytes(&mut self, bytes: u64) {
        self.raw_bytes += bytes;
    }

    fn encoded_bytes(&mut self, bytes: u64) {
        self.encoded_bytes += bytes;
    }

    fn new_file(&mut self) {
        self.new_files += 1
    }

    fn changed_file(&mut self) {
        self.changed_files += 1
    }

    fn unchanged_file(&mut self) {
        self.unchanged_files += 1;
    }

    fn deleted_file(&mut self) {
        self.deleted_files += 1;
    }

    fn new_dir(&mut self) {
        self.new_dirs += 1;
    }

    fn changed_dir(&mut self) {
        self.changed_dirs += 1;
    }

    fn deleted_dir(&mut self) {
        self.deleted_dirs += 1;
    }

    fn unchanged_dir(&mut self) {
        self.unchanged_dirs += 1;
    }
}
