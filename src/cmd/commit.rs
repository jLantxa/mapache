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
    sync::Arc,
};

use anyhow::{Result, bail};
use chrono::Utc;
use clap::{ArgGroup, Args};
use colored::Colorize;

use crate::{
    backend::localfs::LocalFS,
    cli::{self, GlobalArgs},
    filesystem::tree::Tree,
    repository::{repo::Repository, snapshot::Snapshot},
    utils::{Hash, format_size},
};

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
    pub naive: bool,

    /// Use a snapshot as parent. This snapshot will be the base when analyzing differences.
    #[arg(long, value_parser, group = "scan_mode")]
    pub parent: Option<String>,
}

#[derive(Debug, Default)]
enum ScanMode {
    Full,
    #[default]
    Last,
    Parent(String),
}

#[derive(Debug, Default)]
struct CommitResult {
    pub num_new_files: usize,
    pub num_files_changed: usize,
    pub num_new_dirs: usize,
    pub num_dirs_changed: usize,
    pub bytes_commited: usize, // Total size of the files commited
    pub bytes_written: usize,  // Total size written to disk (after compression)
}

impl CommitResult {
    pub fn merge(&mut self, other: CommitResult) {
        self.num_new_files += other.num_new_files;
        self.num_files_changed += other.num_files_changed;
        self.num_new_dirs += other.num_new_dirs;
        self.num_dirs_changed += other.num_dirs_changed;
        self.bytes_commited += other.bytes_commited;
        self.bytes_written += other.bytes_written;
    }
}

pub fn run(global: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let password = cli::request_password();
    let repo_path = Path::new(&global.repo);

    let backend = Arc::new(LocalFS::new());
    let repo = Repository::open(backend, repo_path, password)?;

    let scan_mode = if args.naive {
        ScanMode::Full
    } else if let Some(parent_id) = &args.parent {
        ScanMode::Parent(parent_id.to_owned())
    } else {
        ScanMode::Last
    };

    let parent_root_hash: Option<Hash> = find_parent_root_hash(&repo, &scan_mode)?;

    if let Some(hash) = &parent_root_hash {
        cli::log!(format!("Using snapshot \'{}\' as parent", hash));
    }

    cli::log!("Scanning tree...");
    let (snapshot_tree, scan_result) = Tree::scan_from_paths(&args.paths)?;

    cli::log_cyan(
        "to commit",
        &format!(
            "{} files, {} directories, {}",
            scan_result.num_files,
            scan_result.num_dirs,
            format_size(scan_result.total_bytes)
        ),
    );

    let parent_tree = parent_root_hash.and_then(|root_hash| repo.get_tree(&root_hash).ok());

    let commit_result = commit_tree(&repo, &snapshot_tree, &parent_tree)?;

    let root_hash = repo.put_tree(&snapshot_tree)?;
    let snapshot = Snapshot {
        timestamp: Utc::now(),
        root: root_hash,
        description: args.description.clone(),
    };
    let snapshot_hash = repo.save_snapshot(&snapshot)?;

    cli::log!(format!(
        "{} new files, {} changed",
        commit_result.num_new_files, commit_result.num_files_changed
    ));
    cli::log!(format!(
        "{} new directories, {} changed",
        commit_result.num_new_dirs, commit_result.num_dirs_changed
    ));
    cli::log!(format!(
        "Total size: {} -> {} ({})",
        format_size(scan_result.total_bytes),
        format!("{} commited", format_size(commit_result.bytes_commited)).cyan(),
        format!("{} written", format_size(commit_result.bytes_written)).purple()
    ));
    cli::log_green("Finished", &format!("Created snapshot {}", &snapshot_hash));

    Ok(())
}

fn find_parent_root_hash(repo: &Repository, scan_mode: &ScanMode) -> Result<Option<Hash>> {
    match scan_mode {
        ScanMode::Full => {
            // In Full mode, there is no parent to compare against
            Ok(None)
        }
        ScanMode::Last => {
            let snapshots: Vec<(String, Snapshot)> = repo.get_snapshots_sorted()?;
            if snapshots.is_empty() {
                cli::log!("No snapshots found. Doing full scan.");
                Ok(None)
            } else {
                Ok(snapshots
                    .last()
                    .map(|(_, parent_snapshot)| parent_snapshot.root.clone()))
            }
        }
        ScanMode::Parent(parent_id) => {
            let snapshots = repo.get_snapshots()?;
            let found_parent = snapshots.iter().find(|(id, _)| id == parent_id);

            match found_parent {
                Some((_, parent_snapshot)) => {
                    // Found the parent, return its root hash
                    Ok(Some(parent_snapshot.root.clone()))
                }
                None => {
                    bail!("Parent snapshot \'{}\' does not exist", parent_id);
                }
            }
        }
    }
}

fn commit_tree(repo: &Repository, tree: &Tree, parent_tree: &Option<Tree>) -> Result<CommitResult> {
    todo!()
}
