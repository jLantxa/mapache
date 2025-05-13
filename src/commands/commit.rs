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
use clap::{ArgGroup, Args};
use colored::Colorize;

use crate::{
    archiver::Archiver,
    cli::{self, GlobalArgs},
    repository::{
        self,
        backend::{RepositoryBackend, SnapshotId},
        storage::SecureStorage,
        tree::FSNodeStreamer,
    },
    storage_backend::{backend::make_dry_backend, localfs::LocalFS},
    utils,
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
    pub full_scan: bool,

    /// Use a snapshot as parent. This snapshot will be the base when analyzing differences.
    #[arg(long, value_parser, group = "scan_mode")]
    pub parent: Option<SnapshotId>,

    /// Number of cuncurrent workers to process backup items
    #[arg(long, value_parser, default_value_t = 2)]
    pub workers: usize,

    /// Dry run
    #[arg(long, default_value_t = false)]
    pub dry_run: bool,
}

pub fn run(global: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let password = cli::request_password();
    let repo_path = Path::new(&global.repo);

    let storage_backend = Arc::new(LocalFS::new());
    let storage_backend = make_dry_backend(storage_backend, args.dry_run);

    let key = repository::backend::retrieve_key(password, storage_backend.clone(), &repo_path)?;
    let secure_storage = Arc::new(
        SecureStorage::new(storage_backend.clone())
            .with_key(key)
            .with_compression(zstd::DEFAULT_COMPRESSION_LEVEL),
    );

    let repo: Arc<dyn RepositoryBackend> = Arc::from(repository::backend::open(
        storage_backend,
        repo_path,
        secure_storage,
    )?);

    let source_paths = &args.paths;

    let parent_snapshot = match &args.parent {
        None => None,
        Some(id) => {
            let found_parent_snapshot = repo.load_snapshot(&id)?;
            match found_parent_snapshot {
                Some((_snapshot_id, snapshot)) => Some(snapshot),
                None => bail!("No snapshot found with id \'{}\'", id),
            }
        }
    };

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
        source_paths,
        parent_snapshot,
        args.workers,
        args.full_scan,
    )?;

    if let Some(description) = args.description.as_ref() {
        new_snapshot.description = Some(description.clone());
    }

    let snapshot_id: SnapshotId = repo.save_snapshot(&new_snapshot)?;
    cli::log!();
    cli::log!("New snapshot \'{}\'", format!("{}", snapshot_id).bold());

    if args.dry_run {
        cli::log!();
        cli::log!("{} Nothing was saved", "[DRY RUN]".bold().cyan());
    }

    Ok(())
}
