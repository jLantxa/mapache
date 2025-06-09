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

use std::{path::PathBuf, sync::Arc, time::Instant};

use anyhow::{Context, Result};
use clap::Args;

use crate::{
    backend::new_backend_with_prompt,
    commands::{GlobalArgs, UseSnapshot},
    global::ID,
    repository::{self, RepositoryBackend, snapshot::SnapshotStreamer},
    restorer::{Resolution, Restorer},
    ui::{cli, restore_progress::RestoreProgressReporter},
    utils,
};

impl std::fmt::Display for Resolution {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Resolution::Skip => write!(f, "skip"),
            Resolution::Overwrite => write!(f, "overwrite"),
            Resolution::Fail => write!(f, "fail"),
        }
    }
}

#[derive(Args, Debug)]
pub struct CmdArgs {
    /// A path where the files will be restored.
    #[clap(long, required = true)]
    pub target: PathBuf,

    /// The ID of the snapshot to restore, or 'latest' to restore the most recent snapshot saved.
    #[clap(long, value_parser = clap::value_parser!(UseSnapshot), default_value_t=UseSnapshot::Latest)]
    pub snapshot: UseSnapshot,

    /// A list of paths to restore.
    #[clap(long)]
    pub include: Option<Vec<PathBuf>>,

    /// A list of paths to exclude.
    #[clap(long)]
    pub exclude: Option<Vec<PathBuf>>,

    /// Method for conflict resolution in case a file or directory already exists in the target location.
    ///
    /// skip: Skips restoring the conflicting item.
    /// overwrite: Overwrites the item in the target location.
    /// fail: Terminates the command with an error.
    #[clap(long, default_value_t=Resolution::Fail)]
    pub resolution: Resolution,
}

pub fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let backend = new_backend_with_prompt(&global_args.repo)?;
    let repo: Arc<dyn RepositoryBackend> = repository::try_open(global_args.key.as_ref(), backend)?;

    let (_snapshot_id, snapshot) = match &args.snapshot {
        UseSnapshot::Latest => {
            let mut snapshots = SnapshotStreamer::new(repo.clone())?;
            snapshots.latest()
        }
        UseSnapshot::SnapshotId(id_hex) => {
            let id = ID::from_hex(&id_hex)?;
            match repo.load_snapshot(&id) {
                Ok(s) => Some((id.clone(), s)),
                Err(_) => None,
            }
        }
    }
    .with_context(|| "No snapshot was found")?;

    const NUM_SHOWN_PROCESSING_ITEMS: usize = 1;
    let num_expected_items = snapshot.summary.processed_items_count;
    let progress_reporter = Arc::new(RestoreProgressReporter::new(
        num_expected_items,
        NUM_SHOWN_PROCESSING_ITEMS,
    ));

    let start = Instant::now();

    Restorer::restore(
        repo.clone(),
        &snapshot,
        &args.resolution,
        &args.target,
        args.include.clone(),
        args.exclude.clone(),
        progress_reporter.clone(),
    )?;

    progress_reporter.finalize();

    cli::log!(
        "Finished in {}",
        utils::pretty_print_duration(start.elapsed())
    );

    Ok(())
}
