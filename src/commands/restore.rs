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

use std::{path::PathBuf, str::FromStr, sync::Arc};

use anyhow::{Context, Error, Result, anyhow, bail};
use clap::{Args, ValueEnum};

use crate::{
    backend::new_backend_with_prompt,
    cli::{self, GlobalArgs},
    repository::{
        self,
        repository::{RepositoryBackend, SnapshotId},
        storage::SecureStorage,
        tree::SerializedNodeStreamer,
    },
    restorer,
};

#[derive(Debug, Clone, PartialEq)]
pub enum RestoreSnapshot {
    Latest,
    Snapshot(SnapshotId),
}

impl FromStr for RestoreSnapshot {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "latest" => Ok(RestoreSnapshot::Latest),
            _ if !s.is_empty() => Ok(RestoreSnapshot::Snapshot(s.to_string())),
            _ => Err(anyhow!(
                "Invalid snapshot value: must be 'latest' or a snapshot ID"
            )),
        }
    }
}

impl std::fmt::Display for RestoreSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RestoreSnapshot::Latest => write!(f, "latest"),
            RestoreSnapshot::Snapshot(id) => write!(f, "{}", id),
        }
    }
}

#[derive(Debug, Clone, PartialEq, ValueEnum)]
pub enum Resolution {
    Skip,
    Overwrite,
    Fail,
}

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
    #[clap(long, value_parser = clap::value_parser!(RestoreSnapshot), default_value_t=RestoreSnapshot::Latest)]
    pub snapshot: RestoreSnapshot,

    /// A list of paths to restore.
    #[clap(long, required = false)]
    pub include: Vec<PathBuf>,

    /// A list of paths to exclude.
    #[clap(long, required = false)]
    pub exclude: Vec<PathBuf>,

    /// Method for conflict resolution in case a file or directory already exists in the target location.
    ///
    /// skip: Skips restoring the conflicting item.
    /// overwrite: Overwrites the item in the target location.
    /// fail: Terminates the command with an error.
    #[clap(long, default_value_t=Resolution::Fail)]
    pub resolution: Resolution,
}

pub fn run(global: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let backend = new_backend_with_prompt(&global.repo)?;
    let repo_password = cli::request_repo_password();

    let key = repository::repository::retrieve_key(repo_password, backend.clone())?;
    let secure_storage = Arc::new(
        SecureStorage::build()
            .with_key(key)
            .with_compression(zstd::DEFAULT_COMPRESSION_LEVEL),
    );

    let repo: Arc<dyn RepositoryBackend> = Arc::from(repository::repository::open(
        backend,
        secure_storage.clone(),
    )?);

    let (_snapshot_id, snapshot) = match &args.snapshot {
        RestoreSnapshot::Latest => {
            let snapshots_sorted = repo.load_all_snapshots_sorted()?;
            let s = snapshots_sorted.last();
            s.cloned()
        }
        RestoreSnapshot::Snapshot(id) => match repo.load_snapshot(id) {
            Ok(s) => Some((id.clone(), s)),
            Err(_) => None,
        },
    }
    .with_context(|| "No snapshot was found")?;

    let node_streamer =
        SerializedNodeStreamer::new(repo.clone(), secure_storage.clone(), Some(snapshot.tree));

    for node_res in node_streamer {
        match node_res {
            Ok((path, stream_node)) => {
                let restore_path = args.target.join(path);

                if restore_path.exists() {
                    match args.resolution {
                        Resolution::Skip => continue, // Skip restore
                        Resolution::Overwrite => (),  // Continue with restore
                        Resolution::Fail => {
                            cli::log_error(&format!(
                                "Target \'{}\' already exists",
                                restore_path.display()
                            ));
                            return Err(anyhow!("Failed to restore snapshot"));
                        }
                    }
                }

                restorer::restore_node(
                    repo.as_ref(),
                    secure_storage.as_ref(),
                    &stream_node.node,
                    &restore_path,
                )?
            }
            Err(_) => {
                bail!("Failed to read snapshot tree node");
            }
        }
    }

    Ok(())
}
