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

pub mod node_restorer;

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Result, bail};
use clap::ValueEnum;

use crate::{
    repository::{RepositoryBackend, snapshot::Snapshot, streamers::SerializedNodeStreamer},
    ui::restore_progress::RestoreProgressReporter,
};

#[derive(Debug, Clone, PartialEq, ValueEnum)]
pub enum Resolution {
    Skip,
    Overwrite,
    Fail,
}

pub struct Restorer {}

impl Restorer {
    pub fn restore(
        repo: Arc<dyn RepositoryBackend>,
        snapshot: &Snapshot,
        resolution: &Resolution,
        target_path: &Path,
        include: Option<Vec<PathBuf>>,
        exclude: Option<Vec<PathBuf>>,
        progress_reporter: Arc<RestoreProgressReporter>,
    ) -> Result<()> {
        let tree = snapshot.tree.clone();
        let node_streamer = SerializedNodeStreamer::new(
            repo.clone(),
            Some(tree),
            PathBuf::new(),
            include,
            exclude,
        )?;

        for node_res in node_streamer {
            let (path, stream_node) = node_res?;
            progress_reporter.processing_file(path.clone());

            let restore_path = target_path.join(&path);

            if restore_path.exists() {
                match resolution {
                    Resolution::Skip => {
                        progress_reporter.processed_file(path);
                        continue;
                    }
                    Resolution::Overwrite => {}
                    Resolution::Fail => {
                        bail!("Target \'{}\' already exists", restore_path.display());
                    }
                }
            }

            // Attempt to restore the node.
            if let Err(e) =
                node_restorer::restore_node_to_path(repo.as_ref(), &stream_node.node, &restore_path)
            {
                bail!(
                    "Failed to restore item \'{}\': {}",
                    restore_path.display(),
                    e
                )
            }
            progress_reporter.processed_file(path);
        }

        Ok(())
    }
}
