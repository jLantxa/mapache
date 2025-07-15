// mapache is an incremental backup tool
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
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Ok, Result, bail};
use clap::ValueEnum;

use crate::{
    repository::{RepositoryBackend, snapshot::Snapshot, streamers::SerializedNodeStreamer},
    ui::restore_progress::RestoreProgressReporter,
    utils,
};

#[derive(Debug, Clone, PartialEq, ValueEnum)]
pub enum Resolution {
    Skip,
    Overwrite,
    Fail,
}

pub struct Options {
    pub resolution: Resolution,
    pub strip_prefix: Option<PathBuf>,
    pub verify: bool,
    pub dry_run: bool,
}

pub struct Restorer {}

impl Restorer {
    #[allow(clippy::too_many_arguments)]
    pub fn restore(
        repo: Arc<dyn RepositoryBackend>,
        snapshot: &Snapshot,
        target_path: &Path,
        include: Option<Vec<PathBuf>>,
        exclude: Option<Vec<PathBuf>>,
        opts: Options,
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

        // Exclude prefix components
        let strip_excludes = if let Some(ref prefix) = opts.strip_prefix {
            let (_, inter) =
                utils::get_intermediate_paths(&PathBuf::new(), &[prefix.to_path_buf()]);
            inter.into_keys().collect()
        } else {
            BTreeSet::new()
        };

        // Stack directories to restore file times later
        // Modifying the metadata of a node changes the file times of the parent directory.
        // Since the SerializedNodeStreamer emits paths in lexicographical order, we can
        // pop them in reverse order from the stack.
        let mut dir_stack = Vec::new();

        let mut verified_blobs = BTreeSet::new();

        for node_res in node_streamer {
            let (mut path, stream_node) = node_res?;
            progress_reporter.processing_file(path.clone());

            if let Some(prefix) = &opts.strip_prefix {
                if strip_excludes.contains(&path) {
                    continue;
                }

                path = path
                    .strip_prefix(prefix)
                    .with_context(|| "Failed to strip prefix from restore path")?
                    .to_path_buf();

                if path.to_str().unwrap() == "" {
                    continue;
                }
            }

            let restore_path = target_path.join(&path);

            if restore_path.exists() {
                match opts.resolution {
                    Resolution::Skip => {
                        progress_reporter.processed_file(&path);
                        continue;
                    }
                    Resolution::Overwrite => {}
                    Resolution::Fail => {
                        bail!("Target \'{}\' already exists", restore_path.display());
                    }
                }
            }

            if stream_node.node.is_dir() {
                let path = restore_path.clone();
                let atime = stream_node.node.metadata.accessed_time;
                let mtime = stream_node.node.metadata.modified_time;
                dir_stack.push((path, atime, mtime));
            }

            // Attempt to restore the node.
            if let Err(e) = node_restorer::restore_node_to_path(
                repo.as_ref(),
                progress_reporter.clone(),
                &stream_node.node,
                &restore_path,
                opts.verify,
                &mut verified_blobs,
                opts.dry_run,
            ) {
                bail!(
                    "Failed to restore item \'{}\': {}",
                    restore_path.display(),
                    e
                )
            }

            progress_reporter.processed_file(&path);
        }

        // Second pass for the directory file times
        if !opts.dry_run {
            while let Some((path, atime, mtime)) = dir_stack.pop() {
                node_restorer::restore_times(&path, atime.as_ref(), mtime.as_ref())?;
            }
        }

        Ok(())
    }
}
