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

use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result};

use crate::{
    repository::{
        RepositoryBackend,
        streamers::{NodeDiff, StreamNode},
        tree::NodeType,
    },
    ui::snapshot_progress::SnapshotProgressReporter,
};

use super::chunker;

pub(crate) fn process_item(
    (path, prev_node, next_node, diff_type): (
        PathBuf,
        Option<StreamNode>,
        Option<StreamNode>,
        NodeDiff,
    ),
    repo: Arc<dyn RepositoryBackend>,
    progress_reporter: Arc<SnapshotProgressReporter>,
) -> Result<Option<(PathBuf, StreamNode)>> {
    match diff_type {
        NodeDiff::Deleted => {
            // Deleted item: We don't need to save anything and this node will not be present in the
            // serialized tree. We just ignore it.

            // Notify the reporter about the deleted item.
            let prev_node =
                prev_node.with_context(|| "Deleted item but the previous node was not provided")?;
            if prev_node.node.is_dir() {
                progress_reporter.deleted_dir();
            } else {
                progress_reporter.deleted_file();
            }
            Ok(None)
        }

        NodeDiff::Unchanged => {
            // Unchanged item: No need to save content, but we still need to serialize the node.
            // Use `prev_node` as it contains the list of blobs from the previous snapshot.
            let stream_node_info = prev_node
                .with_context(|| "Unchanged item but the previous node was not provided")?;

            // Notify reporter based on node type.
            if stream_node_info.node.is_file() {
                let bytes_processed = stream_node_info.node.metadata.size;
                progress_reporter.processed_bytes(bytes_processed);
                progress_reporter.unchanged_file();
            } else if stream_node_info.node.is_dir() {
                progress_reporter.unchanged_dir();
            } else {
                // Catches symlinks, block devices, char devices, fifos, sockets.
                progress_reporter.unchanged_file(); // Treat non-dir as file for progress reporting.
            }

            Ok(Some((path, stream_node_info)))
        }

        NodeDiff::New | NodeDiff::Changed => {
            // New or changed item: We need to save the contents (if a file) and serialize the node.
            let mut stream_node_info = next_node
                .with_context(|| "New or changed item but the next node was not provided")?;

            // If the node is a file, save its contents to the repository.
            if stream_node_info.node.is_file() {
                let blobs_ids = chunker::save_file(
                    repo, // `repo` is an Arc, so it can be moved here.
                    &path,
                    &stream_node_info.node,
                    progress_reporter.clone(),
                )?;
                stream_node_info.node.blobs = Some(blobs_ids);
            }

            // Notify reporter based on diff type and node type.
            match stream_node_info.node.node_type {
                NodeType::File
                | NodeType::Symlink
                | NodeType::BlockDevice
                | NodeType::CharDevice
                | NodeType::Fifo
                | NodeType::Socket => {
                    if diff_type == NodeDiff::New {
                        progress_reporter.new_file();
                    } else {
                        // NodeDiff::Changed
                        progress_reporter.changed_file();
                    }
                }
                NodeType::Directory => {
                    if diff_type == NodeDiff::New {
                        progress_reporter.new_dir();
                    } else {
                        // NodeDiff::Changed
                        progress_reporter.changed_dir();
                    }
                }
            }

            Ok(Some((path, stream_node_info)))
        }
    }
}
