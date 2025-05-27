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

use anyhow::{Result, bail};

use crate::{
    repository::{
        RepositoryBackend,
        tree::{NodeDiff, NodeType, StreamNode},
    },
    ui::snapshot_progress::SnapshotProgressReporter,
};

use super::file_saver;

pub(crate) fn process_item(
    item: (PathBuf, Option<StreamNode>, Option<StreamNode>, NodeDiff),
    repo: &dyn RepositoryBackend,
    progress_reporter: &Arc<SnapshotProgressReporter>,
) -> Result<Option<(PathBuf, StreamNode)>> {
    let (path, prev_node, next_node, diff_type) = item;

    match diff_type {
        // Deleted item: We don't need to save anything and this node will not be present in the
        // serialized tree. We just ignore it. Maybe notify a progress reporter.
        NodeDiff::Deleted => {
            // Notify the reporter
            match prev_node {
                Some(node_info) => {
                    if node_info.node.is_dir() {
                        progress_reporter.deleted_dir();
                    } else {
                        progress_reporter.deleted_file();
                    }
                }
                None => bail!("Item deleted but the node was not provided"),
            }

            Ok(None)
        }

        // Unchanged item: No need to save the content, but we still need to serialize the node.
        // Use the prev_node, since it comes from the serialized tree and contains the list of blobs.
        NodeDiff::Unchanged => match prev_node {
            None => bail!("Item unchanged but the node was not provided"),
            Some(stream_node_info) => {
                // Notify reporter
                if stream_node_info.node.is_file() {
                    let bytes_processed = stream_node_info.node.metadata.size;
                    progress_reporter.processed_bytes(bytes_processed);
                    progress_reporter.unchanged_file();
                } else if stream_node_info.node.is_dir() {
                    progress_reporter.unchanged_dir();
                } else {
                    // Simlinks, block devices, etc.
                    progress_reporter.unchanged_file();
                }

                return Ok(Some((path, stream_node_info)));
            }
        },

        // New or changed item: We need to save the contents and serialize the node.
        NodeDiff::New | NodeDiff::Changed => match next_node {
            None => bail!("Item new or changed but the node was not provided"),
            Some(mut stream_node_info) => {
                // If node is a file, save the contents
                if stream_node_info.node.is_file() {
                    let blobs_ids = file_saver::save_file(
                        repo,
                        &path,
                        &stream_node_info.node,
                        progress_reporter,
                    )?;
                    stream_node_info.node.contents = Some(blobs_ids);
                }

                match stream_node_info.node.node_type {
                    NodeType::File
                    | NodeType::Symlink
                    | NodeType::BlockDevice
                    | NodeType::CharDevice
                    | NodeType::Fifo
                    | NodeType::Socket => {
                        // Notify reporter
                        if diff_type == NodeDiff::New {
                            progress_reporter.new_file();
                        } else if diff_type == NodeDiff::Changed {
                            progress_reporter.changed_file();
                        }
                    }
                    NodeType::Directory => {
                        // Notify reporter
                        if diff_type == NodeDiff::New {
                            progress_reporter.new_dir();
                        } else if diff_type == NodeDiff::Changed {
                            progress_reporter.changed_dir();
                        }
                    }
                }

                return Ok(Some((path, stream_node_info)));
            }
        },
    }
}
