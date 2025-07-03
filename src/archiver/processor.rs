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

use std::{
    fs::File,
    io::BufReader,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, bail};
use fastcdc::v2020::{Normalization, StreamCDC};

use crate::{
    global::{self, BlobType, ID},
    repository::{
        RepositoryBackend,
        streamers::{NodeDiff, StreamNode},
        tree::{Node, NodeType},
    },
    ui::snapshot_progress::SnapshotProgressReporter,
};

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
            let mut stream_node_info = next_node
                .with_context(|| "Unchanged item but the previous node was not provided")?;

            // We take the `next` node, but we need to copy the list of blobs
            stream_node_info.node.blobs = prev_node.unwrap().node.blobs;

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
                let blobs_ids = save_file(
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

/// Puts a file into the repository
///
/// This function will split the file into chunks for deduplication, which will be compressed,
/// encrypted and stored in the repository. Files smaller than the minimum chunk size are stored
/// directly as blobs.
pub(crate) fn save_file(
    repo: Arc<dyn RepositoryBackend>,
    src_path: &Path,
    node: &Node,
    progress_reporter: Arc<SnapshotProgressReporter>,
) -> Result<Vec<ID>> {
    // Do not chunk if the file is smaller than the minimum chunk size
    if node.metadata.size < global::defaults::MIN_CHUNK_SIZE {
        let data = std::fs::read(src_path)?;
        let (id, raw_size, encoded_size) =
            repo.save_blob(BlobType::Data, data, global::SaveID::CalculateID)?;
        progress_reporter.written_data_bytes(raw_size, encoded_size);
        progress_reporter.processed_bytes(node.metadata.size);

        Ok(vec![id])
    } else {
        chunk_and_save_blobs(repo, src_path, progress_reporter)
    }
}

// Chunks the file and saves the blobs in the repository.
fn chunk_and_save_blobs(
    repo: Arc<dyn RepositoryBackend>,
    src_path: &Path,
    progress_reporter: Arc<SnapshotProgressReporter>,
) -> Result<Vec<ID>> {
    let source = File::open(src_path)
        .with_context(|| format!("Could not open file \'{}\'", src_path.display()))?;
    let reader = BufReader::new(source);

    let mut chunk_ids = Vec::new();

    // The chunker parameters must remain stable across versions, otherwise
    // same contents will no longer produce same chunks and IDs.
    let chunker = StreamCDC::with_level(
        reader,
        global::defaults::MIN_CHUNK_SIZE as u32,
        global::defaults::AVG_CHUNK_SIZE as u32,
        global::defaults::MAX_CHUNK_SIZE as u32,
        Normalization::Level1,
    );

    for result in chunker {
        let chunk = result.with_context(|| "Failed to chunk file")?;

        let id: ID = ID::from_content(&chunk.data);
        chunk_ids.push(id.clone());

        let repo_clone = repo.clone();
        let pr = progress_reporter.clone();

        let processed_size = chunk.data.len() as u64;
        let save_blob_res =
            repo_clone.save_blob(BlobType::Data, chunk.data, global::SaveID::WithID(id));

        match save_blob_res {
            Ok((_id, raw_size, encoded_size)) => {
                pr.written_data_bytes(raw_size, encoded_size);
                pr.processed_bytes(processed_size);
            }
            Err(e) => bail!("Failed to save blob to repository: {:?}", e),
        }
    }

    Ok(chunk_ids)
}
