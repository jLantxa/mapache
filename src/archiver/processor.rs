use std::{
    fs::File,
    io::{BufReader, Read},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result};
use fastcdc::v2020::{Normalization, StreamCDC};

use crate::{
    fs::{
        node::{Node, NodeType},
        tree::{NodeDiff, StreamNode},
    },
    mapache::{self, BlobType, ID, SaveID},
    repository::repo::Repository,
    ui::snapshot_progress::SnapshotProgressReporter,
};

pub(crate) fn process_item(
    (path, prev_node, next_node, diff_type): (
        PathBuf,
        Option<StreamNode>,
        Option<StreamNode>,
        NodeDiff,
    ),
    repo: Arc<Repository>,
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
                let blobs_ids = chunk_and_store_file(
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
fn chunk_and_store_file(
    repo: Arc<Repository>,
    src_path: &Path,
    node: &Node,
    progress_reporter: Arc<SnapshotProgressReporter>,
) -> Result<Vec<ID>> {
    let mut source_file = File::open(src_path)
        .with_context(|| format!("Could not open file '{}'", src_path.display()))?;

    // Do not chunk if the file is smaller than the minimum chunk size
    if node.metadata.size <= mapache::defaults::MIN_CHUNK_SIZE {
        let mut data = Vec::with_capacity(node.metadata.size as usize);
        source_file
            .read_to_end(&mut data)
            .with_context(|| format!("Failed to read source file '{}'", src_path.display()))?;

        let (id, (raw_data_size, encoded_data_size), (raw_meta_size, encoded_meta_size)) =
            repo.encode_and_save_blob(BlobType::Data, data, SaveID::CalculateID)?;
        progress_reporter.written_data_bytes(raw_data_size, encoded_data_size);
        progress_reporter.written_meta_bytes(raw_meta_size, encoded_meta_size);
        progress_reporter.processed_bytes(node.metadata.size);

        return Ok(vec![id]);
    }

    let reader = BufReader::with_capacity(mapache::defaults::MIN_CHUNK_SIZE as usize, source_file);

    let file_size = reader.get_ref().metadata()?.len();
    let estimated_num_chunks = (file_size / mapache::defaults::AVG_CHUNK_SIZE).max(1) as usize;
    let mut chunk_ids = Vec::with_capacity(estimated_num_chunks);

    // The chunker parameters must remain stable across versions, otherwise
    // same contents will no longer produce same chunks and IDs.
    let chunker = StreamCDC::with_level(
        reader,
        mapache::defaults::MIN_CHUNK_SIZE as u32,
        mapache::defaults::AVG_CHUNK_SIZE as u32,
        mapache::defaults::MAX_CHUNK_SIZE as u32,
        Normalization::Level0,
    );

    for result in chunker {
        let chunk = result.with_context(|| "Failed to chunk file")?;
        progress_reporter.processed_bytes(chunk.data.len() as u64);

        let (id, (raw_data_size, encoded_data_size), (raw_meta_size, encoded_meta_size)) = repo
            .encode_and_save_blob(BlobType::Data, chunk.data, SaveID::CalculateID)
            .with_context(|| format!("Failed to save blob from '{}'", src_path.display()))?;

        chunk_ids.push(id);
        progress_reporter.written_data_bytes(raw_data_size, encoded_data_size);
        progress_reporter.written_meta_bytes(raw_meta_size, encoded_meta_size);
    }

    Ok(chunk_ids)
}
