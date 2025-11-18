use std::{
    io::{BufReader, Read},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result};

use chunker::Chunker;

use crate::{
    fs::{
        node::Node,
        tree::{NodeDiff, StreamNode},
    },
    mapache::{self, BlobType, ID, SaveID},
    repository::repo::Repository,
    ui::snapshot_progress::SnapshotProgressReporter,
};

/// Reusable chunker instance.
pub(crate) const DEFAULT_CHUNKER: Chunker = Chunker::new(
    mapache::defaults::MIN_CHUNK_SIZE as usize,
    mapache::defaults::NORMAL_CHUNK_SIZE as usize,
    mapache::defaults::MAX_CHUNK_SIZE as usize,
    mapache::defaults::CHUNKER_NORMALIZATION,
);

pub(crate) fn process_item(
    (path, prev_node, next_node, diff_type): (
        &Path,
        Option<StreamNode>,
        Option<StreamNode>,
        NodeDiff,
    ),
    repo: Arc<Repository>,
    progress_reporter: Arc<SnapshotProgressReporter>,
) -> Result<Option<(PathBuf, StreamNode)>> {
    match diff_type {
        NodeDiff::Deleted => {
            let prev_node =
                prev_node.with_context(|| "Deleted item but the previous node was not provided")?;

            report_node_diff(&prev_node.node, diff_type, &progress_reporter);
            Ok(None)
        }

        NodeDiff::Unchanged => {
            let mut stream_node_info =
                next_node.with_context(|| "Unchanged item but the next node was not provided")?;
            let prev_node = prev_node
                .with_context(|| "Unchanged item but the previous node was not provided")?;

            // Copy the list of blobs from the previous snapshot
            stream_node_info.node.blobs = prev_node.node.blobs;

            // Report progress
            if stream_node_info.node.is_file() {
                progress_reporter.processed_bytes(stream_node_info.node.metadata.size);
            }
            report_node_diff(&stream_node_info.node, diff_type, &progress_reporter);

            Ok(Some((path.to_path_buf(), stream_node_info)))
        }

        NodeDiff::New | NodeDiff::Changed => {
            let mut stream_node_info = next_node
                .with_context(|| "New or changed item but the next node was not provided")?;

            let source_file = std::fs::File::open(path)?;
            let mut reader =
                BufReader::with_capacity(mapache::defaults::MIN_CHUNK_SIZE as usize, source_file);

            // Only chunk and store the file content if it's a file
            if stream_node_info.node.is_file() {
                let blobs_ids = chunk_and_store_file(
                    repo,
                    &mut reader,
                    &stream_node_info.node,
                    progress_reporter.clone(),
                )?;
                stream_node_info.node.blobs = Some(blobs_ids);
            }

            // Report progress based on the current node and diff type
            report_node_diff(&stream_node_info.node, diff_type, &progress_reporter);

            Ok(Some((path.to_path_buf(), stream_node_info)))
        }
    }
}

fn report_node_diff(
    node: &Node,
    diff_type: NodeDiff,
    progress_reporter: &SnapshotProgressReporter,
) {
    let is_dir = node.is_dir();

    match diff_type {
        NodeDiff::Deleted => {
            if is_dir {
                progress_reporter.deleted_dir();
            } else {
                progress_reporter.deleted_file();
            }
        }
        NodeDiff::Unchanged => {
            if is_dir {
                progress_reporter.unchanged_dir();
            } else {
                progress_reporter.unchanged_file();
            }
        }
        NodeDiff::New => {
            if is_dir {
                progress_reporter.new_dir();
            } else {
                progress_reporter.new_file();
            }
        }
        NodeDiff::Changed => {
            if is_dir {
                progress_reporter.changed_dir();
            } else {
                progress_reporter.changed_file();
            }
        }
    }
}

/// Puts a file into the repository
///
/// This function will split the file into chunks for deduplication, which will be compressed,
/// encrypted and stored in the repository. Files smaller than the minimum chunk size are stored
/// directly as blobs.
pub(crate) fn chunk_and_store_file<R: Read>(
    repo: Arc<Repository>,
    reader: &mut R,
    node: &Node,
    progress_reporter: Arc<SnapshotProgressReporter>,
) -> Result<Vec<ID>> {
    // Do not chunk if the file is smaller than the minimum chunk size
    if node.metadata.size <= mapache::defaults::MIN_CHUNK_SIZE {
        return store_small_file(repo, reader, node, progress_reporter);
    }

    let file_size = node.metadata.size;
    let estimated_num_chunks = (file_size / mapache::defaults::NORMAL_CHUNK_SIZE).max(1) as usize;
    let mut chunk_ids = Vec::with_capacity(estimated_num_chunks);

    for result in DEFAULT_CHUNKER.stream(reader) {
        let chunk = result.context("Failed to chunk file")?;
        progress_reporter.processed_bytes(chunk.data.len() as u64);

        let (id, (raw_data_size, encoded_data_size), (raw_meta_size, encoded_meta_size)) = repo
            .encode_and_save_blob(BlobType::Data, chunk.data, SaveID::CalculateID)
            .context("Failed to save blob")?;

        chunk_ids.push(id);
        progress_reporter.written_data_bytes(raw_data_size, encoded_data_size);
        progress_reporter.written_meta_bytes(raw_meta_size, encoded_meta_size);
    }

    Ok(chunk_ids)
}

/// Stores a small file as a single blob and reports progress.
fn store_small_file<R: Read>(
    repo: Arc<Repository>,
    reader: &mut R,
    node: &Node,
    progress_reporter: Arc<SnapshotProgressReporter>,
) -> Result<Vec<ID>> {
    let mut data = Vec::with_capacity(node.metadata.size as usize);
    reader.read_to_end(&mut data)?;

    let (id, (raw_data_size, encoded_data_size), (raw_meta_size, encoded_meta_size)) =
        repo.encode_and_save_blob(BlobType::Data, data, SaveID::CalculateID)?;

    progress_reporter.written_data_bytes(raw_data_size, encoded_data_size);
    progress_reporter.written_meta_bytes(raw_meta_size, encoded_meta_size);
    progress_reporter.processed_bytes(node.metadata.size);

    Ok(vec![id])
}
