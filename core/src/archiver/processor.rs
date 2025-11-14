use std::{
    fs::File,
    io::{BufReader, Read},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result};
#[cfg(not(feature = "custom-chunker"))]
use fastcdc::v2020::StreamCDC;

#[cfg(feature = "custom-chunker")]
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
#[cfg(feature = "custom-chunker")]
pub(crate) const DEFAULT_CHUNKER: Chunker = Chunker::new(
    mapache::defaults::MIN_CHUNK_SIZE as usize,
    mapache::defaults::AVG_CHUNK_SIZE as usize,
    mapache::defaults::MAX_CHUNK_SIZE as usize,
    mapache::defaults::CHUNKER_NORMALIZATION,
);

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

            Ok(Some((path, stream_node_info)))
        }

        NodeDiff::New | NodeDiff::Changed => {
            let mut stream_node_info = next_node
                .with_context(|| "New or changed item but the next node was not provided")?;

            // Only chunk and store the file content if it's a file
            if stream_node_info.node.is_file() {
                #[cfg(not(feature = "custom-chunker"))]
                let blobs_ids = chunk_and_store_file(
                    repo,
                    &path,
                    &stream_node_info.node,
                    progress_reporter.clone(),
                )?;
                #[cfg(feature = "custom-chunker")]
                let blobs_ids = custom_chunk_and_store_file(
                    repo,
                    &path,
                    &stream_node_info.node,
                    progress_reporter.clone(),
                )?;
                stream_node_info.node.blobs = Some(blobs_ids);
            }

            // Report progress based on the current node and diff type
            report_node_diff(&stream_node_info.node, diff_type, &progress_reporter);

            Ok(Some((path, stream_node_info)))
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
#[cfg(not(feature = "custom-chunker"))]
fn chunk_and_store_file(
    repo: Arc<Repository>,
    src_path: &Path,
    node: &Node,
    progress_reporter: Arc<SnapshotProgressReporter>,
) -> Result<Vec<ID>> {
    let source_file = File::open(src_path)
        .with_context(|| format!("Could not open file '{}'", src_path.display()))?;

    // Do not chunk if the file is smaller than the minimum chunk size
    if node.metadata.size <= mapache::defaults::MIN_CHUNK_SIZE {
        return store_small_file(repo, source_file, src_path, node, progress_reporter);
    }

    let reader = BufReader::with_capacity(mapache::defaults::MIN_CHUNK_SIZE as usize, source_file);

    let file_size = reader.get_ref().metadata()?.len();
    let estimated_num_chunks = (file_size / mapache::defaults::AVG_CHUNK_SIZE).max(1) as usize;
    let mut chunk_ids = Vec::with_capacity(estimated_num_chunks);

    let chunker = StreamCDC::with_level(
        reader,
        mapache::defaults::MIN_CHUNK_SIZE as u32,
        mapache::defaults::AVG_CHUNK_SIZE as u32,
        mapache::defaults::MAX_CHUNK_SIZE as u32,
        mapache::defaults::CHUNKER_NORMALIZATION,
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

/// Puts a file into the repository
///
/// This function will split the file into chunks for deduplication, which will be compressed,
/// encrypted and stored in the repository. Files smaller than the minimum chunk size are stored
/// directly as blobs.
#[cfg(feature = "custom-chunker")]
fn custom_chunk_and_store_file(
    repo: Arc<Repository>,
    src_path: &Path,
    node: &Node,
    progress_reporter: Arc<SnapshotProgressReporter>,
) -> Result<Vec<ID>> {
    let source_file = File::open(src_path)
        .with_context(|| format!("Could not open file '{}'", src_path.display()))?;

    // Do not chunk if the file is smaller than the minimum chunk size
    if node.metadata.size <= mapache::defaults::MIN_CHUNK_SIZE {
        return store_small_file(repo, source_file, src_path, node, progress_reporter);
    }

    let reader = BufReader::with_capacity(mapache::defaults::MIN_CHUNK_SIZE as usize, source_file);

    let file_size = reader.get_ref().metadata()?.len();
    let estimated_num_chunks = (file_size / mapache::defaults::AVG_CHUNK_SIZE).max(1) as usize;
    let mut chunk_ids = Vec::with_capacity(estimated_num_chunks);

    for result in DEFAULT_CHUNKER.stream(reader) {
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

/// Stores a small file as a single blob and reports progress.
fn store_small_file(
    repo: Arc<Repository>,
    mut file: File,
    src_path: &Path,
    node: &Node,
    progress_reporter: Arc<SnapshotProgressReporter>,
) -> Result<Vec<ID>> {
    let mut data = Vec::with_capacity(node.metadata.size as usize);
    file.read_to_end(&mut data)
        .with_context(|| format!("Failed to read source file '{}'", src_path.display()))?;

    let (id, (raw_data_size, encoded_data_size), (raw_meta_size, encoded_meta_size)) =
        repo.encode_and_save_blob(BlobType::Data, data, SaveID::CalculateID)?;

    progress_reporter.written_data_bytes(raw_data_size, encoded_data_size);
    progress_reporter.written_meta_bytes(raw_meta_size, encoded_meta_size);
    progress_reporter.processed_bytes(node.metadata.size);

    Ok(vec![id])
}
