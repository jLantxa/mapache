use std::{io::Read, path::Path, sync::Arc};

use anyhow::{Context, Result};

use chunker::Chunker;

use crate::{
    fs::{
        node::Node,
        tree::{NodeDiff, StreamNode},
    },
    mapache::{self, BlobType, ID, SaveID},
    repository::{repo::Repository, storage::EncodingContext},
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
    encoding_context: &mut EncodingContext,
    progress_reporter: &SnapshotProgressReporter,
) -> Result<Option<StreamNode>> {
    let out = match diff_type {
        NodeDiff::Deleted => {
            let prev_node =
                prev_node.context("Deleted item but the previous node was not provided")?;
            report_node_diff(&prev_node.node, diff_type, progress_reporter);
            None
        }

        NodeDiff::Unchanged => {
            let mut stream_node_info =
                next_node.context("Unchanged item but the next node was not provided")?;
            let prev_node =
                prev_node.context("Unchanged item but the previous node was not provided")?;

            stream_node_info.node.blobs = prev_node.node.blobs;

            if stream_node_info.node.is_file() {
                progress_reporter.processed_bytes(stream_node_info.node.metadata.size);
            }
            report_node_diff(&stream_node_info.node, diff_type, progress_reporter);

            Some(stream_node_info)
        }

        NodeDiff::New | NodeDiff::Changed => {
            let mut stream_node_info =
                next_node.context("New or changed item but the next node was not provided")?;

            if stream_node_info.node.is_file() {
                let file = open_for_sequential_read(path)?;
                let mut reader = std::io::BufReader::with_capacity(
                    mapache::defaults::NORMAL_CHUNK_SIZE as usize,
                    file,
                );

                let blobs_ids = chunk_and_store_file(
                    repo,
                    encoding_context,
                    &mut reader,
                    &stream_node_info.node,
                    progress_reporter,
                )?;
                stream_node_info.node.blobs = Some(blobs_ids);
            }

            report_node_diff(&stream_node_info.node, diff_type, progress_reporter);

            Some(stream_node_info)
        }
    };

    progress_reporter.processed_node(path);

    Ok(out)
}

#[inline]
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

/// Split file into chunks and store blobs.
pub(crate) fn chunk_and_store_file<R: Read>(
    repo: Arc<Repository>,
    encoding_context: &mut EncodingContext,
    reader: &mut R,
    node: &Node,
    progress_reporter: &SnapshotProgressReporter, // borrow
) -> Result<Vec<ID>> {
    if node.metadata.size <= mapache::defaults::MIN_CHUNK_SIZE {
        return store_small_file(repo, encoding_context, reader, node, progress_reporter);
    }

    let file_size = node.metadata.size;
    let estimated_num_chunks = (file_size / mapache::defaults::NORMAL_CHUNK_SIZE).max(1) as usize;
    let mut chunk_ids = Vec::with_capacity(estimated_num_chunks);

    for result in DEFAULT_CHUNKER.stream(reader) {
        let chunk = result.context("Failed to chunk file")?;
        progress_reporter.processed_bytes(chunk.data.len() as u64);

        let (id, data_size, meta_size) = repo
            .encode_and_save_blob(
                encoding_context,
                BlobType::Data,
                chunk.data,
                SaveID::CalculateID,
            )
            .context("Failed to save blob")?;

        chunk_ids.push(id);
        progress_reporter.written_data_bytes(data_size);
        progress_reporter.written_meta_bytes(meta_size);
    }

    Ok(chunk_ids)
}

fn store_small_file<R: Read>(
    repo: Arc<Repository>,
    encoding_context: &mut EncodingContext,
    reader: &mut R,
    node: &Node,
    progress_reporter: &SnapshotProgressReporter, // borrow
) -> Result<Vec<ID>> {
    let size = node.metadata.size as usize;

    let mut data = vec![0u8; size];
    reader.read_exact(&mut data)?;

    let (id, data_size, meta_size) =
        repo.encode_and_save_blob(encoding_context, BlobType::Data, data, SaveID::CalculateID)?;

    progress_reporter.written_data_bytes(data_size);
    progress_reporter.written_meta_bytes(meta_size);
    progress_reporter.processed_bytes(node.metadata.size);

    Ok(vec![id])
}

fn open_for_sequential_read(path: &Path) -> std::io::Result<std::fs::File> {
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        const FILE_FLAG_SEQUENTIAL_SCAN: u32 = 0x0800_0000;
        std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(FILE_FLAG_SEQUENTIAL_SCAN)
            .open(path)
    }
    #[cfg(not(windows))]
    {
        std::fs::File::open(path)
    }
}
