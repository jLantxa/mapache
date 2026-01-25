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
    utils::size,
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
            let prev = prev_node.with_context(|| {
                format!("Inconsistent state: Deleted diff but no prev_node for {path:?}")
            })?;
            report_node_diff(&prev.node, diff_type, progress_reporter);
            None
        }

        NodeDiff::Unchanged => {
            let mut next = next_node.with_context(|| {
                format!("Inconsistent state: Unchanged diff but no next_node for {path:?}")
            })?;
            let prev = prev_node.with_context(|| {
                format!("Inconsistent state: Unchanged diff but no prev_node for {path:?}")
            })?;

            next.node.blobs = prev.node.blobs;

            if next.node.is_file() {
                progress_reporter.processed_bytes(next.node.metadata.size);
            }
            report_node_diff(&next.node, diff_type, progress_reporter);
            Some(next)
        }

        NodeDiff::New | NodeDiff::Changed => {
            let mut next = next_node.with_context(|| {
                format!("Inconsistent state: New/Changed diff but no next_node for {path:?}")
            })?;

            if next.node.is_file() {
                let file = open_for_sequential_read(path)
                    .with_context(|| format!("Failed to open: {}", path.display()))?;

                let file_size = next.node.metadata.size;
                let capacity = (file_size as usize).min(size::MiB as usize);
                let mut reader = std::io::BufReader::with_capacity(capacity, file);

                let blobs_ids = chunk_and_store_file(
                    repo,
                    encoding_context,
                    &mut reader,
                    &next.node,
                    progress_reporter,
                )
                .with_context(|| format!("Failed to process blobs for: {}", path.display()))?;

                next.node.blobs = Some(blobs_ids);
            }

            report_node_diff(&next.node, diff_type, progress_reporter);
            Some(next)
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
    progress_reporter: &SnapshotProgressReporter,
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

        let id = repo
            .encode_and_save_blob(
                encoding_context,
                BlobType::Data,
                chunk.data,
                SaveID::CalculateID,
            )
            .context("Failed to save blob")?;

        chunk_ids.push(id);
    }

    Ok(chunk_ids)
}

fn store_small_file<R: Read>(
    repo: Arc<Repository>,
    encoding_context: &mut EncodingContext,
    reader: &mut R,
    node: &Node,
    progress_reporter: &SnapshotProgressReporter,
) -> Result<Vec<ID>> {
    let size = node.metadata.size as usize;

    let mut data = vec![0u8; size];
    reader.read_exact(&mut data)?;

    let id =
        repo.encode_and_save_blob(encoding_context, BlobType::Data, data, SaveID::CalculateID)?;

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
