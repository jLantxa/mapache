use std::{
    io::Read,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use anyhow::{Context, Result, anyhow};
use chunker::Chunker;

use crate::{
    archiver::SnapshotProgress,
    backend::WriteContents,
    fs::{
        node::Node,
        tree::{NodeDiff, StreamNode},
    },
    mapache::{self, BlobType, ID, SaveID},
    repository::repo::Repository,
    ui::snapshot::SnapshotProgressReporter,
};

/// Reusable chunker instance.
pub(crate) const DEFAULT_CHUNKER: Chunker = Chunker::new(
    mapache::defaults::MIN_CHUNK_SIZE as usize,
    mapache::defaults::NORMAL_CHUNK_SIZE as usize,
    mapache::defaults::MAX_CHUNK_SIZE as usize,
    mapache::defaults::CHUNKER_NORMALIZATION,
);

/// Processes an item from the diff stream.
pub(crate) async fn process_item(
    (path, prev_node_res, next_node_res, diff_type): (
        &Path,
        Option<Result<StreamNode>>,
        Option<Result<StreamNode>>,
        NodeDiff,
    ),
    repo: Arc<Repository>,
    progress: Arc<SnapshotProgress>,
    progress_reporter: Arc<dyn SnapshotProgressReporter>,
    shutdown_signal: Arc<AtomicBool>,
) -> Result<Option<StreamNode>> {
    // Check if next_node has an error. If so, report it and skip the node.
    if let Some(Err(e)) = next_node_res {
        progress_reporter.warning(&format!("Skipping {}: {}", path.display(), e));
        progress.processed_node();
        return Ok(None);
    }

    // From here on, next_node_res is either None or Some(Ok(next_node)).
    let next_node = next_node_res.map(|r| r.unwrap());
    let prev_node = match prev_node_res {
        Some(Ok(n)) => Some(n),
        Some(Err(_)) => None, // Should not happen in practice for SerializedNodeStream
        None => None,
    };

    progress_reporter.processing_node(path, diff_type);

    let out = match diff_type {
        NodeDiff::Deleted => {
            let prev = prev_node.with_context(|| {
                format!("Inconsistent state: Deleted diff but no prev_node for {path:?}")
            })?;
            report_node_diff(&prev.node, diff_type, progress.as_ref());
            None
        }

        NodeDiff::Unchanged => {
            let mut next = next_node.with_context(|| {
                format!("Inconsistent state: Unchanged diff but no next_node for {path:?}")
            })?;
            let prev = prev_node.with_context(|| {
                format!("Inconsistent state: Unchanged diff but no prev_node for {path:?}")
            })?;

            if next.node.is_file() {
                progress.processed_bytes(next.node.metadata.size);
                progress_reporter.processed_bytes(next.node.metadata.size);
            }
            report_node_diff(&next.node, diff_type, progress.as_ref());

            // Use the previous node's metadata to ensure bit-identical trees,
            // but keep the current structure (like next.num_children) to allow
            // correctly building the tree even if excludes changed.
            next.node.metadata = prev.node.metadata;
            // Also keep blobs if it's a file
            if next.node.is_file() {
                next.node.blobs = prev.node.blobs;
            }
            Some(next)
        }

        NodeDiff::New | NodeDiff::Changed => {
            let mut next = next_node.with_context(|| {
                format!("Inconsistent state: New/Changed diff but no next_node for {path:?}")
            })?;

            if next.node.is_file() {
                let repo_clone = repo.clone();
                let path_clone = path.to_path_buf();
                let progress_clone = progress.clone();
                let reporter_clone = progress_reporter.clone();
                let shutdown_signal_clone = shutdown_signal.clone();
                let node_clone = next.node.clone();

                let blobs_ids = tokio::task::spawn_blocking(move || {
                    let file = open_for_sequential_read(&path_clone)?;
                    let file_size = node_clone.metadata.size;

                    if file_size <= mapache::defaults::MIN_CHUNK_SIZE {
                        store_small_file(
                            repo_clone,
                            file,
                            &node_clone,
                            progress_clone.as_ref(),
                            reporter_clone.as_ref(),
                        )
                    } else {
                        chunk_and_store_file(
                            repo_clone,
                            file,
                            &node_clone,
                            progress_clone,
                            reporter_clone,
                            shutdown_signal_clone,
                        )
                    }
                })
                .await
                .context("Blob processing panicked")??;

                next.node.blobs = Some(blobs_ids);
            }

            report_node_diff(&next.node, diff_type, progress.as_ref());
            Some(next)
        }
    };

    progress.processed_node();
    progress_reporter.processed_node(path, diff_type);

    Ok(out)
}

#[inline]
fn report_node_diff(node: &Node, diff_type: NodeDiff, progress: &SnapshotProgress) {
    let is_dir = node.is_dir();
    progress.increment_diff(is_dir, &diff_type);
}

/// Split file into chunks and store blobs.
pub(crate) fn chunk_and_store_file<R: Read + Send + 'static>(
    repo: Arc<Repository>,
    reader: R,
    node: &Node,
    progress: Arc<SnapshotProgress>,
    progress_reporter: Arc<dyn SnapshotProgressReporter>,
    shutdown_signal: Arc<AtomicBool>,
) -> Result<Vec<ID>> {
    let file_size = node.metadata.size;
    let stream = chunker::ChunkStream::new(reader, &DEFAULT_CHUNKER, file_size as usize);
    let mut ids = Vec::new();

    for result in stream {
        if shutdown_signal.load(Ordering::Relaxed) {
            return Err(anyhow!("Shutdown signal received"));
        }

        let chunk = result?;
        let chunk_len = chunk.data.len() as u64;

        let id = repo.encode_and_save_blob(
            BlobType::Data,
            WriteContents::Borrowed(&chunk.data),
            SaveID::CalculateID,
        )?;

        ids.push(id);
        progress.processed_bytes(chunk_len);
        progress_reporter.processed_bytes(chunk_len);
    }
    Ok(ids)
}

fn store_small_file<R: Read>(
    repo: Arc<Repository>,
    mut reader: R,
    node: &Node,
    progress: &SnapshotProgress,
    progress_reporter: &dyn SnapshotProgressReporter,
) -> Result<Vec<ID>> {
    let size = node.metadata.size as usize;
    let mut data = Vec::with_capacity(size);

    unsafe {
        // SAFETY: Memory is allocated but uninitialized; set_len is deferred until read_exact
        // guarantees initialization, ensuring no UB if a panic or error occurs during I/O.
        let slice = std::slice::from_raw_parts_mut(
            data.as_mut_ptr() as *mut std::mem::MaybeUninit<u8>,
            size,
        );
        let buffer = &mut *(slice as *mut [std::mem::MaybeUninit<u8>] as *mut [u8]);
        reader.read_exact(buffer)?;
        data.set_len(size);
    }

    let id = repo.encode_and_save_blob(
        BlobType::Data,
        WriteContents::Owned(data),
        SaveID::CalculateID,
    )?;

    progress.processed_bytes(node.metadata.size);
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
