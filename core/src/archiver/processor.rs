use std::{
    io::Read,
    mem::MaybeUninit,
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
    utils::size,
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
    (path, prev_node, next_node, diff_type): (
        &Path,
        Option<StreamNode>,
        Option<StreamNode>,
        NodeDiff,
    ),
    repo: Arc<Repository>,
    progress: &SnapshotProgress,
    progress_reporter: &dyn SnapshotProgressReporter,
    shutdown_signal: Arc<AtomicBool>,
) -> Result<Option<StreamNode>> {
    progress_reporter.processing_node(path, diff_type);

    let out = match diff_type {
        NodeDiff::Deleted => {
            let prev = prev_node.with_context(|| {
                format!("Inconsistent state: Deleted diff but no prev_node for {path:?}")
            })?;
            report_node_diff(&prev.node, diff_type, progress);
            None
        }

        NodeDiff::Unchanged => {
            let mut next = next_node.with_context(|| {
                format!("Inconsistent state: Unchanged diff but no next_node for {path:?}")
            })?;
            let prev = prev_node.with_context(|| {
                format!("Inconsistent state: Unchanged diff but no prev_node for {path:?}")
            })?;

            // Re-use blobs from the previous snapshot
            next.node.blobs = prev.node.blobs;

            if next.node.is_file() {
                progress.processed_bytes(next.node.metadata.size);
                progress_reporter.processed_bytes(next.node.metadata.size);
            }
            report_node_diff(&next.node, diff_type, progress);
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
                let reader = std::io::BufReader::with_capacity(capacity, file);

                let blobs_ids = chunk_and_store_file(
                    repo,
                    &next.node,
                    reader,
                    progress,
                    progress_reporter,
                    shutdown_signal,
                )
                .await
                .with_context(|| format!("Failed to process blobs for: {}", path.display()))?;

                next.node.blobs = Some(blobs_ids);
            }

            report_node_diff(&next.node, diff_type, progress);
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
pub(crate) async fn chunk_and_store_file<R: Read + Send + 'static>(
    repo: Arc<Repository>,
    node: &Node,
    reader: R,
    progress: &SnapshotProgress,
    progress_reporter: &dyn SnapshotProgressReporter,
    shutdown_signal: Arc<AtomicBool>,
) -> Result<Vec<ID>> {
    if node.metadata.size <= mapache::defaults::MIN_CHUNK_SIZE {
        return store_small_file(repo, reader, node, progress, progress_reporter).await;
    }

    let (tx, mut rx) = tokio::sync::mpsc::channel::<(WriteContents<'static>, u64)>(2);
    let file_size = node.metadata.size;

    let shutdown_chunker = Arc::clone(&shutdown_signal);
    let chunker_handle = tokio::task::spawn_blocking(move || {
        let stream = chunker::ChunkStream::new(reader, &DEFAULT_CHUNKER, file_size as usize);
        for result in stream {
            if shutdown_chunker.load(Ordering::Relaxed) {
                return Err(anyhow!("Shutdown signal received"));
            }

            let chunk = result?;
            let len = chunk.data.len() as u64;

            tx.blocking_send((WriteContents::Owned(chunk.data), len))
                .context("Failed to send chunk: receiver dropped")?;
        }
        Ok::<(), anyhow::Error>(())
    });

    let mut chunk_ids = Vec::new();

    while let Some((data, chunk_len)) = rx.recv().await {
        if shutdown_signal.load(Ordering::Relaxed) {
            return Err(anyhow!("Shutdown signal received during processing"));
        }

        progress.processed_bytes(chunk_len);
        progress_reporter.processed_bytes(chunk_len);

        let id = repo
            .encode_and_save_blob(BlobType::Data, data, SaveID::CalculateID)
            .await?;

        chunk_ids.push(id);
    }

    chunker_handle.await.context("Chunker panicked")??;

    Ok(chunk_ids)
}

async fn store_small_file<R: Read>(
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
        let slice = std::slice::from_raw_parts_mut(data.as_mut_ptr() as *mut MaybeUninit<u8>, size);
        let buffer = &mut *(slice as *mut [MaybeUninit<u8>] as *mut [u8]);
        reader.read_exact(buffer)?;
        data.set_len(size);
    }

    let id = repo
        .encode_and_save_blob(
            BlobType::Data,
            WriteContents::Owned(data),
            SaveID::CalculateID,
        )
        .await?;

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
