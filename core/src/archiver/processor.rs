//! The processor module handles the actual reading and chunking of files
//! during the snapshot process.

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
    mapache::traits::BlobSaver,
    mapache::{self, BlobType, ID, SaveID},
    ui::SnapshotProgressReporter,
};

/// Reusable chunker instance.
pub(crate) const DEFAULT_CHUNKER: Chunker = Chunker::new(
    mapache::defaults::MIN_CHUNK_SIZE as usize,
    mapache::defaults::NORMAL_CHUNK_SIZE as usize,
    mapache::defaults::MAX_CHUNK_SIZE as usize,
    mapache::defaults::CHUNKER_NORMALIZATION,
);

/// Core sync processing logic. No spawn_blocking, no async.
/// Can be called from dedicated threadpool threads.
#[allow(clippy::too_many_arguments)]
pub(crate) fn process_item_sync(
    path: &Path,
    prev_node: Option<&StreamNode>,
    next_node: Option<&StreamNode>,
    diff_type: NodeDiff,
    blob_saver: Arc<dyn BlobSaver>,
    progress: &SnapshotProgress,
    progress_reporter: &dyn SnapshotProgressReporter,
    shutdown_signal: &AtomicBool,
) -> Result<Option<StreamNode>> {
    let size_hint = next_node
        .as_ref()
        .map(|n| n.node.metadata.size)
        .or_else(|| prev_node.as_ref().map(|n| n.node.metadata.size));

    progress_reporter.processing_node(path, diff_type, size_hint);

    let out = match diff_type {
        NodeDiff::Deleted => {
            let prev = prev_node.with_context(|| {
                format!("Inconsistent state: Deleted diff but no prev_node for {path:?}")
            })?;
            report_node_diff(&prev.node, diff_type, progress);
            None
        }

        NodeDiff::Unchanged => {
            let mut next = next_node
                .with_context(|| {
                    format!("Inconsistent state: Unchanged diff but no next_node for {path:?}")
                })?
                .clone();
            let prev = prev_node.with_context(|| {
                format!("Inconsistent state: Unchanged diff but no prev_node for {path:?}")
            })?;

            if next.node.is_file() {
                progress.processed_bytes(next.node.metadata.size);
                progress_reporter.processed_bytes(next.node.metadata.size);
            }
            report_node_diff(&next.node, diff_type, progress);

            next.node.metadata = prev.node.metadata.clone();
            if next.node.is_file() {
                next.node.blobs = prev.node.blobs.clone();
            }
            Some(next)
        }

        NodeDiff::New | NodeDiff::Changed => {
            let mut next = next_node
                .with_context(|| {
                    format!("Inconsistent state: New/Changed diff but no next_node for {path:?}")
                })?
                .clone();

            if next.node.is_file() {
                let file_size = next.node.metadata.size;

                let chunk_result = (|| -> Result<Vec<ID>> {
                    let file = open_for_sequential_read(path)?;

                    if file_size <= mapache::defaults::MIN_CHUNK_SIZE {
                        store_small_file(
                            blob_saver.as_ref(),
                            file,
                            file_size,
                            progress,
                            progress_reporter,
                        )
                    } else {
                        chunk_and_store_file(
                            blob_saver.as_ref(),
                            file,
                            file_size,
                            progress,
                            progress_reporter,
                            shutdown_signal,
                        )
                    }
                })();

                match chunk_result {
                    Ok(ids) => {
                        next.node.blobs = Some(ids);
                    }
                    Err(e) => {
                        progress_reporter.warning(&format!("Skipping {}: {}", path.display(), e));
                        progress.processed_bytes(file_size);
                        progress_reporter.processed_bytes(file_size);
                    }
                }
            }

            report_node_diff(&next.node, diff_type, progress);
            Some(next)
        }
    };

    if diff_type != NodeDiff::Deleted {
        progress.processed_node();
        progress_reporter.processed_node(path, diff_type, size_hint);
    }

    Ok(out)
}

/// Reports the difference in a node to the progress tracker.
#[inline]
fn report_node_diff(node: &Node, diff_type: NodeDiff, progress: &SnapshotProgress) {
    let is_dir = node.is_dir();
    progress.increment_diff(is_dir, &diff_type);
}

/// Reads a file, chunks it using the CDC chunker, and stores the chunks in the repository.
pub(crate) fn chunk_and_store_file<R: Read + Send + 'static>(
    blob_saver: &dyn BlobSaver,
    reader: R,
    file_size: u64,
    progress: &SnapshotProgress,
    progress_reporter: &dyn SnapshotProgressReporter,
    shutdown_signal: &AtomicBool,
) -> Result<Vec<ID>> {
    let stream = chunker::ChunkStream::new(reader, &DEFAULT_CHUNKER, file_size as usize);
    let mut ids = Vec::new();

    for result in stream {
        if shutdown_signal.load(Ordering::Relaxed) {
            return Err(anyhow!("Shutdown signal received"));
        }

        let chunk = result?;
        let chunk_len = chunk.data.len() as u64;

        // Perform the intensive save_blob directly in the worker thread.
        let id = blob_saver.save_blob(
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

/// Stores a small file as a single blob without chunking.
fn store_small_file<R: Read>(
    blob_saver: &dyn BlobSaver,
    mut reader: R,
    file_size: u64,
    progress: &SnapshotProgress,
    progress_reporter: &dyn SnapshotProgressReporter,
) -> Result<Vec<ID>> {
    let size = file_size as usize;
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

    // Perform the intensive save_blob directly in the worker thread.
    let id = blob_saver.save_blob(
        BlobType::Data,
        WriteContents::Owned(data),
        SaveID::CalculateID,
    )?;

    progress.processed_bytes(file_size);
    progress_reporter.processed_bytes(file_size);

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
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;

        let file = std::fs::File::open(path)?;

        // Try to set O_NOATIME to prevent updating the file's access time on read.
        // This call fails when we're not the owner of the file or root, which is fine.
        let fd = file.as_raw_fd();
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
        if flags >= 0 {
            unsafe {
                libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NOATIME);
            }
        }

        // Inform the kernel that we will read this file sequentially.
        // This triggers the kernel's internal read-ahead optimization.
        unsafe {
            libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_SEQUENTIAL);
        }

        Ok(file)
    }
    #[cfg(all(unix, not(target_os = "linux")))]
    {
        std::fs::File::open(path)
    }
}
