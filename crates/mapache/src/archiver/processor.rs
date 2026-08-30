//! The processor module handles the actual reading and chunking of files
//! during the snapshot process.

use std::{
    io::{self, Read},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};

use mapache_chunker::Chunker;

use crate::{
    archiver::SnapshotProgress,
    backend::WriteContents,
    common::error::{MapacheError, Result},
    common::{self, BlobType, ID, SaveID, traits::BlobSaver},
    fs::{
        node::Node,
        tree::{NodeDiff, StreamNode},
    },
    ui::events::{BackupEvent, Event, EventSender, emit_event},
};

/// Returns `true` if every byte in `data` is zero.
/// Fast path: O(1) when first byte is non-zero (the common case).
#[inline]
pub(crate) fn is_all_zero(data: &[u8]) -> bool {
    data.first().is_some_and(|&b| b == 0) && data.iter().all(|&b| b == 0)
}

/// Reusable buffers that persist across processing calls in a pool thread.
#[derive(Default)]
pub(crate) struct ReusableBuffers {
    pub small_buf: Vec<u8>,
}

/// A Read wrapper around stdin that is `Send` and counts bytes read.
/// Each `read()` call acquires and releases the stdin lock,
/// so no lock is held across yield points.
pub(crate) struct StdinReader {
    count: u64,
}

impl StdinReader {
    pub(crate) fn new() -> Self {
        StdinReader { count: 0 }
    }

    pub(crate) fn bytes_read(&self) -> u64 {
        self.count
    }
}

impl Read for StdinReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = io::stdin().lock().read(buf)?;
        self.count += n as u64;
        Ok(n)
    }
}

impl Default for StdinReader {
    fn default() -> Self {
        Self::new()
    }
}

/// Reusable chunker instance.
pub(crate) const DEFAULT_CHUNKER: Chunker = Chunker::new(
    common::defaults::MIN_CHUNK_SIZE as usize,
    common::defaults::NORMAL_CHUNK_SIZE as usize,
    common::defaults::MAX_CHUNK_SIZE as usize,
    common::defaults::CHUNKER_NORMALIZATION,
);

/// Environment context for processing items during snapshot archiving.
pub(crate) struct ItemContext<'a> {
    pub blob_saver: Arc<dyn BlobSaver>,
    pub progress: &'a SnapshotProgress,
    pub event_sender: &'a EventSender,
    pub shutdown_signal: &'a AtomicBool,
    pub bufs: Option<&'a mut ReusableBuffers>,
}

/// Core sync processing logic for regular filesystem items.
/// Opens the file by path, chunks it, and stores the blobs.
/// When `bufs` is `Some`, the small file buffer is reused to avoid per-file allocation.
pub(crate) fn process_item_sync(
    path: &Path,
    prev_node: Option<&StreamNode>,
    next_node: Option<&StreamNode>,
    diff_type: NodeDiff,
    ctx: &mut ItemContext<'_>,
) -> Result<Option<StreamNode>> {
    let size_hint = next_node
        .as_ref()
        .map(|n| n.node.metadata.size)
        .or_else(|| prev_node.as_ref().map(|n| n.node.metadata.size));

    emit_event(
        ctx.event_sender,
        Event::Backup(BackupEvent::NodeProcessing {
            path: path.to_path_buf(),
            diff: diff_type,
            size_hint,
        }),
    );

    let out = match diff_type {
        NodeDiff::Deleted => {
            let _prev = prev_node.ok_or_else(|| {
                MapacheError::Internal(format!(
                    "inconsistent state: Deleted diff but no prev_node for {path:?}"
                ))
            })?;
            None
        }

        NodeDiff::Unchanged => {
            let mut next = next_node
                .ok_or_else(|| {
                    MapacheError::Internal(format!(
                        "inconsistent state: Unchanged diff but no next_node for {path:?}"
                    ))
                })?
                .clone();
            let prev = prev_node.ok_or_else(|| {
                MapacheError::Internal(format!(
                    "inconsistent state: Unchanged diff but no prev_node for {path:?}"
                ))
            })?;

            if next.node.is_file() {
                ctx.progress.processed_bytes(next.node.metadata.size);
                emit_event(
                    ctx.event_sender,
                    Event::Backup(BackupEvent::BytesProcessed(next.node.metadata.size)),
                );
            }

            // Reuse the stored blobs; keep the freshly scanned metadata so
            // metadata-only changes (chmod/chown/xattr/flags) are recorded
            // instead of being clobbered by the previous snapshot.
            if next.node.is_file() {
                next.node.blobs = prev.node.blobs.clone();
            }
            Some(next)
        }

        NodeDiff::New | NodeDiff::Changed => {
            let mut next = next_node
                .ok_or_else(|| {
                    MapacheError::Internal(format!(
                        "inconsistent state: New/Changed diff but no next_node for {path:?}"
                    ))
                })?
                .clone();

            if next.node.is_file() {
                let file_size = next.node.metadata.size;
                let blob_saver = ctx.blob_saver.clone();
                let progress = ctx.progress;
                let event_sender = ctx.event_sender.clone();
                let shutdown_signal = ctx.shutdown_signal;
                let mut fallback_small = Vec::new();
                let small_buf = match ctx.bufs.as_mut() {
                    Some(b) => &mut b.small_buf,
                    None => &mut fallback_small,
                };

                let chunk_result = (|| -> Result<(Vec<ID>, u64)> {
                    let file = open_for_sequential_read(path)?;

                    // Re-stat the opened file: the size from the directory scan
                    // may be stale (TOCTOU). Reading a stale size would silently
                    // truncate a file that grew or error on one that shrank.
                    let current_size = file.metadata()?.len();
                    if current_size != file_size {
                        emit_event(
                            ctx.event_sender,
                            Event::Backup(BackupEvent::Warning(format!(
                                "File {} changed size during backup ({} -> {}); backing up actual contents",
                                path.display(),
                                file_size,
                                current_size
                            ))),
                        );
                    }

                    if current_size <= common::defaults::MIN_CHUNK_SIZE {
                        store_small_file(
                            blob_saver.as_ref(),
                            file,
                            current_size,
                            progress,
                            &event_sender,
                            small_buf,
                        )
                    } else {
                        chunk_and_store_file(
                            blob_saver.as_ref(),
                            file,
                            current_size,
                            progress,
                            &event_sender,
                            shutdown_signal,
                        )
                    }
                })();

                match chunk_result {
                    Ok((ids, bytes_stored)) => {
                        next.node.blobs = Some(ids);
                        next.node.metadata.size = bytes_stored;
                    }
                    Err(e) => {
                        emit_event(
                            ctx.event_sender,
                            Event::Backup(BackupEvent::Warning(format!(
                                "Skipping {}: {}",
                                path.display(),
                                e
                            ))),
                        );
                        ctx.progress.processed_bytes(file_size);
                        emit_event(
                            ctx.event_sender,
                            Event::Backup(BackupEvent::BytesProcessed(file_size)),
                        );
                        report_node_diff(&next.node, diff_type, ctx.progress);
                        ctx.progress.processed_node();
                        emit_event(
                            ctx.event_sender,
                            Event::Backup(BackupEvent::NodeProcessed {
                                path: path.to_path_buf(),
                                diff: diff_type,
                                size_hint,
                            }),
                        );
                        return Ok(None);
                    }
                }
            }

            Some(next)
        }
    };

    if diff_type != NodeDiff::Deleted {
        report_node_diff(
            &out.as_ref()
                .expect("non-Deleted branch always returns Some")
                .node,
            diff_type,
            ctx.progress,
        );
        ctx.progress.processed_node();
        emit_event(
            ctx.event_sender,
            Event::Backup(BackupEvent::NodeProcessed {
                path: path.to_path_buf(),
                diff: diff_type,
                size_hint,
            }),
        );
    }

    Ok(out)
}

/// Dedicated processing for stdin backup data.
/// Always writes to the virtual path `/stdin`.
/// Takes a generic reader so it can be tested with byte slices or other sources.
/// Always uses the streaming chunker (never the small-file path).
pub(crate) fn process_stdin_sync(
    next_node: Option<&StreamNode>,
    mut reader: StdinReader,
    blob_saver: Arc<dyn BlobSaver>,
    progress: &SnapshotProgress,
    event_sender: &EventSender,
    shutdown_signal: &AtomicBool,
) -> Result<Option<StreamNode>> {
    const STDIN_PATH: &str = "/stdin";
    let stdin_path = Path::new(STDIN_PATH);
    let size_hint = next_node.as_ref().map(|n| n.node.metadata.size);
    emit_event(
        event_sender,
        Event::Backup(BackupEvent::NodeProcessing {
            path: stdin_path.to_path_buf(),
            diff: NodeDiff::New,
            size_hint,
        }),
    );

    let mut next = next_node
        .ok_or_else(|| {
            MapacheError::Internal(format!(
                "inconsistent state: stdin but no next_node for {STDIN_PATH:?}"
            ))
        })?
        .clone();

    if next.node.is_file() {
        let chunk_result = chunk_and_store_file(
            blob_saver.as_ref(),
            &mut reader,
            common::defaults::NORMAL_CHUNK_SIZE,
            progress,
            event_sender,
            shutdown_signal,
        );

        match chunk_result {
            Ok((ids, _)) => {
                next.node.blobs = Some(ids);
                next.node.metadata.size = reader.bytes_read();
            }
            Err(e) => {
                emit_event(
                    event_sender,
                    Event::Backup(BackupEvent::Warning(format!("Error reading stdin: {}", e))),
                );
                progress.processed_node();
                emit_event(
                    event_sender,
                    Event::Backup(BackupEvent::NodeProcessed {
                        path: stdin_path.to_path_buf(),
                        diff: NodeDiff::New,
                        size_hint,
                    }),
                );
                return Err(e);
            }
        }
    }

    report_node_diff(&next.node, NodeDiff::New, progress);
    progress.processed_node();
    emit_event(
        event_sender,
        Event::Backup(BackupEvent::NodeProcessed {
            path: stdin_path.to_path_buf(),
            diff: NodeDiff::New,
            size_hint,
        }),
    );

    Ok(Some(next))
}

/// Reports the difference in a node to the progress tracker.
#[inline]
fn report_node_diff(node: &Node, diff_type: NodeDiff, progress: &SnapshotProgress) {
    let is_dir = node.is_dir();
    progress.increment_diff(is_dir, &diff_type);
}

/// Reads a file, chunks it using the CDC chunker, and stores the chunks in the repository.
///
/// A background producer thread reads the file and runs the CDC chunker, sending raw
/// chunk payloads over a bounded channel (capacity 2). The calling thread receives
/// these chunks and performs the CPU-heavy work (zstd compression + AES encryption)
/// via `save_blob`. This overlaps file I/O and chunk-boundary scanning with
/// compression/encryption, improving throughput on multi-core systems.
///
/// Returns the stored blob IDs and the number of bytes actually read from the
/// file (which may differ from the scanned `file_size`).
pub(crate) fn chunk_and_store_file<R: Read + Send>(
    blob_saver: &dyn BlobSaver,
    reader: R,
    file_size: u64,
    progress: &SnapshotProgress,
    event_sender: &EventSender,
    shutdown_signal: &AtomicBool,
) -> Result<(Vec<ID>, u64)> {
    let (chunk_tx, chunk_rx) = crossbeam_channel::bounded::<Result<Vec<u8>>>(2);
    let rt_handle = tokio::runtime::Handle::try_current().ok();

    thread::scope(|s| {
        s.spawn(move || {
            let _guard = rt_handle.as_ref().map(|h| h.enter());

            let initial_capacity = usize::try_from(file_size).unwrap_or(0);
            let stream =
                mapache_chunker::ChunkStream::new(reader, &DEFAULT_CHUNKER, initial_capacity);
            for result in stream {
                let chunk = match result {
                    Ok(c) => c,
                    Err(e) => {
                        let _ = chunk_tx.send(Err(MapacheError::Chunking(format!("{e}"))));
                        return;
                    }
                };
                if chunk_tx.send(Ok(chunk.data)).is_err() {
                    return;
                }
            }
        });

        let mut ids = Vec::new();
        let mut total_bytes = 0u64;

        for msg in chunk_rx {
            let chunk_data = msg?;

            if shutdown_signal.load(Ordering::Acquire) {
                return Err(MapacheError::Interrupted);
            }

            let chunk_len = chunk_data.len() as u64;
            total_bytes += chunk_len;

            let blob_type = if is_all_zero(&chunk_data) {
                BlobType::Zero
            } else {
                BlobType::Data
            };

            let id = blob_saver.save_blob(
                blob_type,
                WriteContents::Borrowed(&chunk_data),
                SaveID::CalculateID,
            )?;

            ids.push(id);

            progress.processed_bytes(chunk_len);
            emit_event(
                event_sender,
                Event::Backup(BackupEvent::BytesProcessed(chunk_len)),
            );
        }

        Ok((ids, total_bytes))
    })
}

/// Stores a small file as a single blob without chunking.
/// Uses `buf` for storage to reuse allocations across calls.
///
/// Returns the stored blob IDs and the number of bytes actually read from the
/// file (which may differ from the scanned `file_size`).
fn store_small_file<R: Read>(
    blob_saver: &dyn BlobSaver,
    mut reader: R,
    file_size: u64,
    progress: &SnapshotProgress,
    event_sender: &EventSender,
    buf: &mut Vec<u8>,
) -> Result<(Vec<ID>, u64)> {
    if file_size == 0 {
        progress.processed_bytes(0);
        emit_event(event_sender, Event::Backup(BackupEvent::BytesProcessed(0)));
        return Ok((vec![], 0)); // Empty files produce an empty vector of blobs
    }

    let size = usize::try_from(file_size)
        .map_err(|e| MapacheError::Config(format!("file too large for this platform: {e}")))?;
    buf.clear();
    buf.reserve(size);

    // Read to EOF instead of exactly `file_size` bytes: a file that grew since
    // the directory scan would otherwise be silently truncated, and one that
    // shrank would fail with an unexpected-EOF error.
    let bytes = reader.read_to_end(buf)? as u64;

    // Perform the intensive save_blob directly in the worker thread.
    let blob_type = if is_all_zero(buf) {
        BlobType::Zero
    } else {
        BlobType::Data
    };

    let id = blob_saver.save_blob(blob_type, WriteContents::Borrowed(buf), SaveID::CalculateID)?;

    progress.processed_bytes(bytes);
    emit_event(
        event_sender,
        Event::Backup(BackupEvent::BytesProcessed(bytes)),
    );

    Ok((vec![id], bytes))
}

pub(crate) fn open_for_sequential_read(path: &Path) -> std::io::Result<std::fs::File> {
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

        let flags = unsafe {
            // SAFETY: FFI call to fcntl to get current flags. fd is valid.
            libc::fcntl(fd, libc::F_GETFL)
        };
        if flags >= 0 {
            unsafe {
                // SAFETY: FFI call to fcntl to set O_NOATIME. fd is valid.
                libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NOATIME);
            }
        }

        // Inform the kernel that we will read this file sequentially.
        // This triggers the kernel's internal read-ahead optimization.
        unsafe {
            // SAFETY: FFI call to posix_fadvise with a valid file descriptor.
            libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_SEQUENTIAL);
        }

        Ok(file)
    }
    #[cfg(all(unix, not(target_os = "linux")))]
    {
        std::fs::File::open(path)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use zeroize::Zeroizing;

    use crate::{
        backend::mock::MockBackend,
        common::defaults::TEST_REPO_CONFIG,
        repository::repo::{Auth, Repository, THIS_REPOSITORY_VERSION},
        ui::events::noop_sender,
        utils::size,
    };

    use super::*;

    async fn setup_repo() -> Arc<Repository> {
        let auth = Auth {
            username: "test".to_string(),
            password: Zeroizing::new("test".to_string()),
        };
        let backend = Arc::new(MockBackend::new());
        Repository::init(THIS_REPOSITORY_VERSION, &auth, None, backend.clone(), None)
            .await
            .unwrap();
        let (repo, _) = Repository::try_open_unlocked(&auth, None, backend, TEST_REPO_CONFIG)
            .await
            .unwrap();
        repo.init_pack_saver(1).unwrap();
        repo
    }

    /// Helper to create progress and noop sender.
    fn setup_progress() -> (Arc<SnapshotProgress>, EventSender) {
        let progress = Arc::new(SnapshotProgress::new());
        let sender = noop_sender();
        (progress, sender)
    }

    #[tokio::test]
    async fn test_chunk_and_store_empty_data() {
        let repo = setup_repo().await;
        let (progress, sender) = setup_progress();
        let shutdown = Arc::new(AtomicBool::new(false));

        let data: &[u8] = &[];
        let (ids, bytes) =
            chunk_and_store_file(repo.as_ref(), data, 0, &progress, &sender, &shutdown)
                .expect("chunk_and_store_file should succeed for empty data");

        // Empty input produces no blobs (the chunker yields no chunks for empty input).
        assert!(ids.is_empty(), "empty data should produce no blobs");
        assert_eq!(bytes, 0, "empty data should store zero bytes");
    }

    #[tokio::test]
    async fn test_store_small_file_empty() {
        let repo = setup_repo().await;
        let (progress, sender) = setup_progress();
        let mut buf = Vec::new();

        let (ids, bytes) = store_small_file(
            repo.as_ref(),
            std::io::empty(),
            0,
            &progress,
            &sender,
            &mut buf,
        )
        .expect("store_small_file should succeed for empty file");

        assert!(
            ids.is_empty(),
            "empty file should produce no blobs, got {} blob(s)",
            ids.len()
        );
        assert_eq!(bytes, 0, "empty file should store zero bytes");
    }

    #[tokio::test]
    async fn test_chunk_and_store_small_data() {
        let repo = setup_repo().await;
        let (progress, sender) = setup_progress();
        let shutdown = Arc::new(AtomicBool::new(false));

        let data = b"hello world this is test data for stdin chunking";
        let (ids, bytes) = chunk_and_store_file(
            repo.as_ref(),
            data.as_slice(),
            data.len() as u64,
            &progress,
            &sender,
            &shutdown,
        )
        .expect("chunk_and_store_file should succeed");

        assert_eq!(bytes, data.len() as u64, "all data should be stored");

        assert!(
            !ids.is_empty(),
            "non-empty data should produce at least one blob"
        );
        // All returned IDs should be non-nil.
        for id in &ids {
            assert_ne!(*id, ID::default(), "blob ID should not be nil");
        }
    }

    #[tokio::test]
    async fn test_chunk_and_store_large_data() {
        let repo = setup_repo().await;
        let (progress, sender) = setup_progress();
        let shutdown = Arc::new(AtomicBool::new(false));

        // Use non-uniform data to ensure the CDC chunker finds multiple boundaries.
        // Need > NORMAL_CHUNK_SIZE (1 MiB) to reliably get multiple chunks.
        let mut data = Vec::with_capacity(4 * size::MiB as usize);
        for i in 0..4 * size::MiB as usize {
            data.push((i ^ (i >> 8) ^ (i >> 16)) as u8);
        }
        let (ids, bytes) = chunk_and_store_file(
            repo.as_ref(),
            data.as_slice(),
            data.len() as u64,
            &progress,
            &sender,
            &shutdown,
        )
        .expect("chunk_and_store_file should succeed for large data");

        assert_eq!(
            bytes,
            data.len() as u64,
            "all data should be stored, got {bytes} bytes"
        );

        assert!(
            !ids.is_empty(),
            "large data should produce at least one blob"
        );
        // With 4 MiB and normal chunk size of 1 MiB, we expect multiple chunks.
        assert!(
            ids.len() > 1,
            "4 MiB should produce multiple chunks, got {}",
            ids.len()
        );
    }
}
