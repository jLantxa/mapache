//! The restorer module implements the logic for restoring files, directories, and
//! symlinks from a repository snapshot to a local filesystem.
//! It uses a pack-centric approach with background prefetching and concurrent
//! restoration for high performance.

pub(crate) mod node_restorer;

mod metadata;
mod pack_restorer;
mod planner;
mod sync;

pub(crate) use planner::RestorePlan;

pub use sync::{SyncOpts, delete_nodes};

#[cfg(not(unix))]
use std::io::{Seek, Write};
#[cfg(unix)]
use std::os::unix::io::AsRawFd;
use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use clap::ValueEnum;
use futures::StreamExt;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::{
    common::{
        ID,
        error::{MapacheError, Result},
    },
    fs::{node::Node, tree::SerializedNodeStream},
    repository::{repo::Repository, snapshot::Snapshot},
    ui::events::{Event, EventSender, RestoreEvent, emit_event},
    utils,
};

/// Strategy for handling existing files during restoration.
#[derive(Debug, Clone, PartialEq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Strategy {
    Fail,
    Overwrite,
    Skip,
    Newer,
}

impl std::str::FromStr for Strategy {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "fail" => Ok(Self::Fail),
            "overwrite" => Ok(Self::Overwrite),
            "skip" => Ok(Self::Skip),
            "newer" => Ok(Self::Newer),
            _ => Err(format!(
                "invalid strategy: {s}. Must be one of: fail, overwrite, skip, newer"
            )),
        }
    }
}

/// Options for the restoration process.
pub struct RestoreOptions {
    pub strategy: Strategy,
    pub strip_prefix: Option<PathBuf>,
    pub dry_run: bool,
    pub quit_on_error: bool,
    pub preallocate: bool,
    pub verify: bool,
    pub include: Option<Vec<PathBuf>>,
    pub exclude: Option<Vec<PathBuf>>,
    /// Maximum number of files per plan chunk (None = no limit, all at once).
    pub batch_size: Option<usize>,
}

/// Performs the restoration of a snapshot to a target path.
pub async fn restore(
    repo: Arc<Repository>,
    snapshot: &Snapshot,
    target_path: &Path,
    opts: RestoreOptions,
    event_sender: EventSender,
    shutdown_signal: Arc<AtomicBool>,
) -> Result<()> {
    let restorer = Restorer::new(
        repo,
        target_path.to_path_buf(),
        opts,
        event_sender,
        shutdown_signal,
    );

    restorer.restore(snapshot).await
}

/// The Restorer is responsible for coordinating the restoration process.
///
/// It implements a streaming, pack-centric approach:
/// 1. Walks the snapshot tree and accumulates file/blob references.
/// 2. Every `batch_size` files, flushes the accumulated plan:
///    downloads pack segments, decodes blobs, writes files.
/// 3. After all files are restored, creates hardlinks.
/// 4. Restores metadata in a separate bottom-up pass.
pub(crate) struct Restorer {
    pub(crate) repo: Arc<Repository>,
    pub(crate) event_sender: EventSender,
    pub(crate) shutdown_signal: Arc<AtomicBool>,
    pub(crate) target_path: PathBuf,
    pub(crate) opts: RestoreOptions,
}

pub(crate) type PackMap = HashMap<ID, Vec<(ID, BlobRestoreRequest)>>;
/// file_idx -> list of (offset_in_file, raw_length) for zero blobs
pub(crate) type ZeroBatchMap = HashMap<usize, Vec<(u64, u32)>>;
type HardlinkByPath = (PathBuf, PathBuf);
type PrimaryHardlinks = Arc<Mutex<HashMap<(u64, u64), (PathBuf, ID)>>>;

pub(crate) struct FileRestorePlan {
    pub(crate) path: PathBuf,
    pub(crate) num_blobs: u32,
    pub(crate) size: u64,
    pub(crate) is_hardlink: bool,
    /// When true, the file was opened without truncation and only changed blobs
    /// were written. Used by the incremental restorer (--verify).
    pub(crate) is_selective: bool,
}

#[derive(Clone)]
pub(crate) struct BlobRestoreRequest {
    pub(crate) file_idx: usize,
    pub(crate) offset_in_file: u64,
}

/// A cache for open file handles during restoration.
pub(crate) struct FileHandleCache {
    handles: HashMap<usize, File>,
    order: std::collections::VecDeque<usize>,
    max_handles: usize,
}

impl FileHandleCache {
    fn new(max_handles: usize) -> Self {
        Self {
            handles: HashMap::new(),
            order: std::collections::VecDeque::new(),
            max_handles,
        }
    }

    fn touch(&mut self, file_idx: usize) {
        if let Some(pos) = self.order.iter().position(|&idx| idx == file_idx) {
            self.order.remove(pos);
            self.order.push_back(file_idx);
        }
    }

    fn get_handle(
        &mut self,
        file_idx: usize,
        path: &Path,
        plan: &FileRestorePlan,
        initialized: &std::sync::atomic::AtomicBool,
        restorer: &Restorer,
    ) -> Result<&File> {
        if self.handles.contains_key(&file_idx) {
            self.touch(file_idx);
            return self.handles.get(&file_idx).ok_or_else(|| {
                MapacheError::Internal("file handle disappeared from cache".to_string())
            });
        }

        if self.handles.len() >= self.max_handles
            && let Some(oldest_key) = self.order.pop_front()
        {
            self.handles.remove(&oldest_key);
        }

        let file = if !initialized.load(std::sync::atomic::Ordering::Acquire) {
            if let Ok(m) = fs::symlink_metadata(path) {
                if m.file_type().is_symlink() {
                    fs::remove_file(path).map_err(|e| {
                        MapacheError::Internal(format!(
                            "failed to remove symlink at {}: {e}",
                            path.display()
                        ))
                    })?;
                } else {
                    restorer.clear_readonly_attribute(path)?;
                }
            }

            Restorer::ensure_parent_dir(path)?;

            let f = if plan.is_selective {
                // Selective restore: open without truncating so unchanged bytes are preserved.
                Self::open_file_for_restore(path, false).map_err(|e| {
                    MapacheError::Internal(format!(
                        "failed to open file for selective restore: {}: {e}",
                        path.display()
                    ))
                })?
            } else {
                let mut f = Self::open_file_for_restore(path, true).map_err(|e| {
                    MapacheError::Internal(format!(
                        "failed to create/truncate file: {}: {e}",
                        path.display()
                    ))
                })?;

                if plan.size > 0 {
                    if restorer.opts.preallocate {
                        if let Err(e) = restorer.preallocate_file(&mut f, plan.size) {
                            tracing::warn!(target: "restorer", "Failed to preallocate file {}: {e}", path.display());
                        }
                    } else {
                        f.set_len(plan.size).map_err(|e| {
                            MapacheError::Internal(format!(
                                "failed to set length for sparse file: {}: {e}",
                                path.display()
                            ))
                        })?;
                    }
                }
                f
            };

            initialized.store(true, std::sync::atomic::Ordering::Release);
            f
        } else {
            Self::open_file_for_restore(path, false).map_err(|e| {
                MapacheError::Internal(format!(
                    "failed to open file for writing: {}: {e}",
                    path.display()
                ))
            })?
        };

        self.handles.insert(file_idx, file);
        self.order.push_back(file_idx);
        self.handles.get(&file_idx).ok_or_else(|| {
            MapacheError::Internal("failed to retrieve file handle after insertion".to_string())
        })
    }

    fn open_file_for_restore(path: &Path, create: bool) -> io::Result<std::fs::File> {
        let mut opts = OpenOptions::new();
        opts.write(true);
        if create {
            opts.create(true).truncate(true);
        }
        opts.open(path)
    }
}

/// A sharded wrapper around FileHandleCache to reduce lock contention.
pub(crate) struct ShardedFileHandleCache {
    shards: Box<[Mutex<FileHandleCache>]>,
    num_shards: usize,
}

impl ShardedFileHandleCache {
    fn new(max_total_handles: usize) -> Self {
        let num_shards = (std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1)
            * 4)
        .next_power_of_two()
        .min(64);
        let handles_per_shard = (max_total_handles / num_shards).max(1);

        let shards: Box<[Mutex<FileHandleCache>]> = (0..num_shards)
            .map(|_| Mutex::new(FileHandleCache::new(handles_per_shard)))
            .collect();

        Self { shards, num_shards }
    }

    fn get_shard(&self, file_idx: usize) -> &Mutex<FileHandleCache> {
        &self.shards[file_idx % self.num_shards]
    }
}

impl Restorer {
    fn new(
        repo: Arc<Repository>,
        target_path: PathBuf,
        opts: RestoreOptions,
        event_sender: EventSender,
        shutdown_signal: Arc<AtomicBool>,
    ) -> Self {
        Self {
            repo,
            target_path,
            opts,
            event_sender,
            shutdown_signal,
        }
    }

    /// Handles an error during restoration: if `quit_on_error` is set, returns
    /// an error; otherwise emits a warning/error event and continues.
    fn handle_quit_on_error(&self, msg: String, err: impl std::fmt::Display) -> Result<()> {
        if self.opts.quit_on_error {
            return Err(MapacheError::Internal(format!("{err}")));
        }
        emit_event(&self.event_sender, Event::Restore(RestoreEvent::Error(msg)));
        Ok(())
    }

    /// Creates all parent directories for a path if they don't exist.
    fn ensure_parent_dir(path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                MapacheError::Internal(format!(
                    "failed to create parent directory for {}: {e}",
                    path.display()
                ))
            })?;
        }
        Ok(())
    }

    /// Creates a shallow clone of the restorer for worker threads.
    pub(crate) fn clone_for_workers(&self) -> Self {
        Self {
            repo: self.repo.clone(),
            event_sender: self.event_sender.clone(),
            shutdown_signal: self.shutdown_signal.clone(),
            target_path: self.target_path.clone(),
            opts: RestoreOptions {
                strategy: self.opts.strategy.clone(),
                strip_prefix: self.opts.strip_prefix.clone(),
                dry_run: self.opts.dry_run,
                quit_on_error: self.opts.quit_on_error,
                preallocate: self.opts.preallocate,
                verify: self.opts.verify,
                include: self.opts.include.clone(),
                exclude: self.opts.exclude.clone(),
                batch_size: self.opts.batch_size,
            },
        }
    }

    fn preallocate_file(&self, file: &mut File, length: u64) -> Result<()> {
        if length == 0 {
            return Ok(());
        }

        #[cfg(all(unix, not(target_os = "macos")))]
        {
            let fd = file.as_raw_fd();
            // SAFETY: FFI call to posix_fallocate with a valid file descriptor.
            let result = unsafe { libc::posix_fallocate(fd, 0, length as libc::off_t) };
            if result != 0 {
                return Err(MapacheError::Io(std::io::Error::from_raw_os_error(result)));
            }
            Ok(())
        }

        #[cfg(target_os = "macos")]
        {
            let fd = file.as_raw_fd();

            const F_VOLPOSMODE: i32 = 1;
            const F_STARTPOSMODE: i32 = 3;

            let mut store = libc::fstore_t {
                fst_flags: libc::F_ALLOCATEALL,
                fst_posmode: F_VOLPOSMODE,
                fst_offset: 0,
                fst_length: length as libc::off_t,
                fst_bytesalloc: 0,
            };

            // SAFETY: FFI call to fcntl with a valid file descriptor and fstore_t pointer.
            let mut res = unsafe { libc::fcntl(fd, libc::F_PREALLOCATE, &store) };

            if res == -1 {
                store.fst_posmode = F_STARTPOSMODE;
                // SAFETY: FFI call to fcntl as fallback with same valid descriptor and pointer.
                res = unsafe { libc::fcntl(fd, libc::F_PREALLOCATE, &store) };
            }

            if res == -1 {
                return Err(MapacheError::Io(std::io::Error::last_os_error()));
            }

            file.set_len(length)?;
            Ok(())
        }

        #[cfg(not(unix))]
        {
            file.set_len(length)?;
            if length > 0 {
                file.seek(std::io::SeekFrom::Start(length - 1))?;
                file.write_all(&[0])?;
                file.set_len(length)?;
            }
            Ok(())
        }
    }

    #[cfg_attr(windows, allow(clippy::permissions_set_readonly_false))]
    fn clear_readonly_attribute(&self, path: &Path) -> Result<()> {
        let metadata = fs::metadata(path).map_err(|e| {
            MapacheError::Internal(format!(
                "Failed to get metadata for permission change: {}: {e}",
                path.display()
            ))
        })?;

        let mut perms = metadata.permissions();

        #[cfg(windows)]
        {
            if perms.readonly() {
                perms.set_readonly(false);
                fs::set_permissions(path, perms).map_err(|e| {
                    MapacheError::Internal(format!(
                        "Failed to clear readonly attribute on {}: {e}",
                        path.display()
                    ))
                })?;
            }
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = perms.mode();
            if mode & 0o200 == 0 {
                perms.set_mode(mode | 0o200);
                fs::set_permissions(path, perms).map_err(|e| {
                    MapacheError::Internal(format!(
                        "Failed to set write permission on {}: {e}",
                        path.display()
                    ))
                })?;
            }
        }

        Ok(())
    }

    async fn restore(&self, snapshot: &Snapshot) -> Result<()> {
        let tree_id = snapshot.tree;
        tracing::info!(target: "restorer", "Starting restore of tree {tree_id}");

        if !self.opts.dry_run {
            fs::create_dir_all(&self.target_path)?;
        }

        let index = self.repo.index();
        let secure_storage = self.repo.secure_storage();
        let batch_size = self.opts.batch_size.unwrap_or(usize::MAX);
        let dry_run = self.opts.dry_run;

        let (total_items, total_bytes) = self.count_restore_work(tree_id).await;
        emit_event(
            &self.event_sender,
            Event::Restore(RestoreEvent::PlanBuilt {
                total_items,
                total_bytes,
            }),
        );

        // Second pass: streaming restore
        let mut node_stream = SerializedNodeStream::new(
            self.repo.clone(),
            Some(tree_id),
            PathBuf::new(),
            self.opts.include.clone(),
            self.opts.exclude.clone(),
        )
        .await?;

        // Streaming accumulator state
        let mut chunk_files: Vec<FileRestorePlan> = Vec::new();
        let mut chunk_packs: PackMap = HashMap::new();
        // Persistent hardlink index: (dev, inode) → primary file path
        let primary_hardlinks: PrimaryHardlinks = Arc::new(Mutex::new(HashMap::new()));
        // Cross-chunk hardlinks stored as (secondary_path, primary_path)
        let pending_hardlinks: Arc<Mutex<Vec<HardlinkByPath>>> = Arc::new(Mutex::new(Vec::new()));

        let mut node_visited = 0u64;

        while let Some(node_res) = node_stream.next().await {
            if self.shutdown_signal.load(Ordering::Acquire) {
                return Err(MapacheError::Interrupted);
            }

            node_visited += 1;
            if node_visited.is_multiple_of(1024) {
                emit_event(
                    &self.event_sender,
                    Event::Restore(RestoreEvent::NodeVisited(node_visited)),
                );
            }

            let (mut path, stream_node_res) = match node_res {
                Ok(res) => res,
                Err(e) => {
                    emit_event(
                        &self.event_sender,
                        Event::Restore(RestoreEvent::Error(format!("Error reading node: {e}"))),
                    );
                    continue;
                }
            };

            let stream_node = match stream_node_res {
                Ok(node) => node,
                Err(e) => {
                    emit_event(
                        &self.event_sender,
                        Event::Restore(RestoreEvent::Warning(format!(
                            "Error deserializing node {}: {}",
                            path.display(),
                            e
                        ))),
                    );
                    continue;
                }
            };
            let node = stream_node.node;

            if let Some(prefix) = &self.opts.strip_prefix {
                path = match path.strip_prefix(prefix) {
                    Ok(p) => {
                        if p.as_os_str().is_empty() {
                            continue;
                        }
                        p.to_path_buf()
                    }
                    Err(_) => continue,
                };
            }

            let restore_path = match utils::secure_join(&self.target_path, &path) {
                Ok(p) => p,
                Err(e) => {
                    emit_event(
                        &self.event_sender,
                        Event::Restore(RestoreEvent::Error(format!(
                            "Secure join failed for {path:?}: {e}"
                        ))),
                    );
                    continue;
                }
            };

            // Directory: create, then continue
            if node.is_dir() {
                if !dry_run && let Err(e) = fs::create_dir_all(&restore_path) {
                    emit_event(
                        &self.event_sender,
                        Event::Restore(RestoreEvent::Error(format!(
                            "Failed to create directory {}: {}",
                            restore_path.display(),
                            e
                        ))),
                    );
                }
                emit_event(
                    &self.event_sender,
                    Event::Restore(RestoreEvent::ItemProcessed(restore_path)),
                );
                continue;
            }

            // Symlink: restore immediately (metadata applied in restore_node_to_path)
            if node.is_symlink() {
                if !dry_run {
                    // With --strategy overwrite, remove an existing entry so the
                    // new symlink can be created (previously EEXIST).
                    if matches!(self.opts.strategy, Strategy::Overwrite)
                        && let Err(e) = std::fs::remove_file(&restore_path)
                        && e.kind() != std::io::ErrorKind::NotFound
                    {
                        emit_event(
                            &self.event_sender,
                            Event::Restore(RestoreEvent::Warning(format!(
                                "Failed to remove existing entry for {}: {}",
                                restore_path.display(),
                                e
                            ))),
                        );
                    }
                    if let Err(_e) = node_restorer::restore_node_to_path(
                        &self.event_sender,
                        &node,
                        &restore_path,
                        false,
                    )
                    .await
                    {
                        emit_event(
                            &self.event_sender,
                            Event::Restore(RestoreEvent::Warning(format!(
                                "Failed to restore symlink {}",
                                restore_path.display(),
                            ))),
                        );
                    }
                }
                emit_event(
                    &self.event_sender,
                    Event::Restore(RestoreEvent::ItemProcessed(restore_path)),
                );
                continue;
            }

            // File: check strategy, then add to accumulator
            if node.is_file() {
                let restore_plan = self
                    .should_restore_node(&node, &restore_path, index.clone())
                    .await;

                match restore_plan {
                    Ok(RestorePlan::Skip) => {
                        let total_blob_count = node.blobs.as_ref().map_or(0, |b| b.len());
                        let total_bytes = node.metadata.size;
                        if total_blob_count > 0 || total_bytes > 0 {
                            if self.opts.verify {
                                emit_event(
                                    &self.event_sender,
                                    Event::Restore(RestoreEvent::BlobsSkipped {
                                        count: total_blob_count as u64,
                                        bytes: total_bytes,
                                    }),
                                );
                            } else {
                                emit_event(
                                    &self.event_sender,
                                    Event::Restore(RestoreEvent::BytesProcessed(total_bytes)),
                                );
                            }
                        }
                        emit_event(
                            &self.event_sender,
                            Event::Restore(RestoreEvent::ItemProcessed(restore_path)),
                        );
                        continue;
                    }
                    Ok(RestorePlan::SelectiveRestore { changed_blobs }) => {
                        // Look up all blobs to compute offsets, but only add changed ones to pack map
                        let mut file_blobs = Vec::new();
                        let mut file_ok = true;
                        if let Some(blobs) = &node.blobs {
                            let mut offset_in_file = 0u64;
                            for blob_id in blobs {
                                match index.get(blob_id).await {
                                    Some(locator) => {
                                        file_blobs.push((*blob_id, locator, offset_in_file));
                                        offset_in_file += locator.raw_length as u64;
                                    }
                                    None => {
                                        emit_event(
                                            &self.event_sender,
                                            Event::Restore(RestoreEvent::Error(format!(
                                                "Blob {blob_id} not found in index; skipping {}",
                                                restore_path.display(),
                                            ))),
                                        );
                                        file_ok = false;
                                        break;
                                    }
                                };
                            }
                        }
                        if !file_ok {
                            continue;
                        }

                        // Emit BlobsSkipped event for incremental restore progress
                        let total_blob_count = node.blobs.as_ref().map_or(0, |b| b.len());
                        let skipped_count =
                            (total_blob_count as u32).saturating_sub(changed_blobs.len() as u32);
                        if skipped_count > 0 {
                            let total_bytes: u64 = file_blobs
                                .iter()
                                .map(|(_, loc, _)| loc.raw_length as u64)
                                .sum();
                            let changed_bytes: u64 = changed_blobs
                                .iter()
                                .filter_map(|&idx| file_blobs.get(idx))
                                .map(|(_, loc, _)| loc.raw_length as u64)
                                .sum();
                            emit_event(
                                &self.event_sender,
                                Event::Restore(RestoreEvent::BlobsSkipped {
                                    count: skipped_count as u64,
                                    bytes: total_bytes - changed_bytes,
                                }),
                            );
                        }

                        let file_idx = chunk_files.len();
                        chunk_files.push(FileRestorePlan {
                            path: restore_path.clone(),
                            num_blobs: changed_blobs.len() as u32,
                            size: node.metadata.size,
                            is_hardlink: false,
                            is_selective: true,
                        });

                        // Hardlink detection
                        let (is_secondary, primary_fp) = detect_hardlink(
                            &node,
                            &restore_path,
                            &primary_hardlinks,
                            &pending_hardlinks,
                        );

                        if is_secondary {
                            chunk_files[file_idx].is_hardlink = true;
                        } else {
                            for &blob_idx in &changed_blobs {
                                if let Some((blob_id, locator, offset_in_file)) =
                                    file_blobs.get(blob_idx)
                                {
                                    chunk_packs.entry(locator.pack_id).or_default().push((
                                        *blob_id,
                                        BlobRestoreRequest {
                                            file_idx,
                                            offset_in_file: *offset_in_file,
                                        },
                                    ));
                                }
                            }

                            if let Some(fingerprint) = primary_fp
                                && let (Some(dev), Some(inode)) =
                                    (node.metadata.dev, node.metadata.inode)
                            {
                                primary_hardlinks
                                    .lock()
                                    .entry((dev, inode))
                                    .or_insert_with(|| (restore_path.clone(), fingerprint));
                            }
                        }
                        // Flush if the chunk is full
                        if chunk_files.len() >= batch_size {
                            let chunk_files = Arc::new(std::mem::take(&mut chunk_files));
                            let chunk_packs = std::mem::take(&mut chunk_packs);
                            pack_restorer::restore_packs(
                                self,
                                chunk_files,
                                chunk_packs,
                                index.clone(),
                                secure_storage.clone(),
                                dry_run,
                            )
                            .await?;
                        }
                        continue;
                    }
                    Ok(RestorePlan::FullRestore) => { /* fall through to full restore below */ }
                    Err(e) => {
                        emit_event(
                            &self.event_sender,
                            Event::Restore(RestoreEvent::Error(format!(
                                "Error checking {}: {}",
                                restore_path.display(),
                                e
                            ))),
                        );
                        if self.opts.quit_on_error || matches!(self.opts.strategy, Strategy::Fail) {
                            return Err(MapacheError::Internal(format!("{e}")));
                        }
                        continue;
                    }
                }

                // Full restore: look up all blobs
                let mut file_blobs = Vec::new();
                let mut file_ok = true;
                if let Some(blobs) = &node.blobs {
                    let mut offset_in_file = 0;
                    for blob_id in blobs {
                        let locator = match index.get(blob_id).await {
                            Some(loc) => loc,
                            None => {
                                emit_event(
                                    &self.event_sender,
                                    Event::Restore(RestoreEvent::Error(format!(
                                        "Blob {blob_id} not found in index; skipping {}",
                                        restore_path.display(),
                                    ))),
                                );
                                file_ok = false;
                                break;
                            }
                        };
                        file_blobs.push((*blob_id, locator, offset_in_file));
                        offset_in_file += locator.raw_length as u64;
                    }
                }
                if !file_ok {
                    continue;
                }

                let file_idx = chunk_files.len();
                chunk_files.push(FileRestorePlan {
                    path: restore_path.clone(),
                    num_blobs: 0,
                    size: node.metadata.size,
                    is_hardlink: false,
                    is_selective: false,
                });

                // Hardlink detection with content verification
                let (is_secondary, _fp) =
                    detect_hardlink(&node, &restore_path, &primary_hardlinks, &pending_hardlinks);

                if is_secondary {
                    chunk_files[file_idx].is_hardlink = true;
                    if !dry_run
                        && let Some(parent) = restore_path.parent()
                        && let Err(e) = fs::create_dir_all(parent)
                    {
                        emit_event(
                            &self.event_sender,
                            Event::Restore(RestoreEvent::Error(format!(
                                "Failed to create parent for hardlink {}: {}",
                                restore_path.display(),
                                e
                            ))),
                        );
                    }
                } else {
                    let num_blobs = file_blobs.len().min(u32::MAX as usize) as u32;
                    for (blob_id, locator, offset_in_file) in &file_blobs {
                        chunk_packs.entry(locator.pack_id).or_default().push((
                            *blob_id,
                            BlobRestoreRequest {
                                file_idx,
                                offset_in_file: *offset_in_file,
                            },
                        ));
                    }
                    chunk_files[file_idx].num_blobs = num_blobs;
                }

                // Flush if the chunk is full
                if chunk_files.len() >= batch_size {
                    let chunk_files = Arc::new(std::mem::take(&mut chunk_files));
                    let chunk_packs = std::mem::take(&mut chunk_packs);
                    pack_restorer::restore_packs(
                        self,
                        chunk_files,
                        chunk_packs,
                        index.clone(),
                        secure_storage.clone(),
                        dry_run,
                    )
                    .await?;
                }
            }
        }

        // Flush remaining chunk
        if !chunk_files.is_empty() {
            let chunk_files = Arc::new(std::mem::take(&mut chunk_files));
            let chunk_packs = std::mem::take(&mut chunk_packs);
            pack_restorer::restore_packs(
                self,
                chunk_files,
                chunk_packs,
                index.clone(),
                secure_storage.clone(),
                dry_run,
            )
            .await?;
        }

        // Create pending hardlinks (cross-chunk)
        if !dry_run {
            let hardlinks = std::mem::take(&mut *pending_hardlinks.lock());
            for (secondary, primary) in &hardlinks {
                if self.shutdown_signal.load(Ordering::Acquire) {
                    return Err(MapacheError::Interrupted);
                }
                if let Some(parent) = secondary.parent()
                    && let Err(e) = fs::create_dir_all(parent)
                {
                    let msg = format!(
                        "Failed to create parent for hardlink {}: {}",
                        secondary.display(),
                        e
                    );
                    self.handle_quit_on_error(msg, &e)?;
                    continue;
                }
                if let Err(e) = fs::remove_file(secondary)
                    && e.kind() != std::io::ErrorKind::NotFound
                {
                    let msg = format!(
                        "Failed to remove target for hardlink {}: {}",
                        secondary.display(),
                        e
                    );
                    self.handle_quit_on_error(msg, &e)?;
                    continue;
                }
                if let Err(e) = fs::hard_link(primary, secondary) {
                    let msg = format!(
                        "Failed to create hardlink {} -> {}: {}",
                        secondary.display(),
                        primary.display(),
                        e
                    );
                    self.handle_quit_on_error(msg, &e)?;
                    emit_event(
                        &self.event_sender,
                        Event::Restore(RestoreEvent::Warning(format!(
                            "Hardlink failed, falling back to copy {} -> {}",
                            primary.display(),
                            secondary.display(),
                        ))),
                    );
                    if let Err(copy_err) = fs::copy(primary, secondary) {
                        let msg = format!(
                            "Failed to copy {} -> {}: {}",
                            primary.display(),
                            secondary.display(),
                            copy_err
                        );
                        self.handle_quit_on_error(msg, &copy_err)?;
                    }
                }
            }
        }

        // Second pass: restore metadata by streaming the tree again.
        // This is cheaper on memory than accumulating metadata during the
        // first pass, at the cost of re-reading tree blobs.
        tracing::info!(target: "restorer", "Restoring metadata");
        self.restore_metadata(
            tree_id,
            self.opts.include.clone(),
            self.opts.exclude.clone(),
        )
        .await?;

        tracing::info!(target: "restorer", "Restoration finished");
        Ok(())
    }

    async fn count_restore_work(&self, tree_id: ID) -> (u64, u64) {
        let mut node_stream = match SerializedNodeStream::new(
            self.repo.clone(),
            Some(tree_id),
            PathBuf::new(),
            self.opts.include.clone(),
            self.opts.exclude.clone(),
        )
        .await
        {
            Ok(s) => s,
            Err(e) => {
                emit_event(
                    &self.event_sender,
                    Event::Restore(RestoreEvent::Error(format!(
                        "Failed to create node stream for counting: {e}"
                    ))),
                );
                return (0, 0);
            }
        };
        let mut items = 0u64;
        let mut bytes = 0u64;
        while let Some(node_res) = node_stream.next().await {
            if self.shutdown_signal.load(Ordering::Acquire) {
                return (items, bytes);
            }

            let (_path, stream_node_res) = match node_res {
                Ok(res) => res,
                Err(_) => continue,
            };
            let stream_node = match stream_node_res {
                Ok(n) => n,
                Err(_) => continue,
            };
            let node = stream_node.node;

            items += 1;
            if node.is_file() {
                bytes += node.metadata.size;
            }
        }

        (items, bytes)
    }
}

/// Content fingerprint for hardlink verification using blake3 over blob IDs.
fn compute_blob_fingerprint(blobs: &[ID]) -> ID {
    let mut hasher = blake3::Hasher::new();
    for blob_id in blobs {
        hasher.update(&blob_id.0);
    }
    ID(hasher.finalize().into())
}

/// Detects whether a file node is a hardlink secondary (already seen primary).
/// Returns `(is_secondary, primary_fingerprint_if_first_seen)`.
/// The fingerprint is returned to avoid recomputing it when registering the primary.
fn detect_hardlink(
    node: &Node,
    restore_path: &Path,
    primary_hardlinks: &PrimaryHardlinks,
    pending_hardlinks: &Arc<Mutex<Vec<HardlinkByPath>>>,
) -> (bool, Option<ID>) {
    if let (Some(dev), Some(inode)) = (node.metadata.dev, node.metadata.inode) {
        let nlink = node.metadata.nlink;
        if nlink.unwrap_or(0) > 1 {
            let node_blobs = node.blobs.as_deref().unwrap_or(&[]);
            let fingerprint = compute_blob_fingerprint(node_blobs);
            let mut idx = primary_hardlinks.lock();
            if let Some((primary_path, primary_fp)) = idx.get(&(dev, inode)) {
                if *primary_fp == fingerprint {
                    let secondary = restore_path.to_path_buf();
                    let primary = primary_path.clone();
                    drop(idx);
                    pending_hardlinks.lock().push((secondary, primary));
                    return (true, None);
                } else {
                    drop(idx);
                    return (false, None);
                }
            } else {
                idx.insert((dev, inode), (restore_path.to_path_buf(), fingerprint));
                drop(idx);
                return (false, Some(fingerprint));
            }
        }
    }
    (false, None)
}

#[cfg(test)]
mod tests {
    use crate::fs::node::{Metadata, NodeType};

    use super::*;

    fn make_file_node(name: &str, blobs: Vec<ID>, dev: u64, inode: u64, nlink: u64) -> Node {
        Node {
            name: name.into(),
            node_type: NodeType::File,
            metadata: Metadata {
                dev: Some(dev),
                inode: Some(inode),
                nlink: Some(nlink),
                ..Default::default()
            },
            blobs: Some(blobs),
            ..Default::default()
        }
    }

    #[test]
    fn test_compute_blob_fingerprint_deterministic() {
        let blobs = vec![ID::from_content(b"blob-a"), ID::from_content(b"blob-b")];
        let fp1 = compute_blob_fingerprint(&blobs);
        let fp2 = compute_blob_fingerprint(&blobs);
        assert_eq!(fp1, fp2);
    }

    #[test]
    fn test_compute_blob_fingerprint_different_blobs() {
        let a = vec![ID::from_content(b"x"), ID::from_content(b"y")];
        let b = vec![ID::from_content(b"y"), ID::from_content(b"x")];
        assert_ne!(compute_blob_fingerprint(&a), compute_blob_fingerprint(&b));
    }

    #[test]
    fn test_detect_hardlink_primary() {
        let primary = make_file_node("a.txt", vec![ID::from_content(b"same")], 10, 20, 2);
        let path = PathBuf::from("/restore/a.txt");
        let primaries: PrimaryHardlinks = Arc::new(Mutex::new(HashMap::new()));
        let pending: Arc<Mutex<Vec<HardlinkByPath>>> = Arc::new(Mutex::new(Vec::new()));

        let (is_secondary, fp) = detect_hardlink(&primary, &path, &primaries, &pending);
        assert!(!is_secondary);
        assert!(fp.is_some());
        assert!(primaries.lock().contains_key(&(10, 20)));
    }

    #[test]
    fn test_detect_hardlink_secondary() {
        let blobs = vec![ID::from_content(b"shared")];
        let primary = make_file_node("a.txt", blobs.clone(), 10, 20, 2);
        let secondary = make_file_node("b.txt", blobs, 10, 20, 2);
        let primary_path = PathBuf::from("/restore/a.txt");
        let secondary_path = PathBuf::from("/restore/b.txt");
        let primaries: PrimaryHardlinks = Arc::new(Mutex::new(HashMap::new()));
        let pending: Arc<Mutex<Vec<HardlinkByPath>>> = Arc::new(Mutex::new(Vec::new()));

        let (is_sec1, _) = detect_hardlink(&primary, &primary_path, &primaries, &pending);
        assert!(!is_sec1);
        let (is_sec2, _) = detect_hardlink(&secondary, &secondary_path, &primaries, &pending);
        assert!(is_sec2);
        let p = pending.lock();
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].0, secondary_path);
        assert_eq!(p[0].1, primary_path);
    }

    #[test]
    fn test_detect_hardlink_content_mismatch() {
        let primary = make_file_node("a.txt", vec![ID::from_content(b"content1")], 10, 20, 2);
        let different = make_file_node("b.txt", vec![ID::from_content(b"content2")], 10, 20, 2);
        let primary_path = PathBuf::from("/restore/a.txt");
        let different_path = PathBuf::from("/restore/b.txt");
        let primaries: PrimaryHardlinks = Arc::new(Mutex::new(HashMap::new()));
        let pending: Arc<Mutex<Vec<HardlinkByPath>>> = Arc::new(Mutex::new(Vec::new()));

        detect_hardlink(&primary, &primary_path, &primaries, &pending);
        let (is_secondary, _) = detect_hardlink(&different, &different_path, &primaries, &pending);
        assert!(
            !is_secondary,
            "different content should not be detected as hardlink secondary"
        );
    }

    #[test]
    fn test_detect_hardlink_no_nlink() {
        let node = make_file_node("a.txt", vec![ID::from_content(b"data")], 10, 20, 1);
        let path = PathBuf::from("/restore/a.txt");
        let primaries: PrimaryHardlinks = Arc::new(Mutex::new(HashMap::new()));
        let pending: Arc<Mutex<Vec<HardlinkByPath>>> = Arc::new(Mutex::new(Vec::new()));

        let (is_secondary, _) = detect_hardlink(&node, &path, &primaries, &pending);
        assert!(!is_secondary);
        assert!(
            primaries.lock().is_empty(),
            "nlink=1 should not register primary"
        );
    }
}
