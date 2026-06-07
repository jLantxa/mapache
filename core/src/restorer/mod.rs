//! The restorer module implements the logic for restoring files, directories, and
//! symlinks from a repository snapshot to a local filesystem.
//! It uses a pack-centric approach with background prefetching and concurrent
//! restoration for high performance.

pub(crate) mod node_restorer;

#[cfg(not(unix))]
use std::io::{Seek, Write};
#[cfg(unix)]
use std::os::unix::{fs::FileExt, io::AsRawFd};
#[cfg(windows)]
use std::os::windows::fs::FileExt;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use anyhow::{Context, Result, anyhow, bail};
use clap::ValueEnum;
use futures::StreamExt;
use parking_lot::Mutex;
use tokio::task::spawn_blocking;

use crate::{
    backend::Handle,
    fs::{
        self as repo_fs,
        node::Node,
        tree::{SerializedNodeStream, SerializedTreeStream},
    },
    mapache::{
        BlobType, ContentIdType, ID,
        defaults::{self},
        hash,
    },
    repository::{
        index::{BlobLocator, MasterIndex},
        loader,
        repo::Repository,
        snapshot::Snapshot,
        storage::SecureStorage,
    },
    ui::RestoreProgressReporter,
    utils::{self, size},
};

/// Strategy for handling existing files during restoration.
#[derive(Debug, Clone, PartialEq, ValueEnum)]
pub enum Strategy {
    /// Fail if the file already exists.
    Fail,
    /// Overwrite existing files.
    Overwrite,
    /// Skip files that already exist.
    Skip,
    /// Only overwrite if the snapshot version is newer.
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
                "Invalid strategy: {s}. Must be one of: fail, overwrite, skip, newer"
            )),
        }
    }
}

/// Options for the restoration process.
pub struct RestoreOptions {
    /// How to handle existing files.
    pub strategy: Strategy,
    /// Strip this prefix from the source paths.
    pub strip_prefix: Option<PathBuf>,
    /// If true, do not perform any actual restoration.
    pub dry_run: bool,
    /// If true, stop the entire restoration process on the first error.
    pub quit_on_error: bool,
    /// If true, preallocate disk space for files before writing.
    pub preallocate: bool,
    /// If true, verify the content of existing files by hashing them.
    pub verify: bool,
}

#[allow(clippy::too_many_arguments)]
/// Performs the restoration of a snapshot to a target path.
pub async fn restore(
    repo: Arc<Repository>,
    snapshot: &Snapshot,
    target_path: &Path,
    include: Option<Vec<PathBuf>>,
    exclude: Option<Vec<PathBuf>>,
    opts: RestoreOptions,
    progress_reporter: Arc<dyn RestoreProgressReporter>,
    shutdown_signal: Arc<AtomicBool>,
) -> Result<()> {
    let restorer = Restorer::new(
        repo,
        target_path.to_path_buf(),
        opts,
        progress_reporter,
        shutdown_signal,
    );

    restorer.restore(snapshot.tree, include, exclude).await
}

/// The Restorer is responsible for coordinating the restoration process.
///
/// It implements a high-performance, pack-centric approach. Instead of restoring
/// file by file (which causes random I/O and many small backend requests), it:
/// 1. Scans the snapshot tree to build a restoration plan.
/// 2. Groups all required blobs by the pack file they reside in.
/// 3. Downloads packs (or relevant ranges) sequentially.
/// 4. Distributes downloaded blobs to their target files in parallel.
/// 5. Restores metadata in a separate bottom-up pass.
pub(crate) struct Restorer {
    repo: Arc<Repository>,
    progress_reporter: Arc<dyn RestoreProgressReporter>,
    shutdown_signal: Arc<AtomicBool>,
    target_path: PathBuf,
    opts: RestoreOptions,
    /// Pool of reusable buffers to reduce allocations during restoration.
    buffers: Arc<Mutex<VecDeque<Vec<u8>>>>,
}

type PackMap = HashMap<ID, Vec<(ID, BlobRestoreRequest)>>;

/// Information about a single file to be restored.
struct FileRestorePlan {
    /// Full path to the file in the target filesystem.
    path: PathBuf,
    /// Number of blobs this file expects (0 for hardlink secondaries, symlinks, empty files).
    num_blobs: u32,
}

/// The complete plan for the restoration process.
struct RestorePlan {
    /// List of files to be restored.
    files: Arc<Vec<FileRestorePlan>>,
    /// Mapping of pack IDs to the blobs and their target locations in files.
    packs: Arc<PackMap>,
    /// List of directories to be restored (path and metadata).
    directories: Vec<(PathBuf, crate::fs::node::Metadata)>,
    /// Total number of items (files, dirs, symlinks) in the plan.
    total_items: u64,
    /// Total number of bytes to be restored.
    total_bytes: u64,
    /// Bytes skipped because the local file was already up to date.
    skipped_bytes: u64,
    /// Hardlinks to create after data restoration: (secondary_file_idx, primary_file_idx).
    hardlinks: Vec<(usize, usize)>,
}

/// A request to restore a specific blob into a file.
#[derive(Clone)]
struct BlobRestoreRequest {
    /// Index into the RestorePlan's files list.
    file_idx: usize,
    /// Offset within the target file where the blob should be written.
    offset_in_file: u64,
    /// Locator for the blob in the repository's index (without pack_id, which is in the map key).
    blob_offset: u32,
    blob_length: u32,
    raw_length: u32,
}

/// A cache for open file handles during restoration.
/// This helps avoid repeated open/close operations when writing blobs to files.
struct FileHandleCache {
    /// Map of file indices to open file handles.
    handles: HashMap<usize, File>,
    /// LRU order of file indices.
    order: VecDeque<usize>,
    /// Maximum number of file handles to keep open.
    max_handles: usize,
}

impl FileHandleCache {
    /// Creates a new FileHandleCache with the specified maximum number of open handles.
    fn new(max_handles: usize) -> Self {
        Self {
            handles: HashMap::new(),
            order: VecDeque::new(),
            max_handles,
        }
    }

    /// Moves a file index to the end of the LRU queue.
    fn touch(&mut self, file_idx: usize) {
        if let Some(pos) = self.order.iter().position(|&idx| idx == file_idx) {
            self.order.remove(pos);
            self.order.push_back(file_idx);
        }
    }

    /// Retrieves an open file handle for the specified file index, opening it if necessary.
    /// If the cache is full, the least recently used handle is closed.
    fn get_handle(&mut self, file_idx: usize, path: &Path) -> Result<&File> {
        if self.handles.contains_key(&file_idx) {
            self.touch(file_idx);
            return self
                .handles
                .get(&file_idx)
                .ok_or_else(|| anyhow!("File handle disappeared from cache"));
        }

        if self.handles.len() >= self.max_handles
            && let Some(oldest_key) = self.order.pop_front()
        {
            self.handles.remove(&oldest_key);
        }

        // NOTE: build_plan already handles create(true) and truncate(true).
        // The cache only opens files for writing to existing, pre-allocated files.
        let file = OpenOptions::new()
            .write(true)
            .open(path)
            .with_context(|| format!("Failed to open file for writing: {}", path.display()))?;

        self.handles.insert(file_idx, file);
        self.order.push_back(file_idx);
        self.handles
            .get(&file_idx)
            .ok_or_else(|| anyhow!("Failed to retrieve file handle after insertion"))
    }
}

/// A sharded wrapper around FileHandleCache to reduce lock contention.
struct ShardedFileHandleCache {
    shards: Vec<Mutex<FileHandleCache>>,
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

        let mut shards = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            shards.push(Mutex::new(FileHandleCache::new(handles_per_shard)));
        }

        Self { shards, num_shards }
    }

    fn get_shard(&self, file_idx: usize) -> &Mutex<FileHandleCache> {
        &self.shards[file_idx % self.num_shards]
    }
}

impl Restorer {
    /// Creates a new Restorer instance.
    fn new(
        repo: Arc<Repository>,
        target_path: PathBuf,
        opts: RestoreOptions,
        progress_reporter: Arc<dyn RestoreProgressReporter>,
        shutdown_signal: Arc<AtomicBool>,
    ) -> Self {
        let d = defaults::runtime();
        // Allow up to concurrent_restores buffers in the pool
        let num_buffers = d.restore_max_open_files;
        Self {
            repo,
            target_path,
            opts,
            progress_reporter,
            shutdown_signal,
            buffers: Arc::new(Mutex::new(VecDeque::with_capacity(num_buffers))),
        }
    }

    fn get_buffer(&self, capacity: usize) -> Vec<u8> {
        let mut buffers = self.buffers.lock();
        if let Some(mut buf) = buffers.pop_front() {
            if buf.capacity() < capacity {
                buf.reserve(capacity - buf.capacity());
            }
            buf
        } else {
            Vec::with_capacity(capacity)
        }
    }

    pub(crate) fn return_buffer(&self, mut buf: Vec<u8>) {
        buf.clear();
        self.buffers.lock().push_back(buf);
    }

    /// Preallocates disk space for a file.
    /// This is used as an optimization to reduce disk fragmentation and ensure sufficient space,
    /// but it is slower than sparse file creation.
    fn preallocate_file(&self, file: &mut File, length: u64) -> Result<()> {
        if length == 0 {
            return Ok(());
        }

        #[cfg(all(unix, not(target_os = "macos")))]
        {
            let fd = file.as_raw_fd();
            let result = unsafe { libc::posix_fallocate(fd, 0, length as libc::off_t) };
            if result != 0 {
                return Err(anyhow!(std::io::Error::from_raw_os_error(result)));
            }
            Ok(())
        }

        #[cfg(target_os = "macos")]
        {
            let fd = file.as_raw_fd();

            // Constants for fcntl F_PREALLOCATE
            const F_VOLPOSMODE: i32 = 1; // Allocate from the current position/offset
            const F_STARTPOSMODE: i32 = 3; // Allocate from the start of the file

            let mut store = libc::fstore_t {
                fst_flags: libc::F_ALLOCATEALL,
                fst_posmode: F_VOLPOSMODE,
                fst_offset: 0,
                fst_length: length as libc::off_t,
                fst_bytesalloc: 0,
            };

            let mut res = unsafe { libc::fcntl(fd, libc::F_PREALLOCATE, &store) };

            // If F_VOLPOSMODE fails, try the "start of file" mode
            if res == -1 {
                store.fst_posmode = F_STARTPOSMODE;
                res = unsafe { libc::fcntl(fd, libc::F_PREALLOCATE, &store) };
            }

            if res == -1 {
                // If preallocation is purely an optimization, we could log and continue.
                // But since your test expects eager allocation, we capture the error.
                return Err(anyhow!(std::io::Error::last_os_error()));
            }

            // Crucial: fcntl doesn't update the i-node size, only the blocks on disk.
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

    /// Performs the restoration of a snapshot tree to the target path.
    async fn restore(
        &self,
        tree_id: ID,
        include: Option<Vec<PathBuf>>,
        exclude: Option<Vec<PathBuf>>,
    ) -> Result<()> {
        tracing::info!(target: "restorer", "Starting restoration of tree {tree_id}");
        let node_stream = SerializedNodeStream::new(
            self.repo.clone(),
            Some(tree_id),
            PathBuf::new(),
            include.clone(),
            exclude.clone(),
        )
        .await?;

        if !self.opts.dry_run {
            fs::create_dir_all(&self.target_path)?;
        }

        let index = self.repo.index();
        tracing::info!(target: "restorer", "Building restoration plan");
        let plan = self.build_plan(node_stream, index).await?;
        tracing::info!(
            target: "restorer",
            "Plan built: {} items, {}",
            plan.total_items,
            utils::format_size_binary(plan.total_bytes, 1)
        );

        // Initialize progress reporter with stats gathered during planning
        self.progress_reporter
            .resize_workload(plan.total_items, plan.total_bytes);

        // Count bytes of files that were skipped (already up to date) as processed
        // so the progress bar accurately reflects all included files.
        if plan.skipped_bytes > 0 {
            self.progress_reporter.processed_bytes(plan.skipped_bytes);
        }

        let plan_files = plan.files.clone();
        let plan_packs = plan.packs.clone();
        let dry_run = self.opts.dry_run;
        let secure_storage = self.repo.secure_storage();

        tracing::info!(target: "restorer", "Restoring data packs");
        self.restore_packs(plan_files, plan_packs, secure_storage, dry_run)
            .await?;

        // Create hardlinks for secondary hardlink files (they share inode with the primary).
        // Must happen before metadata restoration, as creating a hardlink modifies the
        // parent directory's mtime.
        if !self.opts.dry_run && !plan.hardlinks.is_empty() {
            tracing::info!(
                target: "restorer",
                "Creating {} hardlinks",
                plan.hardlinks.len()
            );
            for (sec_idx, prim_idx) in &plan.hardlinks {
                let primary_path = &plan.files[*prim_idx].path;
                let secondary_path = &plan.files[*sec_idx].path;
                if let Some(parent) = secondary_path.parent()
                    && let Err(e) = fs::create_dir_all(parent)
                {
                    let err_msg = format!(
                        "Failed to create parent directory for hardlink {}: {}",
                        secondary_path.display(),
                        e
                    );
                    if self.opts.quit_on_error {
                        bail!(err_msg);
                    }
                    self.progress_reporter.error(&err_msg);
                }
                if let Err(e) = fs::remove_file(secondary_path)
                    && e.kind() != std::io::ErrorKind::NotFound
                {
                    let err_msg = format!(
                        "Failed to remove existing file for hardlink {}: {}",
                        secondary_path.display(),
                        e
                    );
                    if self.opts.quit_on_error {
                        bail!(err_msg);
                    }
                    self.progress_reporter.error(&err_msg);
                }
                if let Err(e) = fs::hard_link(primary_path, secondary_path) {
                    let err_msg = format!(
                        "Failed to create hardlink {} -> {}: {}",
                        secondary_path.display(),
                        primary_path.display(),
                        e
                    );
                    if self.opts.quit_on_error {
                        bail!(err_msg);
                    }
                    self.progress_reporter.error(&err_msg);
                }
            }
        }

        tracing::info!(target: "restorer", "Restoring metadata");
        self.restore_metadata(tree_id, include, exclude, plan.directories)
            .await?;

        tracing::info!(target: "restorer", "Restoration finished");
        Ok(())
    }

    /// Builds a restoration plan by walking the snapshot tree and determining
    /// which nodes need to be restored.
    async fn build_plan(
        &self,
        node_stream: SerializedNodeStream,
        index: Arc<MasterIndex>,
    ) -> Result<RestorePlan> {
        let files = Arc::new(Mutex::new(Vec::new()));
        let directories = Arc::new(Mutex::new(Vec::new()));
        let packs = Arc::new(dashmap::DashMap::<ID, Vec<(ID, BlobRestoreRequest)>>::new());
        let node_count = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let total_items = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let total_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let skipped_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        // HardlinkIndex: (dev, inode) → file_idx of first occurrence
        let hardlink_index = Arc::new(parking_lot::Mutex::new(HashMap::<(u64, u64), usize>::new()));
        let hardlinks = Arc::new(parking_lot::Mutex::new(Vec::<(usize, usize)>::new()));

        self.progress_reporter
            .set_message("Planning...".to_string());

        let d = defaults::runtime();
        let num_workers = d.restore_blob_concurrency;

        node_stream
            .for_each_concurrent(num_workers, |node_res| {
                let index = index.clone();
                let files = files.clone();
                let directories = directories.clone();
                let packs = packs.clone();
                let node_count = node_count.clone();
                let total_items = total_items.clone();
                let total_bytes = total_bytes.clone();
                let skipped_bytes = skipped_bytes.clone();
                let progress_reporter = self.progress_reporter.clone();
                let shutdown_signal = self.shutdown_signal.clone();
                let hardlink_index = hardlink_index.clone();
                let hardlinks = hardlinks.clone();

                async move {
                    if shutdown_signal.load(Ordering::Acquire) {
                        return;
                    }

                    let visited = node_count.fetch_add(1, Ordering::Relaxed) + 1;
                    progress_reporter.set_visited_nodes(visited);

                    let (mut path, stream_node_res) = match node_res {
                        Ok(res) => res,
                        Err(e) => {
                            progress_reporter.error(&format!("Error during planning: {e}"));
                            return;
                        }
                    };

                    let stream_node = match stream_node_res {
                        Ok(node) => node,
                        Err(e) => {
                            progress_reporter.error(&format!(
                                "Error reading node {}: {}",
                                path.display(),
                                e
                            ));
                            return;
                        }
                    };
                    let node = stream_node.node;

                    if let Some(prefix) = &self.opts.strip_prefix {
                        path = match path.strip_prefix(prefix) {
                            Ok(stripped_path) => {
                                if stripped_path.as_os_str().is_empty() {
                                    return;
                                }
                                stripped_path.to_path_buf()
                            }
                            Err(_) => return,
                        };
                    }

                    total_items.fetch_add(1, Ordering::Relaxed);
                    if node.is_file() {
                        total_bytes.fetch_add(node.metadata.size, Ordering::Relaxed);
                    }

                    let restore_path = match utils::secure_join(&self.target_path, &path) {
                        Ok(p) => p,
                        Err(e) => {
                            progress_reporter
                                .error(&format!("Secure join failed for {path:?}: {e}"));
                            return;
                        }
                    };

                    match self
                        .should_restore_node(&node, &restore_path, index.clone())
                        .await
                    {
                        Ok(true) => {}
                        Ok(false) => {
                            if node.is_file() {
                                skipped_bytes.fetch_add(node.metadata.size, Ordering::Relaxed);
                            }
                            return;
                        }
                        Err(e) => {
                            progress_reporter.error(&format!(
                                "Error checking {}: {}",
                                path.display(),
                                e
                            ));
                            return;
                        }
                    }

                    if node.is_dir() {
                        if !self.opts.dry_run
                            && let Err(e) = fs::create_dir_all(&restore_path)
                        {
                            progress_reporter.error(&format!(
                                "Failed to create directory {}: {}",
                                restore_path.display(),
                                e
                            ));
                            return;
                        }
                        directories.lock().push((restore_path, node.metadata));
                        return;
                    }

                    if node.is_file() {
                        let mut file_blobs = Vec::new();
                        if let Some(blobs) = &node.blobs {
                            let mut offset_in_file = 0;
                            for blob_id in blobs {
                                let locator = match index.get_data(blob_id) {
                                    Some(loc) => loc,
                                    None => {
                                        let err_msg = format!("Blob {blob_id} not found in index");
                                        progress_reporter.error(&err_msg);
                                        return;
                                    }
                                };
                                file_blobs.push((*blob_id, locator, offset_in_file));
                                offset_in_file += locator.raw_length as u64;
                            }
                        }

                        let file_idx = {
                            let mut files_lock = files.lock();
                            let idx = files_lock.len();
                            files_lock.push(FileRestorePlan {
                                path: restore_path.clone(),
                                num_blobs: 0,
                            });
                            idx
                        };

                        // Check if this is a secondary hardlink (same dev,inode seen before)
                        let is_hardlink_secondary = {
                            if let (Some(dev), Some(inode)) =
                                (node.metadata.dev, node.metadata.inode)
                            {
                                let nlink = node.metadata.nlink;
                                // Fast-path: nlink <= 1 means no hardlinks
                                if nlink.unwrap_or(0) > 1 {
                                    let mut idx = hardlink_index.lock();
                                    if let Some(&primary_idx) = idx.get(&(dev, inode)) {
                                        hardlinks.lock().push((file_idx, primary_idx));
                                        true
                                    } else {
                                        idx.insert((dev, inode), file_idx);
                                        false
                                    }
                                } else {
                                    false
                                }
                            } else {
                                false
                            }
                        };

                        if is_hardlink_secondary {
                            // Secondary hardlink: no content to restore, just count size
                            total_bytes.fetch_add(node.metadata.size, Ordering::Relaxed);
                            if !self.opts.dry_run
                                && let Some(parent) = restore_path.parent()
                                && let Err(e) = fs::create_dir_all(parent) {
                                    progress_reporter.error(&format!(
                                        "Failed to create parent directory for secondary hardlink {}: {}",
                                        restore_path.display(),
                                        e
                                    ));
                            }
                        } else {
                            let num_blobs = file_blobs.len().min(u32::MAX as usize) as u32;
                            for (blob_id, locator, offset_in_file) in file_blobs {
                                packs.entry(locator.pack_id).or_default().push((
                                    blob_id,
                                    BlobRestoreRequest {
                                        file_idx,
                                        offset_in_file,
                                        blob_offset: locator.offset,
                                        blob_length: locator.length,
                                        raw_length: locator.raw_length,
                                    },
                                ));
                            }

                            if self.opts.dry_run {
                                return;
                            }

                            if let Some(parent) = restore_path.parent()
                                && let Err(e) = fs::create_dir_all(parent)
                            {
                                progress_reporter.error(&format!(
                                    "Failed to create parent directory for {}: {}",
                                    restore_path.display(), e
                                ));
                            }

                            if let Ok(m) = fs::symlink_metadata(&restore_path) {
                                if m.file_type().is_symlink() {
                                    if let Err(e) = fs::remove_file(&restore_path) {
                                        progress_reporter.error(&format!(
                                            "Failed to remove symlink {}: {}",
                                            restore_path.display(), e
                                        ));
                                    }
                                } else if let Err(e) = self.clear_readonly_attribute(&restore_path) {
                                    progress_reporter.error(&format!(
                                        "Failed to clear readonly attribute on {}: {}",
                                        restore_path.display(), e
                                    ));
                                }
                            }

                            let mut file = match OpenOptions::new()
                                .create(true).write(true).truncate(true)
                                .open(&restore_path)
                            {
                                Ok(f) => f,
                                Err(e) => {
                                    progress_reporter.error(&format!(
                                        "Failed to create file {}: {}", restore_path.display(), e
                                    ));
                                    return;
                                }
                            };

                            if self.opts.preallocate {
                                if let Err(e) = self.preallocate_file(&mut file, node.metadata.size) {
                                    tracing::warn!(target: "restorer", "Failed to preallocate file {}: {e}", restore_path.display());
                                }
                            } else if let Err(e) = file.set_len(node.metadata.size) {
                                tracing::warn!(target: "restorer", "Failed to set file length for {}: {e}", restore_path.display());
                            }

                            files.lock()[file_idx].num_blobs = num_blobs;
                        }
                    } else if node.is_symlink() {
                        files.lock().push(FileRestorePlan {
                            path: restore_path.clone(),
                            num_blobs: 0,
                        });

                        if !self.opts.dry_run
                            && let Err(e) = node_restorer::restore_node_to_path(
                                self,
                                progress_reporter.clone(),
                                &node,
                                &restore_path,
                                false,
                            )
                            .await
                        {
                            progress_reporter.error(&e.to_string());
                        }
                    }
                }
            })
            .await;

        let final_visited = node_count.load(Ordering::Relaxed);
        self.progress_reporter.set_visited_nodes(final_visited);

        let files = Arc::into_inner(files)
            .context("Internal error: multiple Arc references to files remained after planning")?
            .into_inner();
        let directories = Arc::into_inner(directories)
            .context(
                "Internal error: multiple Arc references to directories remained after planning",
            )?
            .into_inner();
        let packs_map = Arc::into_inner(packs)
            .context("Internal error: multiple Arc references to packs remained after planning")?
            .into_iter()
            .collect();
        let hardlinks = Arc::into_inner(hardlinks)
            .context(
                "Internal error: multiple Arc references to hardlinks remained after planning",
            )?
            .into_inner();

        Ok(RestorePlan {
            files: Arc::new(files),
            packs: Arc::new(packs_map),
            directories,
            total_items: total_items.load(Ordering::Relaxed),
            total_bytes: total_bytes.load(Ordering::Relaxed),
            skipped_bytes: skipped_bytes.load(Ordering::Relaxed),
            hardlinks,
        })
    }

    /// Checks if a node should be restored based on the current restoration strategy.
    async fn should_restore_node(
        &self,
        node: &Node,
        restore_path: &Path,
        index: Arc<MasterIndex>,
    ) -> Result<bool> {
        if !repo_fs::path_exists(restore_path).await {
            return Ok(true);
        }

        // If a file already exists at the restore path, we can skip it if the size and modified time match.
        if node.is_file()
            && let Ok(local_metadata) = fs::symlink_metadata(restore_path)
        {
            let local_size = local_metadata.len();
            let local_mtime = local_metadata.modified().ok();

            if local_size == node.metadata.size {
                let mtime_matches = node
                    .metadata
                    .times_match(local_mtime, node.metadata.modified_time);

                // If mtime and size match, we skip by default (trust metadata).
                // If --verify is set, we hash anyway to be sure.
                if mtime_matches && !self.opts.verify {
                    return Ok(false);
                }

                // If mtime differs but size is the same, OR if --verify is set,
                // we check the actual content hashes.
                let content_matches = match self
                    .verify_file_content(node, restore_path, index)
                    .await
                {
                    Ok(matches) => matches,
                    Err(e) => {
                        tracing::warn!(target: "restorer", "Could not verify file {:?}: {e}", restore_path);
                        false
                    }
                };
                if content_matches {
                    // Content matches! Skip restoration of this file.
                    return Ok(false);
                }
            }
        }

        match self.opts.strategy {
            Strategy::Overwrite => Ok(true),
            Strategy::Skip => Ok(false),
            Strategy::Newer => {
                let local_metadata = fs::symlink_metadata(restore_path).with_context(|| {
                    format!(
                        "Failed to get metadata for local file {}",
                        restore_path.display()
                    )
                })?;

                if let Some(repo_mtime) = node.metadata.modified_time {
                    let local_mtime = local_metadata.modified().with_context(|| {
                        format!(
                            "Failed to get modified time for local file {}",
                            restore_path.display()
                        )
                    })?;

                    let local_size = local_metadata.len();
                    if local_mtime < repo_mtime {
                        return Ok(true);
                    }

                    if local_mtime == repo_mtime && local_size != node.metadata.size {
                        return Ok(true);
                    }

                    return Ok(false);
                }

                Ok(true)
            }
            Strategy::Fail => {
                // For directories, we check if they already exist.
                // If they exist, we allow it (as multiple snapshots might restore to the same base).
                if node.is_dir() {
                    return Ok(true);
                }
                bail!("Target {} exists already", restore_path.display());
            }
        }
    }

    /// Verifies that the content of a local file matches the blobs in the repository.
    async fn verify_file_content(
        &self,
        node: &Node,
        local_path: &Path,
        index: Arc<MasterIndex>,
    ) -> Result<bool> {
        let blobs = match &node.blobs {
            Some(b) => b.clone(),
            None => return Ok(true),
        };

        let local_path = local_path.to_path_buf();
        spawn_blocking(move || {
            let file = File::open(&local_path)?;
            let mut offset = 0;

            // Use a fixed-size buffer to hash the file content in chunks.
            // This avoids allocating many large vectors during verification.
            const VERIFY_BUFFER_SIZE: usize = 64 * size::KiB as usize;
            let mut buffer = vec![0u8; VERIFY_BUFFER_SIZE];

            for blob_id in blobs {
                let locator = index
                    .get_data(&blob_id)
                    .ok_or_else(|| anyhow!("Blob {} not found in index", blob_id))?;

                let mut hasher = hash::Hasher::new();
                let mut remaining = locator.raw_length as u64;
                let mut blob_offset = offset;

                while remaining > 0 {
                    let to_read = (remaining as usize).min(VERIFY_BUFFER_SIZE);
                    let chunk = &mut buffer[..to_read];

                    #[cfg(unix)]
                    file.read_exact_at(chunk, blob_offset)?;
                    #[cfg(windows)]
                    {
                        let mut read_total = 0;
                        while read_total < to_read {
                            let n = file.seek_read(
                                &mut chunk[read_total..],
                                blob_offset + read_total as u64,
                            )?;
                            if n == 0 {
                                anyhow::bail!("Unexpected EOF while reading blob for verification");
                            }
                            read_total += n;
                        }
                    }

                    hasher.update(chunk);
                    let read_bytes = to_read as u64;
                    remaining -= read_bytes;
                    blob_offset += read_bytes;
                }

                let actual_id = hasher.finalize();
                if actual_id != blob_id {
                    return Ok(false);
                }
                offset += locator.raw_length as u64;
            }
            Ok(true)
        })
        .await?
    }

    /// Clears the readonly attribute from a file to allow overwriting it.
    /// This handles both Windows readonly attributes and Unix write permissions.
    #[allow(clippy::permissions_set_readonly_false)]
    fn clear_readonly_attribute(&self, path: &Path) -> Result<()> {
        let metadata = fs::metadata(path).with_context(|| {
            format!(
                "Failed to get metadata for permission change: {}",
                path.display()
            )
        })?;

        let mut perms = metadata.permissions();

        #[cfg(windows)]
        {
            if perms.readonly() {
                perms.set_readonly(false);
                fs::set_permissions(path, perms).with_context(|| {
                    format!("Failed to clear readonly attribute on {}", path.display())
                })?;
            }
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = perms.mode();
            // If the file is not writable by the owner, make it writable.
            if mode & 0o200 == 0 {
                perms.set_mode(mode | 0o200);
                fs::set_permissions(path, perms).with_context(|| {
                    format!("Failed to set write permission on {}", path.display())
                })?;
            }
        }

        Ok(())
    }

    async fn restore_packs(
        &self,
        files: Arc<Vec<FileRestorePlan>>,
        packs: Arc<PackMap>,
        secure_storage: Arc<SecureStorage>,
        dry_run: bool,
    ) -> Result<()> {
        if dry_run {
            for blob_requests in packs.values() {
                for (_, request) in blob_requests.iter() {
                    self.progress_reporter
                        .processed_bytes(request.raw_length as u64);
                }
            }
            for file in files.iter() {
                self.progress_reporter.processed_item(&file.path);
            }
            return Ok(());
        }

        // Track remaining blobs per file to detect completion.
        // Use Arc so concurrent pack futures can share it.
        let remaining = Arc::new(
            files
                .iter()
                .map(|f| std::sync::atomic::AtomicU32::new(f.num_blobs))
                .collect::<Vec<_>>(),
        );

        // Count files with 0 blobs (empty files, symlinks, hardlink secondaries) as completed
        for file in files.iter() {
            if file.num_blobs == 0 {
                self.progress_reporter.processed_item(&file.path);
            }
        }

        self.progress_reporter
            .set_message("Restoring...".to_string());

        let d = defaults::runtime();
        let handle_cache = Arc::new(ShardedFileHandleCache::new(d.restore_max_open_files));
        let mut packs_iter = packs.iter();
        let quit_on_error = self.opts.quit_on_error;
        let progress_reporter = self.progress_reporter.clone();
        let shutdown_signal = self.shutdown_signal.clone();

        let mut download_stream = futures::stream::iter(std::iter::from_fn(|| {
            packs_iter
                .next()
                .map(|(pack_id, blob_requests)| (*pack_id, blob_requests.clone()))
        }))
        .map(move |(pack_id, blob_requests)| {
            let repo = self.repo.clone();
            let secure_storage = secure_storage.clone();
            let handle_cache = handle_cache.clone();
            let files = files.clone();
            let remaining = remaining.clone();
            let progress_reporter = progress_reporter.clone();
            let shutdown_signal = shutdown_signal.clone();
            let d = d.clone();

            async move {
                let mut blob_to_targets: HashMap<ID, Vec<BlobRestoreRequest>> = HashMap::new();
                for (blob_id, req) in blob_requests {
                    blob_to_targets.entry(blob_id).or_default().push(req);
                }

                let blobs_vec: Vec<(ID, BlobLocator, Vec<BlobRestoreRequest>)> = blob_to_targets
                    .into_iter()
                    .map(|(id, targets)| {
                        let t0 = &targets[0];
                        let locator = BlobLocator {
                            pack_id,
                            offset: t0.blob_offset,
                            length: t0.blob_length,
                            raw_length: t0.raw_length,
                            blob_type: BlobType::Data, // Only data blobs are in packs map
                        };
                        (id, locator, targets)
                    })
                    .collect();

                let segments = loader::segment_blobs(pack_id, blobs_vec);

                tracing::debug!(target: "restorer", "Processing {} segments from pack {} ({} bytes)", segments.len(), pack_id.to_short_hex(8),
                    segments.iter().map(|s| s.source_len() as u64).sum::<u64>());

                // Process each segment one at a time: download, decode, and flush
                // before moving to the next, bounding encoded-data memory to a
                // single segment's size rather than the whole pack.
                let total_segments = segments.len();
                for (segment_idx, segment) in segments.into_iter().enumerate() {
                    if shutdown_signal.load(Ordering::Acquire) {
                        bail!("Interrupted");
                    }

                    let path = repo.get_path(ContentIdType::Pack, &segment.pack_id);
                    let is_tree = segment
                        .blobs
                        .iter()
                        .all(|(_, loc, _)| loc.blob_type == BlobType::Tree);

                    let segment_data = repo
                        .backend()
                        .read(
                            &Handle::new_with_hint(&path, ContentIdType::Pack, is_tree),
                            segment.min_offset as isize,
                            segment.source_len(),
                        )
                        .await
                        .with_context(|| format!("Failed to read pack {}", segment.pack_id))?;

                    tracing::debug!(target: "restorer", "Segment {}/{} from pack {} downloaded ({} bytes)",
                        segment_idx + 1, total_segments, pack_id.to_short_hex(8), segment_data.len());

                    let data_arc = Arc::new(segment_data);
                    let mut file_batches: HashMap<usize, Vec<(Vec<u8>, u64)>> = HashMap::new();
                    let mut pending_decoded: u64 = 0;

                    for (blob_id, locator, targets) in segment.blobs {
                        let start = (locator.offset as u64 - segment.min_offset) as usize;
                        let end = start + locator.length as usize;
                        let encoded_blob = &data_arc[start..end];

                        let decoded_data = secure_storage
                            .decode_owned(encoded_blob.to_vec())
                            .with_context(|| format!("Failed to decode blob {blob_id}"))?;

                        let raw_len = decoded_data.len() as u64;

                        if targets.len() == 1 {
                            let target = &targets[0];
                            file_batches
                                .entry(target.file_idx)
                                .or_default()
                                .push((decoded_data, target.offset_in_file));
                        } else {
                            for target in &targets {
                                file_batches
                                    .entry(target.file_idx)
                                    .or_default()
                                    .push((decoded_data.clone(), target.offset_in_file));
                        }
                        }

                        pending_decoded += raw_len;

                        // Flush early if we've accumulated too much decoded data.
                        if pending_decoded >= d.restore_decoded_budget {
                            Self::flush_file_batches(
                                &mut file_batches,
                                &handle_cache,
                                &files,
                                remaining.as_ref(),
                                &progress_reporter,
                                quit_on_error,
                                d.restore_blob_concurrency,
                            )
                            .await?;
                            pending_decoded = 0;
                        }
                    }

                    Self::flush_file_batches(
                        &mut file_batches,
                        &handle_cache,
                        &files,
                        remaining.as_ref(),
                        &progress_reporter,
                        quit_on_error,
                        d.restore_blob_concurrency,
                    )
                    .await?;
                }

                Ok::<(), anyhow::Error>(())
            }
        })
        .buffer_unordered(d.restore_pack_prefetch);

        while let Some(res) = download_stream.next().await {
            if self.shutdown_signal.load(Ordering::Acquire) {
                bail!("Interrupted");
            }
            res?;
        }

        Ok(())
    }

    /// Flush accumulated file batches: write each file's blobs in a single
    /// spawn_blocking, processing files concurrently.  This bounds peak memory
    /// to the decoded data accumulated before the flush, preserving per-file
    /// parallelism within the batch.
    async fn flush_file_batches(
        file_batches: &mut HashMap<usize, Vec<(Vec<u8>, u64)>>,
        handle_cache: &Arc<ShardedFileHandleCache>,
        files: &Arc<Vec<FileRestorePlan>>,
        remaining: &[std::sync::atomic::AtomicU32],
        progress_reporter: &Arc<dyn RestoreProgressReporter>,
        quit_on_error: bool,
        concurrency: usize,
    ) -> Result<()> {
        let batches = std::mem::take(file_batches);

        let mut batch_stream = futures::stream::iter(batches)
            .map(|(file_idx, writes)| {
                let num_blobs = writes.len().min(u32::MAX as usize) as u32;
                let total_bytes: u64 = writes.iter().map(|(d, _)| d.len() as u64).sum();
                let file_path = files[file_idx].path.clone();
                let handle_cache = handle_cache.clone();
                let files = files.clone();
                let progress_reporter = progress_reporter.clone();

                async move {
                    let write_result = spawn_blocking(move || -> Result<u64, anyhow::Error> {
                        let mut cache_guard = handle_cache.get_shard(file_idx).lock();
                        let file = cache_guard.get_handle(file_idx, &file_path)?;
                        let mut written = 0u64;
                        for (data, offset) in writes {
                            let mut data_remaining = data.as_slice();
                            let mut write_offset = offset;
                            while !data_remaining.is_empty() {
                                #[cfg(unix)]
                                let n = file
                                    .write_at(data_remaining, write_offset)
                                    .map_err(|e| anyhow!(e))?;
                                #[cfg(windows)]
                                let n = file
                                    .seek_write(data_remaining, write_offset)
                                    .map_err(|e| anyhow!(e))?;
                                if n == 0 {
                                    anyhow::bail!("Failed to write data: wrote 0 bytes");
                                }
                                data_remaining = &data_remaining[n..];
                                write_offset += n as u64;
                            }
                            written += data.len() as u64;
                        }
                        Ok(written)
                    })
                    .await
                    .map_err(|e| anyhow!(e))?;

                    match write_result {
                        Ok(_bytes) => {
                            progress_reporter.processed_bytes(total_bytes);
                            if remaining[file_idx].fetch_sub(num_blobs, Ordering::Relaxed)
                                == num_blobs
                            {
                                progress_reporter.processed_item(&files[file_idx].path);
                            }
                        }
                        Err(e) => {
                            let err_msg = format!("Failed to write to file index {file_idx}: {e}");
                            if quit_on_error {
                                bail!(err_msg);
                            }
                            progress_reporter.error(&err_msg);
                        }
                    }

                    Ok::<(), anyhow::Error>(())
                }
            })
            .buffer_unordered(concurrency);

        while let Some(res) = batch_stream.next().await {
            res?;
        }

        Ok(())
    }

    async fn restore_metadata(
        &self,
        tree_id: ID,
        include: Option<Vec<PathBuf>>,
        exclude: Option<Vec<PathBuf>>,
        directories: Vec<(PathBuf, crate::fs::node::Metadata)>,
    ) -> Result<()> {
        if self.opts.dry_run {
            return Ok(());
        }

        self.progress_reporter
            .set_message("Finishing metadata...".to_string());

        let node_stream = SerializedNodeStream::new(
            self.repo.clone(),
            Some(tree_id),
            PathBuf::new(),
            include,
            exclude,
        )
        .await?;

        // Restore file and symlink metadata in parallel using a second pass.
        // This avoids keeping all metadata in RAM.
        let d = defaults::runtime();
        node_stream
            .for_each_concurrent(d.restore_blob_concurrency, |node_res| {
                let progress_reporter = self.progress_reporter.clone();
                let target_path = self.target_path.clone();
                let opts_strip_prefix = self.opts.strip_prefix.clone();

                async move {
                    let (mut path, stream_node_res) = match node_res {
                        Ok(res) => res,
                        Err(e) => {
                            progress_reporter
                                .warning(&format!("Failed to read node from stream: {e}"));
                            return;
                        }
                    };

                    let stream_node = match stream_node_res {
                        Ok(node) => node,
                        Err(e) => {
                            progress_reporter.warning(&format!("Failed to deserialize node: {e}"));
                            return;
                        }
                    };
                    let node = stream_node.node;

                    if let Some(prefix) = &opts_strip_prefix {
                        path = match path.strip_prefix(prefix) {
                            Ok(stripped_path) => {
                                if stripped_path.as_os_str().is_empty() {
                                    return;
                                }
                                stripped_path.to_path_buf()
                            }
                            Err(_) => return,
                        };
                    }

                    let restore_path = match utils::secure_join(&target_path, &path) {
                        Ok(p) => p,
                        Err(_) => return,
                    };

                    // We only restore metadata for nodes that exist in the target.
                    if !repo_fs::path_exists(&restore_path).await {
                        return;
                    }

                    if !node.is_dir() {
                        node_restorer::try_restore_node_metadata(
                            &node.metadata,
                            node.is_symlink(),
                            &restore_path,
                            progress_reporter.as_ref(),
                        );
                    }
                    // Files and symlinks were counted in restore_packs; only count dirs here
                    if node.is_dir() {
                        progress_reporter.processed_item(&path);
                    }
                }
            })
            .await;

        // Restore directory metadata bottom-up.
        let mut dirs = directories;
        dirs.sort_unstable_by_key(|(p, _)| std::cmp::Reverse(p.as_os_str().len()));
        for (p, meta) in dirs {
            if self.shutdown_signal.load(Ordering::Acquire) {
                bail!("Interrupted");
            }
            node_restorer::try_restore_node_metadata(
                &meta,
                false,
                &p,
                self.progress_reporter.as_ref(),
            );
        }

        Ok(())
    }
}

/// Delete all local nodes not present in a snapshot tree.
/// This function synchronizes the target directory with the snapshot by
/// removing files and directories that are not part of the snapshot.
#[allow(clippy::too_many_arguments)]
pub async fn delete_nodes(
    repo: Arc<Repository>,
    target_path: PathBuf,
    root_tree_id: &ID,
    include: Option<Vec<PathBuf>>,
    exclude: Option<Vec<PathBuf>>,
    dry_run: bool,
    no_preserve_root: bool,
    shutdown_signal: Arc<AtomicBool>,
    reporter: Arc<dyn RestoreProgressReporter>,
) -> Result<()> {
    let mut tree_stream =
        SerializedTreeStream::new(repo, root_tree_id, PathBuf::new(), include.clone(), exclude)
            .await
            .with_context(|| {
                format!("Failed to initialize snapshot tree stream for root ID {root_tree_id:?}")
            })?;

    // If we preserve nodes at the root level, we skip the first node in the
    // stream, which corresponds to the root.
    if !no_preserve_root {
        let _ = tree_stream.next().await;
    }

    while let Some(item_result) = tree_stream.next().await {
        if shutdown_signal.load(Ordering::Acquire) {
            bail!("Interrupted");
        }

        // Handle potential errors from the stream itself.
        // If an error occurs, log a warning and skip to the next item,
        // rather than bailing out entirely, which seems to be the intended behavior.
        let (path, snapshot_tree) = match item_result {
            Ok(data) => data,
            Err(e) => {
                reporter.warning(&format!("Could not read snapshot subtree entry: {e}"));
                tracing::warn!(target: "restorer", "Could not read snapshot subtree entry: {e}");
                continue; // Skip to the next item in the stream
            }
        };

        // Only delete nodes within the include paths
        // The intermediate tree nodes are emitted so the children can be reached.
        // This doesn't mean they must be considered.
        if !path_is_below_includes(&path, include.as_ref()) {
            continue;
        }

        tracing::debug!(target: "restorer", "Syncing local directory {:?} with snapshot tree", path);

        // Pre-process snapshot tree nodes into a HashSet for fast lookups
        let snapshot_node_names: HashSet<&str> = snapshot_tree
            .nodes
            .iter()
            .map(|node| node.name.as_str())
            .collect();

        // Delegate the processing of the local directory to a helper function
        let local_dir = &target_path.join(path);
        process_local_directory(local_dir, &snapshot_node_names, dry_run, reporter.clone())?
    }

    tracing::info!(target: "restorer", "Sync deletion finished");
    Ok(())
}

/// Helper function to process a single local directory to sync.
/// Compares local entries with snapshot nodes and deletes those that don't match.
fn process_local_directory(
    local_dir_path: &Path,
    snapshot_node_names: &HashSet<&str>,
    dry_run: bool,
    reporter: Arc<dyn RestoreProgressReporter>,
) -> Result<()> {
    let local_readdir = match local_dir_path.read_dir() {
        Ok(readdir) => readdir,
        Err(e) => {
            // If the directory does not exist, there's nothing to delete in it.
            if e.kind() == std::io::ErrorKind::NotFound {
                reporter.verbose_1(format!(
                    "Local directory '{}' not found, skipping.",
                    local_dir_path.display()
                ));
                return Ok(());
            } else {
                // For other errors (e.g., permission denied), propagate the error.
                return Err(e).with_context(|| {
                    format!(
                        "Could not read local directory '{}'",
                        local_dir_path.display()
                    )
                });
            }
        }
    };

    // The rest of the function remains the same, as it only executes if read_dir was successful
    for node_res in local_readdir {
        let dir_entry = match node_res {
            Ok(entry) => entry,
            Err(e) => {
                reporter.warning(&format!(
                    "Failed to read local node in '{}': {e}",
                    local_dir_path.display()
                ));
                continue;
            }
        };

        let local_name = dir_entry.file_name();
        let local_name_str = local_name.to_string_lossy();
        let local_path = dir_entry.path();

        if !snapshot_node_names.contains(local_name_str.as_ref()) {
            perform_deletion(&local_path, dry_run, reporter.clone())?;
        }
    }

    Ok(())
}

/// Helper function to perform the actual file/directory deletion or log dry run.
fn perform_deletion(
    path_to_delete: &Path,
    dry_run: bool,
    reporter: Arc<dyn RestoreProgressReporter>,
) -> Result<()> {
    if dry_run {
        reporter.log(format!(
            "[DRY RUN] Would delete '{}'",
            path_to_delete.display()
        ));
        tracing::debug!(target: "restorer", "Dry run: would delete {:?}", path_to_delete);
    } else if path_to_delete.is_dir() {
        reporter.verbose_1(format!("Deleted {path_to_delete:?}"));
        tracing::debug!(target: "restorer", "Deleting directory {:?}", path_to_delete);
        std::fs::remove_dir_all(path_to_delete).with_context(|| {
            format!(
                "Failed to delete local directory '{}'",
                path_to_delete.display()
            )
        })?;
    } else {
        reporter.verbose_1(format!("Deleted {path_to_delete:?}"));
        tracing::debug!(target: "restorer", "Deleting file {:?}", path_to_delete);
        std::fs::remove_file(path_to_delete)
            .with_context(|| format!("Failed to delete local file {path_to_delete:?}"))?;
    }

    Ok(())
}

/// Returns true if a path is contained by any of the include paths.
fn path_is_below_includes(path: &Path, include: Option<&Vec<PathBuf>>) -> bool {
    let Some(includes) = include else {
        return true;
    };

    for ipath in includes {
        if path.starts_with(ipath) {
            return true;
        }
    }

    false
}
