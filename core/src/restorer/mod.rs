//! The restorer module implements the logic for restoring files, directories, and
//! symlinks from a repository snapshot to a local filesystem.
//! It uses a pack-centric approach with background prefetching and concurrent
//! restoration for high performance.

pub(crate) mod node_restorer;

use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

#[cfg(not(unix))]
use std::io::{Seek, Write};

#[cfg(unix)]
use std::os::unix::{fs::FileExt, io::AsRawFd};
#[cfg(windows)]
use std::os::windows::fs::FileExt;

use anyhow::{Context, Result, anyhow, bail};
use clap::ValueEnum;
use futures::StreamExt;
use parking_lot::Mutex;
use tokio::{sync::Semaphore, task::spawn_blocking};

use crate::{
    fs::{self as repo_fs, node::Node, tree::SerializedNodeStream, tree::SerializedTreeStream},
    mapache::{
        BlobType, ID,
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

        let plan_files = plan.files.clone();
        let plan_packs = plan.packs.clone();
        let dry_run = self.opts.dry_run;
        let secure_storage = self.repo.secure_storage();

        tracing::info!(target: "restorer", "Restoring data packs");
        self.restore_packs(plan_files, plan_packs, secure_storage, dry_run)
            .await?;

        // Create hardlinks for secondary hardlink files (they share inode with the primary)
        if !self.opts.dry_run && !plan.hardlinks.is_empty() {
            tracing::info!(
                target: "restorer",
                "Creating {} hardlinks",
                plan.hardlinks.len()
            );
            for (sec_idx, prim_idx) in &plan.hardlinks {
                let primary_path = &plan.files[*prim_idx].path;
                let secondary_path = &plan.files[*sec_idx].path;
                if let Some(parent) = secondary_path.parent() {
                    let _ = fs::create_dir_all(parent);
                }
                // Remove destination if it already exists (e.g. from a previous restore)
                let _ = fs::remove_file(secondary_path);
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
                let progress_reporter = self.progress_reporter.clone();
                let shutdown_signal = self.shutdown_signal.clone();
                let hardlink_index = hardlink_index.clone();
                let hardlinks = hardlinks.clone();

                async move {
                    if shutdown_signal.load(Ordering::Relaxed) {
                        return;
                    }

                    let count = node_count.fetch_add(1, Ordering::Relaxed);
                    let visited = count + 1;
                    if count.is_multiple_of(100) {
                        progress_reporter.set_visited_nodes(visited);
                    }

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
                        Ok(false) => return,
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
                            {
                                let _ = fs::create_dir_all(parent);
                            }
                        } else {
                            // Primary hardlink or non-hardlink: register blobs and create file
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

                            if !self.opts.dry_run {
                                if let Some(parent) = restore_path.parent() {
                                    let _ = fs::create_dir_all(parent);
                                }

                                // If the file exists, it might be a symlink or read-only.
                                // We must NOT follow symlinks when restoring to prevent overwriting
                                // files outside the target directory.
                                if let Ok(m) = fs::symlink_metadata(&restore_path) {
                                    if m.file_type().is_symlink() {
                                        let _ = fs::remove_file(&restore_path);
                                    } else {
                                        let _ = self.clear_readonly_attribute(&restore_path);
                                    }
                                }

                                match OpenOptions::new()
                                    .create(true)
                                    .write(true)
                                    .truncate(true)
                                    .open(&restore_path)
                                {
                                    Ok(mut file) => {
                                        if self.opts.preallocate {
                                            let _ = self
                                                .preallocate_file(&mut file, node.metadata.size);
                                        } else {
                                            let _ = file.set_len(node.metadata.size);
                                        }
                                    }
                                    Err(e) => {
                                        progress_reporter.error(&format!(
                                            "Failed to create file {}: {}",
                                            restore_path.display(),
                                            e
                                        ));
                                    }
                                }
                            }
                        }
                    } else if node.is_symlink() {
                        files.lock().push(FileRestorePlan {
                            path: restore_path.clone(),
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
                if self
                    .verify_file_content(node, restore_path, index)
                    .await
                    .unwrap_or(false)
                {
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
                    file.seek_read(chunk, blob_offset)?;

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
            return Ok(());
        }

        self.progress_reporter
            .set_message("Restoring...".to_string());

        let d = defaults::runtime();
        let handle_cache = Arc::new(ShardedFileHandleCache::new(d.restore_max_open_files));
        let mut packs_iter = packs.iter();

        let max_memory_units =
            d.restore_pack_prefetch_memory_bytes / d.restore_pack_prefetch_memory_unit;
        let memory_budget = Arc::new(Semaphore::new(max_memory_units));

        let mut download_stream = futures::stream::iter(std::iter::from_fn(|| {
            packs_iter
                .next()
                .map(|(pack_id, blob_requests)| (*pack_id, blob_requests.clone()))
        }))
        .map(move |(pack_id, blob_requests)| {
            let repo = self.repo.clone();
            let memory_budget = memory_budget.clone();
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
                let requested_bytes: u64 = segments
                    .iter()
                    .map(|segment| segment.source_len() as u64)
                    .sum();
                let requested_units = (requested_bytes as usize)
                    .div_ceil(d.restore_pack_prefetch_memory_unit)
                    .max(1);
                let permit_units = requested_units.min(max_memory_units) as u32;

                let permit = memory_budget
                    .acquire_many_owned(permit_units)
                    .await
                    .map_err(|_| anyhow!("Interrupted while reserving restore memory"))?;

                tracing::debug!(target: "restorer", "Downloading {} segments from pack {} ({} bytes)", segments.len(), pack_id.to_short_hex(8), requested_bytes);
                let segments = loader::download_pack_segments(repo, segments).await;
                segments.map(|segments| (segments, permit))
            }
        })
        .buffer_unordered(d.restore_pack_prefetch);

        while let Some(res) = download_stream.next().await {
            if self.shutdown_signal.load(Ordering::Relaxed) {
                bail!("Interrupted");
            }

            let (segments, _memory_permit) = res?;
            for (pack_segment, segment_data) in segments {
                let data_arc = Arc::new(segment_data);

                for (blob_id, locator, targets) in pack_segment.blobs {
                    let start = (locator.offset as u64 - pack_segment.min_offset) as usize;
                    let end = start + locator.length as usize;
                    let encoded_blob = &data_arc[start..end];

                    let decoded_data = secure_storage
                        .decode_owned(encoded_blob.to_vec())
                        .with_context(|| format!("Failed to decode blob {blob_id}"))?;

                    let raw_len = decoded_data.len() as u64;

                    if targets.len() == 1 {
                        let target = &targets[0];
                        let file_path = files[target.file_idx].path.clone();
                        let offset = target.offset_in_file;
                        let file_idx = target.file_idx;
                        let handle_cache = handle_cache.clone();
                        let progress_reporter = self.progress_reporter.clone();
                        let quit_on_error = self.opts.quit_on_error;

                        let write_result = spawn_blocking(move || -> Result<u64, anyhow::Error> {
                            let mut cache_guard = handle_cache.get_shard(file_idx).lock();
                            let file = cache_guard.get_handle(file_idx, &file_path)?;
                            #[cfg(unix)]
                            file.write_at(&decoded_data, offset)
                                .map_err(|e| anyhow!(e))?;
                            #[cfg(windows)]
                            file.seek_write(&decoded_data, offset)
                                .map_err(|e| anyhow!(e))?;
                            Ok(raw_len)
                        })
                        .await
                        .map_err(|e| anyhow!(e))?;

                        match write_result {
                            Ok(bytes) => {
                                progress_reporter.processed_bytes(bytes);
                            }
                            Err(e) => {
                                let err_msg =
                                    format!("Failed to write to file index {file_idx}: {e}");
                                if quit_on_error {
                                    bail!(err_msg);
                                }
                                progress_reporter.error(&err_msg);
                            }
                        }
                    } else {
                        let shared_data = Arc::new(decoded_data);
                        let mut write_handles = Vec::with_capacity(targets.len());

                        for target in &targets {
                            let data = shared_data.clone();
                            let file_path = files[target.file_idx].path.clone();
                            let offset = target.offset_in_file;
                            let file_idx = target.file_idx;
                            let handle_cache = handle_cache.clone();

                            let handle = spawn_blocking(move || -> Result<u64, anyhow::Error> {
                                let mut cache_guard = handle_cache.get_shard(file_idx).lock();
                                let file = cache_guard.get_handle(file_idx, &file_path)?;
                                #[cfg(unix)]
                                file.write_at(&data, offset).map_err(|e| anyhow!(e))?;
                                #[cfg(windows)]
                                file.seek_write(&data, offset).map_err(|e| anyhow!(e))?;
                                Ok(raw_len)
                            });
                            write_handles.push((file_idx, handle));
                        }

                        for (file_idx, handle) in write_handles {
                            match handle.await.map_err(|e| anyhow!(e))? {
                                Ok(bytes) => {
                                    self.progress_reporter.processed_bytes(bytes);
                                }
                                Err(e) => {
                                    let err_msg =
                                        format!("Failed to write to file index {file_idx}: {e}");
                                    if self.opts.quit_on_error {
                                        bail!(err_msg);
                                    }
                                    self.progress_reporter.error(&err_msg);
                                }
                            }
                        }
                    }
                }
            }
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
            .set_message("Restoring metadata...".to_string());

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
                        Err(_) => return,
                    };

                    let stream_node = match stream_node_res {
                        Ok(node) => node,
                        Err(_) => return,
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
                    progress_reporter.processed_item(&path);
                }
            })
            .await;

        // Restore directory metadata bottom-up.
        let mut dirs = directories;
        dirs.sort_unstable_by_key(|(p, _)| std::cmp::Reverse(p.as_os_str().len()));
        for (p, meta) in dirs {
            if self.shutdown_signal.load(Ordering::Relaxed) {
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
        if shutdown_signal.load(Ordering::Relaxed) {
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
