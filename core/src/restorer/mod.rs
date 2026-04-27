//! The restorer module implements the logic for restoring files, directories, and
//! symlinks from a repository snapshot to a local filesystem.
//! It uses a pack-centric approach with background prefetching and concurrent
//! restoration for high performance.

pub(crate) mod node_restorer;
pub(crate) mod sync;

use std::{
    collections::{HashMap, VecDeque},
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
    fs::{self as repo_fs, node::Node, tree::SerializedNodeStream},
    mapache::{
        ID,
        defaults::{
            DEFAULT_RESTORE_BLOB_CONCURRENCY, DEFAULT_RESTORE_MAX_OPEN_FILES,
            DEFAULT_RESTORE_PACK_PREFETCH, DEFAULT_RESTORE_PACK_PREFETCH_MEMORY_BYTES,
            DEFAULT_RESTORE_PACK_PREFETCH_MEMORY_UNIT,
        },
    },
    repository::{
        index::{BlobLocator, MasterIndex},
        loader,
        repo::Repository,
        snapshot::Snapshot,
        storage::SecureStorage,
    },
    ui::restore::RestoreProgressReporter,
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
struct Restorer {
    repo: Arc<Repository>,
    progress_reporter: Arc<dyn RestoreProgressReporter>,
    shutdown_signal: Arc<AtomicBool>,
    target_path: PathBuf,
    opts: RestoreOptions,
}

type PackMap = HashMap<ID, Vec<(ID, BlobRestoreRequest)>>;

/// Information about a single file to be restored.
struct FileRestorePlan {
    /// Full path to the file in the target filesystem.
    path: PathBuf,
    /// Relative path within the snapshot.
    rel_path: PathBuf,
}

/// Information about a directory that has been restored.
#[derive(Clone)]
struct RestoredDirectory {
    /// Path to the directory.
    path: PathBuf,
}

/// The complete plan for the restoration process.
struct RestorePlan {
    /// List of files to be restored.
    files: Arc<Vec<FileRestorePlan>>,
    /// Mapping of pack IDs to the blobs and their target locations in files.
    packs: Arc<PackMap>,
    /// List of directories that were created/restored.
    restored_dirs: Vec<RestoredDirectory>,
    /// Mapping of target paths to their original file nodes.
    file_nodes: HashMap<PathBuf, Node>,
    /// Mapping of target paths to their original directory nodes.
    dir_nodes: HashMap<PathBuf, Node>,
    /// Total number of items (files, dirs, symlinks) in the plan.
    total_items: u64,
    /// Total number of bytes to be restored.
    total_bytes: u64,
}

/// A request to restore a specific blob into a file.
#[derive(Clone)]
struct BlobRestoreRequest {
    /// Index into the RestorePlan's files list.
    file_idx: usize,
    /// Offset within the target file where the blob should be written.
    offset_in_file: u64,
    /// Locator for the blob in the repository's index.
    locator: BlobLocator,
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

impl Restorer {
    /// Creates a new Restorer instance.
    fn new(
        repo: Arc<Repository>,
        target_path: PathBuf,
        opts: RestoreOptions,
        progress_reporter: Arc<dyn RestoreProgressReporter>,
        shutdown_signal: Arc<AtomicBool>,
    ) -> Self {
        Self {
            repo,
            target_path,
            opts,
            progress_reporter,
            shutdown_signal,
        }
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
        let mut node_stream = SerializedNodeStream::new(
            self.repo.clone(),
            Some(tree_id),
            PathBuf::new(),
            include,
            exclude,
        )
        .await?;

        if !self.opts.dry_run {
            fs::create_dir_all(&self.target_path)?;
        }

        let index = self.repo.index();
        let plan = self.build_plan(&mut node_stream, index).await?;

        // Initialize progress reporter with stats gathered during planning
        self.progress_reporter
            .resize_workload(plan.total_items, plan.total_bytes);

        let plan_files = plan.files.clone();
        let plan_packs = plan.packs.clone();
        let dry_run = self.opts.dry_run;
        let secure_storage = self.repo.secure_storage();

        self.restore_packs(plan_files, plan_packs, secure_storage, dry_run)
            .await?;
        self.restore_metadata(&plan, dry_run)?;

        Ok(())
    }

    /// Builds a restoration plan by walking the snapshot tree and determining
    /// which nodes need to be restored.
    async fn build_plan(
        &self,
        node_stream: &mut SerializedNodeStream,
        index: Arc<MasterIndex>,
    ) -> Result<RestorePlan> {
        let mut files = Vec::new();
        let mut packs: PackMap = HashMap::new();
        let mut restored_dirs = Vec::new();
        let mut file_nodes: HashMap<PathBuf, Node> = HashMap::new();
        let mut dir_nodes: HashMap<PathBuf, Node> = HashMap::new();
        let mut node_count = 0;
        let mut total_items = 0;
        let mut total_bytes = 0;

        self.progress_reporter
            .set_message("Planning...".to_string());

        while let Some(node_res) = node_stream.next().await {
            if self.shutdown_signal.load(Ordering::Relaxed) {
                bail!("Interrupted");
            }

            node_count += 1;
            if node_count % 100 == 0 {
                self.progress_reporter
                    .set_message(format!("Planning... ({} nodes)", node_count));
            }

            let (mut path, stream_node_res) = node_res?;
            let stream_node = stream_node_res?;
            let node = stream_node.node;

            if let Some(prefix) = &self.opts.strip_prefix {
                path = match path.strip_prefix(prefix) {
                    Ok(stripped_path) => {
                        if stripped_path.as_os_str().is_empty() {
                            continue;
                        }
                        stripped_path.to_path_buf()
                    }
                    Err(_) => continue,
                };
            }

            total_items += 1;
            if node.is_file() {
                total_bytes += node.metadata.size;
            }

            let restore_path = crate::utils::secure_join(&self.target_path, &path)?;
            if !self
                .should_restore_node(&node, &restore_path, &restored_dirs, index.clone())
                .await?
            {
                self.progress_reporter.processed_item(&path);
                if node.is_file() {
                    self.progress_reporter.processed_bytes(node.metadata.size);
                }
                continue;
            }

            if node.is_dir() {
                if !self.opts.dry_run {
                    fs::create_dir_all(&restore_path)?;
                }
                restored_dirs.push(RestoredDirectory {
                    path: restore_path.clone(),
                });
                dir_nodes.insert(restore_path.clone(), node.clone());
                self.progress_reporter.processed_item(&path);
                continue;
            }

            if node.is_file() {
                let file_idx = files.len();
                let mut blobs_found = true;

                if let Some(blobs) = &node.blobs {
                    let mut offset_in_file = 0;
                    for blob_id in blobs {
                        let locator = match index.get_data(blob_id) {
                            Some(loc) => loc,
                            None => {
                                let err_msg = format!("Blob {blob_id} not found in index");
                                if self.opts.quit_on_error {
                                    bail!(err_msg);
                                }
                                self.progress_reporter.error(&err_msg);
                                blobs_found = false;
                                break;
                            }
                        };
                        packs.entry(locator.pack_id).or_default().push((
                            *blob_id,
                            BlobRestoreRequest {
                                file_idx,
                                offset_in_file,
                                locator,
                            },
                        ));
                        offset_in_file += locator.raw_length as u64;
                    }
                }

                if blobs_found {
                    files.push(FileRestorePlan {
                        path: restore_path.clone(),
                        rel_path: path.clone(),
                    });
                    file_nodes.insert(restore_path.clone(), node.clone());

                    if !self.opts.dry_run {
                        if let Some(parent) = restore_path.parent() {
                            fs::create_dir_all(parent)?;
                        }

                        // If the file exists, it might be a symlink or read-only.
                        // We must NOT follow symlinks when restoring to prevent overwriting
                        // files outside the target directory.
                        if let Ok(m) = fs::symlink_metadata(&restore_path) {
                            if m.file_type().is_symlink() {
                                fs::remove_file(&restore_path)?;
                            } else {
                                self.clear_readonly_attribute(&restore_path)?;
                            }
                        }

                        let mut file = OpenOptions::new()
                            .create(true)
                            .write(true)
                            .truncate(true)
                            .open(&restore_path)
                            .with_context(|| {
                                format!("Failed to create file: {}", restore_path.display())
                            })?;

                        if self.opts.preallocate {
                            self.preallocate_file(&mut file, node.metadata.size)
                                .with_context(|| {
                                    format!(
                                        "Failed to preallocate file: {}",
                                        restore_path.display()
                                    )
                                })?;
                        } else {
                            file.set_len(node.metadata.size)?;
                        }
                    }
                } else {
                    self.progress_reporter.processed_item(&path);
                    self.progress_reporter.processed_bytes(node.metadata.size);
                }
            } else if node.is_symlink() {
                if !self.opts.dry_run
                    && let Err(e) = node_restorer::restore_node_to_path(
                        &self.repo,
                        self.progress_reporter.clone(),
                        &node,
                        &restore_path,
                        false,
                    )
                    .await
                {
                    let err_msg = e.to_string();
                    if self.opts.quit_on_error {
                        bail!(err_msg);
                    }
                    self.progress_reporter.error(&err_msg);
                }
                self.progress_reporter.processed_item(&path);
            }
        }

        Ok(RestorePlan {
            files: Arc::new(files),
            packs: Arc::new(packs),
            restored_dirs,
            file_nodes,
            dir_nodes,
            total_items,
            total_bytes,
        })
    }

    /// Checks if a node should be restored based on the current restoration strategy.
    async fn should_restore_node(
        &self,
        node: &Node,
        restore_path: &Path,
        restored_dirs: &[RestoredDirectory],
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
                if !node.is_dir() || !restored_dirs.iter().any(|d| d.path == restore_path) {
                    bail!("Target {} exists already", restore_path.display());
                }
                Ok(true)
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

            for blob_id in blobs {
                let locator = index
                    .get_data(&blob_id)
                    .ok_or_else(|| anyhow!("Blob {} not found in index", blob_id))?;

                let mut chunk = vec![0u8; locator.raw_length as usize];
                #[cfg(unix)]
                file.read_exact_at(&mut chunk, offset)?;
                #[cfg(windows)]
                file.seek_read(&mut chunk, offset)?;

                let actual_id = ID::from_content(&chunk);
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
                        .processed_bytes(request.locator.raw_length as u64);
                }
            }
            return Ok(());
        }

        self.progress_reporter
            .set_message("Restoring...".to_string());

        let handle_cache = Arc::new(Mutex::new(FileHandleCache::new(
            DEFAULT_RESTORE_MAX_OPEN_FILES,
        )));
        let mut packs_iter = packs.iter();

        let max_memory_units =
            DEFAULT_RESTORE_PACK_PREFETCH_MEMORY_BYTES / DEFAULT_RESTORE_PACK_PREFETCH_MEMORY_UNIT;
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
                        let locator = targets[0].locator;
                        (id, locator, targets)
                    })
                    .collect();

                let segments = loader::segment_blobs(pack_id, blobs_vec);
                let requested_bytes: u64 = segments
                    .iter()
                    .map(|segment| segment.source_len() as u64)
                    .sum();
                let requested_units = (requested_bytes as usize)
                    .div_ceil(DEFAULT_RESTORE_PACK_PREFETCH_MEMORY_UNIT)
                    .max(1);
                let permit_units = requested_units.min(max_memory_units) as u32;

                let permit = memory_budget
                    .acquire_many_owned(permit_units)
                    .await
                    .map_err(|_| anyhow!("Interrupted while reserving restore memory"))?;
                let segments = loader::download_pack_segments(repo, segments).await;
                segments.map(|segments| (segments, permit))
            }
        })
        .buffer_unordered(DEFAULT_RESTORE_PACK_PREFETCH);

        while let Some(res) = download_stream.next().await {
            if self.shutdown_signal.load(Ordering::Relaxed) {
                bail!("Interrupted");
            }

            let (segments, _memory_permit) = res?;
            for (pack_segment, segment_data) in segments {
                let data = Arc::new(segment_data);
                let mut blob_stream = futures::stream::iter(pack_segment.blobs)
                    .map(|(blob_id, locator, targets)| {
                        let pack_data = data.clone();
                        let secure_storage = secure_storage.clone();
                        let handle_cache = handle_cache.clone();
                        let progress_reporter = self.progress_reporter.clone();
                        let files = files.clone();
                        let quit_on_error = self.opts.quit_on_error;
                        let min_offset = pack_segment.min_offset;

                        async move {
                            let start = (locator.offset as u64 - min_offset) as usize;
                            let end = start + locator.length as usize;
                            let encoded_blob = &pack_data[start..end];
                            let data = secure_storage
                                .decode_owned(encoded_blob.to_vec())
                                .with_context(|| format!("Failed to decode blob {blob_id}"))?;

                            let chunk_size = data.len() as u64;
                            let reported_bytes = chunk_size.saturating_mul(targets.len() as u64);

                            let write_result =
                                spawn_blocking(move || -> Result<(), anyhow::Error> {
                                    let mut cache = handle_cache.lock();
                                    for target in targets {
                                        let file_plan = &files[target.file_idx];
                                        let file =
                                            cache.get_handle(target.file_idx, &file_plan.path)?;

                                        #[cfg(unix)]
                                        file.write_at(&data, target.offset_in_file)
                                            .map_err(|e| anyhow!(e))?;
                                        #[cfg(windows)]
                                        file.seek_write(&data, target.offset_in_file)
                                            .map_err(|e| anyhow!(e))?;
                                    }
                                    Ok(())
                                })
                                .await
                                .map_err(|e| anyhow!(e))?;

                            if let Err(e) = write_result {
                                let err_msg = format!(
                                    "Failed to write blob {} to target files: {e}",
                                    blob_id
                                );
                                if quit_on_error {
                                    bail!(err_msg);
                                }
                                progress_reporter.error(&err_msg);
                            }

                            progress_reporter.processed_bytes(reported_bytes);
                            Ok::<(), anyhow::Error>(())
                        }
                    })
                    .buffer_unordered(DEFAULT_RESTORE_BLOB_CONCURRENCY);

                while let Some(blob_res) = blob_stream.next().await {
                    blob_res?;
                }
            }
        }

        Ok(())
    }

    fn restore_metadata(&self, plan: &RestorePlan, dry_run: bool) -> Result<()> {
        if dry_run {
            return Ok(());
        }

        self.progress_reporter
            .set_message("Restoring metadata...".to_string());

        for file_plan in plan.files.iter() {
            if let Some(node) = plan.file_nodes.get(&file_plan.path) {
                node_restorer::try_restore_node_metadata(
                    node,
                    &file_plan.path,
                    self.progress_reporter.as_ref(),
                );
            }
            self.progress_reporter.processed_item(&file_plan.rel_path);
        }

        let mut restored_dirs = plan.restored_dirs.clone();
        restored_dirs.sort_unstable_by_key(|b| std::cmp::Reverse(b.path.as_os_str().len()));

        for dir_entry in restored_dirs {
            if self.shutdown_signal.load(Ordering::Relaxed) {
                bail!("Interrupted");
            }
            if let Some(node) = plan.dir_nodes.get(&dir_entry.path) {
                node_restorer::try_restore_node_metadata(
                    node,
                    &dir_entry.path,
                    self.progress_reporter.as_ref(),
                );
            }
        }

        Ok(())
    }
}
