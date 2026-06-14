//! The restorer module implements the logic for restoring files, directories, and
//! symlinks from a repository snapshot to a local filesystem.
//! It uses a pack-centric approach with background prefetching and concurrent
//! restoration for high performance.

pub(crate) mod node_restorer;

mod metadata;
mod pack_restorer;
mod planner;
mod sync;

pub use sync::{SyncOpts, delete_nodes};

#[cfg(not(unix))]
use std::io::{Seek, Write};
#[cfg(unix)]
use std::os::unix::io::AsRawFd;
use std::{
    collections::{HashMap, VecDeque},
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    sync::{Arc, atomic::AtomicBool},
};

use anyhow::{Context, Result, anyhow, bail};
use clap::ValueEnum;
use parking_lot::Mutex;

use crate::{
    fs::tree::SerializedNodeStream,
    mapache::{ID, defaults},
    repository::{repo::Repository, snapshot::Snapshot},
    ui::RestoreProgressReporter,
    utils,
};

/// Strategy for handling existing files during restoration.
#[derive(Debug, Clone, PartialEq, ValueEnum)]
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
                "Invalid strategy: {s}. Must be one of: fail, overwrite, skip, newer"
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
}

/// Performs the restoration of a snapshot to a target path.
pub async fn restore(
    repo: Arc<Repository>,
    snapshot: &Snapshot,
    target_path: &Path,
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

    restorer.restore(snapshot.tree).await
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
    pub(crate) repo: Arc<Repository>,
    pub(crate) progress_reporter: Arc<dyn RestoreProgressReporter>,
    pub(crate) shutdown_signal: Arc<AtomicBool>,
    pub(crate) target_path: PathBuf,
    pub(crate) opts: RestoreOptions,
    pub(crate) buffers: Arc<Mutex<VecDeque<Vec<u8>>>>,
    pub(crate) initialized: Arc<Vec<std::sync::atomic::AtomicBool>>,
}

pub(crate) type PackMap = HashMap<ID, Vec<(ID, BlobRestoreRequest)>>;

pub(crate) struct FileRestorePlan {
    pub(crate) path: PathBuf,
    pub(crate) num_blobs: u32,
    pub(crate) size: u64,
    pub(crate) is_hardlink: bool,
}

pub(crate) struct RestorePlan {
    pub(crate) files: Arc<Vec<FileRestorePlan>>,
    pub(crate) packs: Arc<PackMap>,
    pub(crate) directories: Vec<(PathBuf, crate::fs::node::Metadata)>,
    pub(crate) skipped_item_paths: Vec<PathBuf>,
    pub(crate) total_items: u64,
    pub(crate) total_bytes: u64,
    pub(crate) skipped_bytes: u64,
    pub(crate) hardlinks: Vec<(usize, usize)>,
}

#[derive(Clone)]
pub(crate) struct BlobRestoreRequest {
    pub(crate) file_idx: usize,
    pub(crate) offset_in_file: u64,
    pub(crate) blob_offset: u32,
    pub(crate) blob_length: u32,
    pub(crate) raw_length: u32,
}

/// A cache for open file handles during restoration.
pub(crate) struct FileHandleCache {
    handles: HashMap<usize, File>,
    order: VecDeque<usize>,
    max_handles: usize,
}

impl FileHandleCache {
    fn new(max_handles: usize) -> Self {
        Self {
            handles: HashMap::new(),
            order: VecDeque::new(),
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

        let file = if !initialized.load(std::sync::atomic::Ordering::Acquire) {
            if let Ok(m) = fs::symlink_metadata(path) {
                if m.file_type().is_symlink() {
                    fs::remove_file(path).with_context(|| {
                        format!("Failed to remove symlink at {}", path.display())
                    })?;
                } else {
                    restorer.clear_readonly_attribute(path)?;
                }
            }

            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).with_context(|| {
                    format!("Failed to create parent directory for {}", path.display())
                })?;
            }

            let mut f = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(path)
                .with_context(|| format!("Failed to create/truncate file: {}", path.display()))?;

            if plan.size > 0 {
                if restorer.opts.preallocate {
                    if let Err(e) = restorer.preallocate_file(&mut f, plan.size) {
                        tracing::warn!(target: "restorer", "Failed to preallocate file {}: {e}", path.display());
                    }
                } else {
                    // Default to sparse creation via set_len.
                    // On most modern filesystems (NTFS, APFS, XFS, EXT4), this creates a sparse file.
                    f.set_len(plan.size).with_context(|| {
                        format!("Failed to set length for sparse file: {}", path.display())
                    })?;
                }
            }

            initialized.store(true, std::sync::atomic::Ordering::Release);
            f
        } else {
            OpenOptions::new()
                .write(true)
                .open(path)
                .with_context(|| format!("Failed to open file for writing: {}", path.display()))?
        };

        self.handles.insert(file_idx, file);
        self.order.push_back(file_idx);
        self.handles
            .get(&file_idx)
            .ok_or_else(|| anyhow!("Failed to retrieve file handle after insertion"))
    }
}

/// A sharded wrapper around FileHandleCache to reduce lock contention.
pub(crate) struct ShardedFileHandleCache {
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
    fn new(
        repo: Arc<Repository>,
        target_path: PathBuf,
        opts: RestoreOptions,
        progress_reporter: Arc<dyn RestoreProgressReporter>,
        shutdown_signal: Arc<AtomicBool>,
    ) -> Self {
        let d = defaults::runtime();
        let num_buffers = d.restore_max_open_files;
        Self {
            repo,
            target_path,
            opts,
            progress_reporter,
            shutdown_signal,
            buffers: Arc::new(Mutex::new(VecDeque::with_capacity(num_buffers))),
            initialized: Arc::new(Vec::new()),
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

    /// Creates a shallow clone of the restorer for worker threads.
    /// This is needed because Restorer contains Arc fields that we want to share.
    pub(crate) fn clone_for_workers(&self) -> Self {
        Self {
            repo: self.repo.clone(),
            progress_reporter: self.progress_reporter.clone(),
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
            },
            buffers: self.buffers.clone(),
            initialized: self.initialized.clone(),
        }
    }

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

            const F_VOLPOSMODE: i32 = 1;
            const F_STARTPOSMODE: i32 = 3;

            let mut store = libc::fstore_t {
                fst_flags: libc::F_ALLOCATEALL,
                fst_posmode: F_VOLPOSMODE,
                fst_offset: 0,
                fst_length: length as libc::off_t,
                fst_bytesalloc: 0,
            };

            let mut res = unsafe { libc::fcntl(fd, libc::F_PREALLOCATE, &store) };

            if res == -1 {
                store.fst_posmode = F_STARTPOSMODE;
                res = unsafe { libc::fcntl(fd, libc::F_PREALLOCATE, &store) };
            }

            if res == -1 {
                return Err(anyhow!(std::io::Error::last_os_error()));
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
            if mode & 0o200 == 0 {
                perms.set_mode(mode | 0o200);
                fs::set_permissions(path, perms).with_context(|| {
                    format!("Failed to set write permission on {}", path.display())
                })?;
            }
        }

        Ok(())
    }

    async fn restore(&self, tree_id: ID) -> Result<()> {
        tracing::info!(target: "restorer", "Starting restoration of tree {tree_id}");
        let node_stream = SerializedNodeStream::new(
            self.repo.clone(),
            Some(tree_id),
            PathBuf::new(),
            self.opts.include.clone(),
            self.opts.exclude.clone(),
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

        self.progress_reporter
            .resize_workload(plan.total_items, plan.total_bytes);

        if plan.skipped_bytes > 0 {
            self.progress_reporter.processed_bytes(plan.skipped_bytes);
        }

        for path in &plan.skipped_item_paths {
            self.progress_reporter.processed_item(path);
        }
        for (path, _) in &plan.directories {
            self.progress_reporter.processed_item(path);
        }

        let plan_files = plan.files.clone();
        let dry_run = self.opts.dry_run;
        let secure_storage = self.repo.secure_storage();

        tracing::info!(target: "restorer", "Restoring data packs");
        self.restore_packs(plan_files, plan.packs, secure_storage, dry_run)
            .await?;

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
                    continue;
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
        self.restore_metadata(
            tree_id,
            self.opts.include.clone(),
            self.opts.exclude.clone(),
            plan.directories,
        )
        .await?;

        tracing::info!(target: "restorer", "Restoration finished");
        Ok(())
    }
}
