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
    backend::Handle,
    fs::{self as repo_fs, node::Node, tree::SerializedNodeStream},
    mapache::{
        ContentIdType, ID,
        defaults::{
            DEFAULT_RESTORE_BLOB_CONCURRENCY, DEFAULT_RESTORE_MAX_OPEN_FILES,
            DEFAULT_RESTORE_PACK_PREFETCH, DEFAULT_RESTORE_PACK_PREFETCH_MEMORY_BYTES,
            DEFAULT_RESTORE_PACK_PREFETCH_MEMORY_UNIT, DEFAULT_RESTORE_PACK_SEGMENT_MAX_SIZE,
        },
    },
    repository::{
        index::{BlobLocator, MasterIndex},
        repo::Repository,
        snapshot::Snapshot,
        storage::SecureStorage,
    },
    ui::restore::RestoreProgressReporter,
};

#[derive(Debug, Clone, PartialEq, ValueEnum)]
pub enum Strategy {
    Fail,
    Overwrite,
    Skip,
    Newer,
}

pub struct RestoreOptions {
    pub strategy: Strategy,
    pub strip_prefix: Option<PathBuf>,
    pub dry_run: bool,
    pub quit_on_error: bool,
    pub preallocate: bool,
}

#[allow(clippy::too_many_arguments)]
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

struct Restorer {
    repo: Arc<Repository>,
    progress_reporter: Arc<dyn RestoreProgressReporter>,
    shutdown_signal: Arc<AtomicBool>,
    target_path: PathBuf,
    opts: RestoreOptions,
}

type PackMap = HashMap<ID, Vec<(ID, BlobRestoreRequest)>>;

struct FileRestorePlan {
    path: PathBuf,
    rel_path: PathBuf,
}

#[derive(Clone)]
struct RestoredDirectory {
    path: PathBuf,
}

struct RestorePlan {
    files: Arc<Vec<FileRestorePlan>>,
    packs: Arc<PackMap>,
    restored_dirs: Vec<RestoredDirectory>,
    file_nodes: HashMap<PathBuf, Node>,
    dir_nodes: HashMap<PathBuf, Node>,
}

#[derive(Clone)]
struct BlobRestoreRequest {
    file_idx: usize,
    offset_in_file: u64,
    locator: BlobLocator,
}

const PACK_READ_MERGE_THRESHOLD: u64 = 16 * 1024;

struct DownloadedPackSegment {
    data: Arc<Vec<u8>>,
    blob_to_targets: Arc<HashMap<ID, Vec<BlobRestoreRequest>>>,
    sorted_blobs: Vec<(ID, BlobLocator)>,
    min_offset: u64,
}

impl DownloadedPackSegment {
    fn source_len(&self) -> usize {
        self.sorted_blobs
            .iter()
            .map(|(_, loc)| loc.offset as u64 + loc.length as u64)
            .max()
            .unwrap_or(self.min_offset) as usize
            - self.min_offset as usize
    }
}

/// A cache for open file handles during restoration
struct FileHandleCache {
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

            // Use F_VOLPOSMODE (1) - Allocate from the current position/offset
            // This is often more reliable than F_PEEKOSYNC in test environments
            let mut store = libc::fstore_t {
                fst_flags: libc::F_ALLOCATEALL,
                fst_posmode: 1, // F_VOLPOSMODE
                fst_offset: 0,
                fst_length: length as libc::off_t,
                fst_bytesalloc: 0,
            };

            let mut res = unsafe { libc::fcntl(fd, libc::F_PREALLOCATE, &store) };

            // If F_VOLPOSMODE fails, try the "start of file" mode
            if res == -1 {
                store.fst_posmode = 3; // F_STARTPOSMODE
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

        let plan_files = plan.files.clone();
        let plan_packs = plan.packs.clone();
        let dry_run = self.opts.dry_run;
        let secure_storage = self.repo.secure_storage();

        self.restore_packs(plan_files.clone(), plan_packs, secure_storage, dry_run)
            .await?;
        self.restore_metadata(&plan, dry_run)?;

        Ok(())
    }

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

            let restore_path = self.target_path.join(&path);
            if !self
                .should_restore_node(&node, &restore_path, &restored_dirs)
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
        })
    }

    fn segment_pack_blob_requests(
        blob_requests: Vec<(ID, BlobRestoreRequest)>,
    ) -> Vec<DownloadedPackSegment> {
        let mut blob_to_targets: HashMap<ID, Vec<BlobRestoreRequest>> = HashMap::new();
        for (blob_id, req) in blob_requests {
            blob_to_targets.entry(blob_id).or_default().push(req);
        }

        let mut sorted_blobs = blob_to_targets
            .iter()
            .map(|(blob_id, targets)| (*blob_id, targets[0].locator))
            .collect::<Vec<_>>();
        sorted_blobs.sort_by_key(|(_, loc)| loc.offset);

        let mut segments = Vec::new();
        let mut current_segment = Vec::new();
        let mut segment_min = 0;
        let mut segment_max = 0;
        let max_segment_size = DEFAULT_RESTORE_PACK_SEGMENT_MAX_SIZE;

        for (blob_id, locator) in sorted_blobs.iter().cloned() {
            let blob_start = locator.offset as u64;
            let blob_end = blob_start + locator.length as u64;
            let next_segment_max = segment_max.max(blob_end);
            let next_segment_size = next_segment_max.saturating_sub(segment_min);

            if current_segment.is_empty() {
                segment_min = blob_start;
                segment_max = blob_end;
                current_segment.push((blob_id, locator));
            } else if blob_start <= segment_max + PACK_READ_MERGE_THRESHOLD
                && next_segment_size <= max_segment_size
            {
                segment_max = next_segment_max;
                current_segment.push((blob_id, locator));
            } else {
                segments.push(Self::build_segment_from_current(
                    &blob_to_targets,
                    current_segment,
                    segment_min,
                ));
                current_segment = vec![(blob_id, locator)];
                segment_min = blob_start;
                segment_max = blob_end;
            }
        }

        if !current_segment.is_empty() {
            segments.push(Self::build_segment_from_current(
                &blob_to_targets,
                current_segment,
                segment_min,
            ));
        }

        segments
    }

    fn build_segment_from_current(
        blob_to_targets: &HashMap<ID, Vec<BlobRestoreRequest>>,
        current_segment: Vec<(ID, BlobLocator)>,
        min_offset: u64,
    ) -> DownloadedPackSegment {
        let mut segment_blob_to_targets = HashMap::new();
        for (id, _) in &current_segment {
            segment_blob_to_targets.insert(*id, blob_to_targets.get(id).unwrap().clone());
        }

        DownloadedPackSegment {
            data: Arc::new(Vec::new()),
            blob_to_targets: Arc::new(segment_blob_to_targets),
            sorted_blobs: current_segment,
            min_offset,
        }
    }

    async fn download_pack_segments_for_repo(
        repo: Arc<Repository>,
        pack_id: ID,
        mut segments: Vec<DownloadedPackSegment>,
    ) -> Result<Vec<DownloadedPackSegment>> {
        let path = repo.get_path(ContentIdType::Pack, &pack_id);

        for segment in &mut segments {
            let source_len = segment.source_len();

            let pack_data = repo
                .backend()
                .read(
                    &Handle::new_with_hint(&path, ContentIdType::Pack, false),
                    segment.min_offset as isize,
                    source_len,
                )
                .await
                .with_context(|| format!("Failed to read pack {pack_id}"))?;

            segment.data = Arc::new(pack_data);
        }

        Ok(segments)
    }

    async fn should_restore_node(
        &self,
        node: &Node,
        restore_path: &Path,
        restored_dirs: &[RestoredDirectory],
    ) -> Result<bool> {
        if !repo_fs::path_exists(restore_path).await {
            return Ok(true);
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
                let segments = Restorer::segment_pack_blob_requests(blob_requests);
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
                let segments =
                    Restorer::download_pack_segments_for_repo(repo, pack_id, segments).await;
                segments.map(|segments| (segments, permit))
            }
        })
        .buffer_unordered(DEFAULT_RESTORE_PACK_PREFETCH);

        while let Some(res) = download_stream.next().await {
            if self.shutdown_signal.load(Ordering::Relaxed) {
                bail!("Interrupted");
            }

            let (segments, _memory_permit) = res?;
            for pack_segment in segments {
                let mut blob_stream = futures::stream::iter(pack_segment.sorted_blobs.into_iter())
                    .map(|(blob_id, locator)| {
                        let pack_data = pack_segment.data.clone();
                        let secure_storage = secure_storage.clone();
                        let handle_cache = handle_cache.clone();
                        let progress_reporter = self.progress_reporter.clone();
                        let files = files.clone();
                        let quit_on_error = self.opts.quit_on_error;
                        let targets = pack_segment.blob_to_targets.get(&blob_id).unwrap().clone();
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
        restored_dirs
            .sort_unstable_by(|a, b| b.path.as_os_str().len().cmp(&a.path.as_os_str().len()));

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
