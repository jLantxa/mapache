use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use anyhow::{Context, Result, bail};
use chrono::Duration;
use colored::Colorize;
use futures::{StreamExt, stream};
use parking_lot::Mutex;
use rand::{RngExt, rng};
use zeroize::Zeroizing;

use crate::{
    backend::{Handle, StorageBackend, StorageHint, WriteContents, cache::CacheBackend},
    commands::Compression,
    mapache::{
        self, BlobType, ContentIdType, ID, SaveID,
        traits::{BlobLoader, BlobSaver},
    },
    repository::{
        index::{Index, IndexFile, MasterIndex},
        keys::{self, KeyManager},
        lock::{Lock, LockHandle},
        manifest::Manifest,
        packer::{PackSaver, PackSaverRequest},
        snapshot::Snapshot,
        storage::{EncodingContext, SecureStorage},
    },
    ui::{self},
    utils::{self, collections::IdSet},
};

pub const THIS_REPOSITORY_VERSION: u32 = 1;

pub const OBJECTS_DIR: &str = "objects";
pub const SNAPSHOTS_DIR: &str = "snapshots";
pub const INDEX_DIR: &str = "index";
pub const MANIFEST_PATH: &str = "manifest";
pub const KEYS_DIR: &str = "keys";
pub const LOCKS_DIR: &str = "locks";

pub(crate) const REPO_TMP_EXTENSION: &str = "tmp";
pub(crate) const REPO_DROPPED_EXTENSION: &str = "dropped";

const OBJECTS_DIR_FANOUT: usize = 2;

/// A pair of sizes representing raw and encoded (compressed/encrypted) bytes.
#[derive(Debug, Default, Copy, Clone)]
pub struct SizePair {
    /// Original size of the data.
    pub raw: u64,
    /// Size of the data after encoding.
    pub encoded: u64,
}

impl SizePair {
    pub fn new(raw: u64, encoded: u64) -> Self {
        Self { raw, encoded }
    }

    pub fn zero() -> Self {
        Self { raw: 0, encoded: 0 }
    }
}

impl std::ops::Add for SizePair {
    type Output = SizePair;
    #[inline]
    fn add(self, rhs: SizePair) -> SizePair {
        SizePair {
            raw: self.raw + rhs.raw,
            encoded: self.encoded + rhs.encoded,
        }
    }
}

impl std::ops::AddAssign for SizePair {
    #[inline]
    fn add_assign(&mut self, rhs: SizePair) {
        self.raw += rhs.raw;
        self.encoded += rhs.encoded;
    }
}

/// Authentication credentials of a user.
#[derive(Debug)]
pub struct Auth {
    pub username: String,
    pub password: Zeroizing<String>,
}

/// Configuration options for a repository.
#[derive(Debug, Clone, Copy)]
pub struct RepoConfig {
    /// Maximum size of a pack file in bytes.
    pub pack_size: u64,
    /// Whether to use the local metadata cache.
    pub use_cache: bool,
    /// Compression level to use for new data.
    pub(crate) compression: Compression,
}

/// Thread-safe statistics for a repository.
#[derive(Debug, Default)]
pub struct RepoStats {
    pub raw_bytes: AtomicU64,
    pub encoded_bytes: AtomicU64,
    pub data_blobs: AtomicU64,

    pub meta_raw_bytes: AtomicU64,
    pub meta_encoded_bytes: AtomicU64,
    pub meta_blobs: AtomicU64,

    pub index_raw_bytes: AtomicU64,
    pub index_meta_bytes: AtomicU64,
}

/// A snapshot of repository statistics at a point in time.
#[derive(Debug, Clone, Copy, Default)]
pub struct RepoStatsSnapshot {
    pub data: SizePair,
    pub meta: SizePair,
    pub total: SizePair,
    pub blobs: u64,
    pub meta_blobs: u64,
    pub index: SizePair,
}

impl RepoStats {
    pub fn snapshot(&self) -> RepoStatsSnapshot {
        let rb = self.raw_bytes.load(Ordering::Relaxed);
        let eb = self.encoded_bytes.load(Ordering::Relaxed);
        let mrb = self.meta_raw_bytes.load(Ordering::Relaxed);
        let meb = self.meta_encoded_bytes.load(Ordering::Relaxed);
        let blobs = self.data_blobs.load(Ordering::Relaxed);
        let meta_blobs = self.meta_blobs.load(Ordering::Relaxed);
        let index_raw = self.index_raw_bytes.load(Ordering::Relaxed);
        let index_meta = self.index_meta_bytes.load(Ordering::Relaxed);

        RepoStatsSnapshot {
            data: SizePair::new(rb, eb),
            meta: SizePair::new(mrb, meb),
            total: SizePair::new(rb + mrb, eb + meb),
            blobs,
            meta_blobs,
            index: SizePair::new(index_raw, index_meta),
        }
    }
}

/// The Repository struct is the central entry point for all repository operations.
///
/// It manages the lifecycle of a repository, including:
/// - Authentication and key management through [SecureStorage].
/// - High-level I/O operations for snapshots, trees, and blobs.
/// - Data packing and indexing to optimize storage and retrieval.
/// - Concurrency control via repository locks.
///
/// A Repository instance is usually obtained by calling [Repository::try_open_with_lock]
/// or [Repository::try_open_unlocked].
pub struct Repository {
    manifest: Manifest,

    // Storage
    backend: Arc<dyn StorageBackend>,
    secure_storage: Arc<SecureStorage>,

    // Paths
    objects_path: PathBuf,
    snapshot_path: PathBuf,
    index_path: PathBuf,
    keys_path: PathBuf,
    locks_path: PathBuf,

    master_index: Arc<MasterIndex>,

    // Packing
    max_packer_size: u64,
    pack_saver_tx: parking_lot::RwLock<Option<crossbeam_channel::Sender<PackSaverRequest>>>,
    pack_saver_handle: parking_lot::RwLock<Option<std::thread::JoinHandle<Result<()>>>>,

    // Stats
    pub(super) stats: RepoStats,
}

impl BlobSaver for Repository {
    fn save_blob(
        &self,
        blob_type: BlobType,
        data: WriteContents<'_>,
        save_id: SaveID,
    ) -> Result<ID> {
        self.encode_and_save_blob(blob_type, data, save_id)
    }
}

#[async_trait::async_trait]
impl BlobLoader for Repository {
    async fn load_blob(&self, id: &ID) -> Result<Vec<u8>> {
        self.load_blob(id).await
    }
}

impl Repository {
    /// Create and initialize a new repository
    pub async fn init(
        auth: &Auth,
        keyfile_path: Option<&PathBuf>,
        backend: Arc<dyn StorageBackend>,
    ) -> Result<Manifest> {
        if backend.path_exists(Path::new(MANIFEST_PATH)).await {
            bail!("Repository already exists (manifest found)");
        }

        backend
            .create()
            .await
            .context("Could not create root directory")?;

        let keys_path = PathBuf::from(KEYS_DIR);
        backend.create_dir(&keys_path).await?;

        // Create new key
        let master_key = KeyManager::generate_new_master_key();
        let keyfile = KeyManager::generate_key_file(auth, &master_key.clone())
            .context("Could not generate key")?;
        let secure_storage = Arc::new(
            SecureStorage::new()
                .with_compression(Compression::Fast.to_level())
                .with_key(&master_key),
        );

        let keyfile_json = serde_json::to_string_pretty(&keyfile)?;
        let keyfile_json = secure_storage.compress(keyfile_json.as_bytes())?;
        let keyfile_id = ID::from_content(&keyfile_json);
        match keyfile_path {
            Some(p) => {
                std::fs::write(p, &keyfile_json)?;
            }
            None => {
                let p = keys_path.join(keyfile_id.to_hex());
                let handle = Handle::new_with_hint(&p, ContentIdType::Key, true);
                backend
                    .write(&handle, WriteContents::Owned(keyfile_json))
                    .await?;
            }
        }

        // Init repository structure
        let objects_path = PathBuf::from(OBJECTS_DIR);
        let snapshot_path = PathBuf::from(SNAPSHOTS_DIR);
        let index_path = PathBuf::from(INDEX_DIR);
        let locks_path = PathBuf::from(LOCKS_DIR);

        // Save new manifest
        let manifest = Manifest::new(THIS_REPOSITORY_VERSION);

        let manifest_path = Path::new(MANIFEST_PATH);
        let manifest_json = serde_json::to_string_pretty(&manifest)?;
        let manifest_json = secure_storage.encode(manifest_json.as_bytes())?;
        backend
            .write(
                &Handle::new(manifest_path),
                WriteContents::Owned(manifest_json),
            )
            .await?;

        backend.create_dir(&objects_path).await?;
        let num_folders: usize = 1 << (4 * OBJECTS_DIR_FANOUT);
        for n in 0x00..num_folders {
            backend
                .create_dir(&objects_path.join(format!("{n:0>OBJECTS_DIR_FANOUT$x}")))
                .await?;
        }

        backend.create_dir(&snapshot_path).await?;
        backend.create_dir(&index_path).await?;
        backend.create_dir(&locks_path).await?;

        Ok(manifest)
    }

    /// Try to open a repository and acquire a lock.
    #[allow(clippy::type_complexity)]
    pub async fn try_open_with_lock(
        auth: &Auth,
        key_file_path: Option<&PathBuf>,
        backend: Arc<dyn StorageBackend>,
        config: RepoConfig,
        exclusive_lock: bool,
        retry_duration: Option<Duration>,
    ) -> Result<(Arc<Repository>, Arc<SecureStorage>, LockHandle)> {
        let dry_run = backend.is_dry_run();
        let (repo, secure_storage) =
            Self::try_open_unlocked(auth, key_file_path, backend, config).await?;
        let lock = repo
            .try_acquire_lock_with_retry(exclusive_lock, retry_duration)
            .await?;
        let lock_handle = LockHandle::new(repo.clone(), lock, dry_run);

        Ok((repo, secure_storage, lock_handle))
    }

    /// Try to open a repository without acquiring a lock.
    #[allow(clippy::type_complexity)]
    pub async fn try_open_unlocked(
        auth: &Auth,
        key_file_path: Option<&PathBuf>,
        backend: Arc<dyn StorageBackend>,
        config: RepoConfig,
    ) -> Result<(Arc<Repository>, Arc<SecureStorage>)> {
        let key_manager = KeyManager::new(backend.clone());

        let (_key_id, master_key) = key_manager.retrieve_master_key(auth, key_file_path).await?;

        let secure_storage = Arc::new(
            SecureStorage::new()
                .with_compression(config.compression.to_level())
                .with_key(&master_key),
        );

        let manifest_path = Path::new(MANIFEST_PATH);

        let manifest = backend
            .read(&Handle::new(manifest_path), 0, 0)
            .await
            .context("This is not a mapache repository.")?;
        let manifest = secure_storage
            .decode(&manifest)
            .context("Could not decode the manifest file")?;
        let manifest: Manifest = serde_json::from_slice(&manifest)?;

        let version = manifest.version();
        if version > THIS_REPOSITORY_VERSION {
            bail!("Invalid repository version '{version}'");
        }

        let repo = Repository::open(backend, secure_storage.clone(), config).await?;

        Ok((repo, secure_storage))
    }

    /// Open an existing repository from a directory
    async fn open(
        backend: Arc<dyn StorageBackend>,
        secure_storage: Arc<SecureStorage>,
        config: RepoConfig,
    ) -> Result<Arc<Self>> {
        let manifest: Manifest =
            Self::load_manifest(secure_storage.clone(), backend.clone()).await?;

        // If use cache, wrap the backend in a cache
        let backend = match config.use_cache {
            true => {
                let repo_id = manifest.id().to_hex();
                let cache_dir = CacheBackend::default_dir().join(repo_id);
                Arc::new(CacheBackend::new(cache_dir.to_owned(), backend.clone()))
            }
            false => backend,
        };

        let master_index = Arc::new(MasterIndex::new());

        let repo = Repository {
            manifest,
            backend,
            objects_path: PathBuf::from(OBJECTS_DIR),
            snapshot_path: PathBuf::from(SNAPSHOTS_DIR),
            index_path: PathBuf::from(INDEX_DIR),
            keys_path: PathBuf::from(KEYS_DIR),
            locks_path: PathBuf::from(LOCKS_DIR),
            secure_storage,
            max_packer_size: config.pack_size,
            master_index,
            pack_saver_tx: parking_lot::RwLock::new(None),
            pack_saver_handle: parking_lot::RwLock::new(None),
            stats: RepoStats::default(),
        };

        Ok(Arc::new(repo))
    }

    /// Returns a reference to the repo manifest.
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// Get the repository backend
    pub fn backend(&self) -> Arc<dyn StorageBackend> {
        self.backend.clone()
    }

    /// Get the secure storage
    pub fn secure_storage(&self) -> Arc<SecureStorage> {
        self.secure_storage.clone()
    }

    /// Encodes and saves a blob in the repository. This blob can be packed with other blobs in a pack file.
    /// Returns the ID of the saved blob.
    ///
    /// NOTE: This is a synchronous, CPU-intensive operation (hashing and encryption).
    /// If called from an async context, it should be wrapped in `tokio::task::spawn_blocking`.
    pub fn encode_and_save_blob(
        &self,
        blob_type: BlobType,
        data: WriteContents<'_>,
        save_id: SaveID,
    ) -> Result<ID> {
        let id = match save_id {
            SaveID::CalculateID => ID::from_content(&data),
            SaveID::WithID(id) => id,
        };

        // Fast path for existing blobs
        if self.master_index.contains(&id) || !self.master_index.add_pending_blob(id) {
            return Ok(id);
        }

        let raw_length = data.len() as u64;
        let encoded_data = self.secure_storage.encode(&data)?;

        let tx = {
            let tx_guard = self.pack_saver_tx.read();
            tx_guard
                .as_ref()
                .context("Packer is stopped or not initialized")?
                .clone()
        };

        tx.send(PackSaverRequest::SaveBlob {
            id,
            blob_type,
            data: encoded_data,
            raw_length,
        })
        .map_err(|_| anyhow::anyhow!("Packer channel closed"))?;

        Ok(id)
    }

    /// Loads a blob from the repository.
    pub async fn load_blob(&self, id: &ID) -> Result<Vec<u8>> {
        let blob_entry = self.master_index.get(id);
        match blob_entry {
            Some(locator) => {
                self.load_from_pack(
                    &locator.pack_id,
                    locator.blob_type,
                    locator.offset,
                    locator.length,
                )
                .await
            }
            None => bail!("Could not find blob {id:?} in index"),
        }
    }

    /// Saves a file to the repository
    pub async fn save_file(
        &self,
        id: &SaveID,
        data: &[u8],
        hint: StorageHint,
        with_extension: Option<&str>,
    ) -> Result<(ID, SizePair)> {
        let file_type = hint.file_type;

        let raw_size = data.len() as u64;
        let (encoded_data, encoded_size) = match file_type {
            ContentIdType::Pack => (None, raw_size),
            ContentIdType::Key => {
                let compressed_data = self.secure_storage.compress(data)?;
                let size = compressed_data.len() as u64;
                (Some(compressed_data), size)
            }
            _ => {
                let encoded_data = self.secure_storage.encode(data)?;
                let size = encoded_data.len() as u64;
                (Some(encoded_data), size)
            }
        };

        let bytes_to_write = encoded_data.as_deref().unwrap_or(data);

        // Assign ID after (potentially) encoding
        let id = match id {
            SaveID::CalculateID => &ID::from_content(bytes_to_write),
            SaveID::WithID(id) => id,
        };

        let path = self
            .get_path(file_type, id)
            .with_extension(with_extension.unwrap_or_default());
        let handle = Handle {
            path: &path,
            hint: Some(hint),
        };

        let cow_data = match encoded_data {
            Some(d) => WriteContents::Owned(d),
            None => WriteContents::Borrowed(data),
        };

        self.backend.write(&handle, cow_data).await?;

        Ok((*id, SizePair::new(raw_size, encoded_size)))
    }

    /// Loads a file to the repository
    pub async fn load_file(
        &self,
        id: &ID,
        hint: StorageHint,
        with_extension: Option<&str>,
    ) -> Result<Vec<u8>> {
        let path = self
            .get_path(hint.file_type, id)
            .with_extension(with_extension.unwrap_or_default());
        let file_type = hint.file_type;
        let handle = Handle {
            path: &path,
            hint: Some(hint),
        };
        let data = self.backend.read(&handle, 0, 0).await?;

        match file_type {
            ContentIdType::Pack => Ok(data),
            ContentIdType::Key => self.secure_storage.decompress(&data),
            _ => self.secure_storage.decode_owned(data),
        }
    }

    /// Deletes a file from the repository
    pub async fn delete_file(
        &self,
        file_type: ContentIdType,
        id: &ID,
        with_extension: Option<&str>,
    ) -> Result<u64> {
        let path = self
            .get_path(file_type, id)
            .with_extension(with_extension.unwrap_or_default());
        let size = self.backend.lstat(&path).await?.size;
        self.backend.remove(&path).await?;

        Ok(size.unwrap_or(0))
    }

    /// Sets an extension to a file.
    pub async fn set_extension(
        &self,
        file_type: ContentIdType,
        id: &ID,
        extension: Option<&str>,
    ) -> Result<()> {
        let path = self.get_path(file_type, id);
        let ext_path = path.with_extension(extension.unwrap_or_default());
        self.backend.rename(&path, &ext_path).await?;

        Ok(())
    }

    /// Removes a snapshot from the repository, if it exists.
    pub async fn remove_snapshot(&self, id: &ID) -> Result<()> {
        let snapshot_path = self.snapshot_path.join(id.to_hex());

        if !self.backend.path_exists(&snapshot_path).await {
            bail!("Snapshot {id} doesn't exist")
        }

        self.backend
            .remove(&snapshot_path)
            .await
            .with_context(|| format!("Could not remove snapshot {id}"))
    }

    /// Loads a snapshot by ID
    pub async fn load_snapshot(&self, id: &ID, extension: Option<&str>) -> Result<Snapshot> {
        let snapshot = self
            .load_file(
                id,
                StorageHint {
                    file_type: ContentIdType::Snapshot,
                    is_metadata: true,
                },
                extension,
            )
            .await
            .with_context(|| format!("No snapshot with ID '{id}' exists"))?;
        let snapshot: Snapshot = serde_json::from_slice(&snapshot)?;
        Ok(snapshot)
    }

    /// Recall a dropped snapshot with ID
    pub async fn recall_dropped_snapshot(&self, id: &ID) -> Result<()> {
        let path = self.get_path(ContentIdType::Snapshot, id);
        let dropped_path = path.with_extension(REPO_DROPPED_EXTENSION);
        self.backend.rename(&dropped_path, &path).await?;
        Ok(())
    }

    /// Lists all snapshot IDs
    pub async fn list_snapshot_ids(&self) -> Result<Vec<ID>> {
        let ids = self
            .list_files(ContentIdType::Snapshot)
            .await
            .context("Could not list snapshots")?
            .into_iter()
            .filter_map(|path| {
                path.file_name()
                    .and_then(|s| s.to_str())
                    .and_then(|file_name| ID::from_hex(file_name).ok())
            })
            .collect();

        Ok(ids)
    }

    pub(crate) async fn list_index_ids(&self) -> Result<Vec<ID>> {
        let index_paths = self.list_files(ContentIdType::Index).await?;
        let mut index_ids = Vec::with_capacity(index_paths.len());

        for file_path in index_paths {
            let file_name = file_path
                .file_name()
                .expect("Could not read index file name")
                .to_string_lossy()
                .clone();

            match ID::from_hex(&file_name) {
                Ok(id) => index_ids.push(id),
                Err(_) => continue, // Ignore invalid ID names
            }
        }

        Ok(index_ids)
    }

    /// Lists all .dropped snapshot IDs
    pub(crate) async fn list_dropped_snapshot_ids(&self) -> Result<Vec<ID>> {
        let ids = self
            .list_files_with_extension(ContentIdType::Snapshot, Some(REPO_DROPPED_EXTENSION))
            .await
            .context("Failed to list files with the dropped snapshot extension")?
            .into_iter()
            .filter_map(|path| {
                path.file_stem()
                    .and_then(|s| s.to_str())
                    .and_then(|file_stem| ID::from_hex(file_stem).ok())
            })
            .collect();

        Ok(ids)
    }

    /// Loads a pack.
    pub async fn load_pack(&self, id: &ID) -> Result<Vec<u8>> {
        // We don't really know what kind of pack we are loading, so it is not metadata
        self.load_file(
            id,
            StorageHint {
                file_type: ContentIdType::Pack,
                is_metadata: false,
            },
            None,
        )
        .await
    }

    /// Loads an index file.
    pub async fn load_index(&self, id: &ID) -> Result<IndexFile> {
        let index: Vec<u8> = self
            .load_file(
                id,
                StorageHint {
                    file_type: ContentIdType::Index,
                    is_metadata: true,
                },
                None,
            )
            .await
            .with_context(|| format!("Could not load index {}", id.to_hex()))?;
        let index = serde_json::from_slice(&index)?;
        Ok(index)
    }

    /// Loads the repository manifest.
    async fn load_manifest(
        secure_storage: Arc<SecureStorage>,
        backend: Arc<dyn StorageBackend>,
    ) -> Result<Manifest> {
        let manifest = backend
            .read(&Handle::new(Path::new(MANIFEST_PATH)), 0, 0)
            .await?;
        let manifest = secure_storage.decode(&manifest)?;
        let manifest = serde_json::from_slice(&manifest)?;
        Ok(manifest)
    }

    /// Loads a lock file.
    pub async fn load_lock(&self, id: &ID) -> Result<Lock> {
        let lock: Vec<u8> = self
            .load_file(
                id,
                StorageHint {
                    file_type: ContentIdType::Lock,
                    is_metadata: true,
                },
                None,
            )
            .await
            .with_context(|| format!("Could not load lock file {}", id.to_hex()))?;
        let lock = serde_json::from_slice(&lock)?;
        Ok(lock)
    }

    /// Loads a KeyFile.
    pub async fn load_key(&self, id: &ID) -> Result<keys::KeyFile> {
        let key = self
            .load_file(
                id,
                StorageHint {
                    file_type: ContentIdType::Key,
                    is_metadata: true,
                },
                None,
            )
            .await?;
        let key = serde_json::from_slice(&key)?;
        Ok(key)
    }

    /// Finds a file in the repository using an ID prefix
    pub(crate) async fn find_with_extension(
        &self,
        file_type: ContentIdType,
        prefix: &str,
        extension: Option<&str>,
    ) -> Result<(ID, PathBuf)> {
        if prefix.len() > 2 * mapache::ID_LENGTH {
            // A hex string has 2 characters per byte.
            bail!(
                "Invalid prefix length. The prefix must not be longer than the ID ({} chars)",
                2 * mapache::ID_LENGTH
            );
        } else if prefix.is_empty() {
            // Although it is technically possible to use an empty prefix, which would find a match
            // if only one file of the type exists. let's consider this invalid as it can be
            // potentially ambiguous or lead to errors.
            bail!("Prefix cannot be empty");
        }

        let type_files = self.list_files_with_extension(file_type, extension).await?;
        let mut matches = Vec::new();

        for file_path in type_files {
            let file_stem = match file_path.file_stem() {
                Some(os_str) => os_str.to_string_lossy().into_owned(),
                None => bail!("Failed to list file for type {file_type}"),
            };

            if file_stem.starts_with(prefix) {
                if matches.is_empty() {
                    matches.push((file_stem, file_path));
                } else {
                    bail!("Prefix {prefix} is ambiguous");
                }
            }
        }

        if matches.is_empty() {
            bail!("File type {file_type} with prefix {prefix} doesn't exist");
        }

        let (file_stem, filepath) = matches.pop().unwrap();
        let id = ID::from_hex(&file_stem)?;

        Ok((id, filepath))
    }

    /// Finds a file in the repository using an ID prefix
    pub async fn find(&self, file_type: ContentIdType, prefix: &str) -> Result<(ID, PathBuf)> {
        self.find_with_extension(file_type, prefix, None).await
    }

    pub fn init_pack_saver(self: &Arc<Self>, num_packers: usize) -> Result<()> {
        let (tx, rx) = crossbeam_channel::bounded(2 * num_packers);

        let weak_self = Arc::downgrade(self);
        let storage = self.secure_storage.clone();
        let max_packer_size = self.max_packer_size;

        // Capture the tokio runtime
        let rt_handle = tokio::runtime::Handle::current();

        let handle = std::thread::spawn(move || {
            let pack_saver = PackSaver::new(
                rx,
                rt_handle,
                weak_self,
                storage,
                max_packer_size,
                num_packers,
            )?;
            pack_saver.run()
        });

        *self.pack_saver_tx.write() = Some(tx);
        *self.pack_saver_handle.write() = Some(handle);

        Ok(())
    }

    pub async fn flush_and_finalize_pack_saver(&self) -> Result<RepoStatsSnapshot> {
        // Signal PackSaver to stop by dropping the channel
        let tx = self.pack_saver_tx.write().take();
        drop(tx);

        let handle = self.pack_saver_handle.write().take();
        if let Some(handle) = handle {
            tokio::task::spawn_blocking(move || {
                handle
                    .join()
                    .map_err(|_| anyhow::anyhow!("Pack saver thread panicked"))?
            })
            .await??;
        }

        let index_size = self.index().persist(self).await?;

        self.stats
            .index_raw_bytes
            .fetch_add(index_size.raw, Ordering::Relaxed);
        self.stats
            .index_meta_bytes
            .fetch_add(index_size.encoded, Ordering::Relaxed);

        Ok(self.stats.snapshot())
    }

    pub fn index(&self) -> Arc<MasterIndex> {
        self.master_index.clone()
    }

    pub fn objects_path(&self) -> &Path {
        &self.objects_path
    }

    pub fn snapshot_path(&self) -> &Path {
        &self.snapshot_path
    }

    pub fn index_path(&self) -> &Path {
        &self.index_path
    }

    pub fn keys_path(&self) -> &Path {
        &self.keys_path
    }

    pub fn locks_path(&self) -> &Path {
        &self.locks_path
    }

    pub fn get_encoding_context(&self) -> Result<EncodingContext> {
        self.secure_storage.get_encoding_context()
    }

    /// Reads from a pack file with offset and length.
    /// This function decodes the data.
    pub async fn read_from_pack_and_decode(
        &self,
        blob_type: BlobType,
        id: &ID,
        offset: u64,
        length: u64,
    ) -> Result<Vec<u8>> {
        let path = self.get_path(ContentIdType::Pack, id);
        let data = self
            .backend
            .read(
                &Handle::new_with_hint(&path, ContentIdType::Pack, blob_type == BlobType::Tree),
                offset as isize,
                length as usize,
            )
            .await?;
        self.secure_storage.decode_owned(data)
    }

    /// Lists all packs in the repository.
    pub async fn list_packs(&self) -> Result<IdSet<ID>> {
        let num_folders: usize = 1 << (4 * OBJECTS_DIR_FANOUT);

        let results = stream::iter(0..num_folders)
            .map(|n| {
                let repo = self;
                async move {
                    let mut list = Vec::new();
                    let dir = repo
                        .objects_path
                        .join(format!("{n:0>OBJECTS_DIR_FANOUT$x}"));

                    let entries = repo.backend.list_dir(&dir).await?;
                    for node in entries {
                        let path = node.into_path();
                        let filename = path.file_name().unwrap().to_string_lossy().to_string();
                        if let Ok(id) = ID::from_hex(&filename) {
                            list.push(id);
                        }
                    }
                    Ok::<Vec<ID>, anyhow::Error>(list)
                }
            })
            .buffer_unordered(8) // Process 8 directories in parallel
            .collect::<Vec<_>>()
            .await;

        let mut final_list = IdSet::default();
        for res in results {
            for id in res? {
                final_list.insert(id);
            }
        }

        Ok(final_list)
    }

    /// Returns the path to an object with a given hash in the repository.
    fn get_object_path(objects_path: &Path, id: &ID) -> PathBuf {
        let id_hex = id.to_hex();
        objects_path
            .join(&id_hex[..OBJECTS_DIR_FANOUT])
            .join(&id_hex)
    }

    pub fn get_path(&self, file_type: ContentIdType, id: &ID) -> PathBuf {
        let id_hex = id.to_hex();
        match file_type {
            ContentIdType::Pack => Self::get_object_path(&self.objects_path, id),
            ContentIdType::Snapshot => self.snapshot_path.join(id_hex),
            ContentIdType::Index => self.index_path.join(id_hex),
            ContentIdType::Key => self.keys_path.join(id_hex),
            ContentIdType::Lock => self.locks_path.join(id_hex),
        }
    }

    /// Lists all paths belonging to a file type (objects, snapshots, indices, etc.)
    /// with all extensions included.
    pub async fn list_all_files(&self, file_type: ContentIdType) -> Result<Vec<PathBuf>> {
        match file_type {
            ContentIdType::Snapshot => Ok(self
                .backend
                .list_dir(&self.snapshot_path)
                .await?
                .into_iter()
                .map(|n| n.into_path())
                .collect()),
            ContentIdType::Key => Ok(self
                .backend
                .list_dir(&self.keys_path)
                .await?
                .into_iter()
                .map(|n| n.into_path())
                .collect()),
            ContentIdType::Index => Ok(self
                .backend
                .list_dir(&self.index_path)
                .await?
                .into_iter()
                .map(|n| n.into_path())
                .collect()),
            ContentIdType::Lock => Ok(self
                .backend
                .list_dir(&self.locks_path)
                .await?
                .into_iter()
                .map(|n| n.into_path())
                .collect()),
            ContentIdType::Pack => {
                use futures::stream::{self, StreamExt};

                let backend = self.backend.clone();
                let objects_path = self.objects_path.clone();

                let results = stream::iter(0..(1 << (4 * OBJECTS_DIR_FANOUT)))
                    .map(|n| {
                        let backend = backend.clone();
                        let dir = objects_path.join(format!("{n:0>OBJECTS_DIR_FANOUT$x}"));

                        async move {
                            let entries = backend.list_dir(&dir).await?;
                            Ok::<Vec<PathBuf>, anyhow::Error>(
                                entries.into_iter().map(|n| n.into_path()).collect(),
                            )
                        }
                    })
                    .buffer_unordered(4) // Use conservative concurrency
                    .collect::<Vec<_>>()
                    .await;

                let mut files = Vec::new();
                for res in results {
                    for path in res? {
                        files.push(path);
                    }
                }

                Ok(files)
            }
        }
    }

    /// Lists all paths belonging to a file type (objects, snapshots, indices, etc.)
    /// with extension.
    pub async fn list_files_with_extension(
        &self,
        file_type: ContentIdType,
        extension: Option<&str>,
    ) -> Result<Vec<PathBuf>> {
        let paths = self
            .list_all_files(file_type)
            .await?
            .into_iter()
            .filter(|p| {
                let file_ext = p.extension();

                match extension {
                    Some(ext) => file_ext.map(|os_str| os_str == ext).unwrap_or(false),
                    None => file_ext.is_none(),
                }
            })
            .collect();
        Ok(paths)
    }

    /// Lists all paths belonging to a file type (objects, snapshots, indices, etc.)
    /// with NO EXTENSION.
    #[inline]
    pub async fn list_files(&self, file_type: ContentIdType) -> Result<Vec<PathBuf>> {
        self.list_files_with_extension(file_type, None).await
    }

    /// Load the master index from file
    pub async fn reload_master_index(&self) -> Result<()> {
        let mut files = self.list_files(ContentIdType::Index).await?;
        files.sort_unstable(); // Ensure deterministic order

        self.master_index.clear();

        let indices = stream::iter(files)
            .map(|file_path| {
                let backend = self.backend.clone();
                let secure_storage = self.secure_storage.clone();

                async move {
                    let file_name = file_path
                        .file_name()
                        .expect("Could not read index file name")
                        .to_string_lossy()
                        .to_string();

                    let id = match ID::from_hex(&file_name) {
                        Ok(id) => id,
                        Err(_) => return Ok(None), // Ignore invalid ID names
                    };

                    let index_data = backend
                        .read(
                            &Handle::new_with_hint(&file_path, ContentIdType::Index, true),
                            0,
                            0,
                        )
                        .await?;

                    let index = tokio::task::spawn_blocking(move || {
                        let index_file = secure_storage.decode(&index_data)?;
                        let index_file: IndexFile = serde_json::from_slice(&index_file)
                            .with_context(|| {
                                format!("Failed to load index file {}", id.to_short_hex(4))
                            })?;
                        Ok::<_, anyhow::Error>(Index::from_index_file(index_file, id))
                    })
                    .await??;

                    Ok(Some(index))
                }
            })
            .buffered(16)
            .collect::<Vec<Result<Option<Index>>>>()
            .await;

        let mut num_index_files = 0;
        for res in indices {
            if let Some(index) = res? {
                self.master_index.add_index(index);
                num_index_files += 1;
            }
        }

        ui::cli::verbose_1!("Loaded {} index files", num_index_files);

        Ok(())
    }

    /// Load and decode data from a pack file
    pub async fn load_from_pack(
        &self,
        id: &ID,
        blob_type: BlobType,
        offset: u32,
        length: u32,
    ) -> Result<Vec<u8>> {
        let object_path = Self::get_object_path(&self.objects_path, id);
        let data = self
            .backend
            .read(
                &Handle::new_with_hint(
                    &object_path,
                    ContentIdType::Pack,
                    blob_type == BlobType::Tree,
                ),
                offset as isize,
                length as usize,
            )
            .await?;
        self.secure_storage.decode_owned(data)
    }

    /// Try to acquire a lock with a retry deadline
    async fn try_acquire_lock_with_retry(
        &self,
        exclusive: bool,
        retry_duration: Option<Duration>,
    ) -> Result<Arc<Mutex<Lock>>> {
        let start_time = Instant::now();

        const MIN_BASE_WAIT_INTERVAL_MS: i64 = 5 * 1000;
        const MAX_BASE_WAIT_INTERVAL_MS: i64 = 60 * 1000;
        const MAX_JITTER_MS: i64 = 1000;

        let mut base_wait_interval_ms = MIN_BASE_WAIT_INTERVAL_MS;

        loop {
            match self.try_acquire_lock_once(exclusive).await {
                Ok(lock) => return Ok(lock),

                Err(e) => {
                    let timeout = match retry_duration {
                        Some(t) => t,
                        None => return Err(e),
                    };

                    if start_time.elapsed() >= timeout.to_std().unwrap_or_default() {
                        bail!("Timeout acquiring repository lock");
                    }

                    let mut rng = rng();
                    let jitter_millis = rng.random_range(0..MAX_JITTER_MS);
                    let mean_wait_interval =
                        Duration::milliseconds(base_wait_interval_ms - (MAX_JITTER_MS / 2));
                    let wait_time = mean_wait_interval + Duration::milliseconds(jitter_millis);
                    base_wait_interval_ms =
                        std::cmp::min(MAX_BASE_WAIT_INTERVAL_MS, 2 * base_wait_interval_ms);

                    ui::cli::warning!(
                        "The repository is locked by another process. Waiting {:.0?} seconds before retrying...",
                        wait_time.as_seconds_f32()
                    );

                    tokio::time::sleep(wait_time.to_std()?).await;
                }
            }
        }
    }

    /// Try to acquire a lock just once without retrying
    async fn try_acquire_lock_once(&self, exclusive: bool) -> Result<Arc<Mutex<Lock>>> {
        self.backend.create_dir(&PathBuf::from(LOCKS_DIR)).await?;
        let new_lock = Arc::new(Mutex::new(Lock::new(exclusive)));

        let new_lock_id = *new_lock.lock().id();

        self.save_lock(&new_lock)
            .await
            .context("Failed to write new lock file")?;

        let all_locks = match self.get_locks().await {
            Ok(locks) => locks,
            Err(e) => {
                let _ = self
                    .delete_file(ContentIdType::Lock, &new_lock_id, None)
                    .await;
                return Err(e);
            }
        };

        for lock in all_locks {
            // Skip the lock we just wrote
            if lock.id() == &new_lock_id {
                continue;
            }

            // Clean up stale locks from other processes.
            // A lock is stale if it's expired OR if the process is dead on the same host.
            if lock.is_stale() {
                let _ = self.delete_file(ContentIdType::Lock, lock.id(), None).await;
                continue;
            }

            if exclusive || lock.is_exclusive() {
                // A race condition occurred, or a conflict was already present.
                // The NEWLY written lock must be cleaned up and the attempt must fail.
                let _ = self
                    .delete_file(ContentIdType::Lock, &new_lock_id, None)
                    .await;

                let info = format!(
                    "Conflict detected with existing lock.\n\
                     ID:      {}\n\
                     Host:    {}\n\
                     User:    {}\n\
                     PID:     {}\n\
                     Started: {}\n\
                     Context: {}",
                    lock.id().to_short_hex(4),
                    lock.hostname(),
                    lock.username(),
                    lock.pid(),
                    lock.creation_time()
                        .map(|t| utils::pretty_print_timestamp(&t, None))
                        .unwrap_or_else(|| "unknown".to_string()),
                    lock.context().join(" ")
                );

                bail!(info);
            }
        }

        Ok(new_lock)
    }

    async fn save_lock(&self, lock: &Arc<Mutex<Lock>>) -> Result<()> {
        let (lock_id, lock_bytes) = {
            let lock_guard = lock.lock();
            let id = *lock_guard.id();
            let json = serde_json::to_string(&*lock_guard)?;
            (id, json.into_bytes())
        };

        self.save_file(
            &SaveID::WithID(lock_id),
            &lock_bytes,
            StorageHint {
                file_type: ContentIdType::Lock,
                is_metadata: true,
            },
            None,
        )
        .await?;

        Ok(())
    }

    pub async fn refresh_lock(&self, lock: &Arc<Mutex<Lock>>) -> Result<()> {
        lock.lock().refresh();
        self.save_lock(lock).await
    }

    /// Get all locks in the repository. If a lock file cannot be read, decoded
    /// or deserialized, it will be ignored.
    pub async fn get_locks(&self) -> Result<Vec<Lock>> {
        let all_lock_paths = self.list_files(ContentIdType::Lock).await?;
        let mut locks = Vec::new();

        for path in all_lock_paths {
            // Attempt to read the lock file
            let lock_data_result = self
                .backend
                .read(
                    &Handle::new_with_hint(&path, ContentIdType::Lock, true),
                    0,
                    0,
                )
                .await;

            let lock = match lock_data_result {
                Ok(data) => data,
                Err(_) => continue,
            };

            // Attempt to decode the lock data
            let decoded_lock_result = self.secure_storage.decode(&lock);
            let decoded_lock = match decoded_lock_result {
                Ok(data) => data,
                Err(_) => continue,
            };

            // Attempt to deserialize the lock
            let lock_obj_result: Result<Lock, serde_json::Error> =
                serde_json::from_slice(&decoded_lock);
            match lock_obj_result {
                Ok(lock_obj) => locks.push(lock_obj),
                Err(_) => continue,
            }
        }

        Ok(locks)
    }

    pub fn pack_size(&self) -> u64 {
        self.max_packer_size
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::Local;
    use rstest::rstest;
    use tempfile::tempdir;

    use crate::{
        backend::localfs::LocalFS,
        commands::Compression,
        mapache::{defaults::TEST_REPO_CONFIG, global::set_global_opts_with_args},
        repository::lock::LOCK_EXPIRE_TIMEOUT,
        utils,
    };

    use super::*;

    /// Test init a repo with password and open it
    #[tokio::test]
    async fn test_init_and_open_with_password() -> Result<()> {
        let temp_repo_dir = tempdir()?;
        let temp_repo_path = temp_repo_dir.path().join("repo");

        let auth = Some(Auth {
            username: String::from("mapachito"),
            password: Zeroizing::new(String::from("password")),
        });
        let backend = Arc::new(LocalFS::new(temp_repo_path.to_owned()));

        Repository::init(auth.as_ref().unwrap(), None, backend.to_owned()).await?;
        let (_, _, lock_handle) = Repository::try_open_with_lock(
            auth.as_ref().unwrap(),
            None,
            backend,
            TEST_REPO_CONFIG,
            false,
            None,
        )
        .await?;
        lock_handle.unlock().await;

        Ok(())
    }

    /// Test init a repo with password and open it using a password stored in a file
    #[tokio::test]
    async fn test_init_and_open_with_password_from_file() -> Result<()> {
        let temp_dir = tempdir()?;
        let temp_path = temp_dir.path();
        let temp_repo_path = temp_path.join("repo");
        let password_file_path = temp_path.join("repo_password");

        // Write password to file
        std::fs::write(&password_file_path, "mapachito")?;

        let auth = Some(Auth {
            username: String::from("mapachito"),
            password: Zeroizing::new(String::from("password")),
        });
        let backend = Arc::new(LocalFS::new(temp_repo_path.to_owned()));

        Repository::init(auth.as_ref().unwrap(), None, backend.to_owned()).await?;
        let (_, _, lock_handle) = Repository::try_open_with_lock(
            auth.as_ref().unwrap(),
            None,
            backend,
            TEST_REPO_CONFIG,
            false,
            None,
        )
        .await?;
        lock_handle.unlock().await;

        Ok(())
    }

    #[test]
    fn test_generate_key_file() -> Result<()> {
        let auth = Auth {
            username: "mapachito".to_string(),
            password: Zeroizing::new("password".to_string()),
        };
        let master_key = KeyManager::generate_new_master_key();
        let keyfile = KeyManager::generate_key_file(&auth, &master_key.clone())?;

        let salt = utils::base64::decode(&keyfile.salt)?;
        let encrypted_key = utils::base64::decode(&keyfile.encrypted_key)?;

        let intermediate_key =
            SecureStorage::derive_key::<32>("password", &salt, keyfile.argon2_params())?;
        let ss = SecureStorage::new()
            .with_compression(Compression::Fast.to_level())
            .with_key(&*intermediate_key);

        let decrypted_key = ss.decrypt(&encrypted_key)?.to_vec();

        assert_eq!(*master_key, decrypted_key.as_slice());

        Ok(())
    }

    #[tokio::test]
    #[rstest]
    #[case(false, false)]
    #[case(false, true)]
    #[case(true, false)]
    #[case(true, true)]
    async fn test_acquire_lock_with_non_exclusive_lock(
        #[case] own_lock_exclusive: bool,
        #[case] other_lock_exclusive: bool,
    ) -> Result<()> {
        use crate::{
            commands::{self, Compression, GlobalArgs, cmd_init::CmdArgs},
            mapache::defaults::{DEFAULT_DEFAULT_PACK_SIZE_MIB, TEST_REPO_CONFIG},
        };

        let tmp_dir = tempdir()?;
        let tmp_path = tmp_dir.path();
        let auth = Auth {
            username: "mapachito".to_string(),
            password: Zeroizing::new("password".to_string()),
        };
        let auth_file_path = tmp_path.join("auth");
        std::fs::write(
            &auth_file_path,
            format!("{}\n{}", auth.username, *auth.password),
        )?;

        let repo = String::from("repo");
        let repo_path = tmp_path.join(&repo);

        let global = GlobalArgs {
            repo: repo_path.to_string_lossy().to_string(),
            auth_file: Some(auth_file_path),
            key: None,
            quiet: true,
            json: false,
            verbosity: Some(3),
            ssh_privatekey: None,
            pack_size_mib: DEFAULT_DEFAULT_PACK_SIZE_MIB,
            no_cache: true,
            retry_lock_duration: None,
            compression_level: Compression::Fastest,
            limit_upload: None,
            limit_download: None,
        };
        let args = CmdArgs {};
        set_global_opts_with_args(&global);

        // Init repo
        commands::cmd_init::run(&global, &args)
            .await
            .context("Failed to run cmd_init")?;

        let backend = Arc::new(LocalFS::new(repo_path));

        let (r0, _ss0) =
            Repository::try_open_unlocked(&auth, None, backend.clone(), TEST_REPO_CONFIG).await?;

        let other_lock = Arc::new(Mutex::new(Lock::new(other_lock_exclusive)));
        r0.save_lock(&other_lock).await?;

        let own_repo_open_result = Repository::try_open_with_lock(
            &auth,
            None,
            backend.clone(),
            TEST_REPO_CONFIG,
            own_lock_exclusive,
            None,
        )
        .await;

        let is_err = own_repo_open_result.is_err();
        if let Ok((_, _, lock)) = own_repo_open_result {
            lock.unlock().await;
        }

        match (is_err, own_lock_exclusive, other_lock_exclusive) {
            (true, true, true)
            | (true, true, false)
            | (true, false, true)
            | (false, false, false) => Ok(()),
            (true, false, false) => bail!("Should not fail to acquire lock"),
            (false, true, true) | (false, true, false) | (false, false, true) => {
                bail!("Should fail to acquire lock")
            }
        }
    }

    #[tokio::test]
    async fn test_acquire_lock_deletes_other_expired_lock() -> Result<()> {
        use crate::{
            commands::{self, GlobalArgs, cmd_init::CmdArgs},
            mapache::defaults::DEFAULT_DEFAULT_PACK_SIZE_MIB,
        };

        let tmp_dir = tempdir()?;
        let tmp_path = tmp_dir.path();
        let auth = Auth {
            username: "mapachito".to_string(),
            password: Zeroizing::new("password".to_string()),
        };
        let auth_file_path = tmp_path.join("auth");
        std::fs::write(
            &auth_file_path,
            format!("{}\n{}", auth.username, *auth.password),
        )?;

        let repo = String::from("repo");
        let repo_path = tmp_path.join(&repo);

        let global = GlobalArgs {
            repo: repo_path.to_string_lossy().to_string(),
            auth_file: Some(auth_file_path),
            key: None,
            quiet: true,
            json: false,
            verbosity: Some(3),
            ssh_privatekey: None,
            pack_size_mib: DEFAULT_DEFAULT_PACK_SIZE_MIB,
            no_cache: true,
            retry_lock_duration: None,
            compression_level: Compression::Fastest,
            limit_upload: None,
            limit_download: None,
        };
        let args = CmdArgs {};
        set_global_opts_with_args(&global);

        // Init repo
        commands::cmd_init::run(&global, &args)
            .await
            .context("Failed to run cmd_init")?;

        let backend = Arc::new(LocalFS::new(repo_path));

        let (r0, _ss0) =
            Repository::try_open_unlocked(&auth, None, backend.clone(), TEST_REPO_CONFIG).await?;

        let other_lock = Arc::new(Mutex::new(Lock::new_for_test(
            true,
            Local::now() - LOCK_EXPIRE_TIMEOUT,
        )));
        r0.save_lock(&other_lock).await?;

        let (_, _, lock_handle) = Repository::try_open_with_lock(
            &auth,
            None,
            backend.clone(),
            TEST_REPO_CONFIG,
            true,
            None,
        )
        .await?; // The other expired lock should have been deleted

        lock_handle.unlock().await;

        Ok(())
    }
}
