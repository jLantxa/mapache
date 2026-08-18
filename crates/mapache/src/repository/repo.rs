use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use serde::de::DeserializeOwned;

use async_trait::async_trait;
use chrono::Duration;
use futures::{StreamExt, stream};
use zeroize::Zeroizing;

use crate::{
    backend::{Handle, StorageBackend, StorageHint, WriteContents, cache::CacheBackend},
    commands::Compression,
    common::{
        self, BlobType, ContentIdType, ID, SaveID,
        error::{MapacheError, Result},
        traits::{BlobLoader, BlobSaver},
    },
    repository::{
        index::{self, Index, IndexFile, IndexMode, MasterIndex},
        keys::{self, KeyManager},
        lock::{Lock, LockHandle},
        manifest::{EccConfig, Manifest},
        packer::{PackSaver, PackSaverRequest},
        snapshot::Snapshot,
        storage::{EncodingContext, SecureStorage},
    },
    ui::{self},
    utils::collections::IdSet,
};

pub const THIS_REPOSITORY_VERSION: u32 = 2;

pub const OBJECTS_DIR: &str = "objects";
pub const SNAPSHOTS_DIR: &str = "snapshots";
pub const INDEX_DIR: &str = "index";
pub const MANIFEST_PATH: &str = "manifest";
pub const KEYS_DIR: &str = "keys";
pub const LOCKS_DIR: &str = "locks";

pub(crate) const REPO_TMP_EXTENSION: &str = "tmp";
pub(crate) const REPO_DROPPED_EXTENSION: &str = "dropped";
pub(crate) const REPO_ECC_EXTENSION: &str = "ecc";

pub(crate) type OpenResult = (Arc<Repository>, Arc<SecureStorage>);
pub(crate) type OpenWithLockResult = (Arc<Repository>, Arc<SecureStorage>, LockHandle);

const OBJECTS_DIR_FANOUT: usize = 2;

pub fn warn_v1_deprecated() {
    ui::cli::warning!(
        "Repository format v1 is deprecated and will be unsupported in a future release.\n\
        Consider migrating to v2: `mapache migrate --repo <repo-path>`\n"
    );
}

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
    /// Index loading mode: eager (all in RAM) or lazy (hot + cold).
    pub index_mode: IndexMode,
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

    pub ecc_bytes: AtomicU64,
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
    pub ecc: u64,
}

impl RepoStats {
    pub fn reset(&self) {
        self.raw_bytes.store(0, Ordering::Relaxed);
        self.encoded_bytes.store(0, Ordering::Relaxed);
        self.data_blobs.store(0, Ordering::Relaxed);
        self.meta_raw_bytes.store(0, Ordering::Relaxed);
        self.meta_encoded_bytes.store(0, Ordering::Relaxed);
        self.meta_blobs.store(0, Ordering::Relaxed);
        self.index_raw_bytes.store(0, Ordering::Relaxed);
        self.index_meta_bytes.store(0, Ordering::Relaxed);
        self.ecc_bytes.store(0, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> RepoStatsSnapshot {
        let rb = self.raw_bytes.load(Ordering::Relaxed);
        let eb = self.encoded_bytes.load(Ordering::Relaxed);
        let mrb = self.meta_raw_bytes.load(Ordering::Relaxed);
        let meb = self.meta_encoded_bytes.load(Ordering::Relaxed);
        let blobs = self.data_blobs.load(Ordering::Relaxed);
        let meta_blobs = self.meta_blobs.load(Ordering::Relaxed);
        let index_raw = self.index_raw_bytes.load(Ordering::Relaxed);
        let index_meta = self.index_meta_bytes.load(Ordering::Relaxed);
        let ecc = self.ecc_bytes.load(Ordering::Relaxed);

        RepoStatsSnapshot {
            data: SizePair::new(rb, eb),
            meta: SizePair::new(mrb, meb),
            total: SizePair::new(rb + mrb, eb + meb),
            blobs,
            meta_blobs,
            index: SizePair::new(index_raw, index_meta),
            ecc,
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
    repo_version: u32,

    // Storage
    backend: Arc<dyn StorageBackend>,
    secure_storage: Arc<SecureStorage>,

    /// Compression preset for new blobs (None = store blobs uncompressed).
    compression: Compression,

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

#[async_trait]
impl BlobLoader for Repository {
    async fn load_blob(&self, id: &ID) -> Result<Vec<u8>> {
        self.load_blob(id).await
    }
}

impl Repository {
    /// Create and initialize a new repository
    pub async fn init(
        repo_version: u32,
        auth: &Auth,
        keyfile_path: Option<&PathBuf>,
        backend: Arc<dyn StorageBackend>,
        ecc_config: Option<EccConfig>,
    ) -> Result<Manifest> {
        tracing::info!(target: "repo", "Checking for existing repository");
        if backend.path_exists(Path::new(MANIFEST_PATH)).await {
            tracing::error!(target: "repo", "Repository already exists");
            return Err(MapacheError::RepoAlreadyExists);
        }

        backend
            .create()
            .await
            .inspect_err(
                |e| tracing::error!(target: "repo", "Failed to create root directory: {e}"),
            )
            .map_err(|e| MapacheError::Backend(format!("could not create root directory: {e}")))?;
        tracing::debug!(target: "repo", "Root directory created");

        let keys_path = PathBuf::from(KEYS_DIR);
        backend.create_dir(&keys_path).await?;
        tracing::debug!(target: "repo", "Keys directory created");

        // Create new key
        tracing::info!(target: "repo", "Generating master key and keyfile");
        let master_key = KeyManager::generate_new_master_key();
        let keyfile = KeyManager::generate_key_file(auth, &master_key, repo_version)
            .inspect_err(|e| tracing::error!(target: "repo", "Key generation failed: {e}"))
            .map_err(|e| MapacheError::Crypto(format!("could not generate key: {e}")))?;
        tracing::info!(
            target: "repo",
            "Keyfile generated (Argon2 m={}, t={}, p={})",
            keyfile.m,
            keyfile.t,
            keyfile.p
        );
        let secure_storage = Arc::new(
            SecureStorage::new()
                .with_compression(Compression::Fast.to_level())
                .with_key(&master_key)?,
        );

        let keyfile_json = serde_json::to_string_pretty(&keyfile)?;
        let keyfile_json = secure_storage.compress(keyfile_json.as_bytes())?;
        let keyfile_id = ID::from_content(&keyfile_json);
        match keyfile_path {
            Some(p) => {
                std::fs::write(p, &keyfile_json)
                    .inspect_err(|e| tracing::error!(target: "repo", "Failed to write keyfile to '{}': {e}", p.display()))
                    ?;
                tracing::info!(target: "repo", "Keyfile written to {}", p.display());
            }
            None => {
                let p = keys_path.join(keyfile_id.to_hex());
                let handle = Handle::new_with_hint(&p, ContentIdType::Key, true);
                backend
                    .write(&handle, WriteContents::Owned(keyfile_json))
                    .await
                    .inspect_err(
                        |e| tracing::error!(target: "repo", "Failed to write keyfile to backend: {e}"),
                    )?;
                tracing::info!(target: "repo", "Keyfile written to backend: {}", p.display());
            }
        }

        // Init repository structure
        let objects_path = PathBuf::from(OBJECTS_DIR);
        let snapshot_path = PathBuf::from(SNAPSHOTS_DIR);
        let index_path = PathBuf::from(INDEX_DIR);
        let locks_path = PathBuf::from(LOCKS_DIR);

        // Save new manifest
        tracing::info!(target: "repo", "Creating manifest v{repo_version}");
        let manifest = match ecc_config {
            Some(ecc) => Manifest::new_with_ecc(repo_version, ecc),
            None => Manifest::new(repo_version),
        };

        // TODO(v1-removal): Nonce position depends on repo version.
        secure_storage.set_nonce_at_end(repo_version >= 2);
        let manifest_path = Path::new(MANIFEST_PATH);
        let manifest_bytes = serde_json::to_string_pretty(&manifest)?.into_bytes();
        let encoded_manifest = secure_storage.encode(&manifest_bytes)?;
        backend
            .write(
                &Handle::new(manifest_path),
                WriteContents::Owned(encoded_manifest),
            )
            .await
            .inspect_err(|e| tracing::error!(target: "repo", "Failed to write manifest: {e}"))?;

        backend.create_dir(&objects_path).await?;
        let num_folders: usize = 1 << (4 * OBJECTS_DIR_FANOUT);
        for n in 0x00..num_folders {
            backend
                .create_dir(&objects_path.join(format!("{n:0>OBJECTS_DIR_FANOUT$x}")))
                .await?;
        }
        tracing::debug!(target: "repo", "Objects directory with {num_folders} fanout folders created");

        backend.create_dir(&snapshot_path).await?;
        backend.create_dir(&index_path).await?;
        backend.create_dir(&locks_path).await?;
        tracing::info!(target: "repo", "Repository structure created");

        Ok(manifest)
    }

    /// Try to open a repository and acquire a lock.
    pub async fn try_open_with_lock(
        auth: &Auth,
        key_file_path: Option<&PathBuf>,
        backend: Arc<dyn StorageBackend>,
        config: RepoConfig,
        exclusive_lock: bool,
        retry_duration: Option<Duration>,
    ) -> Result<OpenWithLockResult> {
        let dry_run = backend.is_dry_run();
        let (repo, secure_storage) =
            Self::try_open_unlocked(auth, key_file_path, backend, config).await?;
        tracing::info!(target: "repo", "Acquiring lock (exclusive={exclusive_lock})");
        let lock = repo
            .try_acquire_lock_with_retry(exclusive_lock, retry_duration)
            .await?;
        tracing::info!(target: "repo", "Lock acquired");
        let lock_handle = LockHandle::new(repo.clone(), lock, !dry_run);

        Ok((repo, secure_storage, lock_handle))
    }

    /// Try to open a repository without acquiring a lock.
    pub async fn try_open_unlocked(
        auth: &Auth,
        key_file_path: Option<&PathBuf>,
        backend: Arc<dyn StorageBackend>,
        config: RepoConfig,
    ) -> Result<OpenResult> {
        tracing::info!(target: "repo", "Opening repository");
        let key_manager = KeyManager::new(backend.clone());

        let (_key_id, master_key) = key_manager.retrieve_master_key(auth, key_file_path).await?;
        tracing::info!(target: "repo", "Master key retrieved");

        let secure_storage = Arc::new(
            SecureStorage::new()
                .with_compression(config.compression.to_level())
                .with_key(&master_key)?,
        );

        let repo = Repository::open(backend, secure_storage.clone(), config).await?;
        tracing::info!(target: "repo", "Repository opened");

        let version = repo.manifest().version();
        tracing::info!(target: "repo", "Repository version: {version}");
        if version > THIS_REPOSITORY_VERSION {
            return Err(MapacheError::Repo(format!(
                "invalid repository version '{version}'"
            )));
        }

        // TODO(v1-removal): The v1 format has no per-blob compression marker.
        if version < 2 && matches!(config.compression, Compression::None) {
            return Err(MapacheError::Repo(
                "compression 'none' is not supported in repository format v1; \
                 migrate the repository to v2 first"
                    .to_string(),
            ));
        }

        // TODO(v1-removal): Nonce position depends on repo version.
        let nonce_at_end = version >= 2;
        tracing::info!(target: "repo", "Nonce position: {}", if nonce_at_end { "end" } else { "start" });
        secure_storage.set_nonce_at_end(nonce_at_end);

        if version == 1 {
            warn_v1_deprecated();
        }

        Ok((repo, secure_storage))
    }

    /// Open an existing repository from a directory
    async fn open(
        backend: Arc<dyn StorageBackend>,
        secure_storage: Arc<SecureStorage>,
        config: RepoConfig,
    ) -> Result<Arc<Self>> {
        let (manifest, _manifest_nonce_at_end) =
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

        let master_index = Arc::new(MasterIndex::new(config.index_mode));

        let repo_version = manifest.version();

        let repo = Repository {
            manifest,
            repo_version,
            backend,
            objects_path: PathBuf::from(OBJECTS_DIR),
            snapshot_path: PathBuf::from(SNAPSHOTS_DIR),
            index_path: PathBuf::from(INDEX_DIR),
            keys_path: PathBuf::from(KEYS_DIR),
            locks_path: PathBuf::from(LOCKS_DIR),
            secure_storage,
            max_packer_size: config.pack_size,
            compression: config.compression,
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

    /// Saves the manifest to the backend.
    pub async fn save_manifest(&self, manifest: &Manifest) -> Result<()> {
        let manifest_bytes = serde_json::to_string_pretty(manifest)?.into_bytes();
        let encoded = self.secure_storage.encode(&manifest_bytes)?;
        self.backend
            .write(
                &Handle::new(Path::new(MANIFEST_PATH)),
                WriteContents::Owned(encoded),
            )
            .await
            .map_err(|e| MapacheError::Repo(format!("failed to save manifest: {e}")))
    }

    /// Returns the repository format version.
    pub fn repo_version(&self) -> u32 {
        self.repo_version
    }

    // ── v1/v2 format helpers ──────────────────────────────────────────────
    // Use these instead of `repo_version >= 2` checks so that v1 removal is
    // a single-point edit.  Every site guarded by these helpers is tagged
    // `TODO(v1-removal)`.

    /// v2: nonce at end `[ct | tag | nonce]`.  v1: nonce at start.
    pub fn nonce_at_end(&self) -> bool {
        self.repo_version >= 2
    }

    /// v2: compact binary index.  v1: JSON index.
    pub fn uses_binary_index(&self) -> bool {
        self.repo_version >= 2
    }

    /// v2: high bit of type byte is a compression marker.
    /// v1: always zstd-compressed, bit is not meaningful.
    pub fn has_compression_marker(&self) -> bool {
        self.repo_version >= 2
    }

    /// v2: `--compression none` is supported.
    pub fn supports_compression_none(&self) -> bool {
        self.repo_version >= 2
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
        if self.master_index.contains(&id) {
            return Ok(id);
        }

        // Zero blobs: send to packer with empty encoded data (no bytes in pack data section).
        // They appear in the pack footer as BlobType::Zero with length=0, raw_length=N.
        if blob_type == BlobType::Zero {
            let raw_length = u32::try_from(data.len()).map_err(|e| {
                MapacheError::Integrity(format!("zero blob raw size exceeds u32::MAX: {e}"))
            })? as u64;

            let tx = {
                let tx_guard = self.pack_saver_tx.read();
                tx_guard
                    .as_ref()
                    .ok_or_else(|| {
                        MapacheError::Repo("packer is stopped or not initialized".to_string())
                    })?
                    .clone()
            };

            tx.send(PackSaverRequest::SaveBlob {
                id,
                blob_type,
                data: Vec::new(),
                raw_length,
                compressed: false,
            })
            .map_err(|_| MapacheError::Repo("packer channel closed".to_string()))?;

            self.master_index.add_pending_blob(id);

            return Ok(id);
        }

        let raw_length = data.len() as u64;
        let compressed = !matches!(self.compression, Compression::None);
        let encoded_data = if compressed {
            self.secure_storage.encode(&data)?
        } else {
            self.secure_storage.encrypt(&data)?
        };

        let tx = {
            let tx_guard = self.pack_saver_tx.read();
            tx_guard
                .as_ref()
                .ok_or_else(|| {
                    MapacheError::Repo("packer is stopped or not initialized".to_string())
                })?
                .clone()
        };

        tx.send(PackSaverRequest::SaveBlob {
            id,
            blob_type,
            data: encoded_data,
            raw_length,
            compressed,
        })
        .map_err(|_| MapacheError::Repo("packer channel closed".to_string()))?;

        // Only mark as pending after the packer confirmed receipt,
        // so a send failure doesn't orphan the blob.
        self.master_index.add_pending_blob(id);

        Ok(id)
    }

    /// Loads a blob from the repository.
    pub async fn load_blob(&self, id: &ID) -> Result<Vec<u8>> {
        match self.master_index.get_with_cold(id) {
            index::LookupResult::Found(locator) => {
                if locator.blob_type == BlobType::Zero {
                    return Ok(vec![0u8; locator.raw_length as usize]);
                }
                self.load_from_pack(
                    &locator.pack_id,
                    locator.blob_type,
                    locator.offset,
                    locator.length,
                    locator.compressed,
                )
                .await
            }
            index::LookupResult::ColdHit(cold_idx) => {
                tracing::debug!(target: "repo", "Blob {} in cold index, loading on demand", id.to_short_hex(8));
                // Load the cold index from disk
                if let Some(meta) = self.master_index.load_cold_index(cold_idx) {
                    let loaded_index = self.load_index_from_file(meta.file_id).await?;
                    self.master_index.promote_to_hot(loaded_index);
                    // Retry lookup in hot
                    match self.master_index.get(id) {
                        Some(locator) => {
                            if locator.blob_type == BlobType::Zero {
                                return Ok(vec![0u8; locator.raw_length as usize]);
                            }
                            self.load_from_pack(
                                &locator.pack_id,
                                locator.blob_type,
                                locator.offset,
                                locator.length,
                                locator.compressed,
                            )
                            .await
                        }
                        None => Err(MapacheError::NotInIndex(*id))?,
                    }
                } else {
                    Err(MapacheError::NotInIndex(*id))?
                }
            }
            index::LookupResult::NotFound => Err(MapacheError::NotInIndex(*id))?,
        }
    }

    /// Load a single index file from disk by its ID.
    async fn load_index_from_file(&self, file_id: ID) -> Result<Index> {
        let object_path = Self::get_object_path(&self.index_path, &file_id);
        let index_data = self
            .backend
            .read(
                &Handle::new_with_hint(&object_path, ContentIdType::Index, true),
                0,
                0,
            )
            .await?;

        let secure_storage = self.secure_storage.clone();
        let repo_version = self.repo_version; // TODO(v1-removal): remove after v1 support is dropped

        tokio::task::spawn_blocking(move || {
            let decoded = secure_storage.decode(&index_data)?;
            let index_file =
                index::IndexFile::deserialize(&decoded, repo_version).map_err(|e| {
                    MapacheError::Format(format!(
                        "failed to load index file {}: {e}",
                        file_id.to_short_hex(4)
                    ))
                })?;
            Ok(Index::from_index_file(index_file, file_id))
        })
        .await
        .map_err(|e| MapacheError::task_panicked("index loading", e))?
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

        // Compute ECC sidecar before moving encoded_data.
        let ecc_payload = if file_type == ContentIdType::Pack {
            if let Some(ecc_config) = self.manifest.ecc() {
                let k = ecc_config.data_shards as usize;
                let p = ecc_config.parity_shards as usize;
                let pack_data = bytes_to_write.to_vec();
                let raw_ecc =
                    tokio::task::spawn_blocking(move || mapache_ecc::ecc_encode(&pack_data, k, p))
                        .await
                        .map_err(|e| MapacheError::Internal(format!("ECC task failed: {e}")))?;
                if raw_ecc.is_empty() {
                    None
                } else {
                    let encoded_ecc = self.secure_storage.encode(&raw_ecc)?;
                    Some((raw_ecc.len() as u64, encoded_ecc))
                }
            } else {
                None
            }
        } else {
            None
        };

        let cow_data = match encoded_data {
            Some(d) => WriteContents::Owned(d),
            None => WriteContents::Borrowed(data),
        };

        if matches!(
            file_type,
            ContentIdType::Snapshot
                | ContentIdType::Index
                | ContentIdType::Key
                | ContentIdType::Lock
        ) {
            tracing::info!(target: "repo", "Saving {file_type} to {}", path.display());
        }

        self.backend.write(&handle, cow_data).await?;

        // Write ECC sidecar for packs when ECC is enabled.
        if let Some((_raw_len, encoded_ecc)) = ecc_payload {
            let ecc_path = path.with_extension(REPO_ECC_EXTENSION);
            let ecc_handle = Handle {
                path: &ecc_path,
                hint: Some(hint),
            };
            let ecc_encoded_len = encoded_ecc.len() as u64;
            self.backend
                .write(&ecc_handle, WriteContents::Owned(encoded_ecc))
                .await?;
            self.stats
                .ecc_bytes
                .fetch_add(ecc_encoded_len, Ordering::Relaxed);
        }

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

    /// Loads a file and deserializes its content.
    async fn load_deserialized<T: DeserializeOwned>(
        &self,
        id: &ID,
        file_type: ContentIdType,
        extension: Option<&str>,
    ) -> Result<T> {
        let data = self
            .load_file(
                id,
                StorageHint {
                    file_type,
                    is_metadata: true,
                },
                extension,
            )
            .await?;
        serde_json::from_slice(&data)
            .map_err(|e| MapacheError::Format(format!("failed to deserialize {file_type}: {e}")))
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
        tracing::info!(target: "repo", "Deleting {file_type} at {}", path.display());
        let size = self.backend.lstat(&path).await?.size;
        self.backend.remove(&path).await?;

        // Also delete .ecc sidecar if it exists (best-effort).
        if with_extension.is_none() && file_type == ContentIdType::Pack {
            let ecc_path = path.with_extension(REPO_ECC_EXTENSION);
            if self.backend.path_exists(&ecc_path).await
                && let Err(e) = self.backend.remove(&ecc_path).await
            {
                tracing::warn!(target: "repo", "Failed to delete ECC sidecar {}: {e}", ecc_path.display());
            }
        }

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
            return Err(MapacheError::SnapshotNotFound(format!(
                "snapshot {id} doesn't exist"
            )));
        }

        self.backend
            .remove(&snapshot_path)
            .await
            .map_err(|e| MapacheError::Backend(format!("could not remove snapshot {id}: {e}")))
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
            .map_err(|e| {
                MapacheError::SnapshotNotFound(format!("no snapshot with ID '{id}' exists: {e}"))
            })?;
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
        self.list_ids_for(ContentIdType::Snapshot, None)
            .await
            .map_err(|e| MapacheError::Backend(format!("could not list snapshots: {e}")))
    }

    pub(crate) async fn list_index_ids(&self) -> Result<Vec<ID>> {
        self.list_ids_for(ContentIdType::Index, None).await
    }

    /// Lists all .dropped snapshot IDs
    pub(crate) async fn list_dropped_snapshot_ids(&self) -> Result<Vec<ID>> {
        self.list_ids_for(ContentIdType::Snapshot, Some(REPO_DROPPED_EXTENSION))
            .await
            .map_err(|e| {
                MapacheError::Backend(format!(
                    "failed to list files with the dropped snapshot extension: {e}"
                ))
            })
    }

    /// Lists all IDs for a given file type, optionally filtered by extension.
    async fn list_ids_for(
        &self,
        file_type: ContentIdType,
        extension: Option<&str>,
    ) -> Result<Vec<ID>> {
        let paths = self.list_files_with_extension(file_type, extension).await?;
        Ok(Self::ids_from_paths(paths, extension.is_some()))
    }

    fn ids_from_paths(paths: Vec<PathBuf>, use_stem: bool) -> Vec<ID> {
        paths
            .into_iter()
            .filter_map(|path| {
                let name = if use_stem {
                    path.file_stem()
                } else {
                    path.file_name()
                };
                name.and_then(|s| s.to_str())
                    .and_then(|name| ID::from_hex(name).ok())
            })
            .collect()
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
        let data = self
            .load_file(
                id,
                StorageHint {
                    file_type: ContentIdType::Index,
                    is_metadata: true,
                },
                None,
            )
            .await?;

        // TODO(v1-removal): Remove repo_version parameter after v1 support is dropped.
        index::IndexFile::deserialize(&data, self.repo_version)
    }

    /// Loads the repository manifest.
    ///
    /// Tries decoding with nonce-at-end first, then nonce-at-start, to support
    /// both v1 (nonce at start) and v2 (nonce at end) manifests. The returned
    /// `bool` indicates which nonce position was used.
    // TODO(v1-removal): Remove the nonce-at-start fallback.
    async fn load_manifest(
        secure_storage: Arc<SecureStorage>,
        backend: Arc<dyn StorageBackend>,
    ) -> Result<(Manifest, bool)> {
        let raw = backend
            .read(&Handle::new(Path::new(MANIFEST_PATH)), 0, 0)
            .await?;

        // Try nonce-at-end first, then nonce-at-start.
        let primary_err = match secure_storage.decrypt_inner(&raw, true) {
            Ok(decoded) => {
                let decompressed = secure_storage.decompress(&decoded)?;
                let manifest = serde_json::from_slice(&decompressed)?;
                return Ok((manifest, true));
            }
            Err(e) => e,
        };
        let decoded = secure_storage.decrypt_inner(&raw, false).map_err(|e| {
            MapacheError::Crypto(format!(
                "failed to decrypt manifest with both nonce positions \
                 (primary: {primary_err}, fallback: {e})"
            ))
        })?;
        let decompressed = secure_storage.decompress(&decoded)?;
        let manifest = serde_json::from_slice(&decompressed)?;
        Ok((manifest, false))
    }

    /// Load the manifest to determine the repository version.
    ///
    /// This is useful for commands (like `key add`) that need to know the
    /// repository version without opening the full repository.
    pub async fn load_manifest_version(
        master_key: &[u8],
        backend: Arc<dyn StorageBackend>,
    ) -> Result<u32> {
        let secure_storage = Arc::new(
            SecureStorage::new()
                .with_compression(Compression::Fast.to_level())
                .with_key(master_key)?,
        );
        let (manifest, _) = Self::load_manifest(secure_storage, backend).await?;
        Ok(manifest.version())
    }

    /// Loads a lock file.
    pub async fn load_lock(&self, id: &ID) -> Result<Lock> {
        self.load_deserialized(id, ContentIdType::Lock, None).await
    }

    /// Loads a KeyFile.
    pub async fn load_key(&self, id: &ID) -> Result<keys::KeyFile> {
        self.load_deserialized(id, ContentIdType::Key, None).await
    }

    /// Finds a file in the repository using an ID prefix
    pub(crate) async fn find_with_extension(
        &self,
        file_type: ContentIdType,
        prefix: &str,
        extension: Option<&str>,
    ) -> Result<(ID, PathBuf)> {
        if prefix.len() > 2 * common::ID_LENGTH {
            // A hex string has 2 characters per byte.
            return Err(MapacheError::Format(format!(
                "invalid prefix length. The prefix must not be longer than the ID ({} chars)",
                2 * common::ID_LENGTH
            )));
        } else if prefix.is_empty() {
            // Although it is technically possible to use an empty prefix, which would find a match
            // if only one file of the type exists. let's consider this invalid as it can be
            // potentially ambiguous or lead to errors.
            return Err(MapacheError::Format("prefix cannot be empty".to_string()));
        }

        let type_files = self.list_files_with_extension(file_type, extension).await?;
        let mut matches = Vec::new();

        for file_path in type_files {
            let file_stem = match file_path.file_stem() {
                Some(os_str) => os_str.to_string_lossy().into_owned(),
                None => {
                    return Err(MapacheError::Repo(format!(
                        "failed to list file for type {file_type}"
                    )));
                }
            };

            if file_stem.starts_with(prefix) {
                if matches.is_empty() {
                    matches.push((file_stem, file_path));
                } else {
                    return Err(MapacheError::Format(format!(
                        "prefix {prefix} is ambiguous"
                    )));
                }
            }
        }

        if matches.is_empty() {
            return Err(MapacheError::NotFound(format!(
                "file type {file_type} with prefix {prefix} doesn't exist"
            )));
        }

        let (file_stem, filepath) = matches.pop().ok_or_else(|| {
            MapacheError::Integrity(
                "expected to find matching file after successful prefix search".to_string(),
            )
        })?;
        let id = ID::from_hex(&file_stem)?;

        Ok((id, filepath))
    }

    /// Finds a file in the repository using an ID prefix
    pub async fn find(&self, file_type: ContentIdType, prefix: &str) -> Result<(ID, PathBuf)> {
        self.find_with_extension(file_type, prefix, None).await
    }

    pub fn reset_stats(&self) {
        self.stats.reset();
    }

    pub fn init_pack_saver(self: &Arc<Self>, num_packers: usize) -> Result<()> {
        tracing::info!(target: "repo", "Initializing pack saver (workers={num_packers})");
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
        tracing::info!(target: "repo", "Finalizing pack saver");
        // Signal PackSaver to stop by dropping the channel
        let tx = self.pack_saver_tx.write().take();
        drop(tx);

        let handle = self.pack_saver_handle.write().take();
        if let Some(handle) = handle {
            tokio::task::spawn_blocking(move || {
                handle
                    .join()
                    .map_err(|_| MapacheError::Internal("pack saver thread panicked".to_string()))?
            })
            .await
            .map_err(|e| MapacheError::task_panicked("pack saver", e))??;
        }

        tracing::info!(target: "repo", "Persisting index");
        let index_size = self.index().persist(self).await?;

        self.stats
            .index_raw_bytes
            .fetch_add(index_size.raw, Ordering::Relaxed);
        self.stats
            .index_meta_bytes
            .fetch_add(index_size.encoded, Ordering::Relaxed);

        tracing::info!(target: "repo", "Pack saver finalized");
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

    /// Lists all packs in the repository.
    pub async fn list_packs(&self) -> Result<IdSet<ID>> {
        let (packs, _) = self.list_packs_and_trash().await?;
        Ok(packs)
    }

    /// Lists all packs and trash files (.tmp, .dropped, orphaned .ecc) in the
    /// objects directory.
    ///
    /// An `.ecc` file is considered orphaned (and thus trash) if the
    /// corresponding pack file does not exist.
    pub async fn list_packs_and_trash(&self) -> Result<(IdSet<ID>, Vec<PathBuf>)> {
        let mut packs = IdSet::default();
        let mut trash = Vec::new();
        let mut ecc_files: Vec<(ID, PathBuf)> = Vec::new();

        let entries = self.backend.list_dir_recursive(&self.objects_path).await?;
        for node in entries {
            let path = node.into_path();
            let Some(filename) = path.file_name() else {
                continue;
            };
            let filename = filename.to_string_lossy().to_string();
            if let Ok(id) = ID::from_hex(&filename) {
                packs.insert(id);
            } else if let Some(stem) = filename.strip_suffix(".ecc") {
                if let Ok(id) = ID::from_hex(stem) {
                    ecc_files.push((id, path));
                } else {
                    trash.push(path);
                }
            } else if let Some(ext) = path.extension() {
                let ext_str = ext.to_string_lossy();
                if ext_str == REPO_TMP_EXTENSION || ext_str == REPO_DROPPED_EXTENSION {
                    trash.push(path);
                }
            }
        }

        // Collect orphaned .ecc files (base pack not present).
        for (id, path) in ecc_files {
            if !packs.contains(&id) {
                trash.push(path);
            }
        }

        Ok((packs, trash))
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

    /// Returns the directory path for a file type.
    fn dir_for_type(&self, file_type: ContentIdType) -> Option<&Path> {
        match file_type {
            ContentIdType::Snapshot => Some(&self.snapshot_path),
            ContentIdType::Key => Some(&self.keys_path),
            ContentIdType::Index => Some(&self.index_path),
            ContentIdType::Lock => Some(&self.locks_path),
            ContentIdType::Pack => None,
        }
    }

    /// Lists all paths belonging to a file type (objects, snapshots, indices, etc.)
    /// with all extensions included.
    pub async fn list_all_files(&self, file_type: ContentIdType) -> Result<Vec<PathBuf>> {
        if let Some(dir) = self.dir_for_type(file_type) {
            return Ok(self
                .backend
                .list_dir(dir)
                .await?
                .into_iter()
                .map(|n| n.into_path())
                .collect());
        }

        // Pack: fanout with concurrency
        let backend = self.backend.clone();
        let objects_path = self.objects_path.clone();

        let results = stream::iter(0..(1 << (4 * OBJECTS_DIR_FANOUT)))
            .map(|n| {
                let backend = backend.clone();
                let dir = objects_path.join(format!("{n:0>OBJECTS_DIR_FANOUT$x}"));

                async move {
                    let entries = backend.list_dir(&dir).await?;
                    Ok::<Vec<PathBuf>, MapacheError>(
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

    /// Load the master index from file.
    pub async fn reload_master_index(&self) -> Result<()> {
        self.reload_master_index_with_mode(common::defaults::DEFAULT_INDEX_MODE)
            .await
    }

    /// Load the master index from file with the specified mode.
    /// - Eager: loads all indices into RAM (~50 bytes/blob, fastest lookups)
    /// - Lazy: loads only N most recent indices (hot), rest use cold metadata (~2 bytes/blob)
    pub async fn reload_master_index_with_mode(&self, index_mode: IndexMode) -> Result<()> {
        tracing::info!(target: "repo", "Reloading master index (mode: {:?})", index_mode);
        let mut files = self.list_files(ContentIdType::Index).await?;
        files.sort_unstable(); // Ensure deterministic order

        self.master_index.clear();

        let repo_version = self.repo_version; // TODO(v1-removal): remove after v1 support is dropped
        let hot_count = match index_mode {
            IndexMode::Eager => usize::MAX, // Load all
            IndexMode::Lazy => common::defaults::INDEX_HOT_COUNT,
        };

        let indices = stream::iter(files)
            .map(|file_path| {
                let backend = self.backend.clone();
                let secure_storage = self.secure_storage.clone();

                async move {
                    let Some(file_name) = file_path.file_name() else {
                        return Ok(None);
                    };
                    let file_name = file_name.to_string_lossy().to_string();

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
                        let decoded = secure_storage.decode_owned(index_data)?;
                        let index_file = index::IndexFile::deserialize(&decoded, repo_version)
                            .map_err(|e| {
                                MapacheError::Format(format!(
                                    "failed to load index file {}: {e}",
                                    id.to_short_hex(4)
                                ))
                            })?;
                        Ok::<_, MapacheError>((
                            Index::from_index_file(index_file.clone(), id),
                            index_file,
                            id,
                        ))
                    })
                    .await
                    .map_err(|e| MapacheError::task_panicked("index loading", e))??;

                    Ok(Some(index))
                }
            })
            .buffered(16)
            .collect::<Vec<Result<Option<(Index, index::IndexFile, ID)>>>>()
            .await;

        let mut num_hot = 0;
        let mut num_cold = 0;
        let total_indices: usize = indices
            .iter()
            .filter(|r| r.as_ref().is_ok_and(|o| o.is_some()))
            .count();

        for (i, res) in indices.into_iter().enumerate() {
            if let Some((index, index_file, file_id)) = res? {
                if i >= total_indices.saturating_sub(hot_count) {
                    // Hot: load fully into RAM
                    self.master_index.add_index(index);
                    num_hot += 1;
                } else {
                    // Cold: only store metadata for lazy loading
                    let bf = index::IndexMetadata::bloom_filter_from_index_file(&index_file);
                    let meta = index::IndexMetadata::from_index_file(index_file, bf, file_id);
                    self.master_index.add_cold_metadata(meta);
                    num_cold += 1;
                }
            }
        }

        tracing::info!(target: "repo", "Loaded {num_hot} hot + {num_cold} cold index files");
        ui::cli::verbose_1!("Loaded {} hot + {} cold index files", num_hot, num_cold);

        let total_blobs = self.master_index.num_blobs();
        if total_blobs > 0 {
            tracing::debug!(target: "repo", "Initializing Bloom Filter for {} blobs", total_blobs);
            self.master_index.initialize_bloom_filter(total_blobs);
        }

        Ok(())
    }

    /// Load and decode data from a pack file
    pub async fn load_from_pack(
        &self,
        id: &ID,
        blob_type: BlobType,
        offset: u32,
        length: u32,
        compressed: bool,
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
        self.secure_storage.decode_blob_owned(data, compressed)
    }

    pub fn pack_size(&self) -> u64 {
        self.max_packer_size
    }
}

/// Finds a terminal node in a snapshot tree by name or glob.
pub async fn find_in_snapshot(
    repo: Arc<Repository>,
    snapshot: &Snapshot,
    pattern: &str,
) -> Result<Vec<(PathBuf, crate::fs::node::Node)>> {
    use crate::fs::{filter::GlobRule, tree::SerializedNodeStream};

    let root_tree_id = snapshot.tree;
    let starts_with_slash = pattern.starts_with('/');
    let pattern = pattern.trim_start_matches('/');
    let search_path = if starts_with_slash || pattern.contains('/') {
        PathBuf::from(pattern)
    } else {
        Path::new("**").join(pattern)
    };
    let glob_rule = GlobRule::new(&search_path);
    let mut stream =
        SerializedNodeStream::new(repo, Some(root_tree_id), PathBuf::new(), None, None).await?;
    let mut results = Vec::new();

    while let Some(res) = stream.next().await {
        let (node_path, stream_node_res) = res?;
        let stream_node = stream_node_res?;

        if glob_rule.is_strict_match(&node_path) {
            results.push((node_path, stream_node.node));
        }
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::Local;
    use parking_lot::Mutex;
    use rstest::rstest;
    use tempfile::tempdir;

    use super::*;
    use crate::{
        backend::mock::MockBackend, commands::Compression, common::defaults::TEST_REPO_CONFIG,
        common::error::MapacheError, repository::lock::LOCK_EXPIRE_TIMEOUT, utils,
    };

    fn make_auth() -> Auth {
        Auth {
            username: "mapachito".to_string(),
            password: Zeroizing::new("password".to_string()),
        }
    }

    /// Test init a repo with password and open it
    #[tokio::test]
    async fn test_init_and_open_with_password() -> Result<()> {
        let auth = make_auth();
        let backend: Arc<dyn StorageBackend> = Arc::new(MockBackend::new());

        Repository::init(THIS_REPOSITORY_VERSION, &auth, None, backend.clone(), None).await?;
        let (_, _, lock_handle) =
            Repository::try_open_with_lock(&auth, None, backend, TEST_REPO_CONFIG, false, None)
                .await?;
        lock_handle.unlock().await;

        Ok(())
    }

    /// Test init a repo with password and open it using a password stored in a file
    #[tokio::test]
    async fn test_init_and_open_with_password_from_file() -> Result<()> {
        let temp_dir = tempdir()?;
        let password_file_path = temp_dir.path().join("repo_password");

        std::fs::write(&password_file_path, "mapachito\npassword")?;

        let auth = utils::get_auth(&Some(password_file_path))?.unwrap();
        let backend: Arc<dyn StorageBackend> = Arc::new(MockBackend::new());

        Repository::init(THIS_REPOSITORY_VERSION, &auth, None, backend.clone(), None).await?;
        let (_, _, lock_handle) =
            Repository::try_open_with_lock(&auth, None, backend, TEST_REPO_CONFIG, false, None)
                .await?;
        lock_handle.unlock().await;

        Ok(())
    }

    /// Test that opening with a wrong password returns Auth error (retryable)
    #[tokio::test]
    async fn test_open_with_wrong_password_returns_auth_error() -> Result<()> {
        let auth = make_auth();
        let backend: Arc<dyn StorageBackend> = Arc::new(MockBackend::new());
        Repository::init(THIS_REPOSITORY_VERSION, &auth, None, backend.clone(), None).await?;

        let wrong_auth = Auth {
            username: "mapachito".to_string(),
            password: Zeroizing::new("wrong_password".to_string()),
        };

        let result =
            Repository::try_open_unlocked(&wrong_auth, None, backend, TEST_REPO_CONFIG).await;

        let err = match result {
            Ok(_) => panic!("expected Err but got Ok"),
            Err(e) => e,
        };

        assert!(
            matches!(err, MapacheError::Auth(_)),
            "expected Auth error, got: {err}"
        );

        Ok(())
    }

    /// Blob save, flush, and load cycle for data and tree blobs
    #[tokio::test]
    async fn test_blob_save_and_load_cycle() -> Result<()> {
        let auth = make_auth();
        let backend: Arc<dyn StorageBackend> = Arc::new(MockBackend::new());
        Repository::init(THIS_REPOSITORY_VERSION, &auth, None, backend.clone(), None).await?;
        let (repo, _ss) =
            Repository::try_open_unlocked(&auth, None, backend, TEST_REPO_CONFIG).await?;

        repo.init_pack_saver(2)?;

        let data = b"hello world, this is test data for blob cycle";
        let id = repo.encode_and_save_blob(
            BlobType::Data,
            WriteContents::Borrowed(data),
            SaveID::CalculateID,
        )?;

        let stats = repo.flush_and_finalize_pack_saver().await?;
        assert!(stats.data.raw > 0);
        assert!(stats.blobs > 0);

        let loaded = repo.load_blob(&id).await?;
        assert_eq!(loaded, data);

        // Tree blob
        repo.init_pack_saver(1)?;
        let tree_data = br#"{"nodes":[]}"#;
        let tree_id = repo.encode_and_save_blob(
            BlobType::Tree,
            WriteContents::Borrowed(tree_data),
            SaveID::CalculateID,
        )?;
        let _stats2 = repo.flush_and_finalize_pack_saver().await?;

        let loaded_tree = repo.load_blob(&tree_id).await?;
        assert_eq!(loaded_tree, tree_data);

        Ok(())
    }

    /// Index persists across repository reopen
    #[tokio::test]
    async fn test_index_persistence_across_reopen() -> Result<()> {
        let auth = make_auth();
        let backend: Arc<dyn StorageBackend> = Arc::new(MockBackend::new());
        Repository::init(THIS_REPOSITORY_VERSION, &auth, None, backend.clone(), None).await?;

        // First session
        let (repo, _ss) =
            Repository::try_open_unlocked(&auth, None, backend.clone(), TEST_REPO_CONFIG).await?;
        repo.init_pack_saver(2)?;

        let data = b"persistent blob data";
        let id = repo.encode_and_save_blob(
            BlobType::Data,
            WriteContents::Borrowed(data),
            SaveID::CalculateID,
        )?;
        repo.flush_and_finalize_pack_saver().await?;
        drop(repo);

        // Second session
        let (repo2, _ss2) =
            Repository::try_open_unlocked(&auth, None, backend.clone(), TEST_REPO_CONFIG).await?;
        repo2.reload_master_index().await?;

        assert!(repo2.index().contains(&id));

        let loaded = repo2.load_blob(&id).await?;
        assert_eq!(loaded, data);

        Ok(())
    }

    /// Concurrent blob save and load
    #[tokio::test]
    async fn test_concurrent_blob_save_and_load() -> Result<()> {
        use futures::future::join_all;

        let auth = make_auth();
        let backend: Arc<dyn StorageBackend> = Arc::new(MockBackend::new());
        Repository::init(THIS_REPOSITORY_VERSION, &auth, None, backend.clone(), None).await?;
        let (repo, _ss) =
            Repository::try_open_unlocked(&auth, None, backend, TEST_REPO_CONFIG).await?;

        repo.init_pack_saver(4)?;

        let mut handles = Vec::new();
        for i in 0..10 {
            let r = repo.clone();
            handles.push(tokio::spawn(async move {
                let data = format!("concurrent blob {}", i);
                let id = r.encode_and_save_blob(
                    BlobType::Data,
                    WriteContents::Borrowed(data.as_bytes()),
                    SaveID::CalculateID,
                )?;
                Ok::<_, MapacheError>(id)
            }));
        }

        let ids: Vec<ID> = join_all(handles)
            .await
            .into_iter()
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| MapacheError::Internal(e.to_string()))?
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        assert_eq!(ids.len(), 10);

        let stats = repo.flush_and_finalize_pack_saver().await?;
        assert!(stats.blobs > 0, "should have saved blobs");

        // Load all blobs concurrently
        let mut load_handles = Vec::new();
        for id in &ids {
            let r = repo.clone();
            let id = *id;
            load_handles.push(tokio::spawn(async move {
                let loaded = r.load_blob(&id).await?;
                Ok::<_, MapacheError>(loaded)
            }));
        }

        let loaded_data: Vec<Vec<u8>> = join_all(load_handles)
            .await
            .into_iter()
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| MapacheError::Internal(e.to_string()))?
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        assert_eq!(loaded_data.len(), 10);
        for (i, data) in loaded_data.iter().enumerate() {
            let expected = format!("concurrent blob {}", i);
            assert_eq!(data, expected.as_bytes(), "blob {} should match", i);
        }

        Ok(())
    }

    #[test]
    fn test_generate_key_file() -> Result<()> {
        let auth = Auth {
            username: "mapachito".to_string(),
            password: Zeroizing::new("password".to_string()),
        };
        let master_key = KeyManager::generate_new_master_key();
        let keyfile = KeyManager::generate_key_file(&auth, &master_key.clone(), 2)?;

        let salt = utils::base64::decode(&keyfile.salt)?;
        let encrypted_key = utils::base64::decode(&keyfile.encrypted_key)?;

        let intermediate_key =
            SecureStorage::derive_key::<32>("password", &salt, keyfile.argon2_params()?)?;
        let ss = SecureStorage::new()
            .with_compression(Compression::Fast.to_level())
            .with_key(&*intermediate_key)?;

        let decrypted_key = ss.decrypt(&encrypted_key)?.to_vec();

        assert_eq!(*master_key, decrypted_key.as_slice());

        Ok(())
    }

    #[test]
    fn test_generate_key_file_v1() -> Result<()> {
        let auth = Auth {
            username: "mapachito".to_string(),
            password: Zeroizing::new("password".to_string()),
        };
        let master_key = KeyManager::generate_new_master_key();
        let keyfile = KeyManager::generate_key_file(&auth, &master_key.clone(), 1)?;

        let salt = utils::base64::decode(&keyfile.salt)?;
        let encrypted_key = utils::base64::decode(&keyfile.encrypted_key)?;

        let intermediate_key =
            SecureStorage::derive_key::<32>("password", &salt, keyfile.argon2_params()?)?;
        let ss = SecureStorage::new()
            .with_compression(Compression::Fast.to_level())
            .with_key(&*intermediate_key)?;
        // v1 keyfiles use nonce-at-start
        // TODO(v1-removal): Remove this test or adapt to only test v2.
        ss.set_nonce_at_end(false);

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
        let auth = Auth {
            username: "mapachito".to_string(),
            password: Zeroizing::new("password".to_string()),
        };
        let backend: Arc<dyn StorageBackend> = Arc::new(MockBackend::new());

        Repository::init(THIS_REPOSITORY_VERSION, &auth, None, backend.clone(), None).await?;

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
            (true, false, false) => {
                return Err(MapacheError::Integrity(
                    "Should not fail to acquire lock".to_string(),
                ));
            }
            (false, true, true) | (false, true, false) | (false, false, true) => {
                return Err(MapacheError::Integrity(
                    "Should fail to acquire lock".to_string(),
                ));
            }
        }
    }

    #[tokio::test]
    async fn test_acquire_lock_deletes_other_expired_lock() -> Result<()> {
        let auth = Auth {
            username: "mapachito".to_string(),
            password: Zeroizing::new("password".to_string()),
        };
        let backend: Arc<dyn StorageBackend> = Arc::new(MockBackend::new());

        Repository::init(THIS_REPOSITORY_VERSION, &auth, None, backend.clone(), None).await?;

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
