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
use parking_lot::{Mutex, RwLock};
use rand::Rng;

use crate::{
    backend::{Handle, StorageBackend, StorageHint, cache::CacheBackend},
    commands::Compression,
    mapache::{self, BlobType, ContentIdType, ID, SaveID},
    repository::{
        keys::KeyManager,
        lock::{Lock, LockHandle},
        packer::{PackSaver, PackSaverRequest},
        storage::{EncodingContext, SecureStorage},
    },
    ui::{self},
    utils::collections::IdSet,
};

use super::{
    index::{Index, IndexFile, MasterIndex},
    keys,
    manifest::Manifest,
    snapshot::Snapshot,
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

#[derive(Debug, Default, Copy, Clone)]
pub struct SizePair {
    pub raw: u64,
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

/// Authentication credentials of a user
#[derive(Debug)]
pub struct Auth {
    pub username: String,
    pub password: String,
}

#[derive(Debug)]
pub struct RepoConfig {
    pub pack_size: u64,
    pub use_cache: bool,
    pub(crate) compression: Compression,
}

#[derive(Debug, Default)]
pub struct RepoStats {
    pub raw_bytes: AtomicU64,
    pub encoded_bytes: AtomicU64,
    pub data_blobs: AtomicU64,

    pub meta_raw_bytes: AtomicU64,
    pub meta_encoded_bytes: AtomicU64,
    pub meta_blobs: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RepoStatsSnapshot {
    pub data: SizePair,
    pub meta: SizePair,
    pub total: SizePair,
    pub blobs: u64,
    pub meta_blobs: u64,
    pub total_blobs: u64,
}

impl RepoStats {
    pub fn snapshot(&self) -> RepoStatsSnapshot {
        let rb = self.raw_bytes.load(Ordering::Relaxed);
        let eb = self.encoded_bytes.load(Ordering::Relaxed);
        let mrb = self.meta_raw_bytes.load(Ordering::Relaxed);
        let meb = self.meta_encoded_bytes.load(Ordering::Relaxed);
        let bc = self.data_blobs.load(Ordering::Relaxed);
        let mbc = self.meta_blobs.load(Ordering::Relaxed);

        RepoStatsSnapshot {
            data: SizePair::new(rb, eb),
            meta: SizePair::new(mrb, meb),
            total: SizePair::new(rb + mrb, eb + meb),
            blobs: bc,
            meta_blobs: mbc,
            total_blobs: bc + mbc,
        }
    }
}

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
    pack_saver_tx: Mutex<Option<crossbeam_channel::Sender<PackSaverRequest>>>,
    pack_saver_handle: Mutex<Option<std::thread::JoinHandle<Result<()>>>>,

    // Stats
    pub(super) stats: RepoStats,
}

impl Repository {
    /// Create and initialize a new repository
    pub fn init(
        auth: Option<&Auth>,
        keyfile_path: Option<&PathBuf>,
        backend: Arc<dyn StorageBackend>,
    ) -> Result<Manifest> {
        let auth = match auth {
            Some(a) => a,
            None => &ui::cli::request_new_auth(),
        };

        backend
            .create()
            .context("Could not create root directory")?;

        let keys_path = PathBuf::from(KEYS_DIR);
        backend.create_dir(&keys_path)?;

        // Create new key
        let master_key = KeyManager::generate_new_master_key();
        let keyfile = KeyManager::generate_key_file(auth, master_key.clone())
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
                backend.write(&handle, &keyfile_json)?;
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
        backend.write(&Handle::new(manifest_path), &manifest_json)?;

        backend.create_dir(&objects_path)?;
        let num_folders: usize = 1 << (4 * OBJECTS_DIR_FANOUT);
        for n in 0x00..num_folders {
            backend.create_dir(&objects_path.join(format!("{n:0>OBJECTS_DIR_FANOUT$x}")))?;
        }

        backend.create_dir(&snapshot_path)?;
        backend.create_dir(&index_path)?;
        backend.create_dir(&locks_path)?;

        Ok(manifest)
    }

    /// Try to open a repository and acquire a lock.
    /// This function prompts for a password to retrieve a master key.
    #[allow(clippy::type_complexity)]
    pub fn try_open_with_lock(
        auth: Option<&Auth>,
        key_file_path: Option<&PathBuf>,
        backend: Arc<dyn StorageBackend>,
        config: RepoConfig,
        exclusive_lock: bool,
        retry_duration: Option<Duration>,
    ) -> Result<(Arc<Repository>, Arc<SecureStorage>, Arc<RwLock<LockHandle>>)> {
        let (repo, secure_storage) = Self::try_open_unlocked(auth, key_file_path, backend, config)?;
        let lock = repo.try_acquire_lock_with_retry(exclusive_lock, retry_duration)?;
        let lock_handle = Arc::new(RwLock::new(LockHandle::new(repo.clone(), lock)));

        Ok((repo, secure_storage, lock_handle))
    }

    /// Try to open a repository  without acquiring a lock.
    /// This function prompts for a password to retrieve a master key.
    #[allow(clippy::type_complexity)]
    pub fn try_open_unlocked(
        mut auth: Option<&Auth>,
        key_file_path: Option<&PathBuf>,
        backend: Arc<dyn StorageBackend>,
        config: RepoConfig,
    ) -> Result<(Arc<Repository>, Arc<SecureStorage>)> {
        if !backend.root_exists() {
            bail!("Could not open a repository. The path does not exist.");
        }

        let key_manager = KeyManager::new(backend.clone());

        const MAX_PASSWORD_RETRIES: u32 = 3;
        let mut password_try_count = 0;

        let (_key_id, master_key) = {
            if let Some(a) = auth.take() {
                key_manager
                    .retrieve_master_key(a, key_file_path)
                    .context("Incorrect password.")?
            } else {
                loop {
                    let auth_from_console = ui::cli::request_auth();

                    if let Ok(key) =
                        key_manager.retrieve_master_key(&auth_from_console, key_file_path)
                    {
                        break key;
                    } else {
                        password_try_count += 1;
                        if password_try_count < MAX_PASSWORD_RETRIES {
                            ui::cli::log!("Incorrect username or password. Try again.");
                            continue;
                        } else {
                            bail!("Wrong password or no KeyFile found.");
                        }
                    }
                }
            }
        };

        let secure_storage = Arc::new(
            SecureStorage::new()
                .with_compression(config.compression.to_level())
                .with_key(&master_key),
        );

        let manifest_path = Path::new(MANIFEST_PATH);

        let manifest = backend
            .read(&Handle::new(manifest_path), 0, 0)
            .context("Could not load manifest file")?;
        let manifest = secure_storage
            .decode(&manifest)
            .context("Could not decode the manifest file")?;
        let manifest: Manifest = serde_json::from_slice(&manifest)?;

        let version = manifest.version();
        if version > THIS_REPOSITORY_VERSION {
            bail!("Invalid repository version '{version}'");
        }

        let repo = Repository::open(backend, secure_storage.clone(), config)?;

        Ok((repo, secure_storage))
    }

    /// Open an existing repository from a directory
    fn open(
        backend: Arc<dyn StorageBackend>,
        secure_storage: Arc<SecureStorage>,
        config: RepoConfig,
    ) -> Result<Arc<Self>> {
        let manifest: Manifest = Self::load_manifest(secure_storage.clone(), backend.clone())?;

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

        let mut repo = Repository {
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
            pack_saver_tx: Mutex::new(None),
            pack_saver_handle: Mutex::new(None),
            stats: RepoStats::default(),
        };

        repo.load_master_index()?;

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

    /// Encodes and saves a blob in the repository. This blob can be packed with other blobs in an pack file.
    /// Returns a tuple (`ID`, data_size, meta_size)
    pub fn encode_and_save_blob(
        &self,
        encoding_context: &mut EncodingContext,
        blob_type: BlobType,
        data: Vec<u8>,
        save_id: SaveID,
    ) -> Result<ID> {
        let id = match save_id {
            SaveID::CalculateID => ID::from_content(&data),
            SaveID::WithID(id) => id,
        };

        let blob_exists =
            self.master_index.contains(&id) || !self.master_index.add_pending_blob(id);

        if blob_exists {
            return Ok(id);
        }

        //  Encrypt/Compress
        let raw_length = data.len() as u64;
        let encoded_data = self
            .secure_storage
            .encode_managed(encoding_context, &data)?
            .to_vec();

        let tx_guard = self.pack_saver_tx.lock();
        let tx = tx_guard
            .as_ref()
            .context("Packer is stopped or not initialized")?;

        tx.send(PackSaverRequest::SaveBlob {
            id,
            blob_type,
            data: encoded_data,
            raw_length,
        })?;

        Ok(id)
    }

    /// Loads a blob from the repository.
    pub fn load_blob(&self, id: &ID) -> Result<Vec<u8>> {
        let blob_entry = self.master_index.get(id);
        match blob_entry {
            Some(locator) => self.load_from_pack(
                &locator.pack_id,
                locator.blob_type,
                locator.offset,
                locator.length,
            ),
            None => bail!("Could not find blob {id:?} in index"),
        }
    }

    /// Saves a file to the repository
    pub fn save_file(
        &self,
        id: &SaveID,
        data: &[u8],
        hint: StorageHint,
        with_extension: Option<&str>,
    ) -> Result<(ID, SizePair)> {
        let file_type = hint.file_type;

        let raw_size = data.len() as u64;
        let (data, encoded_size) = match file_type {
            ContentIdType::Pack => (data.to_vec(), raw_size),
            ContentIdType::Key => {
                let compressed_data = self.secure_storage.compress(data)?;
                let size = compressed_data.len() as u64;
                (compressed_data, size)
            }
            _ => {
                let encoded_data = self.secure_storage.encode(data)?;
                let size = encoded_data.len() as u64;
                (encoded_data, size)
            }
        };

        // Assign ID after (potentially) encoding
        let id = match id {
            SaveID::CalculateID => &ID::from_content(&data),
            SaveID::WithID(id) => id,
        };

        let path = self
            .get_path(file_type, id)
            .with_extension(with_extension.unwrap_or_default());
        let handle = Handle {
            path: &path,
            hint: Some(hint),
        };

        self.backend.write(&handle, &data)?;

        Ok((*id, SizePair::new(raw_size, encoded_size)))
    }

    /// Loads a file to the repository
    pub fn load_file(
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
        let data = self.backend.read(&handle, 0, 0)?;

        match file_type {
            ContentIdType::Pack => Ok(data),
            ContentIdType::Key => self.secure_storage.decompress(&data),
            _ => self.secure_storage.decode(&data),
        }
    }

    /// Deletes a file from the repository
    pub fn delete_file(
        &self,
        file_type: ContentIdType,
        id: &ID,
        with_extension: Option<&str>,
    ) -> Result<u64> {
        let path = self
            .get_path(file_type, id)
            .with_extension(with_extension.unwrap_or_default());
        let size = self.backend.lstat(&path)?.size;
        self.backend.remove(&path)?;

        Ok(size.unwrap_or(0))
    }

    /// Sets an extension to a file.
    pub fn set_extension(
        &self,
        file_type: ContentIdType,
        id: &ID,
        extension: Option<&str>,
    ) -> Result<()> {
        let path = self.get_path(file_type, id);
        let ext_path = path.with_extension(extension.unwrap_or_default());
        self.backend.rename(&path, &ext_path)?;

        Ok(())
    }

    /// Removes a snapshot from the repository, if it exists.
    pub fn remove_snapshot(&self, id: &ID) -> Result<()> {
        let snapshot_path = self.snapshot_path.join(id.to_hex());

        if !self.backend.path_exists(&snapshot_path) {
            bail!("Snapshot {id} doesn't exist")
        }

        self.backend
            .remove(&snapshot_path)
            .with_context(|| format!("Could not remove snapshot {id}"))
    }

    /// Loads a snapshot by ID
    pub fn load_snapshot(&self, id: &ID, extension: Option<&str>) -> Result<Snapshot> {
        let snapshot = self
            .load_file(
                id,
                StorageHint {
                    file_type: ContentIdType::Snapshot,
                    is_metadata: true,
                },
                extension,
            )
            .with_context(|| format!("No snapshot with ID '{id}' exists"))?;
        let snapshot: Snapshot = serde_json::from_slice(&snapshot)?;
        Ok(snapshot)
    }

    /// Recall a dropped snapshot with ID
    pub fn recall_dropped_snapshot(&self, id: &ID) -> Result<()> {
        let path = self.get_path(ContentIdType::Snapshot, id);
        let dropped_path = path.with_extension(REPO_DROPPED_EXTENSION);
        self.backend.rename(&dropped_path, &path)
    }

    /// Lists all snapshot IDs
    pub fn list_snapshot_ids(&self) -> Result<Vec<ID>> {
        let ids = self
            .list_files(ContentIdType::Snapshot)
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

    /// Lists all .dropped snapshot IDs
    pub(crate) fn list_dropped_snapshot_ids(&self) -> Result<Vec<ID>> {
        let ids = self
            .list_files_with_extension(ContentIdType::Snapshot, Some(REPO_DROPPED_EXTENSION))
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
    pub fn load_pack(&self, id: &ID) -> Result<Vec<u8>> {
        // We don't really know what kind of pack we are loading, so it is not metadata
        self.load_file(
            id,
            StorageHint {
                file_type: ContentIdType::Pack,
                is_metadata: false,
            },
            None,
        )
    }

    /// Loads an index file.
    pub fn load_index(&self, id: &ID) -> Result<IndexFile> {
        let index: Vec<u8> = self
            .load_file(
                id,
                StorageHint {
                    file_type: ContentIdType::Index,
                    is_metadata: true,
                },
                None,
            )
            .with_context(|| format!("Could not load index {}", id.to_hex()))?;
        let index = serde_json::from_slice(&index)?;
        Ok(index)
    }

    /// Loads the repository manifest.
    fn load_manifest(
        secure_storage: Arc<SecureStorage>,
        backend: Arc<dyn StorageBackend>,
    ) -> Result<Manifest> {
        let manifest = backend.read(&Handle::new(Path::new(MANIFEST_PATH)), 0, 0)?;
        let manifest = secure_storage.decode(&manifest)?;
        let manifest = serde_json::from_slice(&manifest)?;
        Ok(manifest)
    }

    /// Loads a lock file.
    pub fn load_lock(&self, id: &ID) -> Result<Lock> {
        let lock: Vec<u8> = self
            .load_file(
                id,
                StorageHint {
                    file_type: ContentIdType::Lock,
                    is_metadata: true,
                },
                None,
            )
            .with_context(|| format!("Could not load lock file {}", id.to_hex()))?;
        let lock = serde_json::from_slice(&lock)?;
        Ok(lock)
    }

    /// Loads a KeyFile.
    pub fn load_key(&self, id: &ID) -> Result<keys::KeyFile> {
        let key = self.load_file(
            id,
            StorageHint {
                file_type: ContentIdType::Key,
                is_metadata: true,
            },
            None,
        )?;
        let key = serde_json::from_slice(&key)?;
        Ok(key)
    }

    /// Finds a file in the repository using an ID prefix
    pub(crate) fn find_with_extension(
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
            // Although it is technically posible to use an empty prefix, which would find a match
            // if only one file of the type exists. let's consider this invalid as it can be
            // potentially ambiguous or lead to errors.
            bail!("Prefix cannot be empty");
        }

        let type_files = self.list_files_with_extension(file_type, extension)?;
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
    pub fn find(&self, file_type: ContentIdType, prefix: &str) -> Result<(ID, PathBuf)> {
        self.find_with_extension(file_type, prefix, None)
    }

    pub fn init_pack_saver(self: &Arc<Self>, write_concurrency: usize) -> Result<()> {
        let (tx, rx) = crossbeam_channel::bounded(16 * write_concurrency);

        let weak_self = Arc::downgrade(self);
        let storage = self.secure_storage.clone();
        let max_packer_size = self.max_packer_size;

        let handle = std::thread::spawn(move || {
            let pack_saver =
                PackSaver::new(rx, weak_self, storage, max_packer_size, write_concurrency)?;
            pack_saver.run()
        });

        *self.pack_saver_tx.lock() = Some(tx);
        *self.pack_saver_handle.lock() = Some(handle);

        Ok(())
    }

    pub fn flush_and_finalize_pack_saver(&self) -> Result<RepoStatsSnapshot> {
        // Signal PackSaver to stop by dropping the channel
        let tx = self.pack_saver_tx.lock().take();
        drop(tx);

        if let Some(handle) = self.pack_saver_handle.lock().take() {
            handle
                .join()
                .map_err(|_| anyhow::anyhow!("Pack saver thread panicked"))??;
        }

        let index_size = self.index().persist(self)?;

        self.stats
            .meta_raw_bytes
            .fetch_add(index_size.raw, Ordering::Relaxed);
        self.stats
            .meta_encoded_bytes
            .fetch_add(index_size.encoded, Ordering::Relaxed);

        Ok(self.stats.snapshot())
    }

    pub fn index(&self) -> Arc<MasterIndex> {
        self.master_index.clone()
    }

    pub fn get_encoding_context(&self) -> Result<EncodingContext> {
        self.secure_storage.get_encoding_context()
    }

    /// Reads from a pack file with offset and length.
    /// This function decodes the data.
    pub fn read_from_pack_and_decode(
        &self,
        blob_type: BlobType,
        id: &ID,
        offset: u64,
        length: u64,
    ) -> Result<Vec<u8>> {
        let path = self.get_path(ContentIdType::Pack, id);
        let data = self.backend.read(
            &Handle::new_with_hint(&path, ContentIdType::Pack, blob_type == BlobType::Tree),
            offset as isize,
            length as usize,
        )?;
        self.secure_storage.decode(&data)
    }

    /// Lists all packs in the repository.
    pub fn list_packs(&self) -> Result<IdSet<ID>> {
        let mut list = IdSet::default();

        let num_folders: usize = 1 << (4 * OBJECTS_DIR_FANOUT);
        for n in 0..num_folders {
            let dir = self
                .objects_path
                .join(format!("{n:0>OBJECTS_DIR_FANOUT$x}"));

            let files = self.backend.list_dir(&dir)?;
            for path in files {
                let filename = path.file_name().unwrap().to_string_lossy().to_string();
                if let Ok(id) = ID::from_hex(&filename) {
                    list.insert(id);
                }
            }
        }

        Ok(list)
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
    pub fn list_all_files(&self, file_type: ContentIdType) -> Result<Vec<PathBuf>> {
        match file_type {
            ContentIdType::Snapshot => self.backend.list_dir(&self.snapshot_path),
            ContentIdType::Key => self.backend.list_dir(&self.keys_path),
            ContentIdType::Index => self.backend.list_dir(&self.index_path),
            ContentIdType::Lock => self.backend.list_dir(&self.locks_path),
            ContentIdType::Pack => {
                let mut files = Vec::new();
                for n in 0x00..(1 << (4 * OBJECTS_DIR_FANOUT)) {
                    let dir_name = self
                        .objects_path
                        .join(format!("{n:0>OBJECTS_DIR_FANOUT$x}"));

                    let sub_files = self.backend.list_dir(&dir_name)?;
                    for file_path in sub_files.into_iter() {
                        files.push(file_path);
                    }
                }

                Ok(files)
            }
        }
    }

    /// Lists all paths belonging to a file type (objects, snapshots, indices, etc.)
    /// with extension.
    pub fn list_files_with_extension(
        &self,
        file_type: ContentIdType,
        extension: Option<&str>,
    ) -> Result<Vec<PathBuf>> {
        let paths = self
            .list_all_files(file_type)?
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
    pub fn list_files(&self, file_type: ContentIdType) -> Result<Vec<PathBuf>> {
        self.list_files_with_extension(file_type, None)
    }

    /// Load the master index from file
    fn load_master_index(&mut self) -> Result<()> {
        let files = self.list_files(ContentIdType::Index)?;
        let num_index_files = files.len();

        for file_path in files {
            let file_name = file_path
                .file_name()
                .expect("Could not read index file name")
                .to_string_lossy()
                .clone();
            let id = match ID::from_hex(&file_name) {
                Ok(id) => id,
                Err(_) => continue, // Ignore invalid ID names
            };
            let index_file = self.backend.read(
                &Handle::new_with_hint(&file_path, ContentIdType::Index, true),
                0,
                0,
            )?;
            let index_file = self.secure_storage.decode(&index_file)?;
            let index_file = match serde_json::from_slice(&index_file) {
                Ok(idx_file) => idx_file,
                Err(e) => bail!("Failed to load index file {}: {}", id.to_short_hex(4), e),
            };

            let index = Index::from_index_file(index_file, id);
            self.master_index.add_index(index);
        }

        ui::cli::verbose_1!("Loaded {} index files", num_index_files);

        Ok(())
    }

    /// Load and decode data from a pack file
    pub fn load_from_pack(
        &self,
        id: &ID,
        blob_type: BlobType,
        offset: u32,
        length: u32,
    ) -> Result<Vec<u8>> {
        let object_path = Self::get_object_path(&self.objects_path, id);
        let data = self.backend.read(
            &Handle::new_with_hint(
                &object_path,
                ContentIdType::Pack,
                blob_type == BlobType::Tree,
            ),
            offset as isize,
            length as usize,
        )?;
        self.secure_storage.decode(&data)
    }

    /// Try to acquire a lock with a retry deadline
    fn try_acquire_lock_with_retry(
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
            match self.try_acquire_lock_once(exclusive) {
                Ok(lock) => return Ok(lock),

                Err(e) => {
                    let timeout = match retry_duration {
                        Some(t) => t,
                        None => return Err(e),
                    };

                    if start_time.elapsed() >= timeout.to_std().unwrap_or_default() {
                        bail!("Timeout acquiring repository lock");
                    }

                    let mut rng = rand::rng();
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

                    std::thread::sleep(wait_time.to_std()?);
                }
            }
        }
    }

    /// Try to acquire a lock just once without retrying
    fn try_acquire_lock_once(&self, exclusive: bool) -> Result<Arc<Mutex<Lock>>> {
        self.backend.create_dir(&PathBuf::from(LOCKS_DIR))?;
        let new_lock = Arc::new(Mutex::new(Lock::new(exclusive)));

        let new_lock_id = *new_lock.lock().id();

        self.save_lock(&new_lock)
            .context("Failed to write new lock file")?;

        let all_locks = match self.get_locks() {
            Ok(locks) => locks,
            Err(e) => {
                let _ = self.delete_file(ContentIdType::Lock, &new_lock_id, None);
                return Err(e);
            }
        };

        for lock in all_locks {
            // Skip the lock we just wrote
            if lock.id() == &new_lock_id {
                continue;
            }

            // Clean up expired locks from other processes
            if lock.is_expired() {
                let _ = self.delete_file(ContentIdType::Lock, lock.id(), None);
                continue;
            }

            if exclusive || lock.is_exclusive() {
                // A race condition occurred, or a conflict was already present.
                // The NEWLY written lock must be cleaned up and the attempt must fail.
                let _ = self.delete_file(ContentIdType::Lock, &new_lock_id, None);

                bail!(
                    "Conflict detected with existing lock (ID: {}).",
                    lock.id().to_short_hex(4)
                );
            }
        }

        Ok(new_lock)
    }

    fn save_lock(&self, lock: &Arc<Mutex<Lock>>) -> Result<()> {
        let lock_guard = lock.lock();

        let lock_json = serde_json::to_string(&*lock_guard)?;

        self.save_file(
            &SaveID::WithID(*lock_guard.id()),
            lock_json.as_bytes(),
            StorageHint {
                file_type: ContentIdType::Lock,
                is_metadata: true,
            },
            None,
        )?;

        Ok(())
    }

    pub fn refresh_lock(&self, lock: &Arc<Mutex<Lock>>) -> Result<()> {
        lock.lock().refresh();
        self.save_lock(lock)
    }

    /// Get all locks in the repository. If a lock file cannot be read, decoded
    /// or deserialized, it will be ignored.
    pub fn get_locks(&self) -> Result<Vec<Lock>> {
        let all_lock_paths = self.list_files(ContentIdType::Lock)?;
        let mut locks = Vec::new();

        for path in all_lock_paths {
            // Attempt to read the lock file
            let lock_data_result = self.backend.read(
                &Handle::new_with_hint(&path, ContentIdType::Lock, true),
                0,
                0,
            );

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

impl Drop for Repository {
    fn drop(&mut self) {
        let _ = self.flush_and_finalize_pack_saver();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use base64::{Engine, engine::general_purpose};
    use chrono::Local;
    use rstest::rstest;
    use tempfile::tempdir;

    use crate::{
        backend::localfs::LocalFS,
        commands::Compression,
        mapache::{defaults::TEST_REPO_CONFIG, global::set_global_opts_with_args},
        repository::lock::LOCK_EXPIRE_TIMEOUT,
    };

    use super::*;

    /// Test init a repo with password and open it
    #[test]
    fn test_init_and_open_with_password() -> Result<()> {
        let temp_repo_dir = tempdir()?;
        let temp_repo_path = temp_repo_dir.path().join("repo");

        let auth = Some(Auth {
            username: String::from("mapachito"),
            password: String::from("password"),
        });
        let backend = Arc::new(LocalFS::new(temp_repo_path.to_owned()));

        Repository::init(auth.as_ref(), None, backend.to_owned())?;
        Repository::try_open_with_lock(
            auth.as_ref(),
            None,
            backend,
            TEST_REPO_CONFIG,
            false,
            None,
        )?;

        Ok(())
    }

    /// Test init a repo with password and open it using a password stored in a file
    #[test]
    fn test_init_and_open_with_password_from_file() -> Result<()> {
        let temp_dir = tempdir()?;
        let temp_path = temp_dir.path();
        let temp_repo_path = temp_path.join("repo");
        let password_file_path = temp_path.join("repo_password");

        // Write password to file
        std::fs::write(&password_file_path, "mapachito")?;

        let auth = Some(Auth {
            username: String::from("mapachito"),
            password: String::from("password"),
        });
        let backend = Arc::new(LocalFS::new(temp_repo_path.to_owned()));

        Repository::init(auth.as_ref(), None, backend.to_owned())?;
        Repository::try_open_with_lock(
            auth.as_ref(),
            None,
            backend,
            TEST_REPO_CONFIG,
            false,
            None,
        )?;

        Ok(())
    }

    /// Test generation of master keys
    #[test]
    fn test_generate_key_file() -> Result<()> {
        let auth = Auth {
            username: "mapachito".to_string(),
            password: "password".to_string(),
        };
        let master_key = KeyManager::generate_new_master_key();
        let keyfile = KeyManager::generate_key_file(&auth, master_key.clone())?;

        let salt = general_purpose::STANDARD.decode(keyfile.salt.clone())?;
        let encrypted_key = general_purpose::STANDARD.decode(keyfile.encrypted_key.clone())?;

        let intermediate_key =
            SecureStorage::derive_key::<32>("password", &salt, keyfile.argon2_params())?;
        let ss = SecureStorage::new()
            .with_compression(Compression::Fast.to_level())
            .with_key(&intermediate_key);

        let decrypted_key = ss.decrypt(&encrypted_key)?;

        assert_eq!(master_key, decrypted_key.as_slice());

        Ok(())
    }

    #[rstest]
    #[case(false, false)]
    #[case(false, true)]
    #[case(true, false)]
    #[case(true, true)]
    fn test_acquire_lock_with_non_exclusive_lock(
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
            password: "password".to_string(),
        };
        let auth_file_path = tmp_path.join("auth");
        std::fs::write(
            &auth_file_path,
            format!("{}\n{}", auth.username, auth.password),
        )?;

        let repo = String::from("repo");
        let repo_path = tmp_path.join(&repo);

        let global = GlobalArgs {
            repo: repo_path.to_string_lossy().to_string(),
            auth_file: Some(auth_file_path),
            key: None,
            quiet: true,
            verbosity: Some(3),
            ssh_pubkey: None,
            ssh_privatekey: None,
            pack_size_mib: DEFAULT_DEFAULT_PACK_SIZE_MIB,
            no_cache: true,
            retry_lock_duration: None,
            compression_level: Compression::Fastest,
        };
        let args = CmdArgs {};
        set_global_opts_with_args(&global);

        // Init repo
        commands::cmd_init::run(&global, &args).context("Failed to run cmd_init")?;

        let backend = Arc::new(LocalFS::new(repo_path));

        let (r0, _ss0) =
            Repository::try_open_unlocked(Some(&auth), None, backend.clone(), TEST_REPO_CONFIG)?;

        let other_lock = Arc::new(Mutex::new(Lock::new(other_lock_exclusive)));
        r0.save_lock(&other_lock)?;

        let own_repo_open_result = Repository::try_open_with_lock(
            Some(&auth),
            None,
            backend.clone(),
            TEST_REPO_CONFIG,
            own_lock_exclusive,
            None,
        );

        match (
            own_repo_open_result.is_err(),
            own_lock_exclusive,
            other_lock_exclusive,
        ) {
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

    #[test]
    fn test_acquire_lock_deletes_other_expired_lock() -> Result<()> {
        use crate::{
            commands::{self, GlobalArgs, cmd_init::CmdArgs},
            mapache::defaults::DEFAULT_DEFAULT_PACK_SIZE_MIB,
        };

        let tmp_dir = tempdir()?;
        let tmp_path = tmp_dir.path();
        let auth = Auth {
            username: "mapachito".to_string(),
            password: "password".to_string(),
        };
        let auth_file_path = tmp_path.join("auth");
        std::fs::write(
            &auth_file_path,
            format!("{}\n{}", auth.username, auth.password),
        )?;

        let repo = String::from("repo");
        let repo_path = tmp_path.join(&repo);

        let global = GlobalArgs {
            repo: repo_path.to_string_lossy().to_string(),
            auth_file: Some(auth_file_path),
            key: None,
            quiet: true,
            verbosity: Some(3),
            ssh_pubkey: None,
            ssh_privatekey: None,
            pack_size_mib: DEFAULT_DEFAULT_PACK_SIZE_MIB,
            no_cache: true,
            retry_lock_duration: None,
            compression_level: Compression::Fastest,
        };
        let args = CmdArgs {};
        set_global_opts_with_args(&global);

        // Init repo
        commands::cmd_init::run(&global, &args).context("Failed to run cmd_init")?;

        let backend = Arc::new(LocalFS::new(repo_path));

        let (r0, _ss0) =
            Repository::try_open_unlocked(Some(&auth), None, backend.clone(), TEST_REPO_CONFIG)?;

        let other_lock = Arc::new(Mutex::new(Lock::new_for_test(
            true,
            Local::now() - LOCK_EXPIRE_TIMEOUT,
        )));
        r0.save_lock(&other_lock)?;

        Repository::try_open_with_lock(
            Some(&auth),
            None,
            backend.clone(),
            TEST_REPO_CONFIG,
            true,
            None,
        )?; // The other expired lock should have been deleted

        Ok(())
    }
}
