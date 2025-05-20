// [backup] is an incremental backup tool
// Copyright (C) 2025  Javier Lancha Vázquez <javier.lancha@gmail.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
};

use aes_gcm::aead::{OsRng, rand_core::RngCore};
use anyhow::{Context, Result, bail};
use chrono::Utc;

use crate::{
    backend::StorageBackend,
    backup::{self, ObjectType},
    cli,
    repository::{self, packer::Packer, storage::SecureStorage},
    utils::{self, Hash},
};

use super::{
    ObjectId, RepoVersion, RepositoryBackend, SnapshotId,
    config::Config,
    index::{Index, IndexFile, MasterIndex},
    snapshot::Snapshot,
};

const REPO_VERSION: RepoVersion = 1;

const OBJECTS_DIR: &str = "objects";
const SNAPSHOTS_DIR: &str = "snapshots";
const INDEX_DIR: &str = "index";

const OBJECTS_DIR_FANOUT: usize = 2;

pub struct Repository {
    backend: Arc<dyn StorageBackend>,

    objects_path: PathBuf,
    snapshot_path: PathBuf,
    index_path: PathBuf,

    secure_storage: Arc<SecureStorage>,

    // Packers.
    // By design, we pack blobs and trees separately so we can potentially cache trees
    // separately.
    max_packer_size: u64,
    data_packer: Arc<Mutex<Packer>>,
    tree_packer: Arc<Mutex<Packer>>,

    index: Arc<Mutex<MasterIndex>>,
}

impl RepositoryBackend for Repository {
    /// Create and initialize a new repository
    fn init(backend: Arc<dyn StorageBackend>, password: String) -> Result<()> {
        if backend.root_exists() {
            bail!("Could not initialize a repository because a directory already exists");
        }

        let timestamp = Utc::now();

        // Init repository structure
        let objects_path = PathBuf::from(OBJECTS_DIR);
        let snapshot_path = PathBuf::from(SNAPSHOTS_DIR);
        let index_path = PathBuf::from(INDEX_DIR);
        let keys_path = PathBuf::from(repository::KEYS_DIR);

        // Create the repository root
        backend
            .create()
            .with_context(|| "Could not create root directory")?;

        backend.create_dir(&objects_path)?;
        let num_folders: usize = 1 << (4 * OBJECTS_DIR_FANOUT);
        for n in 0x00..num_folders {
            backend.create_dir(&objects_path.join(format!("{:0>OBJECTS_DIR_FANOUT$x}", n)))?;
        }

        backend.create_dir(&snapshot_path)?;
        backend.create_dir(&keys_path)?;
        backend.create_dir(&index_path)?;

        // Create new key
        let (key, keyfile) =
            repository::generate_key(&password).with_context(|| "Could not generate key")?;
        let keyfile_json = serde_json::to_string_pretty(&key)?;
        let keyfile_hash = utils::calculate_hash(&keyfile_json);
        let keyfile_path = &keys_path.join(&keyfile_hash);
        backend.write(
            &keyfile_path,
            &SecureStorage::compress(
                serde_json::to_string_pretty(&keyfile)
                    .with_context(|| "")?
                    .as_bytes(),
                zstd::DEFAULT_COMPRESSION_LEVEL, // Compress the key with whatever compression level
            )?,
        )?;

        let mut repo_id_bytes: [u8; 32] = [0; 32];
        OsRng.fill_bytes(&mut repo_id_bytes);
        let repo_id = utils::bytes_to_hex_string(&repo_id_bytes);

        // Save new config
        let config = Config {
            version: REPO_VERSION,
            id: repo_id.clone(),
            created_time: timestamp,
        };

        let secure_storage: SecureStorage = SecureStorage::build()
            .with_key(key)
            .with_compression(zstd::DEFAULT_COMPRESSION_LEVEL);

        let config_path = Path::new("config");
        let config = serde_json::to_string_pretty(&config)?;
        let config = secure_storage.encode(config.as_bytes())?;
        backend.write(config_path, &config)?;

        cli::log!("Created repo with id {:?}", repo_id);

        Ok(())
    }

    /// Open an existing repository from a directory
    fn open(backend: Arc<dyn StorageBackend>, secure_storage: Arc<SecureStorage>) -> Result<Self> {
        let objects_path = PathBuf::from(OBJECTS_DIR);
        let snapshot_path = PathBuf::from(SNAPSHOTS_DIR);
        let index_path = PathBuf::from(INDEX_DIR);

        let data_packer = Arc::new(Mutex::new(Packer::new()));
        let tree_packer = Arc::new(Mutex::new(Packer::new()));

        let index = Arc::new(Mutex::new(MasterIndex::new()));

        let mut repo = Repository {
            backend,
            objects_path,
            snapshot_path,
            index_path,
            secure_storage,

            max_packer_size: backup::defaults::MAX_PACK_SIZE,
            data_packer,
            tree_packer,

            index,
        };

        repo.warmup()?;

        Ok(repo)
    }

    fn save_object(
        &self,
        object_type: ObjectType,
        data: Vec<u8>,
    ) -> Result<(usize, usize, ObjectId)> {
        let raw_size = data.len();
        let id = utils::calculate_hash(&data);

        let data = self.secure_storage.encode(&data)?;
        let encoded_size = data.len();

        let mut index_guard = self.index.lock().unwrap();
        let blob_exists = index_guard.contains(&id) || !index_guard.add_pending_blob(&id);
        drop(index_guard);

        // If the blob was already pending, return early, as we are finished here.
        if blob_exists {
            return Ok((0, 0, id));
        }

        let mut packer_guard = match object_type {
            ObjectType::Data => self.data_packer.lock().unwrap(),
            ObjectType::Tree => self.tree_packer.lock().unwrap(),
        };

        packer_guard.add_blob(&id, data);

        // Flush if the packer is considered full
        if packer_guard.size() > self.max_packer_size {
            self.flush_packer(packer_guard)?;
        }

        Ok((raw_size, encoded_size, id))
    }

    fn load_object(&self, id: &ObjectId) -> Result<Vec<u8>> {
        let index_guard = self.index.lock().unwrap();
        let blob_entry = index_guard.get(id);
        match blob_entry {
            Some((pack_id, offset, length)) => {
                drop(index_guard);
                self.load_from_pack(&pack_id, offset, length)
            }
            None => bail!("Could not find blob {:?} in index", id),
        }
    }

    fn save_snapshot(&self, snapshot: &Snapshot) -> Result<(SnapshotId, u64, u64)> {
        let snapshot_json = serde_json::to_string_pretty(snapshot)?;
        let snapshot_json = snapshot_json.as_bytes();
        let uncompressed_size = snapshot_json.len() as u64;
        let hash = utils::calculate_hash(&snapshot_json);

        let snapshot_path = self.snapshot_path.join(&hash);

        let snapshot_json = self.secure_storage.encode(snapshot_json)?;
        let compressed_size = snapshot_json.len() as u64;

        self.save_with_rename(&snapshot_json, &snapshot_path)?;

        Ok((hash, uncompressed_size, compressed_size))
    }

    fn load_snapshot(&self, id: &SnapshotId) -> Result<Snapshot> {
        let snapshot_path = self.snapshot_path.join(id);
        if !self.backend.exists(&snapshot_path) {
            bail!(format!("No snapshot with ID \'{}\' exists", id));
        }

        let snapshot = self.backend.read(&snapshot_path)?;
        let snapshot = self.secure_storage.decode(&snapshot)?;
        let snapshot: Snapshot = serde_json::from_slice(&snapshot)?;
        Ok(snapshot)
    }

    /// Get all snapshots in the repository
    fn load_all_snapshots(&self) -> Result<Vec<(SnapshotId, Snapshot)>> {
        let mut snapshots = Vec::new();

        let paths = self
            .backend
            .read_dir(&self.snapshot_path)
            .with_context(|| "Could not read snapshots")?;

        for path in paths {
            if self.backend.is_file(&path) {
                if let Some(file_name) = path.file_name().and_then(|s| s.to_str()) {
                    let hash = file_name.to_string(); // Extract hash from filename
                    let snapshot = self.backend.read(&path)?;
                    let snapshot = self.secure_storage.decode(&snapshot)?;
                    let snapshot: Snapshot = serde_json::from_slice(&snapshot)?;
                    snapshots.push((hash, snapshot));
                }
            }
        }

        Ok(snapshots)
    }

    /// Get all snapshots in the repository, sorted by datetime.
    fn load_all_snapshots_sorted(&self) -> Result<Vec<(SnapshotId, Snapshot)>> {
        let mut snapshots = self.load_all_snapshots()?;
        snapshots.sort_by_key(|(_snapshot_id, snapshot)| snapshot.timestamp);
        Ok(snapshots)
    }

    fn save_index(&self, index: IndexFile) -> Result<(u64, u64)> {
        let index_file_json = serde_json::to_string_pretty(&index)?;
        let index_file_json = index_file_json.as_bytes();
        let uncompressed_size = index_file_json.len() as u64;

        let index_file_json = self.secure_storage.encode(index_file_json)?;
        let compressed_size = index_file_json.len() as u64;

        let hash = utils::calculate_hash(&index_file_json);
        let index_path = self.index_path.join(&hash);
        self.backend.write(&index_path, &index_file_json)?;

        Ok((uncompressed_size, compressed_size))
    }

    fn flush(&self) -> Result<(u64, u64)> {
        self.flush_packer(self.data_packer.lock().unwrap())?;
        self.flush_packer(self.tree_packer.lock().unwrap())?;

        self.index.lock().unwrap().save(self)
    }
}

impl Repository {
    /// Returns the path to an object with a given hash in the repository.
    fn get_object_path(&self, hash: &Hash) -> PathBuf {
        self.objects_path
            .join(&hash[..OBJECTS_DIR_FANOUT])
            .join(&hash[OBJECTS_DIR_FANOUT..])
    }

    fn save_with_rename(&self, data: &[u8], path: &Path) -> Result<usize> {
        let tmp_path = path.with_extension("tmp");
        self.backend.write(&tmp_path, data)?;
        self.backend.rename(&tmp_path, path)?;
        Ok(data.len())
    }

    fn flush_packer(&self, mut packer_guard: MutexGuard<Packer>) -> Result<()> {
        let (pack_data, packed_blob_descriptors) = packer_guard.flush();
        drop(packer_guard); // Drop the mutex so other workers can append to the packer

        let pack_id = utils::calculate_hash(&pack_data);
        let pack_path = &self.get_object_path(&pack_id);
        self.backend.write(pack_path, &pack_data)?;

        let mut index_guard = self.index.lock().unwrap();
        index_guard.add_pack(&pack_id, packed_blob_descriptors);
        drop(index_guard);

        Ok(())
    }

    fn load_index(&mut self) -> Result<()> {
        let files = self.backend.read_dir(&self.index_path)?;
        let mut master_index_guard = self.index.lock().unwrap();

        for file in files {
            let index_file = self.backend.read(&file)?;
            let index_file = self.secure_storage.decode(&index_file)?;
            let index_file = serde_json::from_slice(&index_file)?;

            let mut index = Index::from_index_file(index_file);
            index.finalize();

            master_index_guard.add_index(index);
        }

        // Pending index for new blobs
        master_index_guard.add_index(Index::new());

        Ok(())
    }

    fn load_from_pack(&self, id: &ObjectId, offset: u64, length: u64) -> Result<Vec<u8>> {
        let object_path = self.get_object_path(id);
        let data = self.backend.read_seek(&object_path, offset, length)?;
        self.secure_storage.decode(&data)
    }

    fn warmup(&mut self) -> Result<()> {
        self.load_index()
    }
}

#[cfg(test)]
mod test {

    use base64::{Engine, engine::general_purpose};
    use tempfile::tempdir;

    use crate::{
        backend::localfs::LocalFS,
        repository::{self, retrieve_key},
    };

    use super::*;

    /// Test init a repository_v1 with password and open it
    #[test]
    fn test_init_and_open_with_password() -> Result<()> {
        let temp_repo_dir = tempdir()?;
        let temp_repo_path = temp_repo_dir.path().join("repo");

        let backend = Arc::new(LocalFS::new(temp_repo_path.to_owned()));

        Repository::init(backend.to_owned(), String::from("mapachito"))?;

        let key = retrieve_key(String::from("mapachito"), backend.clone())?;
        let secure_storage = Arc::new(
            SecureStorage::build()
                .with_key(key)
                .with_compression(zstd::DEFAULT_COMPRESSION_LEVEL),
        );

        let _ = Repository::open(backend, secure_storage.clone())?;

        Ok(())
    }

    /// Test generation of master keys
    #[test]
    fn test_generate_key() -> Result<()> {
        let (key, keyfile) = repository::generate_key("mapachito")?;

        let salt = general_purpose::STANDARD.decode(keyfile.salt)?;
        let encrypted_key = general_purpose::STANDARD.decode(keyfile.encrypted_key)?;

        let intermediate_key = SecureStorage::derive_key("mapachito", &salt);
        let decrypted_key = SecureStorage::decrypt_with_key(&intermediate_key, &encrypted_key)?;

        assert_eq!(key, decrypted_key);

        Ok(())
    }
}
