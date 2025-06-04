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

use anyhow::{Context, Result, bail};
use chrono::Utc;

use crate::{
    backend::StorageBackend,
    global::{self, FileType, ObjectType, SaveID},
    repository::{self, generate_new_master_key, packer::Packer, storage::SecureStorage},
    ui::{self, cli},
};

use super::{
    ID, KEYS_DIR, RepoVersion, RepositoryBackend,
    index::{Index, IndexFile, MasterIndex},
    manifest::Manifest,
    pack_saver::PackSaver,
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
    keys_path: PathBuf,

    secure_storage: Arc<SecureStorage>,

    // Packers.
    // By design, we pack blobs and trees separately so we can potentially cache trees
    // separately.
    max_packer_size: u64,
    data_packer: Arc<Mutex<Packer>>,
    tree_packer: Arc<Mutex<Packer>>,
    pack_saver: Arc<Mutex<Option<PackSaver>>>,

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
        let master_key = generate_new_master_key();
        let keyfile = repository::generate_key_file(&password, master_key.clone())
            .with_context(|| "Could not generate key")?;
        let keyfile_json = serde_json::to_string_pretty(&keyfile)?;
        let keyfile_id = ID::from_content(&keyfile_json);
        let keyfile_path = &keys_path.join(&keyfile_id.to_hex());
        backend.write(&keyfile_path, keyfile_json.as_bytes())?;

        let repo_id = ID::new_random();

        // Save new manifest
        let manifest = Manifest {
            version: REPO_VERSION,
            id: repo_id.clone(),
            created_time: timestamp,
        };

        let secure_storage: SecureStorage = SecureStorage::build()
            .with_key(master_key)
            .with_compression(zstd::DEFAULT_COMPRESSION_LEVEL);

        let manifest_path = Path::new("manifest");
        let manifest = serde_json::to_string_pretty(&manifest)?;
        let manifest = secure_storage.encode(manifest.as_bytes())?;
        backend.write(manifest_path, &manifest)?;

        ui::cli::log!("Created repo with id {}", repo_id.to_short_hex(5));

        Ok(())
    }

    /// Open an existing repository from a directory
    fn open(
        backend: Arc<dyn StorageBackend>,
        secure_storage: Arc<SecureStorage>,
    ) -> Result<Arc<Self>> {
        let objects_path = PathBuf::from(OBJECTS_DIR);
        let snapshot_path = PathBuf::from(SNAPSHOTS_DIR);
        let index_path = PathBuf::from(INDEX_DIR);
        let keys_path = PathBuf::from(KEYS_DIR);

        // Packer defaults
        let max_packer_size = global::defaults::MAX_PACK_SIZE;
        let pack_data_capacity = max_packer_size as usize;
        let pack_blob_capacity =
            (pack_data_capacity as u64).div_ceil(global::defaults::AVG_CHUNK_SIZE as u64) as usize;

        let data_packer = Arc::new(Mutex::new(Packer::with_capacity(
            pack_data_capacity,
            pack_blob_capacity,
        )));
        let tree_packer = Arc::new(Mutex::new(Packer::with_capacity(
            pack_data_capacity,
            pack_blob_capacity,
        )));

        let index = Arc::new(Mutex::new(MasterIndex::new()));

        let mut repo = Repository {
            backend,

            objects_path,
            snapshot_path,
            index_path,
            keys_path,

            secure_storage,

            max_packer_size,
            data_packer,
            tree_packer,
            pack_saver: Arc::new(Mutex::new(None)),

            index,
        };

        repo.warmup()?;

        Ok(Arc::new(repo))
    }

    fn save_blob(
        &self,
        object_type: ObjectType,
        data: Vec<u8>,
        save_id: SaveID,
    ) -> Result<(ID, u64, u64)> {
        let raw_size = data.len();
        let id = match save_id {
            SaveID::CalculateID => ID::from_content(&data),
            SaveID::WithID(id) => id,
        };

        let mut index_guard = self.index.lock().unwrap();
        let blob_exists = index_guard.contains(&id) || !index_guard.add_pending_blob(&id);
        drop(index_guard);

        // If the blob was already pending, return early, as we are finished here.
        if blob_exists {
            return Ok((id, 0, 0));
        }

        let data = self.secure_storage.encode(&data)?;
        let encoded_size = data.len();

        let mut packer_guard = match object_type {
            ObjectType::Data => self.data_packer.lock().unwrap(),
            ObjectType::Tree => self.tree_packer.lock().unwrap(),
        };

        packer_guard.add_blob(&id, data);

        // Flush if the packer is considered full
        if packer_guard.size() > self.max_packer_size {
            self.flush_packer(packer_guard)?;
        }

        Ok((id, raw_size as u64, encoded_size as u64))
    }

    fn load_blob(&self, id: &ID) -> Result<Vec<u8>> {
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

    fn save_snapshot(&self, snapshot: &Snapshot) -> Result<(ID, u64, u64)> {
        let snapshot_json = serde_json::to_string_pretty(snapshot)?;
        let snapshot_json = snapshot_json.as_bytes();
        let sn_raw_size = snapshot_json.len() as u64;
        let snapshot_id = ID::from_content(&snapshot_json);

        let snapshot_path = self.snapshot_path.join(&snapshot_id.to_hex());

        let snapshot_json = self.secure_storage.encode(snapshot_json)?;
        let sn_encoded_size = snapshot_json.len() as u64;

        self.save_with_rename(&snapshot_json, &snapshot_path)?;

        Ok((snapshot_id, sn_raw_size, sn_encoded_size))
    }

    fn remove_snapshot(&self, id: &ID) -> Result<()> {
        let snapshot_path = self.snapshot_path.join(id.to_hex());

        if !self.backend.exists(&snapshot_path) {
            bail!("Snapshot {} doesn't exist", id)
        }

        self.backend
            .remove_file(&snapshot_path)
            .with_context(|| format!("Could not remove snapshot {}", id))
    }

    fn load_snapshot(&self, id: &ID) -> Result<Snapshot> {
        let snapshot_path = self.snapshot_path.join(id.to_hex());
        if !self.backend.exists(&snapshot_path) {
            bail!(format!("No snapshot with ID \'{}\' exists", id));
        }

        let snapshot = self.backend.read(&snapshot_path)?;
        let snapshot = self.secure_storage.decode(&snapshot)?;
        let snapshot: Snapshot = serde_json::from_slice(&snapshot)?;
        Ok(snapshot)
    }

    fn list_snapshot_ids(&self) -> Result<Vec<ID>> {
        let mut ids = Vec::new();

        let paths = self
            .backend
            .read_dir(&self.snapshot_path)
            .with_context(|| "Could not read snapshots")?;

        for path in paths {
            if self.backend.is_file(&path) {
                if let Some(file_name) = path.file_name().and_then(|s| s.to_str()) {
                    ids.push(ID::from_hex(file_name)?);
                }
            }
        }

        Ok(ids)
    }

    fn save_index(&self, index: IndexFile) -> Result<(u64, u64)> {
        let index_file_json = serde_json::to_string_pretty(&index)?;
        let index_file_json = index_file_json.as_bytes();
        let uncompressed_size = index_file_json.len() as u64;

        let index_file_json = self.secure_storage.encode(index_file_json)?;
        let compressed_size = index_file_json.len() as u64;

        let id = ID::from_content(&index_file_json);
        let index_path = self.index_path.join(&id.to_hex());
        self.save_with_rename(&index_file_json, &index_path)?;

        Ok((uncompressed_size, compressed_size))
    }

    fn flush(&self) -> Result<(u64, u64)> {
        self.flush_packer(self.data_packer.lock().unwrap())?;
        self.flush_packer(self.tree_packer.lock().unwrap())?;

        self.index.lock().unwrap().save(self)
    }

    fn save_object(&self, data: Vec<u8>, save_id: SaveID) -> Result<(ID, u64, u64)> {
        let id = match save_id {
            SaveID::CalculateID => ID::from_content(&data),
            SaveID::WithID(id) => id,
        };

        let object_path = Self::get_object_path(&self.objects_path, &id);
        let uncompressed_size = data.len() as u64;

        let data = self.secure_storage.encode(&data)?;
        let compressed_size = data.len() as u64;
        self.save_with_rename(&data, &object_path)?;

        Ok((id, uncompressed_size, compressed_size))
    }

    fn load_object(&self, id: &ID) -> Result<Vec<u8>> {
        let object_path = Self::get_object_path(&self.objects_path, id);
        let data = self.backend.read(&object_path)?;
        self.secure_storage.decode(&data)
    }

    fn load_index(&self, id: &ID) -> Result<IndexFile> {
        let index_path = self.index_path.join(&id.to_hex());
        let index = self.backend.read(&index_path)?;
        let index: Vec<u8> = self.secure_storage.decode(&index)?;
        let index = serde_json::from_slice(&index)?;
        Ok(index)
    }

    fn load_manifest(&self) -> Result<Manifest> {
        let manifest = self.backend.read(&Path::new("manifest"))?;
        let manifest = self.secure_storage.decode(&manifest)?;
        let manifest = serde_json::from_slice(&manifest)?;
        Ok(manifest)
    }

    fn load_key(&self, id: &ID) -> Result<repository::KeyFile> {
        let key_path = self.keys_path.join(&id.to_hex());
        let key = self.backend.read(&key_path)?;
        let key = SecureStorage::decompress(&key)?;
        let key = serde_json::from_slice(&key)?;
        Ok(key)
    }

    fn find(&self, file_type: FileType, prefix: &String) -> Result<(ID, PathBuf)> {
        if prefix.len() > 2 * global::ID_LENGTH {
            // A hex string has 2 characters per byte.
            bail!(
                "Invalid prefix length. The prefix must not be longer than the ID ({} chars)",
                2 * global::ID_LENGTH
            );
        } else if prefix.is_empty() {
            // Although it is technically posible to use an empty prefix, which would find a match
            // if only one file of the type exists. let's consider this invalid as it can be
            // potentially ambiguous or lead to errors.
            bail!("Prefix cannot be empty");
        }

        let type_files = self.list_files(file_type)?;
        let mut matches = Vec::new();

        for file_path in type_files {
            let filename = match file_path.file_name() {
                Some(os_str) => os_str.to_string_lossy().into_owned(),
                None => bail!("Failed to list file for type {}", file_type),
            };

            if !filename.starts_with(prefix) {
                continue;
            }

            if matches.is_empty() {
                matches.push((filename, file_path));
            } else {
                bail!("Prefix {} is ambiguous", prefix);
            }
        }

        if matches.is_empty() {
            bail!(
                "File type {} with prefix {} doesn't exist",
                file_type,
                prefix
            );
        }

        let (filename, filepath) = matches.pop().unwrap();
        let id = ID::from_hex(&filename)?;

        Ok((id, filepath))
    }

    fn init_pack_saver(&self, concurrency: usize) {
        let backend = self.backend.clone();
        let objects_path = self.objects_path.clone();

        let pack_saver = PackSaver::new(
            concurrency,
            Arc::new(move |data, id| {
                let path = Self::get_object_path(&objects_path, &id);
                if let Err(e) = backend.write(&path, &data) {
                    cli::log_error(&format!("Could not save pack {}: {}", id.to_hex(), e));
                }
            }),
        );
        self.pack_saver.lock().unwrap().replace(pack_saver);
    }
}

impl Drop for Repository {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

impl Repository {
    /// Returns the path to an object with a given hash in the repository.
    fn get_object_path(objects_path: &Path, id: &ID) -> PathBuf {
        let id_hex = id.to_hex();
        objects_path
            .join(&id_hex[..OBJECTS_DIR_FANOUT])
            .join(&id_hex)
    }

    /// Lists all paths belonging to a file type (objects, snapshots, indexes, etc.).
    fn list_files(&self, file_type: FileType) -> Result<Vec<PathBuf>> {
        match file_type {
            FileType::Snapshot => self.backend.read_dir(&self.snapshot_path),
            FileType::Key => self.backend.read_dir(&self.keys_path),
            FileType::Index => self.backend.read_dir(&self.index_path),
            FileType::Manifest => Ok(vec![PathBuf::from("manifest")]),
            FileType::Object => {
                let mut files = Vec::new();
                for n in 0x00..(1 << (4 * OBJECTS_DIR_FANOUT)) {
                    let dir_name = self
                        .objects_path
                        .join(format!("{:0>OBJECTS_DIR_FANOUT$x}", n));

                    let sub_files = self.backend.read_dir(&dir_name)?;
                    for f in sub_files.into_iter() {
                        let object_file_path = self.objects_path.join(&dir_name).join(f);
                        files.push(object_file_path);
                    }
                }

                Ok(files)
            }
        }
    }

    fn save_with_rename(&self, data: &[u8], path: &Path) -> Result<usize> {
        let tmp_path = path.with_extension("tmp");
        self.backend.write(&tmp_path, data)?;
        self.backend.rename(&tmp_path, path)?;
        Ok(data.len())
    }

    fn flush_packer(&self, mut packer_guard: MutexGuard<Packer>) -> Result<()> {
        let (pack_data, packed_blob_descriptors, pack_id) = packer_guard.flush();
        drop(packer_guard);

        let pack_saver_guard = self.pack_saver.lock().unwrap();
        if let Some(pack_saver) = pack_saver_guard.as_ref() {
            pack_saver.save_pack(pack_data)?;
            drop(pack_saver_guard);

            let mut index_guard = self.index.lock().unwrap();
            index_guard.add_pack(&pack_id, packed_blob_descriptors);
            drop(index_guard);
        } else {
            bail!("PackSaver is not initialized. Call `init_pack_saver` first.");
        }

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

    fn load_from_pack(&self, id: &ID, offset: u64, length: u64) -> Result<Vec<u8>> {
        let object_path = Self::get_object_path(&self.objects_path, id);
        let data = self.backend.seek_read(&object_path, offset, length)?;
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
        repository::{self, retrieve_master_key},
    };

    use super::*;

    /// Test init a repository_v1 with password and open it
    #[test]
    fn test_init_and_open_with_password() -> Result<()> {
        let temp_repo_dir = tempdir()?;
        let temp_repo_path = temp_repo_dir.path().join("repo");

        let backend = Arc::new(LocalFS::new(temp_repo_path.to_owned()));

        Repository::init(backend.to_owned(), String::from("mapachito"))?;

        let key = retrieve_master_key(String::from("mapachito"), None, backend.clone())?;
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
    fn test_generate_key_file() -> Result<()> {
        let master_key = generate_new_master_key();
        let keyfile = repository::generate_key_file("mapachito", master_key.clone())?;

        let salt = general_purpose::STANDARD.decode(keyfile.salt)?;
        let encrypted_key = general_purpose::STANDARD.decode(keyfile.encrypted_key)?;

        let intermediate_key = SecureStorage::derive_key("mapachito", &salt);
        let decrypted_key = SecureStorage::decrypt_with_key(&intermediate_key, &encrypted_key)?;

        assert_eq!(master_key, decrypted_key.as_slice());

        Ok(())
    }
}
