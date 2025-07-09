// mapache is an incremental backup tool
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
    collections::HashSet,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, bail};
use chrono::Utc;
use parking_lot::RwLock;

use crate::{
    backend::StorageBackend,
    global::{self, BlobType, FileType, SaveID},
    repository::{
        MANIFEST_PATH,
        packer::{PackSaver, Packer},
        storage::SecureStorage,
    },
    ui::{self, cli},
};

use super::{
    ID, KEYS_DIR, RepoVersion, RepositoryBackend,
    index::{Index, IndexFile, MasterIndex},
    keys,
    manifest::Manifest,
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
    data_packer: Arc<RwLock<Packer>>,
    tree_packer: Arc<RwLock<Packer>>,
    pack_saver: Arc<RwLock<Option<PackSaver>>>,

    index: Arc<RwLock<MasterIndex>>,
}

impl RepositoryBackend for Repository {
    /// Create and initialize a new repository
    fn init(backend: Arc<dyn StorageBackend>, secure_storage: Arc<SecureStorage>) -> Result<()> {
        let timestamp = Utc::now();

        let repo_id = ID::new_random();

        // Init repository structure
        let objects_path = PathBuf::from(OBJECTS_DIR);
        let snapshot_path = PathBuf::from(SNAPSHOTS_DIR);
        let index_path = PathBuf::from(INDEX_DIR);

        // Save new manifest
        let manifest = Manifest {
            version: REPO_VERSION,
            id: repo_id.clone(),
            created_time: timestamp,
        };

        let manifest_path = Path::new(MANIFEST_PATH);
        let manifest = serde_json::to_string_pretty(&manifest)?;
        let manifest = secure_storage.encode(manifest.as_bytes())?;
        backend.write(manifest_path, &manifest)?;

        backend.create_dir(&objects_path)?;
        let num_folders: usize = 1 << (4 * OBJECTS_DIR_FANOUT);
        for n in 0x00..num_folders {
            backend.create_dir(&objects_path.join(format!("{n:0>OBJECTS_DIR_FANOUT$x}")))?;
        }

        backend.create_dir(&snapshot_path)?;
        backend.create_dir(&index_path)?;

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

        // Packer defaults
        let max_packer_size = global::defaults::MAX_PACK_SIZE;
        let pack_data_capacity = max_packer_size as usize;
        let pack_blob_capacity =
            (pack_data_capacity as u64).div_ceil(global::defaults::AVG_CHUNK_SIZE) as usize;

        let data_packer = Arc::new(RwLock::new(Packer::with_capacity(
            pack_data_capacity,
            pack_blob_capacity,
        )));
        let tree_packer = Arc::new(RwLock::new(Packer::with_capacity(
            pack_data_capacity,
            pack_blob_capacity,
        )));

        let index = Arc::new(RwLock::new(MasterIndex::new()));

        let mut repo = Repository {
            backend,
            objects_path,
            snapshot_path,
            index_path,
            keys_path: PathBuf::from(KEYS_DIR),
            secure_storage,
            max_packer_size,
            data_packer,
            tree_packer,
            pack_saver: Arc::new(RwLock::new(None)),
            index,
        };

        repo.load_master_index()?;

        Ok(Arc::new(repo))
    }

    fn save_blob(
        &self,
        object_type: BlobType,
        data: Vec<u8>,
        save_id: SaveID,
    ) -> Result<(ID, (u64, u64), (u64, u64))> {
        let raw_size = data.len() as u64;
        let id = match save_id {
            SaveID::CalculateID => ID::from_content(&data),
            SaveID::WithID(id) => id,
        };

        let mut index_wlock = self.index.write();
        let blob_exists = index_wlock.contains(&id) || !index_wlock.add_pending_blob(id.clone());
        drop(index_wlock);

        // If the blob was already pending, return early, as we are finished here.
        if blob_exists {
            return Ok((id, (0, 0), (0, 0)));
        }

        let data = self.secure_storage.encode(&data)?;
        let encoded_size = data.len() as u64;

        let packer = match object_type {
            BlobType::Data => &self.data_packer,
            BlobType::Tree => &self.tree_packer,
        };

        packer.write().add_blob(id.clone(), object_type, data);

        // Flush if the packer is considered full
        let packer_meta_size = if packer.read().size() > self.max_packer_size {
            self.flush_packer(packer)?
        } else {
            (0, 0)
        };

        Ok((id, (raw_size, encoded_size), packer_meta_size))
    }

    fn load_blob(&self, id: &ID) -> Result<Vec<u8>> {
        let blob_entry = self.index.read().get(id);
        match blob_entry {
            Some((pack_id, _blob_type, offset, length)) => {
                self.load_from_pack(&pack_id, offset, length)
            }
            None => bail!("Could not find blob {:?} in index", id),
        }
    }

    fn save_file(
        &self,
        file_type: FileType,
        data: &[u8],
        save_id: SaveID,
    ) -> Result<(ID, u64, u64)> {
        assert_ne!(file_type, FileType::Key);
        assert_ne!(file_type, FileType::Manifest);

        let raw_size = data.len() as u64;
        let id = match save_id {
            SaveID::CalculateID => ID::from_content(data),
            SaveID::WithID(id) => id,
        };

        let data = self.secure_storage.encode(data)?;
        let encoded_size = data.len() as u64;

        let path = self.get_path(file_type, &id);
        self.save_with_rename(&path, &data)?;

        Ok((id, raw_size, encoded_size))
    }

    fn load_file(&self, file_type: FileType, id: &ID) -> Result<Vec<u8>> {
        assert_ne!(file_type, FileType::Key);
        assert_ne!(file_type, FileType::Manifest);

        let path = self.get_path(file_type, id);
        let data = self.backend.read(&path)?;

        if file_type != FileType::Object {
            return self.secure_storage.decode(&data);
        }

        Ok(data)
    }

    fn delete_file(&self, file_type: FileType, id: &ID) -> Result<()> {
        assert_ne!(file_type, FileType::Key);
        assert_ne!(file_type, FileType::Manifest);

        let path = self.get_path(file_type, id);
        self.backend.remove_file(&path)
    }

    fn remove_snapshot(&self, id: &ID) -> Result<()> {
        let snapshot_path = self.snapshot_path.join(id.to_hex());

        if !self.backend.exists(&snapshot_path) {
            bail!("Snapshot {} doesn't exist", id)
        }

        self.backend
            .remove_file(&snapshot_path)
            .with_context(|| format!("Could not remove snapshot {id}"))
    }

    fn load_snapshot(&self, id: &ID) -> Result<Snapshot> {
        let snapshot = self
            .load_file(FileType::Snapshot, id)
            .with_context(|| format!("No snapshot with ID \'{id}\' exists"))?;
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

    fn flush(&self) -> Result<(u64, u64)> {
        let data_packer_meta_size = self.flush_packer(&self.data_packer)?;
        let tree_packer_meta_size = self.flush_packer(&self.tree_packer)?;

        let (index_raw_size, index_encoded_size) = self.index.write().save(self)?;

        Ok((
            data_packer_meta_size.1 + tree_packer_meta_size.0 + index_raw_size,
            data_packer_meta_size.1 + tree_packer_meta_size.1 + index_encoded_size,
        ))
    }

    fn load_object(&self, id: &ID) -> Result<Vec<u8>> {
        self.load_file(FileType::Object, id)
    }

    fn load_index(&self, id: &ID) -> Result<IndexFile> {
        let index: Vec<u8> = self
            .load_file(FileType::Index, id)
            .with_context(|| format!("Could not load index {}", id.to_hex()))?;
        let index = serde_json::from_slice(&index)?;
        Ok(index)
    }

    fn load_manifest(&self) -> Result<Manifest> {
        let manifest = self.backend.read(Path::new(MANIFEST_PATH))?;
        let manifest = self.secure_storage.decode(&manifest)?;
        let manifest = serde_json::from_slice(&manifest)?;
        Ok(manifest)
    }

    fn load_key(&self, id: &ID) -> Result<keys::KeyFile> {
        let key_path = self.keys_path.join(id.to_hex());
        let key = self.backend.read(&key_path)?;
        let key = SecureStorage::decompress(&key)?;
        let key = serde_json::from_slice(&key)?;
        Ok(key)
    }

    fn find(&self, file_type: FileType, prefix: &str) -> Result<(ID, PathBuf)> {
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
                    cli::error!("Could not save pack {}: {}", id.to_hex(), e);
                }
            }),
        );
        self.pack_saver.write().replace(pack_saver);
    }

    fn finalize_pack_saver(&self) {
        if let Some(pack_saver) = self.pack_saver.write().take() {
            pack_saver.finish();
        }
    }

    fn index(&self) -> Arc<RwLock<MasterIndex>> {
        self.index.clone()
    }

    fn read_from_file(
        &self,
        file_type: FileType,
        id: &ID,
        offset: u64,
        length: u64,
    ) -> Result<Vec<u8>> {
        assert_ne!(file_type, FileType::Key);
        assert_ne!(file_type, FileType::Manifest);

        let path = self.get_path(file_type, id);
        let data = self.backend.seek_read(&path, offset, length)?;
        self.secure_storage.decode(&data)
    }

    fn list_objects(&self) -> Result<HashSet<ID>> {
        let mut list = HashSet::new();

        let num_folders: usize = 1 << (4 * OBJECTS_DIR_FANOUT);
        for n in 0..num_folders {
            let dir = self
                .objects_path
                .join(format!("{n:0>OBJECTS_DIR_FANOUT$x}"));

            let files = self.backend.read_dir(&dir)?;
            for path in files {
                let filename = path.file_name().unwrap().to_string_lossy().to_string();
                if let Ok(id) = ID::from_hex(&filename) {
                    list.insert(id);
                }
            }
        }

        Ok(list)
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

    fn get_path(&self, file_type: FileType, id: &ID) -> PathBuf {
        let id_hex = id.to_hex();
        match file_type {
            FileType::Object => Self::get_object_path(&self.objects_path, id),
            FileType::Snapshot => self.snapshot_path.join(id_hex),
            FileType::Index => self.index_path.join(id_hex),
            FileType::Key => self.keys_path.join(id_hex),
            FileType::Manifest => PathBuf::from(MANIFEST_PATH),
        }
    }

    /// Lists all paths belonging to a file type (objects, snapshots, indexes, etc.).
    fn list_files(&self, file_type: FileType) -> Result<Vec<PathBuf>> {
        match file_type {
            FileType::Snapshot => self.backend.read_dir(&self.snapshot_path),
            FileType::Key => self.backend.read_dir(&self.keys_path),
            FileType::Index => self.backend.read_dir(&self.index_path),
            FileType::Manifest => Ok(vec![PathBuf::from(MANIFEST_PATH)]),
            FileType::Object => {
                let mut files = Vec::new();
                for n in 0x00..(1 << (4 * OBJECTS_DIR_FANOUT)) {
                    let dir_name = self
                        .objects_path
                        .join(format!("{n:0>OBJECTS_DIR_FANOUT$x}"));

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

    fn save_with_rename(&self, path: &Path, data: &[u8]) -> Result<usize> {
        let tmp_path = path.with_extension("tmp");
        self.backend.write(&tmp_path, data)?;
        self.backend.rename(&tmp_path, path)?;
        Ok(data.len())
    }

    fn flush_packer(&self, packer: &Arc<RwLock<Packer>>) -> Result<(u64, u64)> {
        match packer.write().flush(&self.secure_storage)? {
            None => Ok((0, 0)),
            Some(flushed_pack) => {
                if let Some(pack_saver) = self.pack_saver.write().as_ref() {
                    pack_saver
                        .save_pack(flushed_pack.data, SaveID::WithID(flushed_pack.id.clone()))?;
                } else {
                    bail!("PackSaver is not initialized. Call `init_pack_saver` first.");
                }

                let (index_raw, index_encoded) = self.index.write().add_pack(
                    self,
                    &flushed_pack.id,
                    flushed_pack.descriptors,
                )?;

                Ok((
                    flushed_pack.meta_size + index_raw,
                    flushed_pack.meta_size + index_encoded,
                ))
            }
        }
    }

    fn load_master_index(&mut self) -> Result<()> {
        let files = self.backend.read_dir(&self.index_path)?;
        let num_index_files = files.len();

        for file in files {
            let file_name = file
                .file_name()
                .expect("Could not read index file name")
                .to_string_lossy()
                .clone();
            let id = ID::from_hex(&file_name)?;
            let index_file = self.backend.read(&file)?;
            let index_file = self.secure_storage.decode(&index_file)?;
            let index_file = serde_json::from_slice(&index_file)?;

            let mut index = Index::from_index_file(index_file);
            index.finalize();
            index.set_id(id);

            self.index.write().add_index(index);
        }

        ui::cli::verbose_1!("Loaded {} index files", num_index_files);

        Ok(())
    }

    fn load_from_pack(&self, id: &ID, offset: u32, length: u32) -> Result<Vec<u8>> {
        let object_path = Self::get_object_path(&self.objects_path, id);
        let data = self
            .backend
            .seek_read(&object_path, offset as u64, length as u64)?;
        self.secure_storage.decode(&data)
    }
}

#[cfg(test)]
mod tests {}
