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
    fs::OpenOptions,
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, bail};

use crate::{
    backend::StorageBackend,
    repository::storage::SecureStorage,
    utils::{self, Hash},
};

use super::{
    config::Config,
    repository::{self, ObjectId, RepoVersion, RepositoryBackend, SnapshotId},
    snapshot::Snapshot,
    tree::{Node, NodeType},
};

const REPO_VERSION: RepoVersion = 1;

const OBJECTS_DIR: &str = "objects";
const SNAPSHOTS_DIR: &str = "snapshots";

const OBJECTS_DIR_FANOUT: usize = 2;

pub struct Repository {
    backend: Arc<dyn StorageBackend>,
    objects_path: PathBuf,
    snapshot_path: PathBuf,
    secure_storage: Arc<SecureStorage>,
}

impl RepositoryBackend for Repository {
    /// Create and initialize a new repository
    fn init(backend: Arc<dyn StorageBackend>, password: String) -> Result<()> {
        if backend.root_exists() {
            bail!("Could not initialize a repository because a directory already exists");
        }

        // Init repository structure
        let objects_path = PathBuf::from(OBJECTS_DIR);
        let snapshot_path = PathBuf::from(SNAPSHOTS_DIR);
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

        // Save new config
        let config = Config {
            version: REPO_VERSION,
        };

        let secure_storage: SecureStorage = SecureStorage::build()
            .with_key(key)
            .with_compression(zstd::DEFAULT_COMPRESSION_LEVEL);

        let config_path = Path::new("config");
        let config = serde_json::to_string_pretty(&config)?;
        let config = secure_storage.encode(config.as_bytes())?;
        backend.write(config_path, &config)?;

        Ok(())
    }

    /// Open an existing repository from a directory
    fn open(backend: Arc<dyn StorageBackend>, secure_storage: Arc<SecureStorage>) -> Result<Self> {
        let objects_path = PathBuf::from(OBJECTS_DIR);
        let snapshot_path = PathBuf::from(SNAPSHOTS_DIR);

        Ok(Repository {
            backend,
            objects_path,
            snapshot_path,
            secure_storage,
        })
    }

    fn save_object(&self, data: &[u8]) -> Result<(usize, ObjectId)> {
        let encoded_data = self.secure_storage.encode(data)?;
        self.save_object_raw(&encoded_data)
    }

    fn save_object_raw(&self, data: &[u8]) -> Result<(usize, ObjectId)> {
        let hash = utils::calculate_hash(&data);
        let object_path = self.get_object_path(&hash);

        let written_size = if !object_path.exists() {
            self.save_with_rename(&data, &self.get_object_path(&hash))?
        } else {
            0
        };

        Ok((written_size, hash))
    }

    fn load_object(&self, id: &ObjectId) -> Result<Vec<u8>> {
        let data = self.backend.read(&self.get_object_path(id))?;
        self.secure_storage.decode(&data)
    }

    fn load_object_raw(&self, id: &ObjectId) -> Result<Vec<u8>> {
        self.backend.read(&self.get_object_path(id))
    }

    fn save_snapshot(&self, snapshot: &Snapshot) -> Result<SnapshotId> {
        let snapshot_json = serde_json::to_string_pretty(snapshot)?;
        let hash = utils::calculate_hash(&snapshot_json);

        let snapshot_path = self.snapshot_path.join(&hash);

        let snapshot_json = self.secure_storage.encode(snapshot_json.as_bytes())?;
        self.save_with_rename(&snapshot_json, &snapshot_path)?;

        Ok(hash)
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
    fn load_all_snapshots(&self) -> Result<Vec<(Hash, Snapshot)>> {
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
    fn load_all_snapshots_sorted(&self) -> Result<Vec<(Hash, Snapshot)>> {
        let mut snapshots = self.load_all_snapshots()?;
        snapshots.sort_by_key(|(_, snapshot)| snapshot.timestamp);
        Ok(snapshots)
    }

    fn restore_node(&self, node: &Node, dst_path: &Path) -> Result<()> {
        match node.node_type {
            NodeType::File => {
                // TODO: Restore metadata
                let mut dst_file = OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .open(dst_path)
                    .with_context(|| {
                        format!("Could not create destination file '{}'", dst_path.display())
                    })?;

                let chunks = node
                    .contents
                    .as_ref()
                    .expect("File Node must have contents (even if empty)");

                for (index, chunk_hash) in chunks.iter().enumerate() {
                    let chunk_data = self.load_object(&chunk_hash).with_context(|| {
                        format!(
                            "Could not load chunk #{} ({}) for restoring file '{}'",
                            index + 1,
                            chunk_hash,
                            dst_path.display()
                        )
                    })?;

                    dst_file.write_all(&chunk_data).with_context(|| {
                        format!(
                            "Could not restore chunk #{} ({}) to file '{}'",
                            index + 1,
                            chunk_hash,
                            dst_path.display()
                        )
                    })?;

                    if let Some(mtime) = node.metadata.modified_time {
                        dst_file.set_modified(mtime)?;
                    }
                }
            }
            NodeType::Directory => {
                // TODO: Restore metadata
                std::fs::create_dir_all(dst_path)?
            }
            NodeType::Symlink => todo!(),
        }

        Ok(())
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
}

#[cfg(test)]
mod test {

    use base64::{Engine, engine::general_purpose};
    use tempfile::tempdir;

    use crate::{backend::localfs::LocalFS, repository::repository::retrieve_key};

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
