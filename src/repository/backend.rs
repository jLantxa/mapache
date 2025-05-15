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

use std::path::Path;
use std::sync::Arc;

use aes_gcm::aead::OsRng;
use aes_gcm::aead::rand_core::RngCore;
use anyhow::{Context, Result, bail};
use base64::Engine;
use base64::engine::general_purpose;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::cli;
use crate::{
    repository::storage::SecureStorage, storage_backend::backend::StorageBackend, utils::Hash,
};

use super::config::Config;
use super::snapshot::Snapshot;
use super::{repository_v1, tree};

pub type RepoVersion = u32;
pub const LATEST_REPOSITORY_VERSION: RepoVersion = 1;

pub type ObjectId = Hash;
pub type SnapshotId = Hash;

pub trait RepositoryBackend: Sync + Send {
    /// Create and initialize a new repository
    fn init(storage_backend: Arc<dyn StorageBackend>, password: String) -> Result<()>
    where
        Self: Sized;

    /// Open an existing repository from a directory
    fn open(
        storage_backend: Arc<dyn StorageBackend>,
        secure_storage: Arc<SecureStorage>,
    ) -> Result<Self>
    where
        Self: Sized;

    /// Restores a node in the local filesystem
    fn restore_node(&self, file: &tree::Node, dst_path: &Path) -> Result<()>;

    /// Saves a binary object in the repository and encodes (compress + encrypt) the content.
    /// The hash is calculated on the raw data before encoding.
    fn save_object(&self, data: &[u8]) -> Result<(usize, ObjectId)>;

    /// Saves a binary object in the repository without using the SecureStorage.
    fn save_object_raw(&self, data: &[u8]) -> Result<(usize, ObjectId)>;

    /// Loads a binary object from the repository
    fn load_object(&self, id: &ObjectId) -> Result<Vec<u8>>;

    /// Loads a binary object from the repository without using the SecureStorage.
    fn load_object_raw(&self, id: &ObjectId) -> Result<Vec<u8>>;

    /// Saves a snapshot metadata
    fn save_snapshot(&self, snapshot: &Snapshot) -> Result<Hash>;

    /// Get a snapshot by hash
    fn load_snapshot(&self, hash: &SnapshotId) -> Result<Snapshot>;

    /// Get all snapshots in the repository
    fn load_all_snapshots(&self) -> Result<Vec<(SnapshotId, Snapshot)>>;

    /// Get all snapshots in the repository, sorted by datetime.
    fn load_all_snapshots_sorted(&self) -> Result<Vec<(SnapshotId, Snapshot)>>;
}

pub fn init(storage_backend: Arc<dyn StorageBackend>, password: String) -> Result<()> {
    init_repository_with_version(LATEST_REPOSITORY_VERSION, storage_backend, password)
}

pub fn init_repository_with_version(
    version: RepoVersion,
    storage_backend: Arc<dyn StorageBackend>,
    password: String,
) -> Result<()> {
    if version == 1 {
        repository_v1::Repository::init(storage_backend, password)
    } else {
        bail!("Invalid repository version \'{}\'", version);
    }
}
pub fn open(
    storage_backend: Arc<dyn StorageBackend>,
    secure_storage: Arc<SecureStorage>,
) -> Result<Box<dyn RepositoryBackend>> {
    if !storage_backend.root_exists() {
        bail!("Could not open a repository. The path does not exist.");
    }

    let config_path = Path::new("config");

    let config = storage_backend
        .read(&config_path)
        .with_context(|| "Could not load config file")?;
    let config = secure_storage
        .decode(&config)
        .with_context(|| "Could not decode the config file")?;
    let config: Config = serde_json::from_slice(&config)?;

    let version = config.version;

    open_repository_with_version(version, storage_backend, secure_storage)
}

fn open_repository_with_version(
    version: RepoVersion,
    storage_backend: Arc<dyn StorageBackend>,

    secure_storage: Arc<SecureStorage>,
) -> Result<Box<dyn RepositoryBackend>> {
    if version == 1 {
        let repo_v1 = repository_v1::Repository::open(storage_backend, secure_storage)?;
        return Ok(Box::new(repo_v1));
    }

    bail!("Invalid repository version \'{}\'", version);
}

/// A metadata structure that contains information about a repository key
#[derive(Serialize, Deserialize)]
pub struct KeyFile {
    pub created: DateTime<Utc>,
    pub encrypted_key: String,
    pub salt: String,
}

pub const KEYS_DIR: &str = "keys";

/// Generate a new master  key
pub fn generate_key(password: &str) -> Result<(Vec<u8>, KeyFile)> {
    let create_time = Utc::now();

    let mut new_random_key = [0u8; 32];
    OsRng.fill_bytes(&mut new_random_key);

    const SALT_LENGTH: usize = 32;
    let salt = SecureStorage::generate_salt::<SALT_LENGTH>();
    let intermediate_key = SecureStorage::derive_key(password, &salt);

    let encrypted_key = SecureStorage::encrypt_with_key(&intermediate_key, &new_random_key)?;

    let key_file = KeyFile {
        created: create_time,
        encrypted_key: general_purpose::STANDARD.encode(encrypted_key),
        salt: general_purpose::STANDARD.encode(salt),
    };

    Ok((new_random_key.to_vec(), key_file))
}

/// Retrieve the master key from all available keys in a folder
pub fn retrieve_key(password: String, backend: Arc<dyn StorageBackend>) -> Result<Vec<u8>> {
    let keys_path = Path::new(KEYS_DIR);
    for path in backend.read_dir(&keys_path)? {
        // The keys directory should only contain files. We can ignore anything
        // that is not a file, but show a warning anyway.
        if !backend.is_file(&path) {
            cli::log_warning(&format!(
                "Extraneous item \'{}\' in keys directory is not a file",
                path.display()
            ));
            continue;
        }

        // Load keyfile
        let keyfile_str = backend.read(&path)?;
        let keyfile_str = SecureStorage::decompress(&keyfile_str)?;
        let keyfile: KeyFile = serde_json::from_slice(keyfile_str.as_slice())?;

        let salt = general_purpose::STANDARD.decode(keyfile.salt)?;
        let encrypted_key = general_purpose::STANDARD.decode(keyfile.encrypted_key)?;

        let intermediate_key = SecureStorage::derive_key(&password, &salt);
        if let Ok(key) = SecureStorage::decrypt_with_key(&intermediate_key, &encrypted_key) {
            return Ok(key);
        }
    }

    bail!("Could not retrieve key")
}

#[cfg(test)]
mod test {
    use tempfile::tempdir;

    use crate::storage_backend::localfs::LocalFS;

    use super::*;

    /// Test init a repo with password and open it
    #[test]
    fn test_init_and_open_with_password() -> Result<()> {
        let temp_repo_dir = tempdir()?;
        let temp_repo_path = temp_repo_dir.path().join("repo");

        let backend = Arc::new(LocalFS::new(temp_repo_path.to_owned()));

        init(backend.to_owned(), String::from("mapachito"))?;

        let key = retrieve_key(String::from("mapachito"), backend.clone())?;
        let secure_storage = Arc::new(
            SecureStorage::build()
                .with_key(key)
                .with_compression(zstd::DEFAULT_COMPRESSION_LEVEL),
        );

        let _ = open(backend, secure_storage.clone())?;

        Ok(())
    }
}
