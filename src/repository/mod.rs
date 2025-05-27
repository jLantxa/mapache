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

pub mod index;
pub mod manifest;
pub mod packer;
pub mod repository_v1;
pub mod snapshot;
pub mod storage;
pub mod tree;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use aes_gcm::aead::OsRng;
use aes_gcm::aead::rand_core::RngCore;
use anyhow::{Context, Result, bail};
use base64::Engine;
use chrono::{DateTime, Utc};
use index::IndexFile;
use manifest::Manifest;
use serde::{Deserialize, Serialize};
use snapshot::Snapshot;

use crate::global::FileType;
use crate::ui;
use crate::{
    backend::StorageBackend, global::ID, global::ObjectType, repository::storage::SecureStorage,
};

pub type RepoVersion = u32;
pub const LATEST_REPOSITORY_VERSION: RepoVersion = 1;

pub trait RepositoryBackend: Sync + Send {
    /// Create and initialize a new repository
    fn init(backend: Arc<dyn StorageBackend>, password: String) -> Result<()>
    where
        Self: Sized;

    /// Open an existing repository from a directory
    fn open(backend: Arc<dyn StorageBackend>, secure_storage: Arc<SecureStorage>) -> Result<Self>
    where
        Self: Sized;

    /// Saves an object type to the repository
    fn save_object(&self, data: Vec<u8>) -> Result<(ID, u64, u64)>;

    /// Loads an object file from the repository.
    fn load_object(&self, id: &ID) -> Result<Vec<u8>>;

    /// Saves a blob in the repository. This blob can be packed with other blobs in an object file.
    /// Returns a tuple (uncompressed size, encoded_size, object idfn save_blob(&self, object_type: ObjectType, data: Vec<u8>) -> Result<(u64, u64, ID)>;
    fn save_blob(&self, object_type: ObjectType, data: Vec<u8>) -> Result<(ID, u64, u64)>;

    /// Loads a blob from the repository.
    fn load_blob(&self, id: &ID) -> Result<Vec<u8>>;

    /// Saves a snapshot metadata.
    fn save_snapshot(&self, snapshot: &Snapshot) -> Result<(ID, u64, u64)>;

    /// Removes a snapshot from the repository, if it exists.
    fn remove_snapshot(&self, id: &ID) -> Result<()>;

    /// Get a snapshot by hash
    fn load_snapshot(&self, id: &ID) -> Result<Snapshot>;

    /// Get all snapshots in the repository
    fn load_all_snapshots(&self) -> Result<Vec<(ID, Snapshot)>>;

    /// Get all snapshots in the repository, sorted by datetime.
    fn load_all_snapshots_sorted(&self) -> Result<Vec<(ID, Snapshot)>>;

    /// Saves an IndexFile into the repository
    fn save_index(&self, index: IndexFile) -> Result<(u64, u64)>;

    fn load_index(&self, id: &ID) -> Result<IndexFile>;

    fn load_manifest(&self) -> Result<Manifest>;

    /// Loads a KeyFile.
    fn load_key(&self, id: &ID) -> Result<KeyFile>;

    /// Flushes all pending data and saves it.
    fn flush(&self) -> Result<(u64, u64)>;

    /// Finds a file in the repository using an ID prefix
    fn find(&self, file_type: FileType, prefix: &String) -> Result<(ID, PathBuf)>;
}

pub fn init(backend: Arc<dyn StorageBackend>, password: String) -> Result<()> {
    init_repository_with_version(LATEST_REPOSITORY_VERSION, backend, password)
}

pub fn init_repository_with_version(
    version: RepoVersion,
    backend: Arc<dyn StorageBackend>,
    password: String,
) -> Result<()> {
    if version == 1 {
        repository_v1::Repository::init(backend, password)
    } else {
        bail!("Invalid repository version \'{}\'", version);
    }
}
pub fn open(
    backend: Arc<dyn StorageBackend>,
    secure_storage: Arc<SecureStorage>,
) -> Result<Box<dyn RepositoryBackend>> {
    if !backend.root_exists() {
        bail!("Could not open a repository. The path does not exist.");
    }

    let manifest_path = Path::new("manifest");

    let manifest = backend
        .read(&manifest_path)
        .with_context(|| "Could not load manifest file")?;
    let manifest = secure_storage
        .decode(&manifest)
        .with_context(|| "Could not decode the manifest file")?;
    let manifest: Manifest = serde_json::from_slice(&manifest)?;

    let version = manifest.version;

    open_repository_with_version(version, backend, secure_storage)
}

fn open_repository_with_version(
    version: RepoVersion,
    backend: Arc<dyn StorageBackend>,

    secure_storage: Arc<SecureStorage>,
) -> Result<Box<dyn RepositoryBackend>> {
    if version == 1 {
        let repo_v1 = repository_v1::Repository::open(backend, secure_storage)?;
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
        encrypted_key: base64::engine::general_purpose::STANDARD.encode(encrypted_key),
        salt: base64::engine::general_purpose::STANDARD.encode(salt),
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
            ui::cli::log_warning(&format!(
                "Extraneous item \'{}\' in keys directory is not a file",
                path.display()
            ));
            continue;
        }

        // Load keyfile
        let keyfile_str = backend.read(&path)?;
        let keyfile_str = SecureStorage::decompress(&keyfile_str)?;
        let keyfile: KeyFile = serde_json::from_slice(keyfile_str.as_slice())?;

        // Encode salt and key in base64
        let salt = base64::engine::general_purpose::STANDARD.decode(keyfile.salt)?;
        let encrypted_key =
            base64::engine::general_purpose::STANDARD.decode(keyfile.encrypted_key)?;

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

    use crate::backend::localfs::LocalFS;

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
