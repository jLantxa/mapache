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

use crate::{
    archiver::storage::SecureStorage, storage_backend::backend::StorageBackend, utils::Hash,
};

use super::config::Config;
use super::snapshot::Snapshot;
use super::tree::Tree;
use super::{repository_v1, tree};

pub type RepoVersion = u32;
pub const LATEST_REPOSITORY_VERSION: RepoVersion = 1;

pub type ObjectId = Hash;
pub type SnapshotId = Hash;

#[derive(Debug)]
pub struct ChunkResult {
    pub chunks: Vec<Hash>,
    pub total_bytes_read: usize,
    pub total_bytes_written: usize,
}

pub trait RepositoryBackend: Sync + Send {
    /// Create and initialize a new repository
    fn init(
        storage_backend: Arc<dyn StorageBackend>,
        repo_path: &Path,
        password: String,
    ) -> Result<()>
    where
        Self: Sized;

    /// Open an existing repository from a directory
    fn open(
        storage_backend: Arc<dyn StorageBackend>,
        repo_path: &Path,
        secure_storage: Arc<SecureStorage>,
    ) -> Result<Self>
    where
        Self: Sized;

    fn save_file(&self, src_path: &Path, dry_run: bool) -> Result<ChunkResult>;

    fn restore_node(&self, file: &tree::Node, dst_path: &Path) -> Result<()>;

    /// Serializes a Tree into SerializableTreeObject's into the repository storage.
    fn save_tree(&self, tree: &Tree, dry_run: bool) -> Result<ObjectId>;

    /// Restores a Tree from the SerializableTreeObject's in the repository.
    fn load_tree(&self, root_hash: &ObjectId) -> Result<Tree>;

    /// Saves a snapshot metadata
    fn save_snapshot(&self, snapshot: &Snapshot, dry_run: bool) -> Result<Hash>;

    /// Get a snapshot by hash
    fn load_snapshot(&self, hash: &SnapshotId) -> Result<Option<(SnapshotId, Snapshot)>>;

    /// Get all snapshots in the repository
    fn load_snapshots(&self) -> Result<Vec<(SnapshotId, Snapshot)>>;

    /// Get all snapshots in the repository, sorted by datetime.
    fn load_snapshots_sorted(&self) -> Result<Vec<(SnapshotId, Snapshot)>>;
}

pub fn init(
    storage_backend: Arc<dyn StorageBackend>,
    repo_path: &Path,
    password: String,
) -> Result<()> {
    init_repository_with_version(
        LATEST_REPOSITORY_VERSION,
        storage_backend,
        repo_path,
        password,
    )
}

pub fn init_repository_with_version(
    version: RepoVersion,
    storage_backend: Arc<dyn StorageBackend>,
    repo_path: &Path,
    password: String,
) -> Result<()> {
    if repo_path.exists() {
        bail!(
            "Could not initialize a repository because a directory already exists in \'{}\'",
            repo_path.display()
        );
    }

    if version == 1 {
        repository_v1::Repository::init(storage_backend, repo_path, password)
    } else {
        bail!("Invalid repository version \'{}\'", version);
    }
}

pub fn open(
    storage_backend: Arc<dyn StorageBackend>,
    repo_path: &Path,
    secure_storage: Arc<SecureStorage>,
) -> Result<Box<dyn RepositoryBackend>> {
    if !repo_path.exists() {
        bail!(
            "Could not open a repository. \'{}\' doesn't exist",
            repo_path.display()
        );
    } else if !repo_path.is_dir() {
        bail!(
            "Could not open a repository. \'{}\' is not a directory",
            repo_path.display()
        );
    }

    let config_path = repo_path.join("config");
    let config: Config = serde_json::from_slice(
        &secure_storage
            .load_file(&config_path)
            .with_context(|| "Could not load config file")?,
    )?;
    let version = config.version;

    open_repository_with_version(version, storage_backend, repo_path, secure_storage)
}

fn open_repository_with_version(
    version: RepoVersion,
    storage_backend: Arc<dyn StorageBackend>,
    repo_path: &Path,
    secure_storage: Arc<SecureStorage>,
) -> Result<Box<dyn RepositoryBackend>> {
    if version == 1 {
        let repo_v1 = repository_v1::Repository::open(storage_backend, repo_path, secure_storage)?;
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

    let encrypted_key = SecureStorage::encrypt(&intermediate_key, &new_random_key)?;

    let key_file = KeyFile {
        created: create_time,
        encrypted_key: general_purpose::STANDARD.encode(encrypted_key),
        salt: general_purpose::STANDARD.encode(salt),
    };

    Ok((new_random_key.to_vec(), key_file))
}

/// Retrieve the master key from all available keys in a folder
pub fn retrieve_key(
    password: String,
    backend: Arc<dyn StorageBackend>,
    repo_path: &Path,
) -> Result<Vec<u8>> {
    let keys_path = repo_path.join(KEYS_DIR);
    for path in backend.read_dir(&keys_path)? {
        // TODO:
        // I should assert that path is a file and not a folder, but I need to implement
        // that in the StorageBackend. For now, let's assume that nobody is messing with
        // the repository.

        // Load keyfile
        let keyfile_str = backend.read(&path)?;
        let keyfile_str = SecureStorage::decompress(&keyfile_str)?;
        let keyfile: KeyFile = serde_json::from_slice(keyfile_str.as_slice())?;

        let salt = general_purpose::STANDARD.decode(keyfile.salt)?;
        let encrypted_key = general_purpose::STANDARD.decode(keyfile.encrypted_key)?;

        let intermediate_key = SecureStorage::derive_key(&password, &salt);
        if let Ok(key) = SecureStorage::decrypt(&intermediate_key, &encrypted_key) {
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

        let backend = Arc::new(LocalFS::new());

        init(
            backend.to_owned(),
            &temp_repo_path,
            String::from("mapachito"),
        )?;

        let key = retrieve_key(String::from("mapachito"), backend.clone(), &temp_repo_path)?;
        let secure_storage = Arc::new(
            SecureStorage::new(backend.clone())
                .with_key(key)
                .with_compression(zstd::DEFAULT_COMPRESSION_LEVEL),
        );

        let _ = open(backend, &temp_repo_path, secure_storage.clone())?;

        Ok(())
    }
}
