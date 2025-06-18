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
pub mod pack_saver;
pub mod packer;
pub mod repository_v1;
pub mod snapshot;
pub mod storage;
pub mod streamers;
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
use zstd::DEFAULT_COMPRESSION_LEVEL;

use crate::global::{FileType, SaveID};
use crate::{
    backend::StorageBackend, global::ID, global::ObjectType, repository::storage::SecureStorage,
};
use crate::{repository, ui};

pub type RepoVersion = u32;
pub const LATEST_REPOSITORY_VERSION: RepoVersion = 1;

pub trait RepositoryBackend: Sync + Send {
    /// Create and initialize a new repository
    fn init(backend: Arc<dyn StorageBackend>, secure_storage: Arc<SecureStorage>) -> Result<()>
    where
        Self: Sized;

    /// Open an existing repository from a directory
    fn open(
        backend: Arc<dyn StorageBackend>,
        secure_storage: Arc<SecureStorage>,
    ) -> Result<Arc<Self>>
    where
        Self: Sized;

    fn init_pack_saver(&self, concurrency: usize);

    fn finalize_pack_saver(&self);

    /// Saves an object type to the repository
    /// Returns a tuple (`ID`, raw_size, encoded_size)
    fn save_object(&self, data: Vec<u8>, id: SaveID) -> Result<(ID, u64, u64)>;

    /// Loads an object file from the repository.
    fn load_object(&self, id: &ID) -> Result<Vec<u8>>;

    /// Saves a blob in the repository. This blob can be packed with other blobs in an object file.
    /// Returns a tuple (`ID`, raw_size, encoded_size)
    fn save_blob(
        &self,
        object_type: ObjectType,
        data: Vec<u8>,
        id: SaveID,
    ) -> Result<(ID, u64, u64)>;

    /// Loads a blob from the repository.
    fn load_blob(&self, id: &ID) -> Result<Vec<u8>>;

    /// Saves a snapshot metadata.
    /// Returns a tuple (`ID`, raw_size, encoded_size)
    fn save_snapshot(&self, snapshot: &Snapshot) -> Result<(ID, u64, u64)>;

    /// Removes a snapshot from the repository, if it exists.
    fn remove_snapshot(&self, id: &ID) -> Result<()>;

    /// Get a snapshot by hash
    fn load_snapshot(&self, id: &ID) -> Result<Snapshot>;

    fn list_snapshot_ids(&self) -> Result<Vec<ID>>;

    /// Saves an IndexFile into the repository
    /// Returns a tuple (raw_size, encoded_size)
    fn save_index(&self, index: IndexFile) -> Result<(u64, u64)>;

    /// Loads an index file
    fn load_index(&self, id: &ID) -> Result<IndexFile>;

    /// Loads the repository manifest
    fn load_manifest(&self) -> Result<Manifest>;

    /// Loads a KeyFile.
    fn load_key(&self, id: &ID) -> Result<KeyFile>;

    /// Deletes a file from the repository
    fn delete_file(&self, file_type: FileType, id: &ID) -> Result<()>;

    /// Flushes all pending data and saves it.
    /// Returns a tuple (raw_size, encoded_size)
    fn flush(&self) -> Result<(u64, u64)>;

    /// Finds a file in the repository using an ID prefix
    fn find(&self, file_type: FileType, prefix: &String) -> Result<(ID, PathBuf)>;
}

/// Initialize a repository using the latest repository version.
/// This function prompts for a password to create a master key.
pub fn init(
    password: Option<String>,
    keyfile_path: Option<&PathBuf>,
    backend: Arc<dyn StorageBackend>,
) -> Result<()> {
    init_repository_with_version(password, keyfile_path, LATEST_REPOSITORY_VERSION, backend)
}

/// Initialize a repository with a version number.
/// This function prompts for a password to create a master key.
pub fn init_repository_with_version(
    password: Option<String>,
    keyfile_path: Option<&PathBuf>,
    version: RepoVersion,
    backend: Arc<dyn StorageBackend>,
) -> Result<()> {
    if version == 1 {
        let secure_storage = init_common(password, keyfile_path, backend.clone())?;
        repository_v1::Repository::init(backend, secure_storage)
    } else {
        bail!("Invalid repository version \'{}\'", version);
    }
}

/// Initialize common repository objects (manifest and keys).
/// This function prompts for a password to create a master key and returns a SecureStorage.
fn init_common(
    password: Option<String>,
    keyfile_path: Option<&PathBuf>,
    backend: Arc<dyn StorageBackend>,
) -> Result<Arc<SecureStorage>> {
    let pass = match password {
        Some(p) => p,
        None => ui::cli::request_password_with_confirmation(
            "Enter new password for repository",
            "Confirm password",
            "Passwords don't match",
        ),
    };

    // Create the repository root
    if backend.root_exists() {
        bail!("Could not initialize a repository because a directory already exists");
    }

    backend
        .create()
        .with_context(|| "Could not create root directory")?;

    let keys_path = PathBuf::from(KEYS_DIR);
    backend.create_dir(&keys_path)?;

    // Create new key
    let master_key = generate_new_master_key();
    let keyfile = repository::generate_key_file(&pass, master_key.clone())
        .with_context(|| "Could not generate key")?;
    let keyfile_json = serde_json::to_string_pretty(&keyfile)?;
    let keyfile_id = ID::from_content(&keyfile_json);
    match keyfile_path {
        Some(p) => std::fs::write(p, keyfile_json.as_bytes())?,
        None => {
            let p = keys_path.join(&keyfile_id.to_hex());
            backend.write(&p, keyfile_json.as_bytes())?;
        }
    }

    let secure_storage = Arc::new(
        SecureStorage::build()
            .with_compression(DEFAULT_COMPRESSION_LEVEL)
            .with_key(master_key),
    );

    Ok(secure_storage)
}

/// Try to open a repository.
/// This function prompts for a password to retrieve a master key.
pub fn try_open(
    mut password: Option<String>,
    key_file_path: Option<&PathBuf>,
    backend: Arc<dyn StorageBackend>,
) -> Result<Arc<dyn RepositoryBackend>> {
    if !backend.root_exists() {
        bail!("Could not open a repository. The path does not exist.");
    }

    const MAX_PASSWORD_RETRIES: u32 = 3;
    let mut password_try_count = 0;

    let master_key = {
        if let Some(p) = password.take() {
            retrieve_master_key(&p, key_file_path, backend.clone())
                .with_context(|| "Incorrect password.")?
        } else {
            loop {
                let pass_from_console = ui::cli::request_password("Enter repository password");

                if let Ok(key) =
                    retrieve_master_key(&pass_from_console, key_file_path, backend.clone())
                {
                    break key;
                } else {
                    password_try_count += 1;
                    if password_try_count < MAX_PASSWORD_RETRIES {
                        ui::cli::log!("Incorrect password. Try again.");
                        continue;
                    } else {
                        bail!("Wrong password or no KeyFile found.");
                    }
                }
            }
        }
    };

    let secure_storage = Arc::new(
        SecureStorage::build()
            .with_compression(DEFAULT_COMPRESSION_LEVEL)
            .with_key(master_key),
    );

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
) -> Result<Arc<dyn RepositoryBackend>> {
    if version == 1 {
        let repo_v1 = repository_v1::Repository::open(backend, secure_storage)?;
        return Ok(repo_v1);
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

pub fn generate_new_master_key() -> Vec<u8> {
    let mut new_random_key = vec![0u8; 32];
    OsRng.fill_bytes(&mut new_random_key);
    new_random_key
}

/// Generates a new KeyFile for the master key with a new password
pub fn generate_key_file(password: &str, master_key: Vec<u8>) -> Result<KeyFile> {
    let create_time = Utc::now();

    const SALT_LENGTH: usize = 32;
    let salt = SecureStorage::generate_salt::<SALT_LENGTH>();
    let intermediate_key = SecureStorage::derive_key(password, &salt);

    let encrypted_key = SecureStorage::encrypt_with_key(&intermediate_key, &master_key)?;

    let key_file = KeyFile {
        created: create_time,
        encrypted_key: base64::engine::general_purpose::STANDARD.encode(encrypted_key),
        salt: base64::engine::general_purpose::STANDARD.encode(salt),
    };

    Ok(key_file)
}

/// Retrieve the master key from all available keys in a folder
pub fn retrieve_master_key(
    password: &str,
    keyfile_path: Option<&PathBuf>,
    backend: Arc<dyn StorageBackend>,
) -> Result<Vec<u8>> {
    match keyfile_path {
        Some(path) => {
            let file = std::fs::File::open(&path)
                .with_context(|| format!("Could not open KeyFile at {:?}", path))?;
            let keyfile: KeyFile = serde_json::from_reader(file)
                .with_context(|| format!("KeyFile at {:?} is invalid", path))?;

            decode_master_key(&password, keyfile)
        }
        None => {
            let keys_path = Path::new(KEYS_DIR);
            let entries = backend.read_dir(&keys_path)?;

            for path in entries {
                // The keys directory should only contain files. We can ignore anything
                // that is not a file, but show a warning anyway.
                if !backend.is_file(&path) {
                    ui::cli::warning!(
                        "Extraneous item \'{}\' in keys directory is not a file",
                        path.display()
                    );
                    continue;
                }

                // Load keyfile
                let keyfile_str = backend.read(&path)?;
                let keyfile: KeyFile = match serde_json::from_slice(keyfile_str.as_slice()) {
                    Ok(kf) => kf,
                    Err(e) => {
                        ui::cli::warning!("Failed to parse keyfile at {}: {}", path.display(), e);
                        continue;
                    }
                };

                if let Ok(master_key) = decode_master_key(&password, keyfile) {
                    return Ok(master_key);
                }
            }

            Err(anyhow::anyhow!(
                "No valid KeyFile found for the provided password in the keys directory."
            )
            .into())
        }
    }
}

fn decode_master_key(password: &str, keyfile: KeyFile) -> Result<Vec<u8>> {
    // Decode salt and key from base64
    let salt = base64::engine::general_purpose::STANDARD.decode(keyfile.salt)?;
    let encrypted_key = base64::engine::general_purpose::STANDARD.decode(keyfile.encrypted_key)?;

    let intermediate_key = SecureStorage::derive_key(&password, &salt);
    SecureStorage::decrypt_with_key(&intermediate_key, &encrypted_key)
        .with_context(|| "Could not retrieve master key from this keyfile")
}

#[cfg(test)]
mod tests {
    use base64::engine::general_purpose;
    use tempfile::tempdir;

    use crate::{backend::localfs::LocalFS, utils};

    use super::*;

    /// Test init a repo with password and open it
    #[test]
    fn test_init_and_open_with_password() -> Result<()> {
        let temp_repo_dir = tempdir()?;
        let temp_repo_path = temp_repo_dir.path().join("repo");

        let password = Some(String::from("mapachito"));

        let backend = Arc::new(LocalFS::new(temp_repo_path.to_owned()));

        init(password.clone(), None, backend.to_owned())?;
        let _ = try_open(password, None, backend)?;

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

        let password = utils::get_password_from_file(&Some(password_file_path))?;
        let backend = Arc::new(LocalFS::new(temp_repo_path.to_owned()));

        init(password.clone(), None, backend.to_owned())?;
        let _ = try_open(password, None, backend)?;

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
