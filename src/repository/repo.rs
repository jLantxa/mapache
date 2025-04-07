/*
 * [backup] is an incremental backup tool
 * Copyright (C) 2025  Javier Lancha Vázquez <javier.lancha@gmail.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{BufReader, Write},
    path::{Path, PathBuf},
};

use aes_gcm::aead::{OsRng, rand_core::RngCore};
use anyhow::{Context, Result, bail};
use base64::{Engine as _, engine::general_purpose};
use blake3::Hasher;
use chrono::{DateTime, Utc};
use fastcdc::v2020::{Normalization, StreamCDC};
use serde::{Deserialize, Serialize};

use crate::{
    filesystem::DirectoryMetadata,
    utils::{
        self,
        hashing::{Hash, Hashable},
        json,
    },
};

use super::{
    config::Config,
    snapshot::Snapshot,
    storage::SecureStorage,
    tree::{DirectoryNode, FileEntry},
};

const DATA_DIR: &str = "data";
const SNAPSHOT_DIR: &str = "snapshot";
const TREE_DIR: &str = "tree";
const KEYS_DIR: &str = "keys";

pub struct Repository {
    root_path: PathBuf,
    data_path: PathBuf,
    snapshot_path: PathBuf,
    tree_path: PathBuf,

    secure_storage: SecureStorage,
    config: Config,
}

#[derive(Serialize, Deserialize)]
struct KeyFile {
    created: DateTime<Utc>,

    encrypted_key: String,
    salt: String,
}

#[derive(Debug)]
pub struct ChunkResult {
    pub chunks: Vec<Hash>,
    pub total_bytes_read: usize,
    pub total_bytes_written: usize,
}

impl Hashable for KeyFile {
    fn hash(&self) -> Hash {
        let mut hasher = Hasher::new();

        hasher.update(self.created.to_rfc3339().as_bytes());
        hasher.update(self.encrypted_key.as_bytes());
        hasher.update(self.salt.as_bytes());

        let hash = hasher.finalize();
        format!("{}", hash)
    }
}

impl Repository {
    /// Create and initialize a new repository
    pub fn init(repo_path: &Path, password: String) -> Result<Self> {
        if repo_path.exists() {
            bail!(format!(
                "Could not initialize a repository because a directory already exists in \'{}\'",
                repo_path.to_string_lossy()
            ));
        }

        // Init repository structure
        let data_path = repo_path.join(DATA_DIR);
        let snapshot_path = repo_path.join(SNAPSHOT_DIR);
        let tree_path = repo_path.join(TREE_DIR);
        let keys_path = repo_path.join(KEYS_DIR);

        std::fs::create_dir_all(repo_path).with_context(|| "Could not create root directory")?;

        std::fs::create_dir(&data_path)?;
        for n in 0x00..=0xff {
            std::fs::create_dir(&data_path.join(format!("{:02x}", n)))?;
        }

        std::fs::create_dir(&snapshot_path)?;
        std::fs::create_dir(&tree_path)?;

        std::fs::create_dir(&keys_path)?;

        // Secure storage
        let (key, keyfile) = generate_key(&password).with_context(|| "Could not generate key")?;
        let storage = SecureStorage::new().with_compression(10).with_key(key);

        let keyfile_hash = keyfile.hash();
        let keyfile_path = &keys_path.join(keyfile_hash);
        json::save_json_pretty(&keyfile, keyfile_path)?;

        let repo = Repository {
            root_path: repo_path.to_owned(),
            data_path,
            snapshot_path,
            tree_path,
            secure_storage: storage,
            config: Config::default(),
        };

        repo.persist()?;

        Ok(repo)
    }

    /// Open an existing repository from a directory
    pub fn open(repo_path: &Path, password: String) -> Result<Self> {
        if !repo_path.exists() {
            bail!(
                "Could not open a repository. \'{}\' doesn't exist",
                repo_path.to_string_lossy()
            );
        } else if !repo_path.is_dir() {
            bail!(
                "Could not open a repository. \'{}\' is not a directory",
                repo_path.to_string_lossy()
            );
        }

        let key = retrieve_key(&password, &repo_path.join(KEYS_DIR))
            .with_context(|| "Incorrect password")?;
        let storage = SecureStorage::new().with_compression(10).with_key(key);

        let data_path = repo_path.join(DATA_DIR);
        let snapshot_path = repo_path.join(SNAPSHOT_DIR);
        let tree_path = repo_path.join(TREE_DIR);

        let config = storage.load_json(&repo_path.join("config"))?;

        let repo = Repository {
            root_path: repo_path.to_owned(),
            data_path,
            snapshot_path,
            tree_path,
            secure_storage: storage,
            config,
        };

        Ok(repo)
    }

    pub fn get_config(&self) -> &Config {
        &self.config
    }

    pub fn set_config(&mut self, config: &Config) {
        self.config = config.clone();
    }

    /// Persist all metadata files
    pub fn persist(&self) -> Result<()> {
        self.persist_config()?;

        Ok(())
    }

    /// Persist a FileSystemNode into the repo metadata
    pub fn persist_tree(&self, tree: &DirectoryNode) -> Result<Hash> {
        // TODO: Transform into iterative traversal

        let mut serializable_files: Vec<FileEntry> = Vec::new();
        let mut serializable_children: Vec<Hash> = Vec::new();

        for (_, child_node) in &tree.children {
            serializable_children.push(self.persist_tree(child_node)?);
        }

        for (_, file_entry) in &tree.files {
            serializable_files.push(file_entry.clone());
        }

        let serializable_directory_node = SerializableDirectoryNode {
            name: tree.name.clone(),
            metadata: tree.metadata.clone(),
            children: serializable_children,
            files: serializable_files,
        };

        let hash = serializable_directory_node.hash();
        let serialized_tree_path = self.tree_path.join(&hash);

        /*
         * Serialize only if the tree does not exist yet in the repo.
         * I am assuming that if two trees have the same hash, the content is the same.
         */
        if !serialized_tree_path.exists() {
            self.secure_storage
                .save_json(&serializable_directory_node, &serialized_tree_path)
                .with_context(|| format!("Could not serialize tree \'{}\'", &hash))?;
        }

        Ok(hash)
    }

    /// Load a FileSystemNode from the repo metadata
    pub fn load_tree(&self, hash: &Hash) -> Result<DirectoryNode> {
        // TODO: Transform into iterative traversal

        let tree_path = self.tree_path.join(hash);
        if !tree_path.exists() || !tree_path.is_file() {
            bail!(format!(
                "Could not load tree \'{}\'. Tree file does not exist.",
                hash
            ));
        }

        let serialized_node: SerializableDirectoryNode = self
            .secure_storage
            .load_json(&tree_path)
            .with_context(|| format!("Could not deserialize metadata tree \'{}\'", hash))?;

        let mut files: BTreeMap<String, FileEntry> = BTreeMap::new();
        let mut children = BTreeMap::new();

        for file_entry in serialized_node.files {
            files.insert(file_entry.name.clone(), file_entry);
        }

        for child_hash in serialized_node.children {
            let serialized_child_node = self.load_tree(&child_hash)?;
            children.insert(serialized_child_node.name.clone(), serialized_child_node);
        }

        let tree_node = DirectoryNode {
            name: serialized_node.name,
            metadata: serialized_node.metadata,
            files: files,
            children: children,
        };

        Ok(tree_node)
    }

    /// Get all snapshots in the repository
    pub fn get_snapshots(&self) -> Result<Vec<(Hash, Snapshot)>> {
        let mut snapshots = Vec::new();

        let entries =
            std::fs::read_dir(&self.snapshot_path).with_context(|| "Could not read snapshots")?;

        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() {
                if let Some(file_name) = path.file_name().and_then(|s| s.to_str()) {
                    let hash = file_name.to_string(); // Extract hash from filename
                    let snapshot: Snapshot = self.secure_storage.load_json(&path)?;
                    snapshots.push((hash, snapshot));
                }
            }
        }

        Ok(snapshots)
    }

    /// Get all snapshots in the repository, sorted by datetime
    pub fn get_snapshots_sorted(&self) -> Result<Vec<(Hash, Snapshot)>> {
        let mut snapshots = self.get_snapshots()?;
        snapshots.sort_by_key(|(_, snapshot)| snapshot.timestamp);
        Ok(snapshots)
    }

    /// Persist the config metadata
    fn persist_config(&self) -> Result<()> {
        self.secure_storage
            .save_json(&self.config, &self.root_path.join("config"))
            .with_context(|| "Could not persist config file")
    }

    pub fn commit_file(&self, src_path: &Path) -> Result<ChunkResult> {
        const MIN_CHUNK_SIZE: u32 = 4096;
        const AVG_CHUNK_SIZE: u32 = 16384;
        const MAX_CHUNK_SIZE: u32 = 65535;

        let source = File::open(src_path)
            .with_context(|| format!("Could not open file \'{}\'", src_path.to_string_lossy()))?;
        let reader = BufReader::new(source);

        let chunker = StreamCDC::with_level(
            reader,
            MIN_CHUNK_SIZE,
            AVG_CHUNK_SIZE,
            MAX_CHUNK_SIZE,
            Normalization::Level1,
        );

        let mut chunk_hashes = Vec::new();
        let mut total_bytes_read = 0;
        let mut total_bytes_written = 0;

        for result in chunker {
            let chunk = result?;

            // Use our hashing function. FastCDC uses a short hash.
            let content_hash = utils::hashing::calculate_hash(&chunk.data);
            chunk_hashes.push(content_hash.clone());

            // The first two characters on the hash map to a folder in data
            // data/01/23456789abcdef...
            let chunk_path = &self
                .data_path
                .join(&content_hash[0..2])
                .join(&content_hash[2..]);

            total_bytes_read += chunk.length;
            total_bytes_written += self
                .secure_storage
                .save_file(&chunk.data, chunk_path)
                .with_context(|| {
                    format!(
                        "Could not save chunk {} ({}) for file \'{}\'",
                        chunk_hashes.len(),
                        content_hash,
                        src_path.to_string_lossy()
                    )
                })?;
        }

        Ok(ChunkResult {
            chunks: chunk_hashes,
            total_bytes_read,
            total_bytes_written,
        })
    }

    pub fn restore_file(&self, file: &FileEntry, dst_path: &Path) -> Result<()> {
        let mut dst_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(dst_path)
            .with_context(|| {
                format!(
                    "Could not create destination file '{}'",
                    dst_path.to_string_lossy()
                )
            })?;

        for (index, chunk_hash) in file.chunks.iter().enumerate() {
            // The first two characters on the hash map to a folder in data
            // data/01/23456789abcdef...
            let chunk_path = self
                .data_path
                .join(&chunk_hash[0..2])
                .join(&chunk_hash[2..]);

            let chunk_data = self
                .secure_storage
                .load_file(&chunk_path)
                .with_context(|| {
                    format!(
                        "Could not load chunk {} ({}) for restoring file '{}'",
                        index + 1,
                        chunk_hash,
                        dst_path.to_string_lossy()
                    )
                })?;

            dst_file.write_all(&chunk_data).with_context(|| {
                format!(
                    "Could not write chunk {} ({}) to file '{}'",
                    index + 1,
                    chunk_hash,
                    dst_path.to_string_lossy()
                )
            })?;
        }

        Ok(())
    }
}

/// Generate a new master  key
fn generate_key(password: &str) -> Result<(Vec<u8>, KeyFile)> {
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
fn retrieve_key(password: &str, keys_path: &Path) -> Result<Vec<u8>> {
    for entry in std::fs::read_dir(keys_path)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() {
            let keyfile: KeyFile = json::load_json(&path)?;
            let salt = general_purpose::STANDARD.decode(keyfile.salt)?;
            let encrypted_key = general_purpose::STANDARD.decode(keyfile.encrypted_key)?;

            let intermediate_key = SecureStorage::derive_key(password, &salt);
            if let Ok(key) = SecureStorage::decrypt(&intermediate_key, &encrypted_key) {
                return Ok(key);
            }
        }
    }

    bail!("Could not retrieve key")
}

impl Drop for Repository {
    fn drop(&mut self) {
        // Persist all object when the repository is dropped
        let _ = self.persist();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableDirectoryNode {
    pub name: String,
    pub metadata: Option<DirectoryMetadata>,
    pub files: Vec<FileEntry>,
    pub children: Vec<Hash>,
}

impl Hashable for SerializableDirectoryNode {
    fn hash(&self) -> Hash {
        let mut hasher = Hasher::new();

        hasher.update(self.name.as_bytes());

        if let Some(meta) = &self.metadata {
            hasher.update(meta.hash().as_bytes());
        }

        for file in &self.files {
            hasher.update(file.hash().as_bytes());
        }

        for child_hash in &self.children {
            hasher.update(child_hash.as_bytes());
        }

        let hash = hasher.finalize();
        format!("{}", hash)
    }
}

#[cfg(test)]
mod test {
    use tempfile::tempdir;

    use crate::{testing, utils};

    use super::*;

    /// Test init a repo with password and open it
    #[test]
    #[ignore]
    fn heavy_test_init_and_open_with_password() -> Result<()> {
        let temp_repo_dir = tempdir()?;
        let temp_repo_path = temp_repo_dir.path().join("repo");

        Repository::init(&temp_repo_path, String::from("mapachito"))?;
        let _ = Repository::open(&temp_repo_path, String::from("mapachito"))?;

        Ok(())
    }

    /// Test saving and loading tree objects
    /// This test creates a repository in a temp folder
    #[test]
    #[ignore]
    fn heavy_test_persist_and_load_tree() -> Result<()> {
        let temp_repo_dir = tempdir()?;
        let temp_repo_path = temp_repo_dir.path().join("repo");

        let repo = Repository::init(&temp_repo_path, String::from("mapachito"))?;

        let root_tree = utils::json::load_json(Path::new("testdata/tree0.json"))?;

        let root_hash = repo.persist_tree(&root_tree)?;
        let deserialized_root = repo.load_tree(&root_hash)?;

        assert_eq!(
            serde_json::to_string_pretty(&root_tree)?,
            serde_json::to_string_pretty(&deserialized_root)?
        );

        Ok(())
    }

    /// Test file chunk and restore
    #[test]
    #[ignore]
    fn heavy_test_chunk_and_restore() -> Result<()> {
        let temp_repo_dir = tempdir()?;
        let temp_repo_path = temp_repo_dir.path().join("repo");

        let src_file_path = testing::get_test_path("tree0.json");
        let dst_file_path = temp_repo_path.join("tree0.json.restored");

        let repo = Repository::init(&temp_repo_path, String::from("mapachito"))?;
        let chunk_result = repo.commit_file(&src_file_path)?;

        let file_entry = FileEntry {
            name: "tree0.json".to_owned(),
            metadata: None,
            chunks: chunk_result.chunks.clone(),
        };

        repo.restore_file(&file_entry, &dst_file_path)?;
        assert_eq!(chunk_result.chunks, file_entry.chunks);

        let src_data = std::fs::read(src_file_path)?;
        let dst_data = std::fs::read(dst_file_path)?;
        assert_eq!(src_data, dst_data);

        Ok(())
    }

    /// Test generation of master keys
    #[test]
    fn test_generate_key() -> Result<()> {
        let (key, keyfile) = generate_key("mapachito")?;

        let salt = general_purpose::STANDARD.decode(keyfile.salt)?;
        let encrypted_key = general_purpose::STANDARD.decode(keyfile.encrypted_key)?;

        let intermediate_key = SecureStorage::derive_key("mapachito", &salt);
        let decrypted_key = SecureStorage::decrypt(&intermediate_key, &encrypted_key)?;

        assert_eq!(key, decrypted_key);

        Ok(())
    }
}
