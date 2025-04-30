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
    fs::{File, OpenOptions},
    io::{BufReader, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use aes_gcm::aead::{OsRng, rand_core::RngCore};
use anyhow::{Context, Result, bail};
use base64::{Engine as _, engine::general_purpose};
use chrono::{DateTime, Utc};
use fastcdc::v2020::{Normalization, StreamCDC};
use serde::{Deserialize, Serialize};

use crate::{
    storage_backend::backend::StorageBackend,
    utils::{self, Hash, size},
};

use super::{
    backend::{ChunkResult, RepoVersion, RepositoryBackend, SnapshotId, write_version},
    config::Config,
    snapshot::Snapshot,
    storage::SecureStorage,
    tree::{Node, NodeType, Tree},
};

const REPO_VERSION: RepoVersion = 1;

const DATA_DIR: &str = "data";
const SNAPSHOT_DIR: &str = "snapshot";
const TREE_DIR: &str = "tree";
const KEYS_DIR: &str = "keys";

const DATA_FOLD_LENGTH: usize = 2;
const TREE_FOLD_LENGTH: usize = 2;

const MIN_CHUNK_SIZE: u32 = 512 * size::KiB as u32;
const AVG_CHUNK_SIZE: u32 = 1 * size::MiB as u32;
const MAX_CHUNK_SIZE: u32 = 8 * size::MiB as u32;

pub struct Repository {
    backend: Arc<dyn StorageBackend>,

    root_path: PathBuf,
    data_path: PathBuf,
    snapshot_path: PathBuf,
    tree_path: PathBuf,

    secure_storage: SecureStorage,
    config: Config,
}

/// A metadata structure that contains information about a repository key
#[derive(Serialize, Deserialize)]
struct KeyFile {
    created: DateTime<Utc>,

    encrypted_key: String,
    salt: String,
}

impl RepositoryBackend for Repository {
    /// Create and initialize a new repository
    fn init(backend: Arc<dyn StorageBackend>, repo_path: &Path, password: String) -> Result<Self> {
        if repo_path.exists() {
            bail!(
                "Could not initialize a repository because a directory already exists in \'{}\'",
                repo_path.display()
            );
        }

        // Init repository structure
        let data_path = repo_path.join(DATA_DIR);
        let snapshot_path = repo_path.join(SNAPSHOT_DIR);
        let tree_path = repo_path.join(TREE_DIR);
        let keys_path = repo_path.join(KEYS_DIR);

        backend
            .create_dir_all(repo_path)
            .with_context(|| "Could not create root directory")?;

        // Version file
        write_version(repo_path, REPO_VERSION)?;

        backend.create_dir(&data_path)?;
        let num_folders: usize = 1 << (4 * DATA_FOLD_LENGTH);
        for n in 0x00..num_folders {
            std::fs::create_dir(&data_path.join(format!("{:0>DATA_FOLD_LENGTH$x}", n)))?;
        }

        backend.create_dir(&tree_path)?;
        let num_folders: usize = 1 << (4 * TREE_FOLD_LENGTH);
        for n in 0x00..num_folders {
            std::fs::create_dir(&tree_path.join(format!("{:0>TREE_FOLD_LENGTH$x}", n)))?;
        }

        backend.create_dir(&snapshot_path)?;
        backend.create_dir(&keys_path)?;

        // Create new key
        let (key, keyfile) =
            Repository::generate_key(&password).with_context(|| "Could not generate key")?;
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
        let config = Config::default();
        let secure_storage: SecureStorage = SecureStorage::new(backend.to_owned())
            .with_key(key)
            .with_compression(config.compression_level.to_i32());
        let config_json = serde_json::to_string_pretty(&config)?;
        secure_storage.save_file_with_rename(&config_json.as_bytes(), &repo_path.join("config"))?;

        Ok(Self {
            backend: backend.to_owned(),
            root_path: repo_path.to_owned(),
            data_path,
            snapshot_path,
            tree_path,
            secure_storage,
            config,
        })
    }

    /// Open an existing repository from a directory
    fn open(backend: Arc<dyn StorageBackend>, repo_path: &Path, password: String) -> Result<Self> {
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

        let key =
            Repository::retrieve_key(&password, backend.to_owned(), &repo_path.join(KEYS_DIR))
                .with_context(|| "Incorrect password")?;
        let storage = SecureStorage::new(backend.to_owned())
            .with_key(key)
            // We don't know the compression level yet, but the config file has compression
            .with_compression(Some(zstd::DEFAULT_COMPRESSION_LEVEL));

        let data_path = repo_path.join(DATA_DIR);
        let snapshot_path = repo_path.join(SNAPSHOT_DIR);
        let tree_path = repo_path.join(TREE_DIR);

        let config_json = storage.load_file(&repo_path.join("config"))?;
        let config: Config = serde_json::from_slice(&config_json)?;
        let storage = storage.with_compression(config.compression_level.to_i32());

        let repo = Repository {
            backend,

            root_path: repo_path.to_owned(),
            data_path,
            snapshot_path,
            tree_path,
            secure_storage: storage,
            config,
        };

        Ok(repo)
    }

    /// Saves a tree in the repository. This function should be called when a tree is complete,
    /// that is, when all the contents and/or tree hashes have been resolved.
    fn save_tree(&self, tree: &Tree) -> Result<Hash> {
        let tree_json = serde_json::to_string_pretty(tree)?;
        let hash = utils::calculate_hash(&tree_json);
        let tree_path = &self.get_tree_path(&hash);

        self.secure_storage
            .save_file_with_rename(tree_json.as_bytes(), &tree_path)?;
        Ok(hash)
    }

    fn load_tree(&self, root_hash: &Hash) -> Result<Tree> {
        let tree_path = self.get_tree_path(root_hash);
        let tree_json = self.secure_storage.load_file(&tree_path)?;
        let tree: Tree = serde_json::from_slice(&tree_json)?;
        Ok(tree)
    }

    fn load_snapshot(&self, hash: &Hash) -> Result<Option<Snapshot>> {
        Ok(self
            .load_snapshots()?
            .iter()
            .find(|(snapshot_hash, _)| snapshot_hash == hash)
            .map(|(_, snapshot)| snapshot.clone()))
    }

    /// Get all snapshots in the repository
    fn load_snapshots(&self) -> Result<Vec<(Hash, Snapshot)>> {
        let mut snapshots = Vec::new();

        let entries =
            std::fs::read_dir(&self.snapshot_path).with_context(|| "Could not read snapshots")?;

        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() {
                if let Some(file_name) = path.file_name().and_then(|s| s.to_str()) {
                    let hash = file_name.to_string(); // Extract hash from filename
                    let snapshot_json = self.secure_storage.load_file(&path)?;
                    let snapshot: Snapshot = serde_json::from_slice(&snapshot_json)?;
                    snapshots.push((hash, snapshot));
                }
            }
        }

        Ok(snapshots)
    }

    /// Get all snapshots in the repository, sorted by datetime.
    fn load_snapshots_sorted(&self) -> Result<Vec<(Hash, Snapshot)>> {
        let mut snapshots = self.load_snapshots()?;
        snapshots.sort_by_key(|(_, snapshot)| snapshot.timestamp);
        Ok(snapshots)
    }

    /// Puts a file into the repository
    ///
    /// This function will split the file into chunks for deduplication, which
    /// will be compressed, encrypted and stored in the repository.
    /// The content hash of each chunk is used to identify the chunk and determine
    /// if the chunk already exists in the repository.
    fn save_file(&self, src_path: &Path) -> Result<ChunkResult> {
        let source = File::open(src_path)
            .with_context(|| format!("Could not open file \'{}\'", src_path.display()))?;
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
            let content_hash = utils::calculate_hash(&chunk.data);
            chunk_hashes.push(content_hash.clone());

            let chunk_path = self.get_data_object_path(&content_hash);

            total_bytes_read += chunk.length;

            // Only save the chunk if it doesn't exist yet
            if !chunk_path.exists() {
                total_bytes_written += self
                    .secure_storage
                    .save_file_with_rename(&chunk.data, &chunk_path)
                    .with_context(|| {
                        format!(
                            "Could not save chunk #{} ({}) for file \'{}\'",
                            chunk_hashes.len(),
                            content_hash,
                            src_path.display()
                        )
                    })?;
            }
        }

        Ok(ChunkResult {
            chunks: chunk_hashes,
            total_bytes_read,
            total_bytes_written,
        })
    }

    fn restore_node(&self, node: &Node, dst_path: &Path) -> Result<()> {
        match node.node_type {
            NodeType::File => {
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
                    let chunk_path = self.get_data_object_path(&chunk_hash);

                    let chunk_data =
                        self.secure_storage
                            .load_file(&chunk_path)
                            .with_context(|| {
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
            NodeType::Directory => todo!(),
            NodeType::Symlink => todo!(),
        }

        Ok(())
    }

    fn save_snapshot(&self, snapshot: &Snapshot) -> Result<SnapshotId> {
        let snapshot_json = serde_json::to_string_pretty(snapshot)?;
        let hash = utils::calculate_hash(&snapshot_json);

        let snapshot_path = self.snapshot_path.join(&hash);
        self.secure_storage
            .save_file_with_rename(&snapshot_json.as_bytes(), &snapshot_path)?;

        Ok(hash)
    }
}

impl Repository {
    /// Returns the path to a tree object with a given hash in the repository.
    fn get_tree_path(&self, hash: &Hash) -> PathBuf {
        self.tree_path
            .join(&hash[..TREE_FOLD_LENGTH])
            .join(&hash[TREE_FOLD_LENGTH..])
    }

    /// Returns the path to a data object with a given hash in the repository.
    fn get_data_object_path(&self, hash: &Hash) -> PathBuf {
        self.data_path
            .join(&hash[..DATA_FOLD_LENGTH])
            .join(&hash[DATA_FOLD_LENGTH..])
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
    fn retrieve_key(
        password: &str,
        backend: Arc<dyn StorageBackend>,
        keys_path: &Path,
    ) -> Result<Vec<u8>> {
        for path in backend.read_dir(keys_path)? {
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

            let intermediate_key = SecureStorage::derive_key(password, &salt);
            if let Ok(key) = SecureStorage::decrypt(&intermediate_key, &encrypted_key) {
                return Ok(key);
            }
        }

        bail!("Could not retrieve key")
    }
}

#[cfg(test)]
mod test {

    use tempfile::tempdir;

    use crate::{repository::tree::FSNodeStreamer, storage_backend::localfs::LocalFS, testing};

    use super::*;

    /// Test init a repo with password and open it
    #[test]

    fn heavy_test_init_and_open_with_password() -> Result<()> {
        let temp_repo_dir = tempdir()?;
        let temp_repo_path = temp_repo_dir.path().join("repo");

        let backend = Arc::new(LocalFS::new());

        Repository::init(
            backend.to_owned(),
            &temp_repo_path,
            String::from("mapachito"),
        )?;
        let _ = Repository::open(backend, &temp_repo_path, String::from("mapachito"))?;

        Ok(())
    }

    /// Test file chunk and restore
    #[test]
    fn heavy_test_chunk_and_restore() -> Result<()> {
        let temp_repo_dir = tempdir()?;
        let temp_repo_path = temp_repo_dir.path().join("repo");

        let src_file_path = testing::get_test_path("tree0.json");
        let dst_file_path = temp_repo_path.join("tree0.json.restored");

        let repo = Repository::init(
            Arc::new(LocalFS::new()),
            &temp_repo_path,
            String::from("mapachito"),
        )?;

        // Scan the FS -> Find the file
        let mut fs_node_streamer = FSNodeStreamer::from_root(&src_file_path)?;
        let (_, mut stream_node) = fs_node_streamer.next().unwrap().unwrap();

        // Chunk the file, obtain Node with content hashes
        let chunk_result = repo.save_file(&src_file_path)?;
        stream_node.node.contents = Some(chunk_result.chunks.clone());

        repo.restore_node(&stream_node.node, &dst_file_path)?;
        assert_eq!(chunk_result.chunks, stream_node.node.contents.unwrap());

        let src_data = std::fs::read(src_file_path)?;
        let dst_data = std::fs::read(dst_file_path)?;
        assert_eq!(src_data, dst_data);

        Ok(())
    }

    /// Test generation of master keys
    #[test]
    fn test_generate_key() -> Result<()> {
        let (key, keyfile) = Repository::generate_key("mapachito")?;

        let salt = general_purpose::STANDARD.decode(keyfile.salt)?;
        let encrypted_key = general_purpose::STANDARD.decode(keyfile.encrypted_key)?;

        let intermediate_key = SecureStorage::derive_key("mapachito", &salt);
        let decrypted_key = SecureStorage::decrypt(&intermediate_key, &encrypted_key)?;

        assert_eq!(key, decrypted_key);

        Ok(())
    }
}
