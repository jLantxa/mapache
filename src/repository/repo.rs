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
    collections::VecDeque,
    fs::{File, OpenOptions},
    io::{BufReader, Write},
    path::{Path, PathBuf},
    rc::Rc,
};

use aes_gcm::aead::{OsRng, rand_core::RngCore};
use anyhow::{Context, Result, bail};
use base64::{Engine as _, engine::general_purpose};
use blake3::Hasher;
use chrono::{DateTime, Utc};
use fastcdc::v2020::{Normalization, StreamCDC};
use serde::{Deserialize, Serialize};

use crate::{
    backend::backend::StorageBackend,
    filesystem::tree::{FileEntry, Node, NodeIndex, SerializableTreeObject, Tree},
    utils::{
        self,
        hashing::{Hash, Hashable},
    },
};

use super::{config::Config, snapshot::Snapshot, storage::SecureStorage};

const DATA_DIR: &str = "data";
const SNAPSHOT_DIR: &str = "snapshot";
const TREE_DIR: &str = "tree";
const KEYS_DIR: &str = "keys";

const DATA_FOLD_LENGTH: usize = 2;

pub struct Repository {
    backend: Rc<dyn StorageBackend>,

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
    pub fn init(
        backend: Rc<dyn StorageBackend>,
        repo_path: &Path,
        password: String,
    ) -> Result<Self> {
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

        backend
            .create_dir_all(repo_path)
            .with_context(|| "Could not create root directory")?;
        backend.create_dir(&data_path)?;

        let num_folders: usize = 1 << (4 * DATA_FOLD_LENGTH);
        for n in 0x00..num_folders {
            std::fs::create_dir(&data_path.join(format!(
                "{:0>width$x}",
                n,
                width = DATA_FOLD_LENGTH
            )))?;
        }

        backend.create_dir(&snapshot_path)?;
        backend.create_dir(&tree_path)?;
        backend.create_dir(&keys_path)?;

        // Create new key
        let (key, keyfile) = generate_key(&password).with_context(|| "Could not generate key")?;
        let keyfile_hash = keyfile.hash();
        let keyfile_path = &keys_path.join(keyfile_hash);
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
        let secure_storage = SecureStorage::new(backend.to_owned())
            .with_key(key)
            .with_compression(config.compression_level.to_i32());
        secure_storage.save_json(&config, &repo_path.join("config"))?;

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
    pub fn open(
        backend: Rc<dyn StorageBackend>,
        repo_path: &Path,
        password: String,
    ) -> Result<Self> {
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

        let key = retrieve_key(&password, backend.to_owned(), &repo_path.join(KEYS_DIR))
            .with_context(|| "Incorrect password")?;
        let storage = SecureStorage::new(backend.to_owned())
            .with_key(key)
            // We don't know the compression level yet, but the config file has compression
            .with_compression(Some(zstd::DEFAULT_COMPRESSION_LEVEL));

        let data_path = repo_path.join(DATA_DIR);
        let snapshot_path = repo_path.join(SNAPSHOT_DIR);
        let tree_path = repo_path.join(TREE_DIR);

        let config: Config = storage.load_json(&repo_path.join("config"))?;
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

    pub fn put_tree(&self, tree: &Tree) -> Result<Hash> {
        for node_index in tree.iter_preorder() {
            let node = tree
                .get(node_index)
                .expect(&format!("Expected node with index \'{}\'", node_index));
            match node {
                crate::filesystem::tree::Node::File { .. } => continue,
                crate::filesystem::tree::Node::Directory { .. } => {
                    let serializable_node = tree.serializable_object(node_index)?;
                    let node_hash = tree
                        .get_hash(node_index)
                        .expect(&format!("Expected hash for index \'{}\'", node_index));
                    let serialized_tree_path = self.tree_path.join(&node_hash);
                    self.secure_storage
                        .save_json(&serializable_node, &serialized_tree_path)?;
                }
            }
        }

        let root_hash = tree
            .get_hash(0)
            .expect("Expected hash for index 0 (root)")
            .to_string();

        Ok(root_hash)
    }

    pub fn get_tree(&self, root_hash: &Hash) -> Result<Tree> {
        let root_obj: SerializableTreeObject = self
            .secure_storage
            .load_json(&self.tree_path.join(root_hash))?;

        // Tree root with index 0
        let mut tree = Tree::new_with_root(root_obj.name, root_obj.metadata);

        let mut hash_stack: VecDeque<(Hash, NodeIndex)> = VecDeque::new();
        for file_entry in root_obj.files {
            tree.add_child(Node::File(file_entry), 0)?;
        }
        for (_, hash) in root_obj.directories {
            hash_stack.push_back((hash, 0));
        }

        // Read all tree objects
        while let Some((hash, parent_index)) = hash_stack.pop_front() {
            let tree_obj: SerializableTreeObject =
                self.secure_storage.load_json(&self.tree_path.join(&hash))?;

            let dir_index = tree.add_child(
                Node::Directory {
                    name: tree_obj.name,
                    metadata: tree_obj.metadata,
                    children: Default::default(),
                },
                parent_index,
            )?;

            for file_entry in tree_obj.files {
                tree.add_child(Node::File(file_entry), dir_index)?;
            }
            for (_, hash) in tree_obj.directories {
                hash_stack.push_back((hash.clone(), dir_index));
            }
        }

        Ok(tree)
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

    pub fn put_file(&self, src_path: &Path) -> Result<ChunkResult> {
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

            let chunk_path = &self
                .data_path
                .join(&content_hash[0..DATA_FOLD_LENGTH])
                .join(&content_hash[DATA_FOLD_LENGTH..]);

            total_bytes_read += chunk.length;
            total_bytes_written += self
                .secure_storage
                .save_file(&chunk.data, chunk_path)
                .with_context(|| {
                    format!(
                        "Could not save chunk #{} ({}) for file \'{}\'",
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
            let chunk_path = self
                .data_path
                .join(&chunk_hash[0..DATA_FOLD_LENGTH])
                .join(&chunk_hash[DATA_FOLD_LENGTH..]);

            let chunk_data = self
                .secure_storage
                .load_file(&chunk_path)
                .with_context(|| {
                    format!(
                        "Could not load chunk #{} ({}) for restoring file '{}'",
                        index + 1,
                        chunk_hash,
                        dst_path.to_string_lossy()
                    )
                })?;

            dst_file.write_all(&chunk_data).with_context(|| {
                format!(
                    "Could not restore chunk #{} ({}) to file '{}'",
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
fn retrieve_key(
    password: &str,
    backend: Rc<dyn StorageBackend>,
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

#[cfg(test)]
mod test {

    use tempfile::tempdir;

    use crate::{
        backend::localfs::LocalFS,
        filesystem::tree::FileEntry,
        testing,
        utils::{self},
    };

    use super::*;

    /// Test init a repo with password and open it
    #[test]

    fn heavy_test_init_and_open_with_password() -> Result<()> {
        let temp_repo_dir = tempdir()?;
        let temp_repo_path = temp_repo_dir.path().join("repo");

        let backend = Rc::new(LocalFS::new());

        Repository::init(
            backend.to_owned(),
            &temp_repo_path,
            String::from("mapachito"),
        )?;
        let _ = Repository::open(backend, &temp_repo_path, String::from("mapachito"))?;

        Ok(())
    }

    /// Test saving and loading tree objects
    /// This test creates a repository in a temp folder
    #[test]

    fn heavy_test_put_and_get_tree() -> Result<()> {
        let temp_repo_dir = tempdir()?;
        let temp_repo_path = temp_repo_dir.path().join("repo");

        let repo = Repository::init(
            Rc::new(LocalFS::new()),
            &temp_repo_path,
            String::from("mapachito"),
        )?;

        let mut tree: Tree = utils::json::load_json(Path::new("testdata/tree0.json"))?;
        tree.update_hashes()?;
        dbg!(&tree);

        let root_hash = repo.put_tree(&tree)?;
        let deserialized_root = repo.get_tree(&root_hash)?;

        assert_eq!(
            serde_json::to_string_pretty(&tree)?,
            serde_json::to_string_pretty(&deserialized_root)?
        );

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
            Rc::new(LocalFS::new()),
            &temp_repo_path,
            String::from("mapachito"),
        )?;
        let chunk_result = repo.put_file(&src_file_path)?;

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
