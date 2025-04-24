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
    sync::Arc,
};

use aes_gcm::aead::{OsRng, rand_core::RngCore};
use anyhow::{Context, Result, bail};
use base64::{Engine as _, engine::general_purpose};
use blake3::Hasher;
use chrono::{DateTime, Utc};
use fastcdc::v2020::{Normalization, StreamCDC};
use serde::{Deserialize, Serialize};

use crate::{
    filesystem::tree::{DirectoryNode, FileNode, Node, NodeIndex, SerializableTreeObject, Tree},
    storage_backend::backend::StorageBackend,
    utils::{self, Hash, Hashable},
};

use super::{
    backend::{ChunkResult, RepoVersion, RepositoryBackend, write_version},
    config::Config,
    snapshot::{Snapshot},
    storage::SecureStorage,
};

const REPO_VERSION: RepoVersion = 1;

const DATA_DIR: &str = "data";
const SNAPSHOT_DIR: &str = "snapshot";
const TREE_DIR: &str = "tree";
const KEYS_DIR: &str = "keys";

const DATA_FOLD_LENGTH: usize = 2;
const TREE_FOLD_LENGTH: usize = 2;

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

impl Hashable for KeyFile {
    fn hash(&self) -> Hash {
        let mut hasher = Hasher::new();
        hasher.update(serde_json::to_string(self).unwrap().as_bytes());
        let hash = hasher.finalize();
        format!("{}", hash)
    }
}

impl RepositoryBackend for Repository {
    /// Create and initialize a new repository
    fn init(backend: Arc<dyn StorageBackend>, repo_path: &Path, password: String) -> Result<Self> {
        if repo_path.exists() {
            bail!(format!(
                "Could not initialize a repository because a directory already exists in \'{}\'",
                repo_path.display()
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

    /// Serializes a Tree into SerializableTreeObject's into the repository storage.
    ///
    /// For each directory node in the tree, we create a serializable tree object with the
    /// contents (files and directories) and metadata of that node. The child directories are
    /// referenced by their hash values.
    /// Each serializable tree object has a unique content hash that is used to identify it in the
    /// repository.
    ///
    /// To avoid potential stack overflows with very deep trees, this function uses a DFS pre-order
    /// iterator.
    ///
    /// This function requires that the `Tree` hashes be updated.
    fn put_tree(&self, tree: &Tree) -> Result<Hash> {
        for node_index in tree.iter_preorder() {
            let node = tree
                .get(node_index)
                .expect(&format!("Expected node with index \'{}\'", node_index));
            match node {
                crate::filesystem::tree::Node::File { .. } => continue,
                crate::filesystem::tree::Node::Directory { .. } => {
                    let serializable_node = tree.to_serializable_object(node_index)?;
                    let node_hash = tree
                        .get_hash(node_index)
                        .expect(&format!("Expected hash for index \'{}\'", node_index));
                    let serialized_tree_path = self.get_tree_object_path(node_hash);
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

    /// Restores a Tree from the SerializableTreeObject's in the repository.
    ///
    /// This function follows the hash references of each node children to deserialize
    /// the children directories and produce a Tree Node.
    ///
    /// To avoid potential stack overflows with very deep trees, this function uses a DFS pre-order
    /// iterator.
    fn get_tree(&self, root_hash: &Hash) -> Result<Tree> {
        let root_obj_path = self.get_tree_object_path(root_hash);
        let root_obj: SerializableTreeObject = self.secure_storage.load_json(&root_obj_path)?;

        // Tree root with index 0
        let mut tree = Tree::new_with_root(Node::new_dir(root_obj.name, root_obj.metadata));

        let mut hash_stack: VecDeque<(Hash, NodeIndex)> = VecDeque::new();
        for file_node in root_obj.files {
            tree.add_child(Node::File(file_node), 0)?;
        }
        for (_, hash) in root_obj.directories {
            hash_stack.push_back((hash, 0));
        }

        // Read all tree objects
        while let Some((hash, parent_index)) = hash_stack.pop_front() {
            let tree_obj_path = self.get_tree_object_path(&hash);
            let tree_obj: SerializableTreeObject = self.secure_storage.load_json(&tree_obj_path)?;

            let dir_index = tree.add_child(
                Node::Directory(DirectoryNode {
                    name: tree_obj.name,
                    metadata: tree_obj.metadata,
                    children: Default::default(),
                }),
                parent_index,
            )?;

            for file_node in tree_obj.files {
                tree.add_child(Node::File(file_node), dir_index)?;
            }
            for (_, hash) in tree_obj.directories {
                hash_stack.push_back((hash.clone(), dir_index));
            }
        }

        Ok(tree)
    }

    fn get_snapshot(&self, hash: &Hash) -> Result<Option<Snapshot>> {
        Ok(self
            .get_snapshots()?
            .iter()
            .find(|(snapshot_hash, _)| snapshot_hash == hash)
            .map(|(_, snapshot)| snapshot.clone()))
    }

    /// Get all snapshots in the repository
    fn get_snapshots(&self) -> Result<Vec<(Hash, Snapshot)>> {
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

    /// Get all snapshots in the repository, sorted by datetime.
    fn get_snapshots_sorted(&self) -> Result<Vec<(Hash, Snapshot)>> {
        let mut snapshots = self.get_snapshots()?;
        snapshots.sort_by_key(|(_, snapshot)| snapshot.timestamp);
        Ok(snapshots)
    }

    /// Puts a file into the repository
    ///
    /// This function will split the file into chunks for deduplication, which
    /// will be compressed, encrypted and stored in the repository.
    /// The content hash of each chunk is used to identify the chunk and determine
    /// if the chunk already exists in the repository.
    fn put_file(&self, src_path: &Path) -> Result<ChunkResult> {
        const MIN_CHUNK_SIZE: u32 = 4096;
        const AVG_CHUNK_SIZE: u32 = 16384;
        const MAX_CHUNK_SIZE: u32 = 65535;

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
                    .save_file(&chunk.data, &chunk_path)
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

    fn restore_file(&self, file: &FileNode, dst_path: &Path) -> Result<()> {
        let mut dst_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(dst_path)
            .with_context(|| {
                format!("Could not create destination file '{}'", dst_path.display())
            })?;

        for (index, chunk_hash) in file.chunks.iter().enumerate() {
            let chunk_path = self.get_data_object_path(&chunk_hash);

            let chunk_data = self
                .secure_storage
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
        }

        Ok(())
    }

    fn save_snapshot(&self, snapshot: &Snapshot) -> Result<Hash> {
        let hash = snapshot.hash();
        let snapshot_path = self.snapshot_path.join(&hash);
        self.secure_storage.save_json(snapshot, &snapshot_path)?;
        Ok(hash)
    }
}

impl Repository {
    /// Returns the path to a tree object with a given hash in the repository.
    fn get_tree_object_path(&self, hash: &Hash) -> PathBuf {
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

    use crate::{
        filesystem::tree::FileNode,
        storage_backend::localfs::LocalFS,
        testing,
        utils::{self},
    };

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

    /// Test saving and loading tree objects
    /// This test creates a repository in a temp folder
    #[test]

    fn heavy_test_put_and_get_tree() -> Result<()> {
        let temp_repo_dir = tempdir()?;
        let temp_repo_path = temp_repo_dir.path().join("repo");

        let repo = Repository::init(
            Arc::new(LocalFS::new()),
            &temp_repo_path,
            String::from("mapachito"),
        )?;

        let mut tree: Tree = utils::load_json(Path::new("testdata/tree0.json"))?;
        tree.refresh_hashes()?;

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
            Arc::new(LocalFS::new()),
            &temp_repo_path,
            String::from("mapachito"),
        )?;
        let chunk_result = repo.put_file(&src_file_path)?;

        let file_node = FileNode {
            name: "tree0.json".to_owned(),
            metadata: None,
            chunks: chunk_result.chunks.clone(),
        };

        repo.restore_file(&file_node, &dst_file_path)?;
        assert_eq!(chunk_result.chunks, file_node.chunks);

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
