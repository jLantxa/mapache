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
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use blake3::Hasher;
use serde::{Deserialize, Serialize};

use crate::utils::hashing::{Hash, Hashable};

use super::{
    config::Config,
    storage::SecureStorage,
    tree::{DirectoryMetadata, DirectoryNode, FileEntry},
};

const DEFAULT_META_COMPRESSION_LEVEL: i32 = 22;

pub struct Repository {
    root_path: PathBuf,
    data_path: PathBuf,
    snapshot_path: PathBuf,
    tree_path: PathBuf,

    secure_storage: SecureStorage,
    config: Config,
}

impl Repository {
    fn new(root_path: &Path, password: String) -> Self {
        Self {
            root_path: root_path.to_owned(),
            data_path: root_path.join("data").to_owned(),
            snapshot_path: root_path.join("snapshot").to_owned(),
            tree_path: root_path.join("tree").to_owned(),

            secure_storage: SecureStorage::new(password),
            config: Config::default(),
        }
    }

    /// Create and initialize a new repository
    pub fn init(repo_path: &Path, password: String) -> Result<Self> {
        if repo_path.exists() {
            bail!(format!(
                "Could not initialize a repository because a directory already exists in \'{}\'",
                repo_path.to_string_lossy()
            ));
        }

        let repo = Self::new(repo_path, password);

        repo.init_structure()
            .with_context(|| "Could not initialize repository structure")?;

        repo.persist_config()?;

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

        let mut repo = Repository::new(repo_path, password);
        repo.load_config()?;

        Ok(repo)
    }

    pub fn get_config(&self) -> &Config {
        &self.config
    }

    pub fn set_config(&mut self, config: &Config) {
        self.config = config.clone();
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
                .save_json(
                    &serializable_directory_node,
                    &serialized_tree_path,
                    DEFAULT_META_COMPRESSION_LEVEL,
                )
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

    /**
     * Create the repository structure.
     * This includes the data subdirectories, meta, etc.
     */
    fn init_structure(&self) -> Result<()> {
        std::fs::create_dir_all(&self.root_path)
            .with_context(|| "Could not create root directory")?;

        std::fs::create_dir(&self.data_path)?;
        for n in 0x00..=0xff {
            std::fs::create_dir(&self.data_path.join(format!("{:02x}", n)))?;
        }

        std::fs::create_dir(&self.snapshot_path)?;
        std::fs::create_dir(&self.tree_path)?;

        Ok(())
    }

    /// Load config
    fn load_config(&mut self) -> Result<()> {
        let config_path = self.root_path.join("config");
        self.config = self.secure_storage.load_json(&config_path)?;

        Ok(())
    }

    fn persist_config(&self) -> Result<()> {
        self.secure_storage
            .save_json(
                &self.config,
                &self.root_path.join("config"),
                DEFAULT_META_COMPRESSION_LEVEL,
            )
            .with_context(|| "Could not persist config file")
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

    use crate::utils;

    use super::*;

    #[test]
    #[ignore]
    /// Test saving and loading tree objects
    /// This test creates a repository in a temp folder
    fn heavy_test_persist_tree() -> Result<()> {
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
}
