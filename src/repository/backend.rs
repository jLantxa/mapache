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

use anyhow::{Context, Result, bail};

use crate::filesystem::tree::{FileNode, Tree};
use crate::storage_backend::backend::StorageBackend;
use crate::utils::Hash;

use super::repository_v1;
use super::snapshot::Snapshot;

pub type RepoVersion = u32;
pub const LATEST_REPOSITORY_VERSION: RepoVersion = 1;

#[derive(Debug)]
pub struct ChunkResult {
    pub chunks: Vec<Hash>,
    pub total_bytes_read: usize,
    pub total_bytes_written: usize,
}

pub trait RepositoryBackend {
    /// Create and initialize a new repository
    fn init(
        storage_backend: Arc<dyn StorageBackend>,
        repo_path: &Path,
        password: String,
    ) -> Result<Self>
    where
        Self: Sized;

    /// Open an existing repository from a directory
    fn open(
        storage_backend: Arc<dyn StorageBackend>,
        repo_path: &Path,
        password: String,
    ) -> Result<Self>
    where
        Self: Sized;

    fn put_file(&self, src_path: &Path) -> Result<ChunkResult>;

    fn restore_file(&self, file: &FileNode, dst_path: &Path) -> Result<()>;

    /// Serializes a Tree into SerializableTreeObject's into the repository storage.
    fn put_tree(&self, tree: &Tree) -> Result<Hash>;

    /// Restores a Tree from the SerializableTreeObject's in the repository.
    fn get_tree(&self, root_hash: &Hash) -> Result<Tree>;

    /// Get a snapshot by hash
    fn get_snapshot(&self, hash: &Hash) -> Result<Option<Snapshot>>;

    /// Get all snapshots in the repository
    fn get_snapshots(&self) -> Result<Vec<(Hash, Snapshot)>>;

    /// Get all snapshots in the repository, sorted by datetime.
    fn get_snapshots_sorted(&self) -> Result<Vec<(Hash, Snapshot)>>;

    /// Saves a snapshot metadata
    fn save_snapshot(&self, snapshot: &Snapshot) -> Result<Hash>;
}

pub fn init_repository_with_version(
    version: RepoVersion,
    storage_backend: Arc<dyn StorageBackend>,
    repo_path: &Path,
    password: String,
) -> Result<Box<dyn RepositoryBackend>> {
    if version == 1 {
        let repo_v1 = repository_v1::Repository::init(storage_backend, repo_path, password)?;
        return Ok(Box::new(repo_v1));
    }

    bail!(format!("Invalid repository version \'{}\'", version));
}

pub fn open(
    storage_backend: Arc<dyn StorageBackend>,
    repo_path: &Path,
    password: String,
) -> Result<Box<dyn RepositoryBackend>> {
    let version = read_version(repo_path)?;
    open_repository_with_version(version, storage_backend, repo_path, password)
}

fn open_repository_with_version(
    version: RepoVersion,
    storage_backend: Arc<dyn StorageBackend>,
    repo_path: &Path,
    password: String,
) -> Result<Box<dyn RepositoryBackend>> {
    if version == 1 {
        let repo_v1 = repository_v1::Repository::open(storage_backend, repo_path, password)?;
        return Ok(Box::new(repo_v1));
    }

    bail!(format!("Invalid repository version \'{}\'", version));
}

const VERSION_FILE_NAME: &str = "version";

pub fn write_version(repo_path: &Path, version: RepoVersion) -> Result<()> {
    let version_file_path = repo_path.join(VERSION_FILE_NAME);
    std::fs::write(version_file_path, version.to_string())
        .with_context(|| "Could not create version file")
}

pub fn read_version(repo_path: &Path) -> Result<RepoVersion> {
    let version_file_path = repo_path.join(VERSION_FILE_NAME);
    let version_str = std::fs::read_to_string(version_file_path)
        .with_context(|| "Could not read the repository version")?;
    version_str
        .parse::<RepoVersion>()
        .with_context(|| format!("Invalid repository version \'{}\'", version_str))
}
