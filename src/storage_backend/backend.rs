use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Result;

use crate::cli;

use super::{dry::DryBackend, localfs::LocalFS, sftp::SftpBackend, url::BackendUrl};

/// Abstraction of a storage backend.
///
/// A backend is a filesystem that can be present in the local machine, a remote
/// machine connected via SFTP, a cloud service, etc.
///
/// This trait provides an interface for file IO operations with the backend.
pub trait StorageBackend: Send + Sync {
    /// Creates the necessary structure (typically just the repo root directory) for the backend
    fn create(&self) -> Result<()>;

    fn root_exists(&self) -> bool;

    /// Reads from file.
    fn read(&self, path: &Path) -> Result<Vec<u8>>;

    /// Reads a specific range of bytes from a file, starting at `offset` and reading `length`` bytes.
    fn read_seek(&self, path: &Path, offset: u64, length: u64) -> Result<Vec<u8>>;

    /// Writes to file, creating the file if necessary.
    fn write(&self, path: &Path, contents: &[u8]) -> Result<()>;

    /// Renames a file.
    fn rename(&self, from: &Path, to: &Path) -> Result<()>;

    /// Removes a file.
    fn remove_file(&self, file_path: &Path) -> Result<()>;

    /// Creates a new, empty directory at the provided path.
    fn create_dir(&self, path: &Path) -> Result<()>;

    /// Recursively create a directory and all of its parent components if they are missing.
    fn create_dir_all(&self, path: &Path) -> Result<()>;

    // List all paths inside a directory.
    fn read_dir(&self, path: &Path) -> Result<Vec<PathBuf>>;

    /// Removes an empty directory.
    fn remove_dir(&self, path: &Path) -> Result<()>;

    /// Removes a directory after removing its contents.
    fn remove_dir_all(&self, path: &Path) -> Result<()>;

    /// Returns true if a path exists.
    fn exists(&self, path: &Path) -> bool;

    // Returns true if the path is a file or an error if the path does not exist.
    fn is_file(&self, path: &Path) -> bool;

    // Returns true if the path is a directory or an error if the path does not exist.
    fn is_dir(&self, path: &Path) -> bool;
}

/// Encapsulates a StorageBackend inside a DryBackend.
#[inline]
pub fn make_dry_backend(backend: Arc<dyn StorageBackend>, dry: bool) -> Arc<dyn StorageBackend> {
    match dry {
        true => Arc::new(DryBackend::new(backend.clone())),
        false => backend,
    }
}

pub fn new_backend_with_prompt(url: &str) -> Result<Arc<dyn StorageBackend>> {
    let backend_url = BackendUrl::from(url)?;

    match backend_url {
        BackendUrl::Local(repo_path) => Ok(Arc::new(LocalFS::new(repo_path))),
        BackendUrl::Sftp(username, host, port, repo_path) => {
            let password = cli::request_password("Enter password for sftp");
            Ok(Arc::new(SftpBackend::new(
                repo_path, username, host, port, password,
            )?))
        }
    }
}
