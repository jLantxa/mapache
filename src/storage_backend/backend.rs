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
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Result;

use super::dry::DryBackend;

/// Abstraction of a storage backend.
///
/// A backend is a filesystem that can be present in the local machine, a remote
/// machine connected via SFTP, a cloud service, etc.
///
/// This trait provides an interface for file IO operations with the backend.
pub trait StorageBackend: Send + Sync {
    /// Read from file.
    fn read(&self, path: &Path) -> Result<Vec<u8>>;

    /// Write to file, creating the file if necessary.
    fn write(&self, path: &Path, contents: &[u8]) -> Result<()>;

    /// Rename a file
    fn rename(&self, from: &Path, to: &Path) -> Result<()>;

    /// Creates a new, empty directory at the provided path.
    fn create_dir(&self, path: &Path) -> Result<()>;

    /// Recursively create a directory and all of its parent components if they are missing.
    fn create_dir_all(&self, path: &Path) -> Result<()>;

    /// Removes an empty directory.
    fn remove_dir(&self, path: &Path) -> Result<()>;

    /// Removes a directory after removing its contents.
    fn remove_dir_all(&self, path: &Path) -> Result<()>;

    /// Returns true if a path exists.
    fn exists(&self, path: &Path) -> Result<bool>;

    // List all paths inside a directory.
    fn read_dir(&self, path: &Path) -> Result<Vec<PathBuf>>;
}

/// Encapsulates a StorageBackend inside a DryBackend.
#[inline]
pub fn make_dry_backend(backend: Arc<dyn StorageBackend>, dry: bool) -> Arc<dyn StorageBackend> {
    match dry {
        true => Arc::new(DryBackend::new(backend.clone())),
        false => backend,
    }
}
