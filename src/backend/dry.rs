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

use super::StorageBackend;

/// A dummy storage backend that sets itself before another backend, redirecting
/// reads but ignoring writes.
pub struct DryBackend {
    backend: Arc<dyn StorageBackend>,
}

impl DryBackend {
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        Self { backend }
    }
}

impl StorageBackend for DryBackend {
    #[inline]
    fn create(&self) -> Result<()> {
        self.backend.create()
    }

    #[inline]
    fn root_exists(&self) -> bool {
        self.backend.root_exists()
    }

    #[inline]
    fn read(&self, path: &Path) -> Result<Vec<u8>> {
        self.backend.read(path)
    }

    #[inline]
    fn read_seek(&self, path: &Path, offset: u64, length: u64) -> Result<Vec<u8>> {
        self.backend.read_seek(path, offset, length)
    }

    #[inline]
    fn write(&self, path: &Path, contents: &[u8]) -> Result<()> {
        let _ = contents;
        let _ = path;
        Ok(())
    }

    #[inline]
    fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let _ = to;
        let _ = from;
        Ok(())
    }

    #[inline]
    fn remove_file(&self, file_path: &Path) -> Result<()> {
        let _ = file_path;
        Ok(())
    }

    #[inline]
    fn create_dir(&self, path: &Path) -> Result<()> {
        let _ = path;
        Ok(())
    }

    #[inline]
    fn create_dir_all(&self, path: &Path) -> Result<()> {
        let _ = path;
        Ok(())
    }

    #[inline]
    fn remove_dir(&self, path: &Path) -> Result<()> {
        let _ = path;
        Ok(())
    }

    #[inline]
    fn remove_dir_all(&self, path: &Path) -> Result<()> {
        let _ = path;
        Ok(())
    }

    #[inline]
    fn exists(&self, path: &Path) -> bool {
        self.backend.exists(path)
    }

    #[inline]
    fn read_dir(&self, path: &Path) -> Result<Vec<PathBuf>> {
        self.backend.read_dir(path)
    }

    #[inline]
    fn is_file(&self, path: &Path) -> bool {
        self.backend.is_file(path)
    }

    #[inline]
    fn is_dir(&self, path: &Path) -> bool {
        self.backend.is_dir(path)
    }
}
