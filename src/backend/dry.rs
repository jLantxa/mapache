// mapache is a secure, de-duplicating, incremental backup tool.
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

use crate::backend::Handle;

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
    fn read(&self, handle: &Handle, offset: isize, length: usize) -> Result<Vec<u8>> {
        self.backend.read(handle, offset, length)
    }

    #[inline]
    fn write(&self, _handle: &Handle, _contents: &[u8]) -> Result<()> {
        Ok(())
    }

    #[inline]
    fn rename(&self, _from: &Path, _to: &Path) -> Result<()> {
        Ok(())
    }

    #[inline]
    fn create_dir(&self, _path: &Path) -> Result<()> {
        Ok(())
    }

    #[inline]
    fn remove(&self, _file_path: &Path) -> Result<()> {
        Ok(())
    }

    #[inline]
    fn path_exists(&self, path: &Path) -> bool {
        self.backend.path_exists(path)
    }

    #[inline]
    fn list_dir(&self, path: &Path) -> Result<Vec<PathBuf>> {
        self.backend.list_dir(path)
    }

    #[inline]
    fn is_file(&self, path: &Path) -> bool {
        self.backend.is_file(path)
    }

    #[inline]
    fn is_dir(&self, path: &Path) -> bool {
        self.backend.is_dir(path)
    }

    fn lstat(&self, path: &Path) -> Result<super::NodeAttr> {
        self.backend.lstat(path)
    }
}
