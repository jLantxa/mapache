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

use std::sync::Arc;

use super::backend::StorageBackend;

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
    fn read(&self, path: &std::path::Path) -> anyhow::Result<Vec<u8>> {
        self.backend.read(path)
    }

    fn write(&self, path: &std::path::Path, contents: &[u8]) -> anyhow::Result<()> {
        let _ = contents;
        let _ = path;
        Ok(())
    }

    fn rename(&self, from: &std::path::Path, to: &std::path::Path) -> anyhow::Result<()> {
        let _ = to;
        let _ = from;
        Ok(())
    }

    fn create_dir(&self, path: &std::path::Path) -> anyhow::Result<()> {
        let _ = path;
        Ok(())
    }

    fn create_dir_all(&self, path: &std::path::Path) -> anyhow::Result<()> {
        let _ = path;
        Ok(())
    }

    fn remove_dir(&self, path: &std::path::Path) -> anyhow::Result<()> {
        let _ = path;
        Ok(())
    }

    fn remove_dir_all(&self, path: &std::path::Path) -> anyhow::Result<()> {
        let _ = path;
        Ok(())
    }

    fn exists(&self, path: &std::path::Path) -> anyhow::Result<bool> {
        let _ = path;
        todo!()
    }

    fn read_dir(&self, path: &std::path::Path) -> anyhow::Result<Vec<std::path::PathBuf>> {
        self.backend.read_dir(path)
    }
}
