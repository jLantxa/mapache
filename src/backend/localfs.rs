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

use std::path::PathBuf;

use anyhow::{Context, Result};

use super::backend::StorageBackend;

#[derive(Default)]
pub struct LocalFS {}

impl LocalFS {
    pub fn new() -> Self {
        Self {}
    }
}

impl StorageBackend for LocalFS {
    fn read(&self, path: &std::path::Path) -> Result<Vec<u8>> {
        let data = std::fs::read(path).with_context(|| {
            format!(
                "Could not read from \'{}\'local backend",
                path.to_string_lossy()
            )
        })?;
        Ok(data)
    }

    fn write(&self, path: &std::path::Path, contents: &[u8]) -> Result<()> {
        std::fs::write(path, contents).with_context(|| {
            format!(
                "Could not write to \'{}\' in local backend",
                path.to_string_lossy()
            )
        })
    }

    fn create_dir(&self, path: &std::path::Path) -> Result<()> {
        std::fs::create_dir(path).with_context(|| {
            format!(
                "Could not create directory \'{}\' in local backend",
                path.to_string_lossy()
            )
        })
    }

    fn create_dir_all(&self, path: &std::path::Path) -> Result<()> {
        std::fs::create_dir_all(path).with_context(|| {
            format!(
                "Could not create directory \'{}\' in local backend",
                path.to_string_lossy()
            )
        })
    }

    fn remove_dir(&self, path: &std::path::Path) -> Result<()> {
        std::fs::remove_dir(path).with_context(|| {
            format!(
                "Could not remove directory \'{}\' in local backend",
                path.to_string_lossy()
            )
        })
    }

    fn remove_dir_all(&self, path: &std::path::Path) -> Result<()> {
        std::fs::remove_dir_all(path).with_context(|| {
            format!(
                "Could not remove directory \'{}\' in local backend",
                path.to_string_lossy()
            )
        })
    }

    fn exists(&self, path: &std::path::Path) -> Result<bool> {
        std::fs::exists(path).with_context(|| {
            format!(
                "Could not check if \'{}\' exists in local backend",
                path.to_string_lossy()
            )
        })
    }

    fn read_dir(&self, path: &std::path::Path) -> Result<Vec<PathBuf>> {
        let mut paths = Vec::new();
        for entry in std::fs::read_dir(path).with_context(|| {
            format!(
                "Could not list directory \'{}\' in local backend",
                path.to_string_lossy()
            )
        })? {
            let entry = entry?;
            paths.push(entry.path());
        }

        Ok(paths)
    }
}

#[cfg(test)]
mod test {
    use tempfile::tempdir;

    use super::*;

    #[test]
    #[ignore]
    fn heavy_test_local_fs() -> Result<()> {
        let temp_dir = tempdir()?;
        let temp_dir = temp_dir.path();
        let local_fs = Box::new(LocalFS::new());

        let write_path = temp_dir.join("file.txt");
        local_fs.write(&write_path, b"Mapachito")?;
        let read_content = local_fs.read(&write_path)?;

        assert!(local_fs.exists(&write_path)?);
        assert_eq!(read_content, b"Mapachito");

        let dir0 = temp_dir.join("dir0");
        let intermediate = dir0.join("intermediate");
        let dir1 = intermediate.join("dir1");
        local_fs.create_dir(&dir0)?;
        local_fs.create_dir_all(&dir1)?;
        assert!(local_fs.exists(&dir0)?);
        assert!(local_fs.exists(&intermediate)?);
        assert!(local_fs.exists(&dir1)?);

        local_fs.remove_dir(&dir1)?;
        assert!(false == local_fs.exists(&dir1)?);
        local_fs.remove_dir_all(&dir0)?;
        assert!(false == local_fs.exists(&dir0)?);
        assert!(false == local_fs.exists(&intermediate)?);
        assert!(false == local_fs.exists(&dir1)?);

        let invalid_path = temp_dir.join("fake_path");
        assert!(false == local_fs.exists(&invalid_path)?);
        assert!(local_fs.read(&invalid_path).is_err());

        Ok(())
    }
}
