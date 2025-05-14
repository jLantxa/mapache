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
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};

use super::backend::StorageBackend;

/// A local file system
#[derive(Default)]
pub struct LocalFS {}

impl LocalFS {
    pub fn new() -> Self {
        Self {}
    }
}

impl StorageBackend for LocalFS {
    fn read(&self, path: &Path) -> Result<Vec<u8>> {
        let data = std::fs::read(path)
            .with_context(|| format!("Could not read \'{}\' from local backend", path.display()))?;
        Ok(data)
    }

    fn read_seek(&self, path: &Path, offset: u64, length: u64) -> Result<Vec<u8>> {
        let mut file = std::fs::File::open(path).context(format!(
            "Could not open file {} for range reading from local filesystem",
            path.display()
        ))?;

        // Seek to the specified offset
        file.seek(SeekFrom::Start(offset)).context(format!(
            "Could not seek to offset {} in local file {:?}",
            offset, path
        ))?;

        // Read the specified number of bytes
        let mut buffer = vec![0; length as usize];
        file.read_exact(&mut buffer).context(format!(
            "Could not read {} bytes from offset {} in local file {}",
            length,
            offset,
            path.display()
        ))?;

        Ok(buffer)
    }

    fn write(&self, path: &Path, contents: &[u8]) -> Result<()> {
        std::fs::write(path, contents)
            .with_context(|| format!("Could not write to \'{}\' in local backend", path.display()))
    }

    fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        std::fs::rename(from, to).with_context(|| {
            format!(
                "Could not rename \'{}\' to \'{}\' in local backend",
                from.display(),
                to.display()
            )
        })
    }

    fn remove_file(&self, file_path: &Path) -> Result<()> {
        std::fs::remove_file(file_path).with_context(|| {
            format!(
                "Could not remove file \'{}\' from local backend",
                file_path.display()
            )
        })
    }

    fn create_dir(&self, path: &Path) -> Result<()> {
        std::fs::create_dir(path).with_context(|| {
            format!(
                "Could not create directory \'{}\' in local backend",
                path.display()
            )
        })
    }

    fn create_dir_all(&self, path: &Path) -> Result<()> {
        std::fs::create_dir_all(path).with_context(|| {
            format!(
                "Could not create directory \'{}\' in local backend",
                path.display()
            )
        })
    }

    fn remove_dir(&self, path: &Path) -> Result<()> {
        std::fs::remove_dir(path).with_context(|| {
            format!(
                "Could not remove directory \'{}\' in local backend",
                path.display()
            )
        })
    }

    fn remove_dir_all(&self, path: &Path) -> Result<()> {
        std::fs::remove_dir_all(path).with_context(|| {
            format!(
                "Could not remove directory \'{}\' in local backend",
                path.display()
            )
        })
    }

    fn exists(&self, path: &Path) -> Result<bool> {
        std::fs::exists(path).with_context(|| {
            format!(
                "Could not check if \'{}\' exists in local backend",
                path.display()
            )
        })
    }

    fn read_dir(&self, path: &Path) -> Result<Vec<PathBuf>> {
        let mut paths = Vec::new();
        for entry in std::fs::read_dir(path).with_context(|| {
            format!(
                "Could not list directory \'{}\' in local backend",
                path.display()
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

    fn test_local_fs() -> Result<()> {
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

        // Read range
        let seek_path = temp_dir.join("seek.txt.");
        local_fs.write(
            &seek_path,
            b"I am just looking for a word in this sentence.",
        )?;
        let range_str = local_fs.read_seek(&seek_path, 10, 7)?;
        assert_eq!(range_str, b"looking");

        Ok(())
    }
}
