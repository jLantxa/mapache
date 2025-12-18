use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};

use crate::{
    backend::{Handle, NodeAttr},
    fs,
    repository::repo::REPO_TMP_EXTENSION,
};

use super::StorageBackend;

/// A local file system
#[derive(Default)]
pub struct LocalFS {
    base_path: PathBuf,
}

impl LocalFS {
    pub fn new(base_path: PathBuf) -> Self {
        Self { base_path }
    }

    fn full_path(&self, path: &Path) -> PathBuf {
        self.base_path.join(path)
    }

    fn exists_exact(&self, path: &Path) -> bool {
        fs::path_exists(path)
    }

    fn set_readonly_status(&self, path: &Path, readonly: bool) -> Result<()> {
        let full_path = self.full_path(path);

        // If unsetting read-only and file doesn't exist, just return Ok
        let metadata = match std::fs::metadata(&full_path) {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && !readonly => return Ok(()),
            Err(e) => return Err(e).context(format!("Metadata error for {}", full_path.display())),
        };

        let mut perms = metadata.permissions();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut mode = perms.mode();
            if readonly {
                mode &= !0o222; // Remove all write bits
            } else {
                mode |= 0o600; // Restore owner read/write
            }
            perms.set_mode(mode);
        }

        #[cfg(windows)]
        {
            perms.set_readonly(readonly);
        }

        std::fs::set_permissions(&full_path, perms)
            .with_context(|| format!("Failed to set permissions on {}", full_path.display()))
    }
}

impl StorageBackend for LocalFS {
    fn create(&self) -> Result<()> {
        // Create the repo root folder
        std::fs::create_dir_all(&self.base_path).context("Could not create repository backend root")
    }

    #[inline]
    fn root_exists(&self) -> bool {
        self.exists_exact(&self.base_path)
    }

    fn read(&self, handle: &Handle, offset: isize, length: usize) -> Result<Vec<u8>> {
        let path = handle.path;
        let full_path = self.full_path(path);

        let mut file = File::open(&full_path)
            .with_context(|| format!("Could not open file '{}'", path.display()))?;

        let file_size = file
            .metadata()
            .with_context(|| format!("Could not get metadata for '{}'", path.display()))?
            .len();

        let start_position: u64 = if offset >= 0 {
            offset as u64
        } else {
            let abs_offset = offset.unsigned_abs() as u64;
            file_size.saturating_sub(abs_offset)
        };

        file.seek(SeekFrom::Start(start_position))
            .with_context(|| format!("Could not seek to position in '{}'", path.display()))?;

        let bytes_remaining: usize = file_size.saturating_sub(start_position) as usize;
        let read_length: usize = match length {
            0 => bytes_remaining,
            _ => std::cmp::min(length, bytes_remaining),
        };

        let mut data = vec![0; read_length];
        file.read_exact(&mut data)
            .with_context(|| format!("Could not read '{}' from local backend", path.display()))?;

        Ok(data)
    }

    fn write(&self, handle: &Handle, contents: &[u8]) -> Result<()> {
        let path = handle.path;
        let tmp_path = path.with_extension(REPO_TMP_EXTENSION);
        let full_tmp_path = self.full_path(&tmp_path);

        // Write to a tmp path
        if std::fs::write(&full_tmp_path, contents).is_err() {
            // If error, try creating the parent directory first and try again.
            let parent_dir = path.parent().with_context(|| {
                format!(
                    "Could not create parent directory for '{}' in local backend",
                    path.display()
                )
            })?;
            let _ = self.create_dir(parent_dir);

            std::fs::write(&full_tmp_path, contents)
                .with_context(|| format!("Could not write to '{}'", tmp_path.display()))?;
        }

        // Renaming on Windows might fail if the destination already exists and is Read-Only
        let full_path_to = self.full_path(path);
        let _ = self.set_readonly_status(&full_path_to, false);

        self.rename(&tmp_path, path)?;
        let _ = self.set_readonly_status(path, true);

        Ok(())
    }

    fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let fullpath_from = self.full_path(from);
        let fullpath_to = self.full_path(to);

        // On Windows, the source must be writable to be renamed/moved
        let _ = self.set_readonly_status(&fullpath_from, false);

        std::fs::rename(fullpath_from, fullpath_to).with_context(|| {
            format!(
                "Could not rename '{}' to '{}' in local backend",
                from.display(),
                to.display()
            )
        })?;

        let _ = self.set_readonly_status(to, true);

        Ok(())
    }

    fn create_dir(&self, path: &Path) -> Result<()> {
        let full_path = self.full_path(path);
        std::fs::create_dir_all(full_path).with_context(|| {
            format!(
                "Could not create directory '{}' in local backend",
                path.display()
            )
        })
    }

    fn remove(&self, path: &Path) -> Result<()> {
        let full_path = self.full_path(path);

        match std::fs::symlink_metadata(&full_path) {
            Ok(metadata) => {
                let _ = self.set_readonly_status(&full_path, false);

                if metadata.is_dir() {
                    std::fs::remove_dir_all(&full_path).with_context(|| {
                        format!(
                            "Could not remove directory '{}' recursively from local backend",
                            path.display()
                        )
                    })
                } else {
                    std::fs::remove_file(&full_path).with_context(|| {
                        format!(
                            "Could not remove file '{}' from local backend",
                            path.display()
                        )
                    })
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e).context(format!(
                "Failed to determine type of path '{}' for removal",
                path.display()
            )),
        }
    }

    fn path_exists(&self, path: &Path) -> bool {
        let full_path = self.full_path(path);
        self.exists_exact(&full_path)
    }

    fn list_dir(&self, path: &Path) -> Result<Vec<PathBuf>> {
        let full_path = self.full_path(path);
        let mut paths = Vec::new();
        for entry in std::fs::read_dir(full_path).with_context(|| {
            format!(
                "Could not list directory '{}' in local backend",
                path.display()
            )
        })? {
            let entry = entry?;
            paths.push(
                entry
                    .path()
                    .strip_prefix(&self.base_path)
                    .unwrap()
                    .to_path_buf(),
            );
        }

        Ok(paths)
    }

    fn is_file(&self, path: &Path) -> bool {
        self.full_path(path).is_file()
    }

    fn is_dir(&self, path: &Path) -> bool {
        self.full_path(path).is_dir()
    }

    fn lstat(&self, path: &Path) -> Result<super::NodeAttr> {
        let full_path = self.full_path(path);
        let meta = std::fs::symlink_metadata(&full_path)?;

        Ok(NodeAttr {
            size: Some(meta.len()),
            uid: None,
            gid: None,
            perm: None,
            atime: Some(meta.accessed()?),
            mtime: Some(meta.modified()?),
        })
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn test_local_fs() -> Result<()> {
        let temp_dir = tempdir()?;
        let temp_dir = temp_dir.path();
        let local_fs = Box::new(LocalFS::new(temp_dir.to_path_buf()));

        let write_handle = Handle::new(Path::new("file.txt"));
        local_fs.write(&write_handle, b"Mapachito")?;
        let read_content = local_fs.read(&write_handle, 0, 0)?;

        assert!(local_fs.path_exists(write_handle.path));
        assert_eq!(read_content, b"Mapachito");

        let dir0 = Path::new("dir0");
        let intermediate = dir0.join("intermediate");
        let dir1 = intermediate.join("dir1");
        local_fs.create_dir(&dir1)?;
        assert!(local_fs.path_exists(dir0));
        assert!(local_fs.path_exists(&intermediate));
        assert!(local_fs.path_exists(&dir1));

        local_fs.remove(&dir1)?;
        assert!(!local_fs.path_exists(&dir1));
        local_fs.remove(dir0)?;
        assert!(!local_fs.path_exists(dir0));
        assert!(!local_fs.path_exists(&intermediate));
        assert!(!local_fs.path_exists(&dir1));

        let invalid_handle = Handle::new(Path::new("fake_path"));
        assert!(!local_fs.path_exists(invalid_handle.path));
        assert!(local_fs.read(&invalid_handle, 0, 0).is_err());

        // Read range
        let seek_handle = Handle::new(Path::new("seek.txt."));
        local_fs.write(
            &seek_handle,
            b"I am just looking for a word in this sentence.",
        )?;
        let range_str = local_fs.read(&seek_handle, 10, 7)?;
        assert_eq!(range_str, b"looking");

        Ok(())
    }
}
