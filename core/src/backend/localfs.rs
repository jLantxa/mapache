use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, anyhow};

use crate::{
    backend::{Handle, NodeAttr},
    fs,
    repository::repo::REPO_TMP_EXTENSION,
};

use super::StorageBackend;

/// A local file system backend.
///
/// This backend stores repository data directly on the host's disk. It implements
/// specific logic to handle cross-platform permission differences and ensures
/// "Atomic" writes by using temporary files and renames.
#[derive(Default)]
pub struct LocalFS {
    base_path: PathBuf,
}

impl LocalFS {
    /// Creates a new `LocalFS` instance anchored at the given `base_path`.
    pub fn new(base_path: PathBuf) -> Self {
        Self { base_path }
    }

    /// Helper to resolve a repository-relative path to an absolute path on disk.
    fn full_path(&self, path: &Path) -> PathBuf {
        self.base_path.join(path)
    }

    #[inline(always)]
    fn exists_exact(&self, path: &Path) -> bool {
        fs::path_exists(path)
    }

    /// Safely sets or unsets the read-only flag on a file.
    ///
    /// This is used to make repository files immutable after they are written,
    /// protecting them from accidental modification. On Windows, this is also
    /// required before a file can be overwritten or renamed.
    fn set_readonly_status(&self, path: &Path, readonly: bool) -> Result<()> {
        let full_path = self.full_path(path);

        // If unsetting read-only and file doesn't exist, just return Ok
        let metadata = match std::fs::metadata(&full_path) {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && !readonly => return Ok(()),
            Err(e) => {
                return Err(e).context(format!(
                    "Failed to retrieve metadata for permission change: {}",
                    full_path.display()
                ));
            }
        };

        let mut perms = metadata.permissions();

        #[cfg(unix)]
        {
            use crate::backend::set_readonly_mode;
            use std::os::unix::fs::PermissionsExt;

            let mode = set_readonly_mode(perms.mode(), readonly, metadata.is_dir());
            perms.set_mode(mode);
        }

        #[cfg(windows)]
        {
            perms.set_readonly(readonly);
        }

        std::fs::set_permissions(&full_path, perms).with_context(|| {
            format!(
                "Failed to set permissions (readonly={}) on {}",
                readonly,
                full_path.display()
            )
        })
    }
}

impl StorageBackend for LocalFS {
    fn create(&self) -> Result<()> {
        std::fs::create_dir_all(&self.base_path).with_context(|| {
            format!(
                "Could not create repository backend root at {}",
                self.base_path.display()
            )
        })
    }

    fn read(&self, handle: &Handle, offset: isize, length: usize) -> Result<Vec<u8>> {
        let path = handle.path;
        let full_path = self.full_path(path);

        let mut file = File::open(&full_path)
            .with_context(|| format!("Could not open file for reading: '{}'", path.display()))?;

        let file_size = file
            .metadata()
            .with_context(|| {
                format!(
                    "Could not get metadata for size calculation: '{}'",
                    path.display()
                )
            })?
            .len();

        let start_position: u64 = if offset >= 0 {
            offset as u64
        } else {
            let abs_offset = offset.unsigned_abs() as u64;
            file_size.saturating_sub(abs_offset)
        };

        file.seek(SeekFrom::Start(start_position))
            .with_context(|| {
                format!(
                    "Could not seek to {} in '{}'",
                    start_position,
                    path.display()
                )
            })?;

        let bytes_remaining: usize = file_size.saturating_sub(start_position) as usize;
        let read_length: usize = match length {
            0 => bytes_remaining,
            _ => std::cmp::min(length, bytes_remaining),
        };

        let mut data = vec![0; read_length];
        file.read_exact(&mut data).with_context(|| {
            format!(
                "Could not read {} bytes from '{}'",
                read_length,
                path.display()
            )
        })?;

        Ok(data)
    }

    fn write(&self, handle: &Handle, contents: &[u8]) -> Result<()> {
        let path = handle.path;
        let tmp_path = path.with_extension(REPO_TMP_EXTENSION);
        let full_tmp_path = self.full_path(&tmp_path);

        // Write to a tmp path. If we fail, the parent might be missing.
        if let Err(e) = std::fs::write(&full_tmp_path, contents) {
            let parent_dir = path
                .parent()
                .ok_or_else(|| anyhow!("Path '{}' has no parent directory", path.display()))?;

            self.create_dir(parent_dir)
                .context("Failed to auto-create missing parent directory during write")?;

            std::fs::write(&full_tmp_path, contents).with_context(|| {
                format!(
                    "Failed to write temporary file '{}' after creating parent: {}",
                    tmp_path.display(),
                    e
                )
            })?;
        }

        // Renaming on Windows might fail if the destination already exists and is Read-Only
        let full_path_to = self.full_path(path);
        let _ = self.set_readonly_status(&full_path_to, false);

        self.rename(&tmp_path, path)
            .context("Failed to commit temporary write via rename")?;

        // Repository files are locked (readonly) after writing to prevent accidental corruption
        let _ = self.set_readonly_status(path, true);

        Ok(())
    }

    fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let fullpath_from = self.full_path(from);
        let fullpath_to = self.full_path(to);

        // On Windows, the source must be writable to be renamed/moved
        let _ = self.set_readonly_status(from, false);

        std::fs::rename(&fullpath_from, &fullpath_to).with_context(|| {
            format!(
                "Could not rename '{}' to '{}' in local backend",
                from.display(),
                to.display()
            )
        })?;

        // Repository files are generally treated as immutable once written
        let _ = self.set_readonly_status(to, true);

        Ok(())
    }

    fn create_dir(&self, path: &Path) -> Result<()> {
        let full_path = self.full_path(path);
        std::fs::create_dir_all(&full_path)?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&full_path, std::fs::Permissions::from_mode(0o700))?;
        }
        Ok(())
    }

    fn remove(&self, path: &Path) -> Result<()> {
        let full_path = self.full_path(path);

        match std::fs::symlink_metadata(&full_path) {
            Ok(metadata) => {
                // Unlock file so it can actually be deleted
                let _ = self.set_readonly_status(path, false);

                if metadata.is_dir() {
                    std::fs::remove_dir_all(&full_path).with_context(|| {
                        format!(
                            "Could not remove directory '{}' recursively",
                            path.display()
                        )
                    })
                } else {
                    std::fs::remove_file(&full_path)
                        .with_context(|| format!("Could not remove file '{}'", path.display()))
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(anyhow!(e).context(format!(
                "Failed to determine type of path '{}' for removal",
                path.display()
            ))),
        }
    }

    fn path_exists(&self, path: &Path) -> bool {
        let full_path = self.full_path(path);
        self.exists_exact(&full_path)
    }

    fn list_dir(&self, path: &Path) -> Result<Vec<PathBuf>> {
        let full_path = self.full_path(path);
        let mut paths = Vec::new();

        let read_dir = std::fs::read_dir(&full_path).with_context(|| {
            format!("Could not list contents of directory '{}'", path.display())
        })?;

        for entry in read_dir {
            let entry = entry?;
            let entry_path = entry.path();

            // Strip the base_path to keep paths relative to the repo root
            let relative = entry_path
                .strip_prefix(&self.base_path)
                .map(Path::to_path_buf)
                .context("Found entry outside of repository base path")?;

            paths.push(relative);
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
        let meta = std::fs::symlink_metadata(&full_path)
            .with_context(|| format!("lstat failed for {}", path.display()))?;

        let (perm, uid, gid) = {
            #[cfg(unix)]
            {
                use std::os::unix::fs::{MetadataExt, PermissionsExt};
                (
                    Some(meta.permissions().mode()),
                    Some(meta.uid()),
                    Some(meta.gid()),
                )
            }
            #[cfg(not(unix))]
            {
                (None, None, None)
            }
        };

        Ok(NodeAttr {
            size: Some(meta.len()),
            uid,
            gid,
            perm,
            atime: meta.accessed().ok(),
            mtime: meta.modified().ok(),
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_local_fs() -> Result<()> {
        let temp_dir = tempdir()?;
        let local_fs = LocalFS::new(temp_dir.path().to_path_buf());

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
        assert!(local_fs.path_exists(&dir1));

        local_fs.remove(dir0)?;
        assert!(!local_fs.path_exists(dir0));

        // Read range test
        let seek_handle = Handle::new(Path::new("seek.txt"));
        local_fs.write(
            &seek_handle,
            b"I am just looking for a word in this sentence.",
        )?;
        let range_str = local_fs.read(&seek_handle, 10, 7)?;
        assert_eq!(range_str, b"looking");

        Ok(())
    }
}
