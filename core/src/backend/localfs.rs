use std::{
    io::SeekFrom,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use crate::{
    backend::{Handle, NodeAttr, WriteContents},
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
    async fn exists_exact(&self, path: &Path) -> bool {
        tokio::fs::symlink_metadata(path).await.is_ok()
    }

    /// Safely sets or unsets the read-only flag on a file.
    ///
    /// This is used to make repository files immutable after they are written,
    /// protecting them from accidental modification. On Windows, this is also
    /// required before a file can be overwritten or renamed.
    async fn set_readonly_status(&self, path: &Path, readonly: bool) -> Result<()> {
        let full_path = self.full_path(path);

        // If unsetting read-only and file doesn't exist, just return Ok
        let metadata = match tokio::fs::metadata(&full_path).await {
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

        tokio::fs::set_permissions(&full_path, perms)
            .await
            .with_context(|| {
                format!(
                    "Failed to set permissions (readonly={}) on {}",
                    readonly,
                    full_path.display()
                )
            })
    }

    /// Synchronous version of set_readonly_status for use in blocking tasks.
    fn set_readonly_status_sync(full_path: &Path, readonly: bool) -> std::io::Result<()> {
        let metadata = match std::fs::metadata(full_path) {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && !readonly => return Ok(()),
            Err(e) => return Err(e),
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

        std::fs::set_permissions(full_path, perms)
    }
}

#[async_trait]
impl StorageBackend for LocalFS {
    async fn create(&self) -> Result<()> {
        tokio::fs::create_dir_all(&self.base_path)
            .await
            .with_context(|| {
                format!(
                    "Could not create repository backend root at {}",
                    self.base_path.display()
                )
            })
    }

    async fn read(&self, handle: &Handle, offset: isize, length: usize) -> Result<Vec<u8>> {
        let path = handle.path;
        let full_path = self.full_path(path);

        let mut file = tokio::fs::File::open(&full_path)
            .await
            .with_context(|| format!("Could not open file for reading: '{}'", path.display()))?;

        let metadata = file.metadata().await.with_context(|| {
            format!(
                "Could not get metadata for size calculation: '{}'",
                path.display()
            )
        })?;
        let file_size = metadata.len();

        let start_position: u64 = if offset >= 0 {
            offset as u64
        } else {
            let abs_offset = offset.unsigned_abs() as u64;
            file_size.saturating_sub(abs_offset)
        };

        file.seek(SeekFrom::Start(start_position))
            .await
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

        let mut data = vec![0u8; read_length];
        file.read_exact(&mut data).await.with_context(|| {
            format!(
                "Could not read {} bytes from '{}'",
                read_length,
                path.display()
            )
        })?;

        Ok(data)
    }

    async fn write(&self, handle: &Handle, contents: WriteContents<'_>) -> Result<()> {
        let path = handle.path.to_path_buf();
        let full_base_path = self.base_path.clone();
        let data = contents.into_owned();

        tokio::task::spawn_blocking(move || {
            let tmp_path = path.with_extension(REPO_TMP_EXTENSION);
            let full_tmp_path = full_base_path.join(&tmp_path);
            let full_path = full_base_path.join(&path);

            // Write to temporary file with durability guarantee
            let write_result = (|| -> std::io::Result<()> {
                use std::io::Write;
                let mut file = std::fs::File::create(&full_tmp_path)?;
                file.write_all(&data)?;
                file.sync_all()?;
                Ok(())
            })();

            if let Err(e) = write_result {
                if e.kind() == std::io::ErrorKind::NotFound {
                    if let Some(parent) = full_tmp_path.parent() {
                        std::fs::create_dir_all(parent)?;
                    }
                    use std::io::Write;
                    let mut file = std::fs::File::create(&full_tmp_path)?;
                    file.write_all(&data)?;
                    file.sync_all()?;
                } else {
                    return Err(anyhow::Error::from(e));
                }
            }

            let _ = Self::set_readonly_status_sync(&full_path, false);

            std::fs::rename(&full_tmp_path, &full_path)?;

            let _ = Self::set_readonly_status_sync(&full_path, true);

            Ok(())
        })
        .await
        .map_err(|e| anyhow!("Blocking task failed: {}", e))?
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let fullpath_from = self.full_path(from);
        let fullpath_to = self.full_path(to);

        // On Windows, the source must be writable to be renamed/moved
        let _ = self.set_readonly_status(from, false).await;

        tokio::fs::rename(&fullpath_from, &fullpath_to)
            .await
            .with_context(|| {
                format!(
                    "Could not rename '{}' to '{}' in local backend",
                    from.display(),
                    to.display()
                )
            })?;

        // Repository files are generally treated as immutable once written
        let _ = self.set_readonly_status(to, true).await;

        Ok(())
    }

    async fn create_dir(&self, path: &Path) -> Result<()> {
        let full_path = self.full_path(path);
        tokio::fs::create_dir_all(&full_path).await?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            tokio::fs::set_permissions(&full_path, std::fs::Permissions::from_mode(0o700)).await?;
        }
        Ok(())
    }

    async fn remove(&self, path: &Path) -> Result<()> {
        let full_path = self.full_path(path);

        match tokio::fs::symlink_metadata(&full_path).await {
            Ok(metadata) => {
                // Unlock file so it can actually be deleted
                let _ = self.set_readonly_status(path, false).await;

                if metadata.is_dir() {
                    tokio::fs::remove_dir_all(&full_path)
                        .await
                        .with_context(|| {
                            format!(
                                "Could not remove directory '{}' recursively",
                                path.display()
                            )
                        })
                } else {
                    tokio::fs::remove_file(&full_path)
                        .await
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

    async fn path_exists(&self, path: &Path) -> bool {
        let full_path = self.full_path(path);
        self.exists_exact(&full_path).await
    }

    async fn list_dir(&self, path: &Path) -> Result<Vec<PathBuf>> {
        let full_path = self.full_path(path);
        let mut paths = Vec::new();

        let mut read_dir = tokio::fs::read_dir(&full_path).await.with_context(|| {
            format!("Could not list contents of directory '{}'", path.display())
        })?;

        while let Some(entry) = read_dir
            .next_entry()
            .await
            .with_context(|| format!("Failed while iterating directory '{}'", path.display()))?
        {
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

    async fn is_file(&self, path: &Path) -> bool {
        self.full_path(path).is_file()
    }

    async fn is_dir(&self, path: &Path) -> bool {
        self.full_path(path).is_dir()
    }

    async fn lstat(&self, path: &Path) -> Result<super::NodeAttr> {
        let full_path = self.full_path(path);
        let meta = tokio::fs::symlink_metadata(&full_path)
            .await
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

    #[tokio::test]
    async fn test_local_fs() -> Result<()> {
        let temp_dir = tempdir()?;
        let local_fs = LocalFS::new(temp_dir.path().to_path_buf());

        let write_handle = Handle::new(Path::new("file.txt"));
        local_fs
            .write(&write_handle, WriteContents::Borrowed(b"Mapachito"))
            .await?;
        let read_content = local_fs.read(&write_handle, 0, 0).await?;

        assert!(local_fs.path_exists(write_handle.path).await);
        assert_eq!(read_content, b"Mapachito");

        let dir0 = Path::new("dir0");
        let intermediate = dir0.join("intermediate");
        let dir1 = intermediate.join("dir1");
        local_fs.create_dir(&dir1).await?;
        assert!(local_fs.path_exists(dir0).await);
        assert!(local_fs.path_exists(&dir1).await);

        local_fs.remove(dir0).await?;
        assert!(!local_fs.path_exists(dir0).await);

        // Read range test
        let seek_handle = Handle::new(Path::new("seek.txt"));
        local_fs
            .write(
                &seek_handle,
                WriteContents::Borrowed(b"I am just looking for a word in this sentence."),
            )
            .await?;
        let range_str = local_fs.read(&seek_handle, 10, 7).await?;
        assert_eq!(range_str, b"looking");

        Ok(())
    }
}
