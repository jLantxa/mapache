use std::{
    io::SeekFrom,
    path::{Path, PathBuf},
};

use crate::common::error::{MapacheError, Result};
use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use crate::{
    backend::{BackendNode, Handle, NodeAttr, StorageBackend, WriteContents},
    repository::repo::REPO_TMP_EXTENSION,
};

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
        super::join_base_path(&self.base_path, path)
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
                return Err(MapacheError::Backend(format!(
                    "failed to retrieve metadata for permission change: {}: {}",
                    full_path.display(),
                    e
                )));
            }
        };

        let mut perms = metadata.permissions();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            use crate::backend::set_readonly_mode;

            let mode = set_readonly_mode(perms.mode(), readonly, metadata.is_dir());
            perms.set_mode(mode);
        }

        #[cfg(windows)]
        {
            perms.set_readonly(readonly);
        }

        tokio::fs::set_permissions(&full_path, perms)
            .await
            .map_err(|e| {
                MapacheError::Backend(format!(
                    "failed to set permissions (readonly={}) on {}: {}",
                    readonly,
                    full_path.display(),
                    e
                ))
            })
    }

    /// Internal synchronous version of set_readonly_status for use in blocking tasks.
    fn set_readonly_status_internal(full_path: &Path, readonly: bool) -> std::io::Result<()> {
        let metadata = match std::fs::metadata(full_path) {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && !readonly => return Ok(()),
            Err(e) => return Err(e),
        };

        let mut perms = metadata.permissions();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            use crate::backend::set_readonly_mode;

            let mode = set_readonly_mode(perms.mode(), readonly, metadata.is_dir());
            perms.set_mode(mode);
        }

        #[cfg(windows)]
        {
            perms.set_readonly(readonly);
        }

        std::fs::set_permissions(full_path, perms)
    }

    /// Appends a temporary suffix to `path` instead of replacing its extension,
    /// so the temp file always maps back to its final destination.
    fn tmp_path(path: &Path) -> PathBuf {
        let mut os = path.as_os_str().to_os_string();
        os.push(format!(".{REPO_TMP_EXTENSION}"));
        PathBuf::from(os)
    }

    /// Persists the rename into the parent directory so the new entry survives
    /// a crash or power loss. Best-effort duplicates are handled by callers.
    fn sync_parent_dir(full_path: &Path) -> std::io::Result<()> {
        let parent = match full_path.parent() {
            Some(p) if !p.as_os_str().is_empty() => p,
            _ => return Ok(()),
        };
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x02000000;
            std::fs::OpenOptions::new()
                .write(true)
                .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
                .open(parent)?
                .sync_all()
        }
        #[cfg(not(windows))]
        {
            std::fs::File::open(parent)?.sync_all()
        }
    }
}

#[async_trait]
impl StorageBackend for LocalFS {
    async fn create(&self) -> Result<()> {
        tokio::fs::create_dir_all(&self.base_path)
            .await
            .map_err(|e| {
                MapacheError::Backend(format!(
                    "could not create repository backend root at {}: {}",
                    self.base_path.display(),
                    e
                ))
            })?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            tokio::fs::set_permissions(&self.base_path, std::fs::Permissions::from_mode(0o700))
                .await?;
        }
        Ok(())
    }

    async fn read(&self, handle: &Handle, offset: isize, length: usize) -> Result<Vec<u8>> {
        let path = handle.path;
        let full_path = self.full_path(path);
        tracing::trace!(target: "backend", "LocalFS: read {:?} (offset={}, length={})", path, offset, length);

        let mut file = tokio::fs::File::open(&full_path).await.map_err(|e| {
            MapacheError::Backend(format!(
                "could not open file for reading: '{}': {}",
                path.display(),
                e
            ))
        })?;

        let metadata = file.metadata().await.map_err(|e| {
            MapacheError::Backend(format!(
                "could not get metadata for size calculation: '{}': {}",
                path.display(),
                e
            ))
        })?;
        let file_size = metadata.len();

        let start_position = super::resolve_read_offset(file_size, offset);

        file.seek(SeekFrom::Start(start_position))
            .await
            .map_err(|e| {
                MapacheError::Backend(format!(
                    "could not seek to {} in '{}': {}",
                    start_position,
                    path.display(),
                    e
                ))
            })?;

        let bytes_remaining: usize = file_size.saturating_sub(start_position) as usize;
        let read_length: usize = match length {
            0 => bytes_remaining,
            _ => std::cmp::min(length, bytes_remaining),
        };

        let mut data = vec![0u8; read_length];
        file.read_exact(&mut data).await.map_err(|e| {
            MapacheError::Backend(format!(
                "could not read {} bytes from '{}': {}",
                read_length,
                path.display(),
                e
            ))
        })?;

        Ok(data)
    }

    async fn write(&self, handle: &Handle, contents: WriteContents<'_>) -> Result<()> {
        let path = handle.path.to_path_buf();
        let full_base_path = self.base_path.clone();
        let data = contents.into_owned();
        tracing::trace!(target: "backend", "LocalFS: write {:?} ({} bytes)", path, data.len());

        tokio::task::spawn_blocking(move || {
            let full_tmp_path = full_base_path.join(Self::tmp_path(&path));
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
                    return Err(MapacheError::Io(e));
                }
            }

            if let Err(e) = Self::set_readonly_status_internal(&full_path, false) {
                tracing::warn!(target: "backend", "LocalFS: failed to unlock {:?} before rename: {e}", full_path);
            }

            std::fs::rename(&full_tmp_path, &full_path)?;

            // Persist the rename itself, otherwise the new entry may be lost
            // even though the file content was synced.
            if let Err(e) = Self::sync_parent_dir(&full_path) {
                return Err(MapacheError::Io(e));
            }

            if let Err(e) = Self::set_readonly_status_internal(&full_path, true) {
                tracing::warn!(target: "backend", "LocalFS: failed to lock {:?} after rename: {e}", full_path);
            }

            Ok(())
        })
        .await
        .map_err(|e| MapacheError::Backend(format!("blocking task failed: {}", e)))?
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let fullpath_from = self.full_path(from);
        let fullpath_to = self.full_path(to);
        tracing::debug!(target: "backend", "LocalFS: rename {:?} -> {:?}", from, to);

        // On Windows, the source must be writable to be renamed/moved
        if let Err(e) = self.set_readonly_status(from, false).await {
            tracing::warn!(target: "backend", "LocalFS: failed to unlock source {:?} before rename: {e}", from);
        }
        // The destination must also be writable if it exists and we want to overwrite it
        if let Err(e) = self.set_readonly_status(to, false).await {
            tracing::warn!(target: "backend", "LocalFS: failed to unlock destination {:?} before rename: {e}", to);
        }

        tokio::fs::rename(&fullpath_from, &fullpath_to)
            .await
            .map_err(|e| {
                MapacheError::Backend(format!(
                    "could not rename '{}' to '{}' in local backend: {}",
                    from.display(),
                    to.display(),
                    e
                ))
            })?;

        // Persist the rename so it survives a crash before the content write.
        if let Err(e) = Self::sync_parent_dir(&fullpath_to) {
            return Err(MapacheError::Io(e));
        }

        // Repository files are generally treated as immutable once written
        if let Err(e) = self.set_readonly_status(to, true).await {
            tracing::warn!(target: "backend", "LocalFS: failed to lock destination {:?} after rename: {e}", to);
        }

        Ok(())
    }

    async fn create_dir(&self, path: &Path) -> Result<()> {
        let full_path = self.full_path(path);
        tracing::debug!(target: "backend", "LocalFS: create_dir {:?}", path);
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
        tracing::debug!(target: "backend", "LocalFS: remove {:?}", path);

        match tokio::fs::symlink_metadata(&full_path).await {
            Ok(metadata) => {
                // Unlock file so it can actually be deleted
                if let Err(e) = self.set_readonly_status(path, false).await {
                    tracing::warn!(target: "backend", "LocalFS: failed to unlock {:?} before remove: {e}", path);
                }

                if metadata.is_dir() {
                    tokio::fs::remove_dir_all(&full_path).await.map_err(|e| {
                        MapacheError::Backend(format!(
                            "could not remove directory '{}' recursively: {}",
                            path.display(),
                            e
                        ))
                    })
                } else {
                    tokio::fs::remove_file(&full_path).await.map_err(|e| {
                        MapacheError::Backend(format!(
                            "could not remove file '{}': {}",
                            path.display(),
                            e
                        ))
                    })
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(MapacheError::Backend(format!(
                "failed to determine type of path '{}' for removal: {}",
                path.display(),
                e
            ))),
        }
    }

    async fn path_exists(&self, path: &Path) -> bool {
        let full_path = self.full_path(path);
        self.exists_exact(&full_path).await
    }

    async fn list_dir(&self, path: &Path) -> Result<Vec<BackendNode>> {
        let full_path = self.full_path(path);
        let mut nodes = Vec::new();
        tracing::debug!(target: "backend", "LocalFS: list_dir {:?}", path);

        let mut read_dir = tokio::fs::read_dir(&full_path).await.map_err(|e| {
            MapacheError::Backend(format!(
                "could not list contents of directory '{}': {}",
                path.display(),
                e
            ))
        })?;

        while let Some(entry) = read_dir.next_entry().await.map_err(|e| {
            MapacheError::Backend(format!(
                "failed while iterating directory '{}': {}",
                path.display(),
                e
            ))
        })? {
            let entry_path = entry.path();
            let metadata = entry.metadata().await.map_err(|e| {
                MapacheError::Backend(format!(
                    "could not get metadata for '{}': {}",
                    entry_path.display(),
                    e
                ))
            })?;

            // Strip the base_path to keep paths relative to the repo root
            let relative = entry_path
                .strip_prefix(&self.base_path)
                .map(Path::to_path_buf)
                .map_err(|_| {
                    MapacheError::Backend("found entry outside of repository base path".into())
                })?;

            nodes.push(super::classify_backend_node(
                relative,
                metadata.is_file(),
                metadata.len(),
            ));
        }

        Ok(nodes)
    }

    async fn is_file(&self, path: &Path) -> bool {
        let full_path = self.full_path(path);
        match tokio::fs::symlink_metadata(&full_path).await {
            Ok(meta) => meta.is_file(),
            Err(_) => false,
        }
    }

    async fn is_dir(&self, path: &Path) -> bool {
        let full_path = self.full_path(path);
        match tokio::fs::symlink_metadata(&full_path).await {
            Ok(meta) => meta.is_dir(),
            Err(_) => false,
        }
    }

    async fn lstat(&self, path: &Path) -> Result<NodeAttr> {
        let full_path = self.full_path(path);
        let meta = tokio::fs::symlink_metadata(&full_path).await.map_err(|e| {
            MapacheError::Backend(format!("lstat failed for {}: {}", path.display(), e))
        })?;

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
    use tempfile::tempdir;

    use super::*;
    use crate::backend::BackendUrl;

    #[test]
    fn test_local_backend_url() {
        assert_eq!(
            BackendUrl::from("/home/target").unwrap(),
            BackendUrl::Local(PathBuf::from("/home/target"))
        );
        assert_eq!(
            BackendUrl::from("base/dir").unwrap(),
            BackendUrl::Local(PathBuf::from("base/dir"))
        );
        assert_eq!(
            BackendUrl::from("dir").unwrap(),
            BackendUrl::Local(PathBuf::from("dir"))
        );
        assert_eq!(
            BackendUrl::from(".").unwrap(),
            BackendUrl::Local(PathBuf::from("."))
        );

        // Standard absolute file URLs
        #[cfg(not(windows))]
        assert_eq!(
            BackendUrl::from("file:///home/target").unwrap(),
            BackendUrl::Local(PathBuf::from("/home/target"))
        );

        #[cfg(windows)]
        assert_eq!(
            BackendUrl::from("file:///C:/path/to/repo").unwrap(),
            BackendUrl::Local(PathBuf::from("C:\\path\\to\\repo"))
        );
    }

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

    #[tokio::test]
    async fn test_local_fs_readonly() -> Result<()> {
        let temp_dir = tempdir()?;
        let local_fs = LocalFS::new(temp_dir.path().to_path_buf());

        let write_handle = Handle::new(Path::new("readonly_file.txt"));
        local_fs
            .write(&write_handle, WriteContents::Borrowed(b"Immutable data"))
            .await?;

        let full_path = temp_dir.path().join("readonly_file.txt");
        let metadata = std::fs::metadata(&full_path)?;
        let perms = metadata.permissions();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            // Should be 0o400 (readonly)
            assert_eq!(perms.mode() & 0o777, 0o400);
        }

        #[cfg(windows)]
        {
            assert!(perms.readonly());
        }

        // Test that we can overwrite it (it should unlock and then relock)
        local_fs
            .write(&write_handle, WriteContents::Borrowed(b"New data"))
            .await?;

        let metadata = std::fs::metadata(&full_path)?;
        let perms = metadata.permissions();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(perms.mode() & 0o777, 0o400);
        }
        #[cfg(windows)]
        {
            assert!(perms.readonly());
        }

        // Test removal (it should unlock and then delete)
        local_fs.remove(Path::new("readonly_file.txt")).await?;
        assert!(!local_fs.path_exists(Path::new("readonly_file.txt")).await);

        Ok(())
    }
}
