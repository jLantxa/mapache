use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Result;
use async_trait::async_trait;
use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::{
    backend::{BackendNode, Handle, NodeAttr, StorageBackend, WriteContents, localfs::LocalFS},
    mapache::{ContentIdType, defaults::APP_NAME, global::BASE_DIRS},
};

/// Represents the state of a file in the download queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DownloadState {
    /// The file is currently being downloaded by one thread.
    Downloading,
    /// The download has failed.
    Failed,
}

/// A cache wrapper for backends. This backend caches selected files from the repository
/// into a local cache folder to speed up reading and reduce download operations.
pub struct CacheBackend {
    backend: Arc<dyn StorageBackend>,
    cache: LocalFS,
    download_queue: Mutex<HashMap<PathBuf, DownloadState>>,
    download_notify: Notify,
}

impl CacheBackend {
    /// Creates a new `CacheBackend` in `path` wrapping the `backend`.
    pub fn new(path: PathBuf, backend: Arc<dyn StorageBackend>) -> Self {
        let cache = LocalFS::new(path);
        Self {
            backend,
            cache,
            download_queue: Mutex::new(HashMap::new()),
            download_notify: Notify::new(),
        }
    }

    pub fn default_dir() -> PathBuf {
        BASE_DIRS.cache_dir().join(APP_NAME)
    }

    /// Returns true if the storage handle is eligible for caching.
    /// This function decides what files must be cached.
    fn should_cache(handle: &Handle) -> bool {
        // Files with any extension are never cached.
        if handle.path.extension().is_some() {
            return false;
        }

        // No caching without a storage hint.
        let hint = match &handle.hint {
            Some(h) => h,
            None => return false,
        };

        // Cache is allowed only for specific ContentIdTypes and conditions.
        match hint.file_type {
            ContentIdType::Snapshot | ContentIdType::Index => true,
            ContentIdType::Pack => hint.is_metadata, // Only cache tree/metadata packs
            _ => false, // All other types (Key, Data, etc.) are excluded
        }
    }
}

impl CacheBackend {
    /// Cache a complete file into the local cache.
    async fn cache_file(&self, path: &Path) -> Result<()> {
        let path_buf = path.to_path_buf();
        let handle = Handle::new(path);

        tracing::debug!(target: "cache", "Caching file {:?}", path);

        // Wait until we can claim this file for download
        loop {
            let should_wait = {
                let mut queue = self.download_queue.lock();
                match queue.get(&path_buf) {
                    Some(DownloadState::Downloading) => true,
                    _ => {
                        queue.insert(path_buf.clone(), DownloadState::Downloading);
                        false
                    }
                }
            };

            if should_wait {
                self.download_notify.notified().await;
            } else {
                break;
            }
        }

        // After waiting, check if another thread finished the download
        if self.cache.path_exists(path).await {
            let mut queue = self.download_queue.lock();
            queue.remove(&path_buf);
            self.download_notify.notify_waiters();
            return Ok(());
        }

        let result = async {
            let data = self.backend.read(&handle, 0, 0).await?;

            if let Some(parent) = path.parent() {
                self.cache.create_dir(parent).await?;
            }
            self.cache
                .write(&handle, WriteContents::Owned(data))
                .await?;

            Ok(())
        }
        .await;

        let mut queue = self.download_queue.lock();

        match &result {
            Ok(_) => {
                tracing::debug!(target: "cache", "File {:?} cached successfully", path);
                queue.remove(&path_buf);
            }
            Err(e) => {
                tracing::error!(target: "cache", "Failed to cache file {:?}: {}", path, e);
                queue.insert(path_buf, DownloadState::Failed);
            }
        }
        drop(queue);
        self.download_notify.notify_waiters();

        result
    }
}

#[async_trait]
impl StorageBackend for CacheBackend {
    async fn create(&self) -> Result<()> {
        self.cache.create().await?;
        self.backend.create().await?;
        Ok(())
    }

    async fn path_exists(&self, path: &Path) -> bool {
        self.backend.path_exists(path).await
    }

    async fn read(&self, handle: &Handle, offset: isize, length: usize) -> Result<Vec<u8>> {
        if !Self::should_cache(handle) {
            // If the handle is not eligible for caching, read directly from the primary.
            return self.backend.read(handle, offset, length).await;
        }

        // Try reading from the cache.
        match self.cache.read(handle, offset, length).await {
            Ok(data) => {
                tracing::trace!(target: "cache", "Cache HIT: {:?}", handle.path);
                Ok(data)
            } // Cache Hit
            Err(_) => {
                tracing::debug!(target: "cache", "Cache MISS: {:?}", handle.path);
                // Cache Miss: Cache the file and read again from cache.
                self.cache_file(handle.path).await?;
                self.cache.read(handle, offset, length).await
            }
        }
    }

    async fn write(&self, handle: &Handle, contents: WriteContents<'_>) -> Result<()> {
        tracing::trace!(target: "cache", "Cache: write {:?}", handle.path);
        if Self::should_cache(handle) {
            // Write to the primary backend with a reference to avoid moving ownership yet
            self.backend
                .write(handle, WriteContents::Borrowed(contents.as_ref()))
                .await?;

            // Then move ownership to the cache write
            if let Some(parent) = handle.path.parent() {
                self.cache.create_dir(parent).await?;
            }
            self.cache.write(handle, contents).await?;
        } else {
            // No caching, just pass the data through
            self.backend.write(handle, contents).await?;
        }

        Ok(())
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        tracing::debug!(target: "cache", "Cache: rename {:?} -> {:?}", from, to);
        // Try to rename the cached path. If it failed, it didn't exist.
        // If this file should be cached, it will be next time it is read.
        let _ = self.cache.rename(from, to).await;

        self.backend.rename(from, to).await?;
        Ok(())
    }

    async fn list_dir(&self, path: &Path) -> Result<Vec<BackendNode>> {
        self.backend.list_dir(path).await
    }

    async fn create_dir(&self, path: &Path) -> Result<()> {
        self.backend.create_dir(path).await
    }

    async fn remove(&self, file_path: &Path) -> Result<()> {
        tracing::debug!(target: "cache", "Cache: remove {:?}", file_path);
        let path_buf = file_path.to_path_buf();

        loop {
            let should_wait = {
                let mut queue = self.download_queue.lock();
                if matches!(queue.get(&path_buf), Some(DownloadState::Downloading)) {
                    true
                } else {
                    queue.remove(&path_buf);
                    false
                }
            };

            if should_wait {
                self.download_notify.notified().await;
            } else {
                break;
            }
        }

        self.cache.remove(file_path).await?;
        self.backend.remove(file_path).await?;

        Ok(())
    }

    async fn is_file(&self, path: &Path) -> bool {
        self.backend.is_file(path).await
    }

    async fn is_dir(&self, path: &Path) -> bool {
        self.backend.is_dir(path).await
    }

    async fn lstat(&self, path: &Path) -> Result<NodeAttr> {
        self.backend.lstat(path).await
    }

    fn is_dry_run(&self) -> bool {
        self.backend.is_dry_run()
    }
}
