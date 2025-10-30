use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::Result;
use parking_lot::Mutex;

use crate::{
    backend::{Handle, StorageBackend, localfs::LocalFS},
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

const DOWNLOAD_WAIT_TIME: Duration = Duration::from_millis(10);

/// A cache wrapper for backends. This backend caches selected files from the repository
/// into a local cache folder to speed up reading and reduce download operations.
pub struct CacheBackend {
    backend: Arc<dyn StorageBackend>,
    cache: LocalFS,
    download_queue: Mutex<HashMap<PathBuf, DownloadState>>,
}

impl CacheBackend {
    /// Creates a new `CacheBackend` in `path` wrapping the `backend`.
    pub fn new(path: PathBuf, backend: Arc<dyn StorageBackend>) -> Self {
        let cache = LocalFS::new(path);
        Self {
            backend,
            cache,
            download_queue: Mutex::new(HashMap::new()),
        }
    }

    pub fn default_dir() -> PathBuf {
        BASE_DIRS.read().cache_dir().join(APP_NAME)
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
    fn cache_file(&self, path: &Path) -> Result<()> {
        let path_buf = path.to_path_buf();
        let handle = Handle::new(path);

        {
            let mut queue = self.download_queue.lock();

            loop {
                match queue.get(&path_buf) {
                    Some(DownloadState::Downloading) => {
                        parking_lot::MutexGuard::unlock_fair(queue);
                        std::thread::sleep(DOWNLOAD_WAIT_TIME);
                        queue = self.download_queue.lock();
                        continue;
                    }
                    Some(DownloadState::Failed) => {
                        queue.remove(&path_buf);
                        break;
                    }
                    None => {
                        queue.insert(path_buf.clone(), DownloadState::Downloading);
                        break;
                    }
                }
            }
        } // Lock is released here.

        let result = (|| {
            let data = self.backend.read(&handle, 0, 0)?;

            if let Some(parent) = path.parent() {
                self.cache.create_dir(parent)?;
            }
            self.cache.write(&handle, &data)?;

            Ok(())
        })();

        let mut queue = self.download_queue.lock();

        match &result {
            Ok(_) => {
                queue.remove(&path_buf);
            }
            Err(_) => {
                queue.insert(path_buf, DownloadState::Failed);
            }
        }

        result
    }
}

impl StorageBackend for CacheBackend {
    fn create(&self) -> Result<()> {
        self.cache.create()?;
        self.backend.create()?;
        Ok(())
    }

    fn root_exists(&self) -> bool {
        self.backend.root_exists()
    }

    fn path_exists(&self, path: &Path) -> bool {
        self.backend.path_exists(path)
    }

    fn read(&self, handle: &Handle, offset: isize, length: usize) -> Result<Vec<u8>> {
        if !Self::should_cache(handle) {
            // If the handle is not eligible for caching, read directly from the primary.
            let data = self.backend.read(handle, offset, length)?;
            return Ok(data);
        }

        // Try reading from the cache.
        match self.cache.read(handle, offset, length) {
            Ok(data) => Ok(data), // Cache Hit
            Err(_) => {
                // Cache Miss: Cache the file and read again from cache.
                self.cache_file(handle.path)?;
                self.cache.read(handle, offset, length)
            }
        }
    }

    fn write(&self, handle: &Handle, contents: &[u8]) -> Result<()> {
        // Write to the primary backend first
        self.backend.write(handle, contents)?;

        // Then write to the cache backend
        if Self::should_cache(handle) {
            if let Some(parent) = handle.path.parent() {
                self.cache.create_dir(parent)?;
            }

            self.cache.write(handle, contents)?;
        }

        Ok(())
    }

    fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        // Try to rename the cached path. If it failed, it didn't exist.
        // If this fill should be cached, it will be next time it is read.
        let _ = self.cache.rename(from, to);

        self.backend.rename(from, to)?;
        Ok(())
    }

    fn list_dir(&self, path: &Path) -> Result<Vec<PathBuf>> {
        self.backend.list_dir(path)
    }

    fn create_dir(&self, path: &Path) -> Result<()> {
        self.backend.create_dir(path)
    }

    fn remove(&self, file_path: &Path) -> Result<()> {
        let path_buf = file_path.to_path_buf();

        loop {
            let state = {
                let mut queue = self.download_queue.lock();
                match queue.get(&path_buf) {
                    Some(DownloadState::Downloading) => Some(DownloadState::Downloading),
                    Some(DownloadState::Failed) => {
                        queue.remove(&path_buf);
                        None
                    }
                    None => None,
                }
            };

            match state {
                Some(DownloadState::Downloading) => {
                    std::thread::sleep(DOWNLOAD_WAIT_TIME);
                }
                _ => {
                    break;
                }
            }
        }

        self.cache.remove(file_path)?;
        self.backend.remove(file_path)?;

        Ok(())
    }

    fn is_file(&self, path: &Path) -> bool {
        self.backend.is_file(path)
    }

    fn is_dir(&self, path: &Path) -> bool {
        self.backend.is_dir(path)
    }

    fn lstat(&self, path: &Path) -> Result<super::NodeAttr> {
        self.backend.lstat(path)
    }
}
