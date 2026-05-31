use std::{path::Path, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;

use crate::backend::{Handle, StorageBackend, WriteContents};

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

#[async_trait]
impl StorageBackend for DryBackend {
    #[inline]
    async fn path_exists(&self, path: &Path) -> bool {
        self.backend.path_exists(path).await
    }

    #[inline]
    async fn is_file(&self, path: &Path) -> bool {
        self.backend.is_file(path).await
    }

    #[inline]
    async fn is_dir(&self, path: &Path) -> bool {
        self.backend.is_dir(path).await
    }

    #[inline]
    async fn create(&self) -> Result<()> {
        Ok(())
    }

    #[inline]
    async fn read(&self, handle: &Handle, offset: isize, length: usize) -> Result<Vec<u8>> {
        self.backend.read(handle, offset, length).await
    }

    #[inline]
    async fn write(&self, _handle: &Handle, _contents: WriteContents<'_>) -> Result<()> {
        Ok(())
    }

    #[inline]
    async fn rename(&self, _from: &Path, _to: &Path) -> Result<()> {
        Ok(())
    }

    #[inline]
    async fn create_dir(&self, _path: &Path) -> Result<()> {
        Ok(())
    }

    #[inline]
    async fn remove(&self, _path: &Path) -> Result<()> {
        Ok(())
    }

    #[inline]
    async fn list_dir(&self, path: &Path) -> Result<Vec<crate::backend::BackendNode>> {
        self.backend.list_dir(path).await
    }

    #[inline]
    async fn lstat(&self, path: &Path) -> Result<crate::backend::NodeAttr> {
        self.backend.lstat(path).await
    }

    fn is_dry_run(&self) -> bool {
        true
    }
}
