use std::sync::Arc;

use crate::common::error::Result;
use parking_lot::Mutex;

use crate::{
    common::{ID, error::MapacheError, traits::BlobLoader},
    fs::tree::Tree,
    utils::collections::Lru,
};

/// A cache for `Tree` objects that uses a Least Recently Used (LRU) eviction policy.
pub(super) struct TreeCache<L: BlobLoader + ?Sized> {
    loader: Arc<L>,
    capacity: usize,
    inner: Mutex<Lru<ID, Tree>>,
}

impl<L: BlobLoader + ?Sized> TreeCache<L> {
    pub(super) fn new(loader: Arc<L>, capacity: usize) -> Self {
        Self {
            loader,
            capacity,
            inner: Mutex::new(Lru::with_max_weight(capacity as u64)),
        }
    }

    pub(super) async fn load(&self, id: &ID) -> Result<Arc<Tree>> {
        {
            let mut inner = self.inner.lock();
            if let Some(value) = inner.record_hit(id) {
                tracing::trace!(target: "fuse", "TreeCache HIT: {}", id.to_short_hex(8));
                return Ok(value);
            }
        }

        tracing::debug!(target: "fuse", "TreeCache MISS: {}", id.to_short_hex(8));
        let tree_blob = self.loader.load_blob(id).await?;
        let tree: Tree = serde_json::from_slice(&tree_blob)
            .map_err(|e| MapacheError::Format(format!("failed to deserialize tree: {e}")))?;
        let tree = Arc::new(tree);

        {
            let mut inner = self.inner.lock();
            if let Some(value) = inner.record_hit(id) {
                tracing::trace!(target: "fuse", "TreeCache HIT (race): {}", id.to_short_hex(8));
                return Ok(value);
            }

            if inner.len() >= self.capacity
                && let Some((lru_id, _, _)) = inner.evict_one()
            {
                tracing::debug!(target: "fuse", "TreeCache EVICT: {}", lru_id.to_short_hex(8));
            }

            inner.insert(*id, Arc::clone(&tree), 1);
        }

        Ok(tree)
    }
}

/// A cache for blobs that uses a Least Recently Used (LRU) eviction policy.
pub(super) struct BlobCache<L: BlobLoader + ?Sized> {
    loader: Arc<L>,
    capacity: u64,
    inner: Mutex<BlobCacheInner>,
}

struct BlobCacheInner {
    lru: Lru<ID, Vec<u8>>,
    size: u64,
}

impl<L: BlobLoader + ?Sized> BlobCache<L> {
    pub(super) fn new(loader: Arc<L>, capacity: u64) -> Self {
        Self {
            loader,
            capacity,
            inner: Mutex::new(BlobCacheInner {
                lru: Lru::with_max_weight(capacity),
                size: 0,
            }),
        }
    }

    pub(super) async fn load(&self, id: &ID) -> Result<Arc<Vec<u8>>> {
        {
            let mut inner = self.inner.lock();
            if let Some(value) = inner.lru.record_hit(id) {
                tracing::trace!(target: "fuse", "BlobCache HIT: {}", id.to_short_hex(8));
                return Ok(value);
            }
        }

        tracing::debug!(target: "fuse", "BlobCache MISS: {}", id.to_short_hex(8));
        let blob = Arc::new(self.loader.load_blob(id).await?);
        let blob_len = blob.len() as u64;

        {
            let mut inner = self.inner.lock();
            if let Some(value) = inner.lru.record_hit(id) {
                tracing::trace!(target: "fuse", "BlobCache HIT (race): {}", id.to_short_hex(8));
                return Ok(value);
            }

            while inner.size + blob_len > self.capacity {
                if let Some((lru_id, evicted, _)) = inner.lru.evict_one() {
                    tracing::debug!(target: "fuse", "BlobCache EVICT: {}", lru_id.to_short_hex(8));
                    inner.size -= evicted.len() as u64;
                } else {
                    break;
                }
            }

            inner.size += blob_len;
            inner.lru.insert(*id, Arc::clone(&blob), blob_len);
        }

        Ok(blob)
    }
}
