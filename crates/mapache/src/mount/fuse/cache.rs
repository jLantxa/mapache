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
    inner: Mutex<Lru<ID, Vec<u8>>>,
}

impl<L: BlobLoader + ?Sized> BlobCache<L> {
    pub(super) fn new(loader: Arc<L>, capacity: u64) -> Self {
        Self {
            loader,
            capacity,
            inner: Mutex::new(Lru::with_max_weight(capacity)),
        }
    }

    pub(super) async fn load(&self, id: &ID) -> Result<Arc<Vec<u8>>> {
        {
            let mut inner = self.inner.lock();
            if let Some(value) = inner.record_hit(id) {
                tracing::trace!(target: "fuse", "BlobCache HIT: {}", id.to_short_hex(8));
                return Ok(value);
            }
        }

        tracing::debug!(target: "fuse", "BlobCache MISS: {}", id.to_short_hex(8));
        let blob = Arc::new(self.loader.load_blob(id).await?);
        let blob_len = blob.len() as u64;

        {
            let mut inner = self.inner.lock();
            if let Some(value) = inner.record_hit(id) {
                tracing::trace!(target: "fuse", "BlobCache HIT (race): {}", id.to_short_hex(8));
                return Ok(value);
            }

            if blob_len <= self.capacity {
                while inner.total_weight() + blob_len > self.capacity {
                    if let Some((lru_id, _, _)) = inner.evict_one() {
                        tracing::debug!(target: "fuse", "BlobCache EVICT: {}", lru_id.to_short_hex(8));
                    } else {
                        break;
                    }
                }

                inner.insert(*id, Arc::clone(&blob), blob_len);
            }
        }

        Ok(blob)
    }

    /// Resolves the decompressed length of a blob from the loader's index
    /// without loading the data (used to skip blobs outside a read range).
    pub(super) async fn blob_len(&self, id: &ID) -> Result<Option<u64>> {
        self.loader.blob_len(id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::collections::HashMap;

    struct MockLoader {
        blobs: HashMap<ID, Vec<u8>>,
    }

    #[async_trait]
    impl BlobLoader for MockLoader {
        async fn load_blob(&self, id: &ID) -> Result<Vec<u8>> {
            self.blobs
                .get(id)
                .cloned()
                .ok_or_else(|| MapacheError::NotFound("blob not found".to_string()))
        }

        async fn blob_len(&self, id: &ID) -> Result<Option<u64>> {
            Ok(self.blobs.get(id).map(|b| b.len() as u64))
        }
    }

    #[tokio::test]
    async fn test_blob_cache_eviction_and_oversized_blob() {
        let mut blobs = HashMap::new();
        let id1 = ID::from_content(b"blob1");
        let id2 = ID::from_content(b"blob2");
        let id_huge = ID::from_content(b"huge_blob_that_exceeds_cache_capacity");

        blobs.insert(id1, vec![1u8; 60]);
        blobs.insert(id2, vec![2u8; 60]);
        blobs.insert(id_huge, vec![3u8; 200]);

        let loader = Arc::new(MockLoader { blobs });
        let cache = BlobCache::new(loader, 100);

        // Load id1 (60 bytes) -> cached
        let res1 = cache.load(&id1).await.unwrap();
        assert_eq!(res1.len(), 60);
        assert_eq!(cache.inner.lock().total_weight(), 60);

        // Load id_huge (200 bytes > 100 capacity) -> loaded but NOT cached, does not corrupt capacity
        let res_huge = cache.load(&id_huge).await.unwrap();
        assert_eq!(res_huge.len(), 200);
        assert_eq!(cache.inner.lock().total_weight(), 60);

        // Load id2 (60 bytes) -> evicts id1 (60 + 60 > 100)
        let res2 = cache.load(&id2).await.unwrap();
        assert_eq!(res2.len(), 60);
        assert_eq!(cache.inner.lock().total_weight(), 60);
        assert!(cache.inner.lock().record_hit(&id2).is_some());
        assert!(cache.inner.lock().record_hit(&id1).is_none());
    }
}
