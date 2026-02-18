use crate::{fs::tree::Tree, mapache::ID, repository::repo::Repository};
use anyhow::Result;
use parking_lot::Mutex;
use std::{collections::BTreeMap, sync::Arc};

/// A cache for `Tree` objects that uses a Least Recently Used (LRU) eviction policy.
pub(super) struct TreeCache {
    repo: Arc<Repository>,
    capacity: usize,
    inner: Mutex<TreeCacheInner>,
}

struct TreeCacheInner {
    trees: BTreeMap<ID, (Arc<Tree>, u64)>,
    order_map: BTreeMap<u64, ID>,
    next_timestamp: u64,
}

impl TreeCache {
    /// Creates a new TreeCache with a maximum `capacity`.
    pub(super) fn new(repo: Arc<Repository>, capacity: usize) -> Self {
        let inner = TreeCacheInner {
            trees: BTreeMap::new(),
            order_map: BTreeMap::new(),
            next_timestamp: 0,
        };
        Self {
            repo,
            capacity,
            inner: Mutex::new(inner),
        }
    }

    /// Looks up a `Tree` in the cache by its `ID`. If not found, it loads the
    /// tree from the repository, stores it in the cache, and applies the LRU policy.
    pub(super) async fn load(&self, id: &ID) -> Result<Arc<Tree>> {
        {
            let mut inner = self.inner.lock();
            let ts = inner.next_timestamp;

            // Check for cache hit
            if let Some((tree, timestamp)) = inner.trees.get_mut(id) {
                let old_timestamp = *timestamp;
                *timestamp = ts;
                let tree_clone = Arc::clone(tree);

                // Perform updates to other fields
                inner.next_timestamp += 1;
                inner.order_map.remove(&old_timestamp);
                inner.order_map.insert(ts, *id);

                return Ok(tree_clone);
            }
        }

        // Cache miss: load from repository outside the lock
        let tree_blob = self.repo.load_blob(id).await?;
        let tree: Tree = serde_json::from_slice(&tree_blob)?;
        let tree_arc = Arc::new(tree);

        {
            let mut inner = self.inner.lock();
            let ts = inner.next_timestamp;

            // Re-check hit (rare race condition)
            if let Some((t, timestamp)) = inner.trees.get_mut(id) {
                let old_timestamp = *timestamp;
                *timestamp = ts;
                let t_clone = Arc::clone(t);

                inner.next_timestamp += 1;
                inner.order_map.remove(&old_timestamp);
                inner.order_map.insert(ts, *id);
                return Ok(t_clone);
            }

            inner.next_timestamp += 1;

            // Evict if full
            if inner.trees.len() >= self.capacity
                && let Some((_, lru_id)) = inner.order_map.pop_first()
            {
                inner.trees.remove(&lru_id);
            }

            inner.trees.insert(*id, (Arc::clone(&tree_arc), ts));
            inner.order_map.insert(ts, *id);
        }

        Ok(tree_arc)
    }
}

/// A cache for blobs that uses a Least Recently Used (LRU) eviction policy.
pub(super) struct BlobCache {
    repo: Arc<Repository>,
    capacity: u64,
    inner: Mutex<BlobCacheInner>,
}

struct BlobCacheInner {
    size: u64,
    blobs: BTreeMap<ID, (Arc<Vec<u8>>, u64)>,
    order_map: BTreeMap<u64, ID>,
    next_timestamp: u64,
}

impl BlobCache {
    /// Creates a new TreeCache with a maximum `capacity`.
    pub(super) fn new(repo: Arc<Repository>, capacity: u64) -> Self {
        let inner = BlobCacheInner {
            size: 0,
            blobs: BTreeMap::new(),
            order_map: BTreeMap::new(),
            next_timestamp: 0,
        };
        Self {
            repo,
            capacity,
            inner: Mutex::new(inner),
        }
    }

    pub(super) async fn load(&self, id: &ID) -> Result<Arc<Vec<u8>>> {
        {
            let mut inner = self.inner.lock();
            let ts = inner.next_timestamp;

            // Check for cache hit
            if let Some((data, timestamp)) = inner.blobs.get_mut(id) {
                let old_timestamp = *timestamp;
                *timestamp = ts;
                let data_clone = Arc::clone(data);

                // Perform updates
                inner.next_timestamp += 1;
                inner.order_map.remove(&old_timestamp);
                inner.order_map.insert(ts, *id);

                return Ok(data_clone);
            }
        }

        // Cache miss: load from repository outside the lock
        let blob_indexed_size = self
            .repo
            .index()
            .get(id)
            .map(|l| l.length as u64)
            .ok_or_else(|| anyhow::anyhow!("Blob is not indexed"))?;

        let blob = self.repo.load_blob(id).await?;
        let blob_len = blob.len() as u64;
        let blob_arc = Arc::new(blob);

        {
            let mut inner = self.inner.lock();
            let ts = inner.next_timestamp;

            // Re-check hit
            if let Some((d, timestamp)) = inner.blobs.get_mut(id) {
                let old_timestamp = *timestamp;
                *timestamp = ts;
                let d_clone = Arc::clone(d);

                inner.next_timestamp += 1;
                inner.order_map.remove(&old_timestamp);
                inner.order_map.insert(ts, *id);
                return Ok(d_clone);
            }

            inner.next_timestamp += 1;

            // Evict until within capacity
            while inner.size + blob_indexed_size > self.capacity {
                if let Some((_, lru_id)) = inner.order_map.pop_first() {
                    if let Some((evicted_data, _)) = inner.blobs.remove(&lru_id) {
                        inner.size -= evicted_data.len() as u64;
                    }
                } else {
                    break;
                }
            }

            inner.size += blob_len;
            inner.blobs.insert(*id, (Arc::clone(&blob_arc), ts));
            inner.order_map.insert(ts, *id);
        }

        Ok(blob_arc)
    }
}
