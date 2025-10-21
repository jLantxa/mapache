// mapache is a secure, de-duplicating, incremental backup tool.
// Copyright (C) 2025  Javier Lancha Vázquez <javier.lancha@gmail.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use std::{collections::BTreeMap, sync::Arc};

use anyhow::{Result, bail};

use crate::{fs::tree::Tree, mapache::ID, repository::repo::Repository};

/// A cache for `Tree` objects that uses a Least Recently Used (LRU) eviction policy.
pub(super) struct TreeCache {
    repo: Arc<Repository>,

    /// Maximum number of elements.
    capacity: usize,

    /// Stores the actual tree data, mapped by ID, along with their last access timestamp.
    /// Key: Tree ID
    /// Value: (Tree data, timestamp)
    trees: BTreeMap<ID, (Tree, u64)>,

    /// Stores timestamps mapped to IDs to quickly find the LRU item.
    /// Key: Timestamp (smaller = older)
    /// Value: Tree ID
    order_map: BTreeMap<u64, ID>,

    /// Monotonically increasing counter for timestamps.
    /// Hopefully, u64 is enough for the entire lifetime of the cache.
    next_timestamp: u64,
}

impl TreeCache {
    /// Creates a new TreeCache with a maximum `capacity`.
    pub(super) fn new(repo: Arc<Repository>, capacity: usize) -> Self {
        Self {
            repo,
            capacity,
            trees: BTreeMap::new(),
            order_map: BTreeMap::new(),
            next_timestamp: 0,
        }
    }

    /// Looks up a `Tree` in the cache by its `ID`. If not found, it loads the
    /// tree from the repository, stores it in the cache, and applies the LRU policy.
    ///
    /// If the cache is full, the least recently used tree will be evicted.
    pub(super) fn load(&mut self, id: &ID) -> Result<&Tree> {
        let current_timestamp = self.next_timestamp;
        self.next_timestamp += 1;

        // Cache hit: update timestamp and return tree
        if self.trees.contains_key(id) {
            let old_timestamp = self.trees.get(id).unwrap().1;

            self.order_map
                .remove(&old_timestamp)
                .expect("Old timestamp not found in order_map for existing ID");

            let (tree, timestamp) = self.trees.get_mut(id).unwrap();
            *timestamp = current_timestamp;
            self.order_map.insert(current_timestamp, *id);

            return Ok(tree);
        }

        // Cache miss: possibly evict
        if self.trees.len() >= self.capacity
            && let Some((_lru_timestamp, lru_id)) = self.order_map.pop_first()
        {
            self.trees
                .remove(&lru_id)
                .expect("LRU ID not found in trees map during eviction");
        }

        // Load from repository
        let tree_blob = self
            .repo
            .load_blob(id)
            .unwrap_or_else(|_| panic!("Failed to load tree {}", id.to_hex()));
        let tree: Tree = serde_json::from_slice(&tree_blob)
            .unwrap_or_else(|_| panic!("Failed to serialize tree {}", id.to_hex()));

        self.trees.insert(*id, (tree, current_timestamp));
        self.order_map.insert(current_timestamp, *id);

        Ok(&self.trees.get(id).unwrap().0)
    }

    /// Returns the current number of items in the cache.
    #[allow(dead_code)]
    pub(super) fn len(&self) -> usize {
        self.trees.len()
    }

    /// Returns `true` if the cache is empty.
    #[allow(dead_code)]
    pub(super) fn is_empty(&self) -> bool {
        self.trees.is_empty()
    }
}

/// A cache for blobs that uses a Least Recently Used (LRU) eviction policy.
pub(super) struct BlobCache {
    repo: Arc<Repository>,

    /// Maximum size
    capacity: u64,

    /// Current size
    size: u64,

    /// Stores the actual blob data, mapped by ID, along with their last access timestamp.
    /// Key: Blob ID
    /// Value: (Blob data, timestamp)
    blobs: BTreeMap<ID, (Vec<u8>, u64)>,

    /// Stores timestamps mapped to IDs to quickly find the LRU item.
    /// Key: Timestamp (smaller = older)
    /// Value: Tree ID
    order_map: BTreeMap<u64, ID>,

    /// Monotonically increasing counter for timestamps.
    /// Hopefully, u64 is enough for the entire lifetime of the cache.
    next_timestamp: u64,
}

impl BlobCache {
    /// Creates a new TreeCache with a maximum `capacity`.
    pub(super) fn new(repo: Arc<Repository>, capacity: u64) -> Self {
        Self {
            repo,
            capacity,
            size: 0,
            blobs: BTreeMap::new(),
            order_map: BTreeMap::new(),
            next_timestamp: 0,
        }
    }

    /// Looks up a blob in the cache by its `ID`. If not found, it loads the
    /// blob from the repository, stores it in the cache, and applies the LRU policy.
    ///
    /// If the cache is full, the least recently used blobs will be evicted until
    /// the cache size is within capacity.
    pub(super) fn load(&mut self, id: &ID) -> Result<&Vec<u8>> {
        let current_timestamp = self.next_timestamp;
        self.next_timestamp += 1;

        // Cache hit: update timestamp and return tree
        if self.blobs.contains_key(id) {
            let old_timestamp = self.blobs.get(id).unwrap().1;

            self.order_map
                .remove(&old_timestamp)
                .expect("Old timestamp not found in order_map for existing ID");

            let (data, timestamp) = self.blobs.get_mut(id).unwrap();
            *timestamp = current_timestamp;
            self.order_map.insert(current_timestamp, *id);

            return Ok(data);
        }

        let blob_indexed_size = match self.repo.index().read().get(id) {
            None => bail!("Blob is not indexed"),
            Some((.., encoded_size)) => encoded_size,
        };

        // Evict all blobs in excess
        while self.size + blob_indexed_size as u64 > self.capacity {
            if let Some((_lru_timestamp, lru_id)) = self.order_map.pop_first() {
                let (evicted_data, ..) = self
                    .blobs
                    .remove(&lru_id)
                    .expect("LRU ID not found in trees map during eviction");
                self.size -= evicted_data.len() as u64;
            }
        }

        // Cache miss: load from repository
        let blob = self
            .repo
            .load_blob(id)
            .unwrap_or_else(|_| panic!("Failed to load blob {}", id.to_hex()));

        self.size += blob.len() as u64;
        self.blobs.insert(*id, (blob, current_timestamp));
        self.order_map.insert(current_timestamp, *id);

        Ok(&self.blobs.get(id).unwrap().0)
    }

    /// Returns the current number of items in the cache.
    #[allow(dead_code)]
    pub(super) fn len(&self) -> usize {
        self.blobs.len()
    }

    /// Returns the current size (bytes) of items the cache.
    #[allow(dead_code)]
    pub(super) fn size(&self) -> u64 {
        self.size
    }

    /// Returns `true` if the cache is empty.
    #[allow(dead_code)]
    pub(super) fn is_empty(&self) -> bool {
        self.size == 0
    }
}
