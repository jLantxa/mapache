// mapache is an incremental backup tool
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

use anyhow::Result;

use crate::{
    global::ID,
    repository::{RepositoryBackend, tree::Tree},
};

/// A cache for `Tree` objects that uses a Least Recently Used (LRU) eviction policy.
pub(super) struct TreeCache {
    repo: Arc<dyn RepositoryBackend>,

    /// Maximum number of elements.
    capacity: usize,

    /// Stores the actual tree data, mapped by their ID, along with their last access timestamp.
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
    pub(super) fn new(repo: Arc<dyn RepositoryBackend>, capacity: usize) -> Self {
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
            self.order_map.insert(current_timestamp, id.clone());

            return Ok(tree);
        }

        // Cache miss: possibly evict
        if self.trees.len() >= self.capacity {
            if let Some((_lru_timestamp, lru_id)) = self.order_map.pop_first() {
                self.trees
                    .remove(&lru_id)
                    .expect("LRU ID not found in trees map during eviction");
            }
        }

        // Load from repository
        let tree_blob = self
            .repo
            .load_blob(id)
            .unwrap_or_else(|_| panic!("Failed to load tree {}", id.to_hex()));
        let tree: Tree = serde_json::from_slice(&tree_blob)
            .unwrap_or_else(|_| panic!("Failed to serialize tree {}", id.to_hex()));

        self.trees.insert(id.clone(), (tree, current_timestamp));
        self.order_map.insert(current_timestamp, id.clone());

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
