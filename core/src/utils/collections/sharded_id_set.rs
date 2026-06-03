use parking_lot::RwLock;

use crate::{mapache::ID, utils::collections::IdSet};

/// A set of IDs that is sharded by the first byte of the ID to reduce lock contention.
#[derive(Debug)]
pub struct ShardedIdSet {
    shards: [RwLock<IdSet<ID>>; 256],
}

impl Default for ShardedIdSet {
    fn default() -> Self {
        Self::new()
    }
}

impl ShardedIdSet {
    pub fn new() -> Self {
        Self {
            shards: std::array::from_fn(|_| RwLock::new(IdSet::default())),
        }
    }

    #[inline]
    fn get_shard(&self, id: &ID) -> &RwLock<IdSet<ID>> {
        &self.shards[id.0[0] as usize]
    }

    pub fn insert(&self, id: ID) -> bool {
        self.get_shard(&id).write().insert(id)
    }

    pub fn contains(&self, id: &ID) -> bool {
        self.get_shard(id).read().contains(id)
    }

    pub fn remove(&self, id: &ID) -> bool {
        self.get_shard(id).write().remove(id)
    }

    pub fn clear(&self) {
        for shard in &self.shards {
            shard.write().clear();
        }
    }
}
