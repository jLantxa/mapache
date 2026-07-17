use parking_lot::RwLock;

use crate::{common::ID, utils::collections::IdSet};

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::ID;

    #[test]
    fn test_insert_and_contains() {
        let set = ShardedIdSet::new();
        let id = ID::from_content(b"hello");
        assert!(set.insert(id));
        assert!(set.contains(&id));
    }

    #[test]
    fn test_insert_duplicate() {
        let set = ShardedIdSet::new();
        let id = ID::from_content(b"hello");
        assert!(set.insert(id));
        assert!(!set.insert(id));
        assert!(set.contains(&id));
    }

    #[test]
    fn test_remove() {
        let set = ShardedIdSet::new();
        let id = ID::from_content(b"hello");
        set.insert(id);
        assert!(set.remove(&id));
        assert!(!set.contains(&id));
    }

    #[test]
    fn test_remove_nonexistent() {
        let set = ShardedIdSet::new();
        let id = ID::from_content(b"hello");
        assert!(!set.remove(&id));
    }

    #[test]
    fn test_clear() {
        let set = ShardedIdSet::new();
        let id1 = ID::from_content(b"hello");
        let id2 = ID::from_content(b"world");
        set.insert(id1);
        set.insert(id2);
        set.clear();
        assert!(!set.contains(&id1));
        assert!(!set.contains(&id2));
    }
}
