use std::{
    collections::{HashMap, HashSet},
    hash::{BuildHasherDefault, Hasher},
};

mod bloom_filter;
mod index_set;
mod lru;
mod sharded_id_set;

pub use bloom_filter::BloomFilter;
pub use index_set::{IdIndexSet, IndexSet, Iter};
pub use lru::Lru;
pub use sharded_id_set::ShardedIdSet;

#[derive(Default)]
pub struct FxHasher {
    hash: u64,
}

impl Hasher for FxHasher {
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.hash = self
                .hash
                .wrapping_mul(0x517cc1b727220a95)
                .wrapping_add(b as u64);
        }
    }
    fn finish(&self) -> u64 {
        self.hash
    }
}

pub type FxHashMap<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher>>;
pub type FxHashSet<V> = HashSet<V, BuildHasherDefault<FxHasher>>;
pub type IdSet<K> = HashSet<K, BuildHasherDefault<FxHasher>>;
pub type IdMap<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher>>;
