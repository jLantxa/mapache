use std::{
    collections::{HashMap, HashSet},
    hash::{BuildHasherDefault, Hasher},
};

pub use bloom_filter::BloomFilter;
pub use index_set::{IdIndexSet, IndexSet, Iter};
pub use lru::Lru;
pub use sharded_id_set::ShardedIdSet;

mod bloom_filter;
mod index_set;
mod lru;
mod sharded_id_set;

#[derive(Default)]
pub struct FxHasher {
    hash: u64,
}

impl Hasher for FxHasher {
    #[inline]
    fn write(&mut self, mut bytes: &[u8]) {
        while bytes.len() >= 8 {
            let mut val = [0u8; 8];
            val.copy_from_slice(&bytes[..8]);
            self.hash = (self.hash.rotate_left(5) ^ u64::from_ne_bytes(val))
                .wrapping_mul(0x517cc1b727220a95);
            bytes = &bytes[8..];
        }
        if bytes.len() >= 4 {
            let mut val = [0u8; 4];
            val.copy_from_slice(&bytes[..4]);
            self.hash = (self.hash.rotate_left(5) ^ (u32::from_ne_bytes(val) as u64))
                .wrapping_mul(0x517cc1b727220a95);
            bytes = &bytes[4..];
        }
        if bytes.len() >= 2 {
            let mut val = [0u8; 2];
            val.copy_from_slice(&bytes[..2]);
            self.hash = (self.hash.rotate_left(5) ^ (u16::from_ne_bytes(val) as u64))
                .wrapping_mul(0x517cc1b727220a95);
            bytes = &bytes[2..];
        }
        if let Some(&byte) = bytes.first() {
            self.hash = (self.hash.rotate_left(5) ^ (byte as u64)).wrapping_mul(0x517cc1b727220a95);
        }
    }

    #[inline]
    fn finish(&self) -> u64 {
        self.hash
    }
}

/// A pass-through hasher for keys that are already cryptographically random (like Blake3 IDs).
/// This avoids performing any multiplications or rotations when hashing ID keys.
#[derive(Default)]
pub struct IdentityHasher {
    hash: u64,
}

impl Hasher for IdentityHasher {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        if bytes.len() >= 8 {
            let mut val = [0u8; 8];
            val.copy_from_slice(&bytes[..8]);
            self.hash = u64::from_ne_bytes(val);
        } else {
            // Fallback for keys smaller than 8 bytes
            for &b in bytes {
                self.hash = self.hash.wrapping_mul(31).wrapping_add(b as u64);
            }
        }
    }

    #[inline]
    fn finish(&self) -> u64 {
        self.hash
    }
}

pub type FxHashMap<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher>>;
pub type FxHashSet<V> = HashSet<V, BuildHasherDefault<FxHasher>>;
pub type IdSet<K> = HashSet<K, BuildHasherDefault<IdentityHasher>>;
pub type IdMap<K, V> = HashMap<K, V, BuildHasherDefault<IdentityHasher>>;
