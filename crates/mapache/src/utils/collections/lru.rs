use std::{collections::BTreeMap, fmt, sync::Arc};

/// A generic LRU (Least Recently Used) cache.
///
/// Tracks access order via a monotonic timestamp and evicts the oldest entry.
pub struct Lru<K, V> {
    entries: BTreeMap<K, (Arc<V>, u64)>,
    order_map: BTreeMap<u64, K>,
    next_timestamp: u64,
}

impl<K, V> fmt::Debug for Lru<K, V>
where
    K: Ord + Copy + fmt::Debug,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Lru")
            .field("len", &self.entries.len())
            .finish()
    }
}

impl<K, V> Lru<K, V>
where
    K: Ord + Copy,
{
    // `Default` would require the same K: Ord + Copy bounds as `new()`,
    // and there is no natural default for an LRU cache.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            order_map: BTreeMap::new(),
            next_timestamp: 0,
        }
    }

    /// Marks an entry as recently used. Returns `Some(Arc<V>)` on hit, `None` on miss.
    pub fn record_hit(&mut self, key: &K) -> Option<Arc<V>> {
        let entry = self.entries.get_mut(key)?;
        let ts = self.next_timestamp;
        self.next_timestamp += 1;
        let old_ts = entry.1;
        entry.1 = ts;
        self.order_map.remove(&old_ts);
        self.order_map.insert(ts, *key);
        Some(Arc::clone(&entry.0))
    }

    /// Evicts the least recently used entry. Returns the key and value.
    pub fn evict_one(&mut self) -> Option<(K, Arc<V>)> {
        let (_, key) = self.order_map.pop_first()?;
        let (value, _) = self.entries.remove(&key)?;
        Some((key, value))
    }

    /// Inserts a new entry into the cache.
    pub fn insert(&mut self, key: K, value: Arc<V>) {
        let ts = self.next_timestamp;
        self.next_timestamp += 1;
        if let Some((_, old_ts)) = self.entries.insert(key, (value, ts)) {
            self.order_map.remove(&old_ts);
        }
        self.order_map.insert(ts, key);
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lru_new() {
        let lru = Lru::<u64, String>::new();
        assert_eq!(lru.len(), 0);
    }

    #[test]
    fn test_lru_record_hit_miss() {
        let mut lru = Lru::<u64, String>::new();
        assert!(lru.record_hit(&1).is_none());
    }

    #[test]
    fn test_lru_insert_and_hit() {
        let mut lru = Lru::new();
        lru.insert(1, Arc::new("a".to_string()));
        assert_eq!(lru.len(), 1);

        let val = lru.record_hit(&1);
        assert_eq!(*val.unwrap(), "a");
    }

    #[test]
    fn test_lru_eviction_order() {
        let mut lru = Lru::new();
        lru.insert(1, Arc::new("a".to_string()));
        lru.insert(2, Arc::new("b".to_string()));
        lru.insert(3, Arc::new("c".to_string()));

        let (key, val) = lru.evict_one().unwrap();
        assert_eq!(key, 1);
        assert_eq!(*val, "a");

        let (key, val) = lru.evict_one().unwrap();
        assert_eq!(key, 2);
        assert_eq!(*val, "b");

        let (key, val) = lru.evict_one().unwrap();
        assert_eq!(key, 3);
        assert_eq!(*val, "c");

        assert!(lru.evict_one().is_none());
    }

    #[test]
    fn test_lru_hit_updates_order() {
        let mut lru = Lru::new();
        lru.insert(1, Arc::new("a".to_string()));
        lru.insert(2, Arc::new("b".to_string()));
        lru.insert(3, Arc::new("c".to_string()));

        lru.record_hit(&1);

        let (key, _) = lru.evict_one().unwrap();
        assert_eq!(key, 2);

        let (key, _) = lru.evict_one().unwrap();
        assert_eq!(key, 3);

        let (key, _) = lru.evict_one().unwrap();
        assert_eq!(key, 1);
    }

    #[test]
    fn test_lru_evict_one_empty() {
        let mut lru = Lru::<u64, String>::new();
        assert!(lru.evict_one().is_none());
    }

    #[test]
    fn test_lru_is_empty() {
        let mut lru = Lru::<u64, String>::new();
        assert!(lru.is_empty());
        lru.insert(1, Arc::new("x".to_string()));
        assert!(!lru.is_empty());
    }

    #[test]
    fn test_lru_reinsert_same_key_removes_stale_timestamp() {
        let mut lru = Lru::new();
        lru.insert(1, Arc::new("a".to_string()));
        lru.insert(1, Arc::new("b".to_string()));
        lru.insert(2, Arc::new("c".to_string()));

        let (key, val) = lru.evict_one().unwrap();
        assert_eq!(key, 1);
        assert_eq!(*val, "b");

        let (key, val) = lru.evict_one().unwrap();
        assert_eq!(key, 2);
        assert_eq!(*val, "c");

        assert!(lru.evict_one().is_none());
    }

    #[test]
    fn test_lru_different_key_type() {
        let mut lru = Lru::<&str, i32>::new();
        lru.insert("x", Arc::new(42));
        let val = lru.record_hit(&"x");
        assert_eq!(*val.unwrap(), 42);
    }
}
