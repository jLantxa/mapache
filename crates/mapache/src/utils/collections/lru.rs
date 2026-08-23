use std::{collections::BTreeMap, fmt, sync::Arc};

/// A generic weighted LRU (Least Recently Used) cache.
///
/// Tracks access order via a monotonic timestamp and evicts the oldest entry.
/// Each entry carries a weight; eviction triggers when total weight exceeds `max_weight`.
pub struct Lru<K, V> {
    entries: BTreeMap<K, (Arc<V>, u64, u64)>,
    order_map: BTreeMap<u64, K>,
    next_timestamp: u64,
    /// Maximum total weight allowed in the cache.
    pub max_weight: u64,
    total_weight: u64,
}

impl<K, V> fmt::Debug for Lru<K, V>
where
    K: Ord + Copy + fmt::Debug,
    V: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Lru")
            .field("len", &self.entries.len())
            .field("total_weight", &self.total_weight)
            .finish()
    }
}

impl<K, V> Lru<K, V>
where
    K: Ord + Copy,
{
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            order_map: BTreeMap::new(),
            next_timestamp: 0,
            max_weight: u64::MAX,
            total_weight: 0,
        }
    }

    /// Creates a weighted LRU cache with a maximum total weight.
    /// Each `insert` must provide the entry's weight; eviction happens by weight, not count.
    pub fn with_max_weight(max_weight: u64) -> Self {
        Self {
            entries: BTreeMap::new(),
            order_map: BTreeMap::new(),
            next_timestamp: 0,
            max_weight,
            total_weight: 0,
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

    /// Evicts the least recently used entry. Returns the key, value, and its weight.
    pub fn evict_one(&mut self) -> Option<(K, Arc<V>, u64)> {
        let (_, key) = self.order_map.pop_first()?;
        let (value, _, weight) = self.entries.remove(&key)?;
        self.total_weight = self.total_weight.saturating_sub(weight);
        Some((key, value, weight))
    }

    /// Inserts a new entry with the given weight. Auto-evicts LRU entries while over weight.
    pub fn insert(&mut self, key: K, value: Arc<V>, weight: u64) {
        let ts = self.next_timestamp;
        self.next_timestamp += 1;
        if let Some((_, old_ts, old_weight)) = self.entries.insert(key, (value, ts, weight)) {
            self.order_map.remove(&old_ts);
            self.total_weight = self.total_weight.saturating_sub(old_weight);
        }
        self.order_map.insert(ts, key);
        self.total_weight += weight;
        // Evict oldest while over weight limit
        while self.max_weight != u64::MAX && self.total_weight > self.max_weight {
            if self.evict_one().is_none() {
                break;
            }
        }
    }

    /// Removes an entry by key. Returns the value if it existed.
    pub fn remove(&mut self, key: &K) -> Option<Arc<V>> {
        let (value, ts, weight) = self.entries.remove(key)?;
        self.order_map.remove(&ts);
        self.total_weight = self.total_weight.saturating_sub(weight);
        Some(value)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn total_weight(&self) -> u64 {
        self.total_weight
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
        lru.insert(1, Arc::new("a".to_string()), 10);
        assert_eq!(lru.len(), 1);
        assert_eq!(lru.total_weight(), 10);

        let val = lru.record_hit(&1);
        assert_eq!(*val.unwrap(), "a");
    }

    #[test]
    fn test_lru_eviction_order() {
        let mut lru = Lru::new();
        lru.insert(1, Arc::new("a".to_string()), 1);
        lru.insert(2, Arc::new("b".to_string()), 1);
        lru.insert(3, Arc::new("c".to_string()), 1);

        let (key, val, _) = lru.evict_one().unwrap();
        assert_eq!(key, 1);
        assert_eq!(*val, "a");

        let (key, val, _) = lru.evict_one().unwrap();
        assert_eq!(key, 2);
        assert_eq!(*val, "b");

        let (key, val, _) = lru.evict_one().unwrap();
        assert_eq!(key, 3);
        assert_eq!(*val, "c");

        assert!(lru.evict_one().is_none());
    }

    #[test]
    fn test_lru_hit_updates_order() {
        let mut lru = Lru::new();
        lru.insert(1, Arc::new("a".to_string()), 1);
        lru.insert(2, Arc::new("b".to_string()), 1);
        lru.insert(3, Arc::new("c".to_string()), 1);

        lru.record_hit(&1);

        let (key, _, _) = lru.evict_one().unwrap();
        assert_eq!(key, 2);

        let (key, _, _) = lru.evict_one().unwrap();
        assert_eq!(key, 3);

        let (key, _, _) = lru.evict_one().unwrap();
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
        lru.insert(1, Arc::new("x".to_string()), 1);
        assert!(!lru.is_empty());
    }

    #[test]
    fn test_lru_reinsert_same_key_removes_stale_timestamp() {
        let mut lru = Lru::new();
        lru.insert(1, Arc::new("a".to_string()), 1);
        lru.insert(1, Arc::new("b".to_string()), 2);
        lru.insert(2, Arc::new("c".to_string()), 1);

        let (key, val, weight) = lru.evict_one().unwrap();
        assert_eq!(key, 1);
        assert_eq!(*val, "b");
        assert_eq!(weight, 2);

        let (key, val, _) = lru.evict_one().unwrap();
        assert_eq!(key, 2);
        assert_eq!(*val, "c");

        assert!(lru.evict_one().is_none());
    }

    #[test]
    fn test_lru_different_key_type() {
        let mut lru = Lru::<&str, i32>::new();
        lru.insert("x", Arc::new(42), 1);
        let val = lru.record_hit(&"x");
        assert_eq!(*val.unwrap(), 42);
    }

    #[test]
    fn test_lru_remove() {
        let mut lru = Lru::new();
        lru.insert(1, Arc::new("a".to_string()), 5);
        lru.insert(2, Arc::new("b".to_string()), 3);

        let removed = lru.remove(&1);
        assert_eq!(*removed.unwrap(), "a");
        assert_eq!(lru.len(), 1);
        assert_eq!(lru.total_weight(), 3);

        assert!(lru.remove(&99).is_none());

        let val = lru.record_hit(&2);
        assert_eq!(*val.unwrap(), "b");
    }

    #[test]
    fn test_lru_weight_eviction() {
        let mut lru = Lru::with_max_weight(10);
        lru.insert(1, Arc::new("a".to_string()), 4);
        lru.insert(2, Arc::new("b".to_string()), 4);
        assert_eq!(lru.len(), 2);
        assert_eq!(lru.total_weight(), 8);

        // This insert brings total to 13, should evict oldest until under 10
        lru.insert(3, Arc::new("c".to_string()), 5);
        assert_eq!(lru.total_weight(), 9);
        // Entry 1 (weight 4) should have been evicted: 8+5-4=9 <= 10
        assert!(lru.record_hit(&1).is_none());
        assert!(lru.record_hit(&2).is_some());
        assert!(lru.record_hit(&3).is_some());
    }

    #[test]
    fn test_lru_weight_eviction_multiple() {
        let mut lru = Lru::with_max_weight(10);
        lru.insert(1, Arc::new("a".to_string()), 3);
        lru.insert(2, Arc::new("b".to_string()), 3);
        lru.insert(3, Arc::new("c".to_string()), 3);
        assert_eq!(lru.total_weight(), 9);

        // Insert weight 5: total would be 14, need to evict until <= 10
        // Evict 1 (3): 14-3=11, still > 10
        // Evict 2 (3): 11-3=8, done
        lru.insert(4, Arc::new("d".to_string()), 5);
        assert_eq!(lru.total_weight(), 8);
        assert!(lru.record_hit(&1).is_none());
        assert!(lru.record_hit(&2).is_none());
        assert!(lru.record_hit(&3).is_some());
        assert!(lru.record_hit(&4).is_some());
    }
}
