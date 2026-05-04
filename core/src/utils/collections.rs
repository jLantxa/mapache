use std::{
    collections::{HashMap, HashSet},
    hash::{BuildHasherDefault, Hash},
};

pub type IdSet<K> = HashSet<K, BuildHasherDefault<rustc_hash::FxHasher>>;
pub type IdMap<K, V> = HashMap<K, V, BuildHasherDefault<rustc_hash::FxHasher>>;

/// IndexSet is a set that can be enumerated by index.
#[derive(Debug, Clone)]
pub struct IndexSet<T, S = std::collections::hash_map::RandomState>
where
    T: Hash + Eq + Clone,
    S: std::hash::BuildHasher,
{
    values: Vec<T>,
    map: HashMap<T, usize, S>,
}

impl<T> Default for IndexSet<T, std::collections::hash_map::RandomState>
where
    T: Hash + Eq + Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T> IndexSet<T, std::collections::hash_map::RandomState>
where
    T: Hash + Eq + Clone,
{
    pub fn new() -> Self {
        Self {
            values: Vec::new(),
            map: HashMap::new(),
        }
    }
}

pub type IdIndexSet<T> = IndexSet<T, BuildHasherDefault<rustc_hash::FxHasher>>;

use crate::mapache::ID;
use parking_lot::RwLock;

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

impl<T> IdIndexSet<T>
where
    T: Hash + Eq + Clone,
{
    pub fn new_id_set() -> Self {
        Self {
            values: Vec::new(),
            map: HashMap::default(),
        }
    }
}

impl<T, S> IndexSet<T, S>
where
    T: Hash + Eq + Clone,
    S: std::hash::BuildHasher + Default,
{
    pub fn insert(&mut self, item: T) -> usize {
        if let Some(&idx) = self.map.get(&item) {
            idx
        } else {
            let index = self.values.len();
            self.values.push(item.clone());
            self.map.insert(item, index);
            index
        }
    }

    pub fn remove(&mut self, item: &T) -> bool {
        if let Some(value_index) = self.map.remove(item) {
            let last_index = self.values.len() - 1;

            if value_index != last_index {
                // Swap the element to be removed with the last element. The element at
                // 'value_index' is now the *old* last element. Then, update the index in the map
                // for the element that was moved.
                // The new index for this element is 'value_index'.
                self.values.swap_remove(value_index);
                let moved_item = &self.values[value_index];
                *self.map.get_mut(moved_item).unwrap() = value_index;
            } else {
                // The item to be removed is the last element.
                // Just pop it, no other indices need updating.
                self.values.pop();
            }

            true
        } else {
            false
        }
    }

    pub fn contains(&self, value: &T) -> bool {
        self.map.contains_key(value)
    }

    pub fn get_index(&self, item: &T) -> Option<&usize> {
        self.map.get(item)
    }

    pub fn get_value(&self, index: usize) -> Option<&T> {
        self.values.get(index)
    }

    pub fn iter(&self) -> Iter<'_, T> {
        Iter {
            iter: self.values.iter(),
        }
    }

    /// Returns the number of unique items in the set.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns `true` if the set contains no elements.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

impl<T> IntoIterator for IndexSet<T>
where
    T: Hash + Eq + Clone,
{
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}

pub struct Iter<'a, T>
where
    T: Hash + Eq + Clone,
{
    iter: std::slice::Iter<'a, T>,
}

impl<'a, T> Iterator for Iter<'a, T>
where
    T: Hash + Eq + Clone,
{
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let set = IndexSet::<String>::new();
        assert!(set.is_empty());
        assert_eq!(set.len(), 0);
    }

    #[test]
    fn test_insert_new_item() {
        let mut set = IndexSet::new();
        let index1 = set.insert("apple".to_string());
        assert!(set.contains(&"apple".to_string()));
        assert_eq!(index1, 0);
        assert_eq!(set.len(), 1);
        assert_eq!(set.get_value(0), Some(&"apple".to_string()));
        assert_eq!(set.get_index(&"apple".to_string()), Some(&0));

        let index2 = set.insert("banana".to_string());
        assert!(set.contains(&"banana".to_string()));
        assert_eq!(index2, 1);
        assert_eq!(set.len(), 2);
        assert_eq!(set.get_value(1), Some(&"banana".to_string()));
        assert_eq!(set.get_index(&"banana".to_string()), Some(&1))
    }

    #[test]
    fn test_insert_existing_item() {
        let mut set = IndexSet::new();
        set.insert("apple".to_string()); // index 0
        let index = set.insert("apple".to_string()); // Should return existing index
        assert!(set.contains(&"apple".to_string()));
        assert_eq!(index, 0);
        assert_eq!(set.len(), 1); // Length should not change
    }

    #[test]
    fn test_get_index() {
        let mut set = IndexSet::new();
        set.insert("apple".to_string());
        set.insert("banana".to_string());

        assert!(set.contains(&"apple".to_string()));
        assert!(set.contains(&"banana".to_string()));
        assert!(!set.contains(&"orange".to_string()));

        assert_eq!(set.get_index(&"apple".to_string()), Some(&0));
        assert_eq!(set.get_index(&"banana".to_string()), Some(&1));
        assert_eq!(set.get_index(&"orange".to_string()), None);
    }

    #[test]
    fn test_get_value() {
        let mut set = IndexSet::new();
        set.insert("apple".to_string());
        set.insert("banana".to_string());

        assert_eq!(set.get_value(0), Some(&"apple".to_string()));
        assert_eq!(set.get_value(1), Some(&"banana".to_string()));
        assert_eq!(set.get_value(2), None);
    }

    #[test]
    fn test_iter() {
        let mut set = IndexSet::new();
        set.insert("apple".to_string());
        set.insert("banana".to_string());
        set.insert("cherry".to_string());

        let mut iter = set.iter();
        assert_eq!(iter.next(), Some(&"apple".to_string()));
        assert_eq!(iter.next(), Some(&"banana".to_string()));
        assert_eq!(iter.next(), Some(&"cherry".to_string()));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_into_iter() {
        let mut set = IndexSet::new();
        set.insert("apple".to_string());
        set.insert("banana".to_string());
        set.insert("cherry".to_string());

        let vec: Vec<String> = set.into_iter().collect();
        assert_eq!(
            vec,
            vec![
                "apple".to_string(),
                "banana".to_string(),
                "cherry".to_string()
            ]
        );
    }

    #[test]
    fn test_len_and_is_empty() {
        let mut set = IndexSet::new();
        assert!(set.is_empty());
        assert_eq!(set.len(), 0);

        set.insert("first".to_string());
        assert!(!set.is_empty());
        assert_eq!(set.len(), 1);

        set.insert("second".to_string());
        assert!(!set.is_empty());
        assert_eq!(set.len(), 2);

        set.insert("first".to_string()); // duplicate
        assert!(!set.is_empty());
        assert_eq!(set.len(), 2); // Length should not change
    }

    #[test]
    fn test_remove_non_existent() {
        let mut set = IndexSet::new();
        set.insert("apple".to_string());
        assert!(!set.remove(&"orange".to_string()));
        assert_eq!(set.len(), 1);
        assert_eq!(set.get_value(0), Some(&"apple".to_string()));
    }

    #[test]
    fn test_remove_last_item() {
        let mut set = IndexSet::new();
        set.insert("apple".to_string()); // index 0
        set.insert("banana".to_string()); // index 1

        // Remove the last item (banana)
        assert!(set.remove(&"banana".to_string()));
        assert_eq!(set.len(), 1);
        assert_eq!(set.get_index(&"banana".to_string()), None);
        assert_eq!(set.get_value(1), None);

        // Check the remaining item
        assert_eq!(set.get_index(&"apple".to_string()), Some(&0));
        assert_eq!(set.get_value(0), Some(&"apple".to_string()));
    }

    #[test]
    fn test_new_id_set() {
        let set = IdIndexSet::<u32>::new_id_set();
        assert!(set.is_empty());
        assert_eq!(set.len(), 0);
    }

    #[test]
    fn test_remove_all_items() {
        let mut set = IndexSet::new();
        set.insert("a".to_string());
        set.insert("b".to_string());
        set.insert("c".to_string());

        assert!(set.remove(&"b".to_string()));
        assert_eq!(set.len(), 2);
        assert!(set.remove(&"a".to_string()));
        assert_eq!(set.len(), 1);
        assert!(set.remove(&"c".to_string()));
        assert_eq!(set.len(), 0);
        assert!(set.is_empty());
    }
}
