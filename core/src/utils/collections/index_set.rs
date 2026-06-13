use std::{
    collections::HashMap,
    hash::{BuildHasherDefault, Hash},
};

use crate::utils::collections::IdentityHasher;

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

pub type IdIndexSet<T> = IndexSet<T, BuildHasherDefault<IdentityHasher>>;

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
                self.values.swap_remove(value_index);
                let moved_item = &self.values[value_index];
                *self
                    .map
                    .get_mut(moved_item)
                    .expect("moved_item must exist in map after swap_remove") = value_index;
            } else {
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

    pub fn len(&self) -> usize {
        self.values.len()
    }

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
        set.insert("apple".to_string());
        let index = set.insert("apple".to_string());
        assert!(set.contains(&"apple".to_string()));
        assert_eq!(index, 0);
        assert_eq!(set.len(), 1);
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

        set.insert("first".to_string());
        assert!(!set.is_empty());
        assert_eq!(set.len(), 2);
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
        set.insert("apple".to_string());
        set.insert("banana".to_string());

        assert!(set.remove(&"banana".to_string()));
        assert_eq!(set.len(), 1);
        assert_eq!(set.get_index(&"banana".to_string()), None);
        assert_eq!(set.get_value(1), None);

        assert_eq!(set.get_index(&"apple".to_string()), Some(&0));
        assert_eq!(set.get_value(0), Some(&"apple".to_string()));
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

    #[test]
    fn test_new_id_set() {
        let set = IdIndexSet::<u32>::new_id_set();
        assert!(set.is_empty());
        assert_eq!(set.len(), 0);
    }
}
