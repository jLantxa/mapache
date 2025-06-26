// [backup] is an incremental backup tool
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

use std::collections::HashMap;
use std::hash::Hash;

/// IndexSet is a set that can be enumerated by index.
#[derive(Default, Debug, Clone)]
pub struct IndexSet<T>
where
    T: Hash + Eq + Clone,
{
    values: Vec<T>,
    map: HashMap<T, usize>,
}

impl<T> IndexSet<T>
where
    T: Hash + Eq + Clone,
{
    pub fn new() -> Self {
        Self {
            values: Vec::new(),
            map: HashMap::new(),
        }
    }

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
        if let Some(_value_index) = self.map.remove(item) {
            // TODO: Remove the value from the vector.
            //
            // This would require updating all values in the map since the vector
            // was shifted after the item was removed. It is ok to leave it like
            // this for now. The unremoved values are simply not used.
            //
            // self.values.remove(value_index);
            //

            true
        } else {
            false
        }
    }

    pub fn contains(&self, value: &T) -> bool {
        self.values.contains(value)
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
        assert_eq!(index1, 0);
        assert_eq!(set.len(), 1);
        assert_eq!(set.get_value(0), Some(&"apple".to_string()));
        assert_eq!(set.get_index(&"apple".to_string()), Some(&0));

        let index2 = set.insert("banana".to_string());
        assert_eq!(index2, 1);
        assert_eq!(set.len(), 2);
        assert_eq!(set.get_value(1), Some(&"banana".to_string()));
        assert_eq!(set.get_index(&"banana".to_string()), Some(&1));
    }

    #[test]
    fn test_insert_existing_item() {
        let mut set = IndexSet::new();
        set.insert("apple".to_string()); // index 0
        let index = set.insert("apple".to_string()); // Should return existing index
        assert_eq!(index, 0);
        assert_eq!(set.len(), 1); // Length should not change
    }

    #[test]
    fn test_get_index() {
        let mut set = IndexSet::new();
        set.insert("apple".to_string());
        set.insert("banana".to_string());

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
}
