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
    fn test_remove_middle_item() {
        let mut set = IndexSet::new();
        set.insert("apple".to_string()); // index 0
        set.insert("banana".to_string()); // index 1 (to be removed)
        set.insert("cherry".to_string()); // index 2 (will be moved to index 1)

        // Remove the middle item (banana)
        assert!(set.remove(&"banana".to_string()));
        assert_eq!(set.len(), 2);

        // banana is gone
        assert_eq!(set.get_index(&"banana".to_string()), None);
        assert_eq!(set.get_value(2), None);

        // apple is unchanged
        assert_eq!(set.get_index(&"apple".to_string()), Some(&0));
        assert_eq!(set.get_value(0), Some(&"apple".to_string()));

        // cherry's index must be updated to 1
        assert_eq!(set.get_index(&"cherry".to_string()), Some(&1));
        assert_eq!(set.get_value(1), Some(&"cherry".to_string()));

        // Check iteration order (apple, cherry)
        let mut iter = set.iter();
        assert_eq!(iter.next(), Some(&"apple".to_string()));
        assert_eq!(iter.next(), Some(&"cherry".to_string()));
        assert_eq!(iter.next(), None);
    }
}
