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
#[derive(Default, Debug)]
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
        if self.map.contains_key(&item) {
            *self.map.get(&item).unwrap()
        } else {
            let index: usize = self.values.len();
            self.values.push(item.clone());
            self.map.insert(item, index);
            index
        }
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
