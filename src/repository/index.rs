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

use std::collections::{HashMap, HashSet};

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::{
    backup::{self, ObjectId},
    utils::indexset::IndexSet,
};

use super::{RepositoryBackend, packer::PackedBlobDescriptor};

#[derive(Debug)]
pub struct Index {
    /// An object_id -> (pack_array_index, offset, length) map
    ids: HashMap<ObjectId, (usize, u64, u64)>,

    /// The Pack ids reference in the index.
    /// With the IndexSet we use a 8 byte reference to the ID instead of
    /// storing the 32 byte ID in all entries, cutting the memory usage to
    /// about 50 %.
    pack_ids: IndexSet<ObjectId>,

    /// If an index is pending, it is still receiving entries from packs
    is_pending: bool,
}

impl Index {
    pub fn new() -> Self {
        Self {
            ids: HashMap::new(),
            pack_ids: IndexSet::new(),
            is_pending: true,
        }
    }

    pub fn finalize(&mut self) {
        self.is_pending = false;
    }

    pub fn is_pending(&self) -> bool {
        self.is_pending
    }

    pub fn from_index_file(index_file: IndexFile) -> Self {
        let mut index = Self::new();
        for pack in index_file.packs {
            for blob in pack.blobs {
                index.insert(&blob.id, &pack.id, blob.offset, blob.length);
            }
        }

        index
    }

    pub fn contains(&self, id: &ObjectId) -> bool {
        self.ids.contains_key(id)
    }

    pub fn get(&self, id: &ObjectId) -> Option<(ObjectId, u64, u64)> {
        match self.ids.get(id) {
            Some((pack_index, offset, length)) => {
                let pack_id = self.pack_ids.get_value(*pack_index).unwrap();
                Some((pack_id.clone(), *offset, *length))
            }
            None => None,
        }
    }

    pub fn insert(&mut self, blob_id: &ObjectId, pack_id: &ObjectId, offset: u64, length: u64) {
        let pack_index = self.pack_ids.insert(pack_id.clone());
        self.ids
            .insert(blob_id.clone(), (pack_index, offset, length));
    }

    pub fn add_pack(
        &mut self,
        pack_id: &ObjectId,
        packed_blob_descriptors: &Vec<PackedBlobDescriptor>,
    ) {
        for blob in packed_blob_descriptors {
            self.insert(&blob.id, pack_id, blob.offset, blob.length);
        }
    }

    pub fn save(&self, repo: &dyn RepositoryBackend) -> Result<(u64, u64)> {
        let mut blob_count: u32 = 0;
        let mut index_file = IndexFile::new();

        let mut uncompressed_size = 0;
        let mut compressed_size = 0;

        for (i, pack_id) in self.pack_ids.iter().enumerate() {
            let mut index_pack_file = IndexFilePack {
                id: pack_id.clone(),
                blobs: Vec::new(),
            };
            for (blob_id, (blob_pack_idx, offset, length)) in &self.ids {
                if *blob_pack_idx == i {
                    index_pack_file.blobs.push(IndexFileBlob {
                        id: blob_id.to_string(),
                        offset: *offset,
                        length: *length,
                    });
                    blob_count += 1;
                }
            }

            index_file.packs.push(index_pack_file);

            if blob_count >= backup::defaults::BLOBS_PER_INDEX_FILE {
                let (u, c) = repo.save_index(std::mem::take(&mut index_file))?;
                uncompressed_size += u;
                compressed_size += c;
                blob_count = 0;
            }
        }

        if blob_count > 0 {
            let (u, c) = repo.save_index(index_file)?;
            uncompressed_size += u;
            compressed_size += c;
        }

        Ok((uncompressed_size, compressed_size))
    }
}

#[derive(Debug)]
pub struct MasterIndex {
    indexes: Vec<Index>,

    // Stores the ids from blobs waiting to be serialized in a packer.
    pending_blobs: HashSet<ObjectId>,
}

impl MasterIndex {
    pub fn new() -> Self {
        Self {
            indexes: Vec::new(),
            pending_blobs: HashSet::new(),
        }
    }

    /// Returns true if the object id is known in some of the indexes.
    pub fn contains(&self, id: &ObjectId) -> bool {
        for idx in &self.indexes {
            if !idx.is_pending {
                if idx.contains(id) {
                    return true;
                }
            }
        }

        self.pending_blobs.contains(id)
    }

    /// Get an entry by looking in the indexes.
    pub fn get(&self, id: &ObjectId) -> Option<(ObjectId, u64, u64)> {
        for idx in &self.indexes {
            if let Some(entry) = idx.get(id) {
                // Found entry in one of the indexes
                return Some(entry);
            }
        }

        // Pending blobs are still not serialized in a Pack.
        // There is no usecase where we need to get the content of a blob
        // that is still pending. We can ignore them.

        None
    }

    /// Insert an index
    pub fn add_index(&mut self, index: Index) {
        self.indexes.push(index);
    }

    /// Adds a blob id to the pending blob set.
    /// Returns true if the id did not exist in the set and was inserted. False otherwise.
    pub fn add_pending_blob(&mut self, id: &ObjectId) -> bool {
        self.pending_blobs.insert(id.clone())
    }

    pub fn add_pack(
        &mut self,
        pack_id: &ObjectId,
        packed_blob_descriptors: Vec<PackedBlobDescriptor>,
    ) {
        for blob in &packed_blob_descriptors {
            self.pending_blobs.remove(&blob.id);
        }

        for idx in &mut self.indexes {
            if idx.is_pending() {
                idx.add_pack(pack_id, &packed_blob_descriptors);
            }
        }
    }

    pub fn save(&self, repo: &dyn RepositoryBackend) -> Result<(u64, u64)> {
        let mut uncompressed_size: u64 = 0;
        let mut compressed_size: u64 = 0;
        for idx in &self.indexes {
            if idx.is_pending() {
                let (uncompressed, compressed) = idx.save(repo)?;
                uncompressed_size += uncompressed;
                compressed_size += compressed;
            }
        }

        Ok((uncompressed_size, compressed_size))
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct IndexFile {
    packs: Vec<IndexFilePack>,
}

impl IndexFile {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct IndexFilePack {
    id: ObjectId,
    blobs: Vec<IndexFileBlob>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct IndexFileBlob {
    id: ObjectId,
    offset: u64,
    length: u64,
}
