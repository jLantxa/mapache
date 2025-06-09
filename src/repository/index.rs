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
    global::{self, ID},
    utils::indexset::IndexSet,
};

use super::{RepositoryBackend, packer::PackedBlobDescriptor};

/// Represents the location and size of a blob within a pack file.
#[derive(Debug)]
struct BlobLocation {
    /// The index into the `pack_ids` `IndexSet` for the pack containing this blob. See Index.
    pub pack_array_index: usize,
    /// The offset of the blob within its pack file.
    pub offset: u64,
    /// The length of the blob within its pack file.
    pub length: u64,
}

/// Manages the mapping of blob IDs to their locations within pack files.
/// An `Index` can be in a 'pending' state, indicating it's still being built.
#[derive(Debug)]
pub struct Index {
    /// An object_id -> BlobLocation map. This is the core lookup table.
    ids: HashMap<ID, BlobLocation>,

    /// The Pack IDs referenced in this index. Using an `IndexSet` allows us
    /// to store a small `usize` index in `BlobLocation` instead of the full `ID`,
    /// significantly reducing memory usage.
    pack_ids: IndexSet<ID>,

    /// If an index is pending, it is still receiving entries from packs and is not yet finalized.
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

    /// Marks the index as finalized. A finalized index no longer accepts new entries
    /// and is typically ready for persistence or read-only operations.
    pub fn finalize(&mut self) {
        self.is_pending = false;
    }

    /// Returns `true` if the index is currently pending (still receiving entries).
    pub fn is_pending(&self) -> bool {
        self.is_pending
    }

    /// Creates an `Index` from a serialized `IndexFile`.
    /// The created index is *not* pending, as it represents a complete, loaded file.
    pub fn from_index_file(index_file: IndexFile) -> Self {
        let mut index = Self::new();
        // An index loaded from a file is considered complete and not pending.
        index.is_pending = false;

        for pack in index_file.packs {
            let pack_index = index.pack_ids.insert(pack.id.clone());
            for blob in pack.blobs {
                index.ids.insert(
                    blob.id,
                    BlobLocation {
                        pack_array_index: pack_index,
                        offset: blob.offset,
                        length: blob.length,
                    },
                );
            }
        }
        index
    }

    /// Checks if the index contains the given object ID.
    pub fn contains(&self, id: &ID) -> bool {
        self.ids.contains_key(id)
    }

    /// Retrieves the pack ID, offset, and length for a given blob ID, if it exists.
    /// Returns `None` if the blob ID is not found.
    pub fn get(&self, id: &ID) -> Option<(ID, u64, u64)> {
        self.ids.get(id).map(|location| {
            // Retrieve the full pack ID from the `IndexSet` using the stored `pack_array_index`.
            let pack_id = self
                .pack_ids
                .get_value(location.pack_array_index)
                .expect("pack_index should always be valid for an existing blob");
            (pack_id.clone(), location.offset, location.length)
        })
    }

    /// Adds all blob descriptors from a specific pack to the index.
    /// This method is optimized for adding multiple blobs from the same pack,
    /// as it only needs to look up the pack ID once.
    pub fn add_pack(&mut self, pack_id: &ID, packed_blob_descriptors: &[PackedBlobDescriptor]) {
        let pack_index = self.pack_ids.insert(pack_id.clone());
        for blob in packed_blob_descriptors {
            self.ids.insert(
                blob.id.clone(),
                BlobLocation {
                    pack_array_index: pack_index,
                    offset: blob.offset,
                    length: blob.length,
                },
            );
        }
    }

    /// Saves the index to the repository. This operation might generate multiple
    /// index files if the total number of blobs exceeds a manifesturable limit.
    ///
    /// Returns the total uncompressed and compressed sizes of the saved index files.
    pub fn finalize_and_save(&mut self, repo: &dyn RepositoryBackend) -> Result<(u64, u64)> {
        // Don't do anything if the index is empty.
        if self.ids.is_empty() {
            return Ok((0, 0));
        }

        self.finalize();

        let mut packs_with_blobs: HashMap<usize, Vec<IndexFileBlob>> = HashMap::new();
        for (blob_id, location) in &self.ids {
            let entry = packs_with_blobs
                .entry(location.pack_array_index)
                .or_default();
            entry.push(IndexFileBlob {
                id: blob_id.clone(),
                offset: location.offset,
                length: location.length,
            });
        }

        let mut index_file = IndexFile::new();

        // Iterate through packs in the order they were inserted into `pack_ids`.
        // This ensures a consistent ordering of packs in the generated index files.
        for (pack_index, pack_id) in self.pack_ids.iter().enumerate() {
            if let Some(blobs) = packs_with_blobs.remove(&pack_index) {
                let index_pack_file = IndexFilePack {
                    id: pack_id.clone(),
                    blobs,
                };
                index_file.packs.push(index_pack_file);
            }
        }

        let (raw_size, encoded_size) = repo.save_index(index_file)?;

        Ok((raw_size, encoded_size))
    }
}

/// Manages a collection of `Index` instances, providing a unified view
/// over all known blobs in the repository.
#[derive(Debug)]
pub struct MasterIndex {
    /// A list of individual indexes, some of which might be pending.
    indexes: Vec<Index>,

    /// Stores the IDs of blobs that are waiting to be serialized into a pack file.
    pending_blobs: HashSet<ID>,
}

impl MasterIndex {
    /// Creates a new, empty `MasterIndex`.
    pub fn new() -> Self {
        Self {
            indexes: Vec::with_capacity(1),
            pending_blobs: HashSet::new(),
        }
    }

    /// Returns `true` if the object ID is known either in a finalized index
    /// or is currently a pending blob.
    pub fn contains(&self, id: &ID) -> bool {
        // Check finalized indexes first
        self.indexes
            .iter()
            .any(|idx| !idx.is_pending && idx.contains(id))
            || self.pending_blobs.contains(id) // Then check pending blobs
    }

    /// Retrieves an entry for a given blob ID by searching through finalized indexes.
    /// Pending blobs (those not yet packed) cannot be retrieved via this method.
    pub fn get(&self, id: &ID) -> Option<(ID, u64, u64)> {
        self.indexes
            .iter()
            .find_map(|idx| if !idx.is_pending { idx.get(id) } else { None })
    }

    /// Adds a fully constructed `Index` to the master index.
    /// This is typically used for adding loaded, finalized indexes.
    pub fn add_index(&mut self, index: Index) {
        self.indexes.push(index);
    }

    /// Adds a blob ID to the set of blobs that are waiting to be packed.
    /// Returns `true` if the ID did not exist in the set and was inserted; `false` otherwise.
    pub fn add_pending_blob(&mut self, id: ID) -> bool {
        self.pending_blobs.insert(id)
    }

    /// Processes a newly created pack of blobs. It removes these blobs from the
    /// `pending_blobs` set and adds them to all currently pending `Index` instances.
    ///
    /// It's assumed that there is at least one pending index that should receive these blobs,
    /// or that a new one will be created as part of the overall backup process if needed.
    pub fn add_pack(
        &mut self,
        pack_id: &ID,
        packed_blob_descriptors: Vec<PackedBlobDescriptor>, // Take ownership as it's consumed
    ) {
        // Remove processed blobs from the pending set
        for blob in &packed_blob_descriptors {
            self.pending_blobs.remove(&blob.id);
        }

        // Add the pack's blobs to all currently pending indexes.
        for idx in &mut self.indexes {
            if idx.is_pending() {
                idx.add_pack(pack_id, &packed_blob_descriptors);
                return;
            }
        }

        // There were no pending indexes. Create a new empty pending index and add the pack.
        let mut new_pending_index = Index::new();
        new_pending_index.add_pack(pack_id, &packed_blob_descriptors);
        self.indexes.push(new_pending_index);
    }

    /// Saves all pending indexes managed by the `MasterIndex` to the repository.
    /// Finalized indexes are not saved again.
    ///
    /// Returns the total uncompressed and compressed sizes of the saved index files.
    pub fn save(&mut self, repo: &dyn RepositoryBackend) -> Result<(u64, u64)> {
        let mut uncompressed_size: u64 = 0;
        let mut compressed_size: u64 = 0;

        for idx in &mut self.indexes {
            if idx.is_pending() {
                let (uncompressed, compressed) = idx.finalize_and_save(repo)?;
                uncompressed_size += uncompressed;
                compressed_size += compressed;
            }
        }

        Ok((uncompressed_size, compressed_size))
    }

    // Finalize and flush all pending indexes. This function saves indexes with
    // a smaller size than the optimum as a trade-off to commit packs often.
    // The garbage collector should merge all small indexes and consolidate them
    // into bigger index files.
    pub fn flush_pending_if_full(&mut self, repo: &dyn RepositoryBackend) -> Result<(u64, u64)> {
        let (mut total_raw_size, mut total_encoded_size) = (0, 0);
        for idx in &mut self.indexes {
            if idx.is_pending() {
                // Save index if the number of packs surpasses the default.
                if idx.pack_ids.len() > global::defaults::PACKS_PER_FLUSHED_INDEX_FILE {
                    let (raw, encoded) = idx.finalize_and_save(repo)?;
                    total_raw_size += raw;
                    total_encoded_size += encoded;
                }
            }
        }

        Ok((total_raw_size, total_encoded_size))
    }
}

/// Represents the on-disk format for an index file.
/// This structure is used for serialization and deserialization of index data.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct IndexFile {
    pub packs: Vec<IndexFilePack>,
}

impl IndexFile {
    pub fn new() -> Self {
        Self::default()
    }
}

/// Represents a pack's entry within an `IndexFile`.
#[derive(Debug, Serialize, Deserialize)]
pub struct IndexFilePack {
    pub id: ID,
    pub blobs: Vec<IndexFileBlob>,
}

/// Represents a blob's entry within an `IndexFilePack`.
#[derive(Debug, Serialize, Deserialize)]
pub struct IndexFileBlob {
    pub id: ID,
    pub offset: u64,
    pub length: u64,
}
