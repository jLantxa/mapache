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

use blake3::Hasher;

use crate::global::ID;

/// Describes a single blob's location and size within a packed file.
/// This metadata is crucial for retrieving individual blobs from a larger pack.
#[derive(Debug, Clone)]
pub struct PackedBlobDescriptor {
    pub id: ID,
    pub offset: u64,
    pub length: u64,
}

pub type FlushedPack = (Vec<u8>, Vec<PackedBlobDescriptor>, ID);

/// The `Packer` is an in-memory buffer designed to efficiently accumulate
/// multiple blob objects and their raw data. When `flush` is called, it
/// releases the combined data and a list of descriptors, ready to be written
/// as a single pack file.
///
/// This design helps minimize memory reallocations by consolidating all blob
/// data into a single `Vec<u8>` and tracking individual blob locations.
#[derive(Debug)]
pub struct Packer {
    data: Vec<u8>,
    blob_descriptors: Vec<PackedBlobDescriptor>,
    hasher: Hasher,

    data_capacity: usize,
    blob_descriptor_capacity: usize,
}

impl Packer {
    /// Creates a new, empty `Packer`.
    pub fn _new() -> Self {
        Self {
            data: Vec::new(),
            blob_descriptors: Vec::new(),
            hasher: Hasher::new(),
            data_capacity: 0,
            blob_descriptor_capacity: 0,
        }
    }

    /// Creates a new `Packer` with pre-allocated capacity for its internal data buffer
    /// and blob descriptor list.
    ///
    /// Using this constructor can improve performance by reducing the number of memory
    /// reallocations if you have a good estimate of the total data size and number of
    /// blobs you intend to add.
    ///
    /// # Arguments
    /// * `data_capacity`: The estimated total byte size of all blobs.
    /// * `blobs_capacity`: The estimated number of individual blobs to be added.
    pub fn with_capacity(data_capacity: usize, blobs_capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(data_capacity),
            blob_descriptors: Vec::with_capacity(blobs_capacity),
            hasher: Hasher::new(),
            data_capacity,
            blob_descriptor_capacity: blobs_capacity,
        }
    }

    /// Returns the current total byte size of all raw data accumulated in the packer.
    #[inline]
    pub fn size(&self) -> u64 {
        self.data.len() as u64
    }

    /// Returns `true` if the packer contains no blob data and no descriptors.
    pub fn _is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Returns the number of individual blob objects currently stored in the packer.
    #[inline]
    pub fn _num_objects(&self) -> usize {
        self.blob_descriptors.len()
    }

    /// Appends a new blob's data to the packer and records its corresponding descriptor.
    ///
    /// The `blob_data` `Vec<u8>` is efficiently moved into the packer's internal
    /// buffer using `Vec::append`, avoiding a costly copy. After this call, `blob_data`
    /// will be empty.
    ///
    /// # Arguments
    /// * `id`: The unique `ID` of the blob. This will be cloned for the descriptor.
    /// * `blob_data`: The raw byte data of the blob. This `Vec` will be consumed (emptied).
    pub fn add_blob(&mut self, id: &ID, mut blob_data: Vec<u8>) {
        let offset = self.data.len() as u64; // The new blob starts at the current end of the data buffer
        let length = blob_data.len() as u64; // The length of the incoming blob data
        self.hasher.update(&blob_data);

        // Efficiently move `blob_data` into `self.data`
        self.data.append(&mut blob_data);

        // Record the descriptor for the newly added blob
        self.blob_descriptors.push(PackedBlobDescriptor {
            id: id.clone(), // Clone the ID for the descriptor
            offset,
            length,
        });
    }

    /// Flushes the contents of the packer, returning the accumulated raw data
    /// and the list of `PackedBlobDescriptor`s.
    ///
    /// After calling `flush`, the `Packer` instance will be reset to an empty state,
    /// ready to accumulate new blobs. Ownership of the `Vec<u8>` and `Vec<PackedBlobDescriptor>`
    /// is transferred to the caller, making this an efficient way to extract the packed content.
    pub fn flush(&mut self) -> FlushedPack {
        // Efficiently take ownership of the vectors, leaving new, empty vectors behind.
        let data = std::mem::replace(&mut self.data, Vec::with_capacity(self.data_capacity));
        let descriptors = std::mem::replace(
            &mut self.blob_descriptors,
            Vec::with_capacity(self.blob_descriptor_capacity),
        );
        let hash = self.hasher.finalize();
        self.hasher.reset();

        (data, descriptors, ID::from_bytes(hash.into()))
    }
}
