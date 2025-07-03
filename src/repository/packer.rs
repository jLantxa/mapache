// mapache is an incremental backup tool
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

use std::{sync::Arc, thread::JoinHandle};

use anyhow::{Context, Result, bail};
use blake3::Hasher;
use crossbeam_channel::Sender;

use crate::global::{BlobType, ID, SaveID};

const HEADER_BLOB_LEN: usize = 32 + 4 + 1; // id (256 bits) + length (u32) + type (u8)

/// Describes a single blob's location and size within a packed file.
/// This metadata is crucial for retrieving individual blobs from a pack.
#[derive(Debug, Clone, PartialEq)]
pub struct PackedBlobDescriptor {
    pub id: ID,
    pub blob_type: BlobType,
    pub offset: u32,
    pub length: u32,
}

/// A tuple representing the flushed contents of a `Packer`:
/// (packed data, list of blob descriptors, pack ID).
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

    data_papacity_hint: usize,
    blob_descriptor_capacity_hint: usize,
}

impl Default for Packer {
    fn default() -> Self {
        Self::new()
    }
}

impl Packer {
    /// Creates a new, empty `Packer` with default capacities.
    ///
    /// For better performance, consider using `Packer::with_capacity` if you have
    /// an estimate of the total data size and number of blobs you intend to add.
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            blob_descriptors: Vec::new(),
            hasher: Hasher::new(),
            data_papacity_hint: 0,
            blob_descriptor_capacity_hint: 0,
        }
    }

    /// Creates a new `Packer` with pre-allocated capacity for its internal data buffer
    /// and blob descriptor list.
    ///
    /// Using this constructor can improve performance by reducing the number of memory
    /// reallocations if you have a good estimate of the total data size and number of
    /// blobs you intend to add. For fix pack sizes, using this can significantly reduce
    /// memory churn.
    pub fn with_capacity(data_capacity: usize, blobs_capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(data_capacity),
            blob_descriptors: Vec::with_capacity(blobs_capacity),
            hasher: Hasher::new(),
            data_papacity_hint: data_capacity,
            blob_descriptor_capacity_hint: blobs_capacity,
        }
    }

    /// Returns the current total byte size of all raw data accumulated in the packer.
    #[inline]
    pub fn size(&self) -> u64 {
        self.data.len() as u64
    }

    /// Returns `true` if the packer contains no blob data and no descriptors.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty() && self.blob_descriptors.is_empty()
    }

    /// Returns the number of individual blob objects currently stored in the packer.
    #[inline]
    pub fn num_objects(&self) -> usize {
        self.blob_descriptors.len()
    }

    /// Appends a new blob's data to the packer and records its corresponding descriptor.
    ///
    /// The `blob_data` `Vec<u8>` is efficiently moved into the packer's internal
    /// buffer using `Vec::append`, avoiding a costly copy. After this call, `blob_data`
    /// will be empty.
    pub fn add_blob(&mut self, id: ID, blob_type: BlobType, mut blob_data: Vec<u8>) {
        let offset = self.data.len() as u32;
        let length = blob_data.len() as u32;

        if !blob_data.is_empty() {
            self.hasher.update(&blob_data);
        }

        self.data.append(&mut blob_data);

        // Record the descriptor for the newly added blob
        self.blob_descriptors.push(PackedBlobDescriptor {
            id,
            blob_type,
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
    /// The internal vectors retain their allocated capacity for future use.
    pub fn flush(&mut self) -> Option<FlushedPack> {
        if self.is_empty() {
            return None;
        }

        // Take ownership of the vectors, effectively swapping them with new empty ones.
        let mut data = std::mem::take(&mut self.data);
        let descriptors = std::mem::take(&mut self.blob_descriptors);

        // Append metadata section
        let mut pack_header = Self::generate_header(&descriptors);
        self.hasher.update(&pack_header);
        data.append(&mut pack_header);

        let hash = self.hasher.finalize();
        self.hasher.reset();

        // Reserve capacity
        self.data.reserve(self.data_papacity_hint);
        self.blob_descriptors
            .reserve(self.blob_descriptor_capacity_hint);

        Some((data, descriptors, ID::from_bytes(hash.into())))
    }

    fn generate_header(descriptors: &Vec<PackedBlobDescriptor>) -> Vec<u8> {
        // blob[id (256 bits), lenght (u32), type (u8)] + header length (u32);
        let mut pack_header = Vec::<u8>::with_capacity(HEADER_BLOB_LEN * descriptors.len() + 4);

        for blob in descriptors {
            let id = blob.id.as_slice();
            pack_header.extend_from_slice(id);

            let length = blob.length.to_le_bytes();
            pack_header.extend_from_slice(&length);

            let blob_type: [u8; 1] = (blob.blob_type.to_owned() as u8).to_le_bytes();
            pack_header.extend_from_slice(&blob_type);
        }

        let header_length = (4 + pack_header.len() as u32).to_le_bytes();
        pack_header.extend_from_slice(&header_length);

        pack_header
    }

    pub fn read_header(pack_data: &[u8]) -> Result<Vec<PackedBlobDescriptor>> {
        let pack_len = pack_data.len();
        if pack_len < 4 {
            bail!(
                "Pack header is invalid: data too short for header length (got {} bytes, need at least 4).",
                pack_len
            );
        }

        let header_length_bytes: [u8; 4] = pack_data[(pack_len - 4)..]
            .try_into()
            .with_context(|| "Could not read pack header length bytes.")?;
        let header_length = u32::from_le_bytes(header_length_bytes) as usize;

        if pack_len < header_length {
            bail!(
                "Pack header is invalid: declared header_length ({}) exceeds total pack_len ({}).",
                header_length,
                pack_len
            );
        }

        let header_blob_info_actual_len = header_length - 4;
        if header_blob_info_actual_len % HEADER_BLOB_LEN != 0 {
            bail!(
                "Pack header is invalid: header blob info length ({}) is not a multiple of expected blob descriptor size ({}).",
                header_blob_info_actual_len,
                HEADER_BLOB_LEN
            );
        }

        let num_blobs = (header_length - 4) / HEADER_BLOB_LEN;
        let header_blob_info = &pack_data[(pack_len - header_length)..];

        let mut blob_descriptors = Vec::new();
        let mut current_offset: u32 = 0;
        for i in 0..num_blobs {
            let blob_info = &header_blob_info[(i * HEADER_BLOB_LEN)..((i + 1) * HEADER_BLOB_LEN)];

            let blob_id_bytes: [u8; 32] = blob_info[0..32].try_into().unwrap();
            let id = ID::from_bytes(blob_id_bytes);

            let offset = current_offset;

            let length_bytes: [u8; 4] = blob_info[32..36].try_into().unwrap();
            let length = u32::from_le_bytes(length_bytes);
            current_offset += length;

            let blob_type: BlobType = blob_info[36].try_into()?;

            let blob_descriptor = PackedBlobDescriptor {
                id,
                blob_type,
                offset,
                length,
            };
            blob_descriptors.push(blob_descriptor);
        }

        Ok(blob_descriptors)
    }
}

pub type QueueFn = Arc<dyn Fn(Vec<u8>, ID) + Send + Sync + 'static>;

pub struct PackSaver {
    tx: Sender<(Vec<u8>, ID)>,
    join_handle: JoinHandle<()>,
}

impl PackSaver {
    pub fn new(concurrency: usize, queue_fn: QueueFn) -> Self {
        let (tx, rx) = crossbeam_channel::bounded(concurrency);

        let worker_queue_fn = Arc::clone(&queue_fn);

        let join_handle = std::thread::spawn(move || {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(concurrency)
                .build()
                .expect("Failed to build thread pool");

            while let Ok((data, id)) = rx.recv() {
                pool.scope(|s| {
                    s.spawn(|_| {
                        worker_queue_fn(data, id);
                    });
                });
            }
        });

        PackSaver { tx, join_handle }
    }

    pub fn save_pack(&self, packer_data: Vec<u8>, save_id: SaveID) -> Result<ID> {
        let pack_id = match save_id {
            SaveID::CalculateID => ID::from_content(&packer_data),
            SaveID::WithID(id) => id,
        };

        self.tx
            .send((packer_data, pack_id.clone()))
            .with_context(|| "Failed to send pack data to PackSaver channel")?;

        Ok(pack_id)
    }

    pub fn finish(self) {
        drop(self.tx);
        self.join_handle
            .join()
            .expect("Packer saver thread panicked");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pack_flush() -> Result<()> {
        let mut packer = Packer::new();

        let blob1: Vec<u8> = b"mapache".to_vec(); // 7 bytes
        packer.add_blob(ID::from_content(&blob1), BlobType::Data, blob1);

        let blob2: Vec<u8> = b"backup".to_vec(); // 6 bytes
        packer.add_blob(ID::from_content(&blob2), BlobType::Data, blob2);

        let blob3: Vec<u8> = b"rust".to_vec(); // 4 bytes
        packer.add_blob(ID::from_content(&blob3), BlobType::Data, blob3);

        assert_eq!(packer.size(), 7 + 6 + 4);
        assert!(!packer.is_empty());

        let (data, descriptors, id) = packer.flush().expect("Flushed pack data must be Some");

        let expected_header_length = 4 + (3 * HEADER_BLOB_LEN);
        assert_eq!(data.len(), (7 + 6 + 4) + expected_header_length);
        assert_eq!(
            id.to_hex(),
            "492d12ce69b75f6ce252969172077bed586ce631fb59b59f0107bee77a93ba01"
        );

        let header_descriptors = Packer::read_header(&data)?;
        assert_eq!(descriptors.len(), 3);
        assert_eq!(descriptors, header_descriptors);

        Ok(())
    }

    #[test]
    fn test_empty_pack_flush() -> Result<()> {
        let mut packer = Packer::new();

        assert_eq!(packer.size(), 0);
        assert!(packer.is_empty());

        let flushed_pack_data = packer.flush();
        assert!(flushed_pack_data.is_none());

        Ok(())
    }
}
