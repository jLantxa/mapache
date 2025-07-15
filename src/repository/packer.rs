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
use crossbeam_channel::Sender;
use rand::Rng;

use crate::{
    backend::StorageBackend,
    global::{BlobType, FileType, ID, SaveID, defaults::HEADER_BLOB_MULTIPLE},
    repository::{RepositoryBackend, storage::SecureStorage},
    utils,
};

pub(crate) const HEADER_BLOB_LEN: usize = 32 + 4 + 1; // id (256 bits) + length (u32) + type (u8)

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
#[derive(Debug)]
pub struct FlushedPack {
    pub data: Vec<u8>,
    pub descriptors: Vec<PackedBlobDescriptor>,
    pub meta_size: u64,
    pub id: ID,
}

/// The `Packer` is an in-memory buffer designed to efficiently accumulate
/// multiple blob objects and their raw data. When `flush` is called, it
/// releases the combined data and a list of descriptors, ready to be written
/// as a single pack file.
///
/// This design helps minimize memory reallocations by consolidating all blob
/// data into a single `Vec<u8>` and tracking individual blob locations.
pub struct Packer {
    blobs: Vec<(ID, BlobType, Vec<u8>)>,
    size: u64,
}

impl Default for Packer {
    fn default() -> Self {
        Self::new()
    }
}

impl Packer {
    pub fn new() -> Self {
        Self {
            blobs: Vec::new(),
            size: 0,
        }
    }

    /// Returns the current total byte size of all raw data accumulated in the packer.
    #[inline]
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Returns `true` if the packer contains no blob data and no descriptors.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.blobs.is_empty()
    }

    /// Returns the number of individual blob objects currently stored in the packer.
    #[inline]
    pub fn num_objects(&self) -> usize {
        self.blobs.len()
    }

    /// Appends a new blob's data to the packer and records its corresponding descriptor.
    ///
    /// The `blob_data` `Vec<u8>` is efficiently moved into the packer's internal
    /// buffer using `Vec::append`, avoiding a costly copy. After this call, `blob_data`
    /// will be empty.
    pub fn add_blob(&mut self, id: ID, blob_type: BlobType, blob_data: Vec<u8>) {
        let length = blob_data.len();
        self.size += length as u64;
        self.blobs.push((id, blob_type, blob_data));
    }

    /// Flushes the contents of the packer, returning the accumulated raw data
    /// and the list of `PackedBlobDescriptor`s.
    ///
    /// After calling `flush`, the `Packer` instance will be reset to an empty state,
    /// ready to accumulate new blobs. Ownership of the `Vec<u8>` and `Vec<PackedBlobDescriptor>`
    /// is transferred to the caller, making this an efficient way to extract the packed content.
    pub fn flush(&mut self, secure_storage: &SecureStorage) -> Result<Option<FlushedPack>> {
        if self.is_empty() {
            return Ok(None);
        }

        let blobs = std::mem::take(&mut self.blobs);
        self.size = 0;

        let mut offset: u32 = 0;
        let mut data = Vec::new();
        let mut descriptors = Vec::new();

        for blob in blobs {
            let mut blob_data = blob.2;
            let length = blob_data.len() as u32;
            descriptors.push(PackedBlobDescriptor {
                id: blob.0,
                blob_type: blob.1,
                offset,
                length,
            });
            data.append(&mut blob_data);
            offset += length;
        }

        let header = Self::generate_header(&mut descriptors);
        let mut header = secure_storage.encode(&header)?;
        let mut header_length_bytes = (header.len() as u32).to_le_bytes().to_vec();
        header.append(&mut header_length_bytes);
        let meta_size: u64 = header.len() as u64;
        data.append(&mut header);

        let hash = utils::calculate_hash(&data);

        Ok(Some(FlushedPack {
            data,
            descriptors,
            meta_size,
            id: ID::from_bytes(hash),
        }))
    }

    /// Generates a pack header given a vector of blob descriptors.
    fn generate_header(descriptors: &mut Vec<PackedBlobDescriptor>) -> Vec<u8> {
        // blob[id (256 bits), lenght (u32), type (u8)] + header length (u32);
        let mut pack_header = Vec::<u8>::with_capacity(HEADER_BLOB_LEN * descriptors.len());

        if descriptors.len() % HEADER_BLOB_MULTIPLE > 0 {
            let num_padding_blobs =
                HEADER_BLOB_MULTIPLE - (descriptors.len() % HEADER_BLOB_MULTIPLE);
            for _ in 0..num_padding_blobs {
                // Add random fields so the compressor cannot reduce the padding size.
                descriptors.push(PackedBlobDescriptor {
                    id: ID::new_random(),
                    blob_type: BlobType::Padding,
                    offset: rand::rng().random(),
                    length: rand::rng().random(),
                });
            }
        }

        for blob in descriptors {
            let id = blob.id.as_slice();
            pack_header.extend_from_slice(id);

            let length = blob.length.to_le_bytes();
            pack_header.extend_from_slice(&length);

            let blob_type: [u8; 1] = (blob.blob_type.to_owned() as u8).to_le_bytes();
            pack_header.extend_from_slice(&blob_type);
        }

        pack_header
    }

    /// Parses the header for a pack with a given ID. This function only reads the header bytes from
    /// the pack file using the seek read trait function from the backend.
    pub fn parse_pack_header(
        repo: &dyn RepositoryBackend,
        backend: &dyn StorageBackend,
        secure_storage: &SecureStorage,
        pack_id: &ID,
    ) -> Result<Vec<PackedBlobDescriptor>> {
        let (_id, pack_path) = repo.find(FileType::Object, &pack_id.to_hex())?;
        let header_length_bytes: [u8; 4] = backend
            .seek_read_from_end(&pack_path, -4, 4)?
            .as_slice()
            .try_into()?;
        let encoded_header_length = u32::from_le_bytes(header_length_bytes) as usize;

        let header_data = backend.seek_read_from_end(
            &pack_path,
            -(4 + encoded_header_length as i64),
            4 + encoded_header_length as u64,
        )?;

        Self::parse_header(secure_storage, &header_data)
    }

    /// Parses a pack header data from a sliice of bytes. `header_data` must contain the header and
    /// the length field. Since this function reads the length field, other bytes before the header
    /// can be still passed and they will be ignored.
    fn parse_header(
        secure_storage: &SecureStorage,
        header_data: &[u8],
    ) -> Result<Vec<PackedBlobDescriptor>> {
        if header_data.len() < 4 {
            bail!(
                "Pack header is invalid: data too short for header length (got {} bytes, need at least 4).",
                header_data.len()
            );
        }

        let header_length_bytes: [u8; 4] = header_data[(header_data.len() - 4)..]
            .try_into()
            .with_context(|| "Could not read pack header length bytes.")?;
        let encoded_header_length = u32::from_le_bytes(header_length_bytes) as usize;

        if header_data.len() < encoded_header_length {
            bail!(
                "Pack header is invalid: declared header_length ({}) exceeds total data length ({}).",
                encoded_header_length,
                header_data.len()
            );
        }

        let header_blob_info = secure_storage.decode(
            &header_data[(header_data.len() - encoded_header_length - 4)..header_data.len() - 4],
        )?;
        let header_len = header_blob_info.len();

        let header_blob_info_actual_len = header_len;
        if header_blob_info_actual_len % HEADER_BLOB_LEN != 0 {
            bail!(
                "Pack header is invalid: header blob info length ({}) is not a multiple of expected blob descriptor size ({}).",
                header_blob_info_actual_len,
                HEADER_BLOB_LEN
            );
        }

        let num_blobs = (header_len) / HEADER_BLOB_LEN;

        let mut blob_descriptors = Vec::new();
        let mut current_offset: u32 = 0;
        for i in 0..num_blobs {
            let blob_info = &header_blob_info[(i * HEADER_BLOB_LEN)..((i + 1) * HEADER_BLOB_LEN)];

            let blob_type: BlobType = blob_info[36].into();
            if matches!(blob_type, BlobType::Padding) {
                // Ignore padding blobs. They "don't exist".
                continue;
            }

            let blob_id_bytes: [u8; 32] = blob_info[0..32].try_into().unwrap();
            let id = ID::from_bytes(blob_id_bytes);

            let offset = current_offset;

            let length_bytes: [u8; 4] = blob_info[32..36].try_into().unwrap();
            let length = u32::from_le_bytes(length_bytes);
            current_offset += length;

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

        assert_eq!(packer.size(), (7 + 6 + 4));
        assert!(!packer.is_empty());

        // We cannot test with encryption enabled because the NONCE is randomized every time.
        let secure_storage = SecureStorage::build();

        let flushed_pack = packer
            .flush(&secure_storage)
            .expect("Failed to flush packer")
            .expect("Flushed pack data must be Some");

        assert_eq!(flushed_pack.data.len(), 2398);
        // Due to obfuscation we cannot make assumptions about the hash

        let header_descriptors = Packer::parse_header(&secure_storage, &flushed_pack.data)?;
        assert_eq!(flushed_pack.descriptors.len(), 64);
        assert_eq!(header_descriptors.len(), 3);
        assert_ne!(flushed_pack.descriptors, header_descriptors);

        Ok(())
    }

    #[test]
    fn test_empty_pack_flush() -> Result<()> {
        let mut packer = Packer::new();

        assert_eq!(packer.size(), 0);
        assert!(packer.is_empty());

        // We cannot test with encryption enabled because the NONCE is randomized every time.
        let secure_storage = SecureStorage::build();

        let flushed_pack_data = packer.flush(&secure_storage)?;
        assert!(flushed_pack_data.is_none());

        Ok(())
    }
}
