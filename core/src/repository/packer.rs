use std::{io::Write, thread::JoinHandle};

use anyhow::{Context, Result, bail};
use crossbeam_channel::Sender;
use rand::Rng;
use rayon::iter::{ParallelBridge, ParallelIterator};

use crate::{
    backend::{Handle, StorageBackend},
    mapache::{BlobType, ContentIdType, ID, SaveID, defaults::FOOTER_BLOB_MULTIPLE},
    repository::{repo::Repository, storage::SecureStorage},
    utils,
};

//   Pack footer format:
//
//   The pack footer consists of a variable-length list of metadata blob entries, each 41 bytes
//   long, followed by a fixed-size trailer. Each entry contains an ID (32 bytes), the
//   blob type (u8), and both the encoded length and raw length (u32) of the associated
//   data blob. The trailer is a single u32 field which stores the total length of the
//   entire pack footer, allowing a parser to efficiently skip directly to the file's data section.
//   All data except the footer length field is encrypted.
//
//   ┌────────┬────────┬─────┬────────┬─────────────────────┐
//   │ Blob 1 │ Blob 2 │ ... │ Blob N │ Footer length (u32) │
//   └────────┴────────┴─────┴────────┴─────────────────────┘
//
//   ┌──────────────────────────────────────────┐
//   │     Pack footer blob entry (41 bytes)    │
//   │────────────────────────┬────────┬────────│
//   │ Field                  │  Size  │ Offset │
//   │────────────────────────┼────────┼────────│
//   │ ID (256-bit raw hash)  │   32   │   0    │  Hash of the raw blob data
//   │────────────────────────┼────────┼────────│
//   │ Blob type (u8)         │    1   │   32   │
//   │────────────────────────┼────────┼────────│
//   │ Encoded length (u32)   │    4   │   33   │  Length of the encoded blob (in-pack)
//   │────────────────────────┼────────┼────────│
//   │ Raw length (u32)       │    4   │   37   │
//   └────────────────────────┴────────┴────────┘
//     ^ (41 bytes)
//

pub const FOOTER_ID_OFFSET: usize = 0;
pub const FOOTER_BLOB_TYPE_OFFSET: usize = 32;
pub const FOOTER_BLOB_LENGTH_OFFSET: usize = 33;
pub const FOOTER_BLOB_RAW_LENGTH_OFFSET: usize = 37;
pub const FOOTER_BLOB_LEN: usize = 41;

/// Describes a single blob's location and size within a packed file.
/// This metadata is crucial for retrieving individual blobs from a pack.
#[derive(Debug, Clone, PartialEq)]
pub struct PackedBlobDescriptor {
    pub id: ID,
    pub blob_type: BlobType,
    pub offset: u32,
    pub length: u32,
    pub raw_length: u32,
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
    blobs: Vec<(ID, BlobType, Vec<u8>, u64)>, // (ID, type, encoded_data, raw_length)
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
    pub fn add_blob(
        &mut self,
        id: ID,
        blob_type: BlobType,
        blob_data: Vec<u8>,
        raw_size: u64,
        encoded_size: u64,
    ) {
        self.size += encoded_size;
        self.blobs.push((id, blob_type, blob_data, raw_size));
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
        let total_data_size: usize = blobs.iter().map(|(_, _, d, _)| d.len()).sum();
        let mut data = Vec::with_capacity(total_data_size);
        let mut descriptors = Vec::with_capacity(blobs.len());

        for blob in blobs {
            let mut blob_data = blob.2;
            let length = blob_data.len() as u32;
            descriptors.push(PackedBlobDescriptor {
                id: blob.0,
                blob_type: blob.1,
                offset,
                length,
                raw_length: blob.3 as u32,
            });
            data.append(&mut blob_data);
            offset += length;
        }

        let footer = Self::generate_footer(&mut descriptors);
        let mut footer = secure_storage.encode(&footer)?;
        let mut footer_length_bytes = (footer.len() as u32).to_le_bytes().to_vec();
        footer.append(&mut footer_length_bytes);
        let meta_size: u64 = footer.len() as u64;
        data.append(&mut footer);

        let hash = utils::calculate_hash(&data);

        Ok(Some(FlushedPack {
            data,
            descriptors,
            meta_size,
            id: ID::from_bytes(hash),
        }))
    }

    /// Generates a pack footer given a vector of blob descriptors.
    fn generate_footer(descriptors: &mut Vec<PackedBlobDescriptor>) -> Vec<u8> {
        // blob[id (256 bits), lenght (u32), type (u8)] + footer length (u32);
        let mut pack_footer = Vec::with_capacity(FOOTER_BLOB_LEN * descriptors.len());
        let mut cursor = std::io::Cursor::new(&mut pack_footer);
        let mut rng = rand::rng();

        if !descriptors.len().is_multiple_of(FOOTER_BLOB_MULTIPLE) {
            let num_padding_blobs =
                FOOTER_BLOB_MULTIPLE - (descriptors.len() % FOOTER_BLOB_MULTIPLE);
            for _ in 0..num_padding_blobs {
                // Add random fields so the compressor cannot reduce the padding size.
                descriptors.push(PackedBlobDescriptor {
                    id: ID::new_random(),
                    blob_type: BlobType::Padding,
                    offset: rng.random(),
                    length: rng.random(),
                    raw_length: rng.random(),
                });
            }
        }

        for blob in descriptors {
            cursor.write_all(blob.id.as_slice()).unwrap();
            cursor
                .write_all(&(blob.blob_type as u8).to_le_bytes())
                .unwrap();
            cursor.write_all(&blob.length.to_le_bytes()).unwrap();
            cursor.write_all(&blob.raw_length.to_le_bytes()).unwrap();
        }
        pack_footer
    }

    /// Parses the footer for a pack with a given ID. This function only reads the footer bytes from
    /// the pack file using the seek read trait function from the backend.
    pub fn parse_pack_footer(
        repo: &Repository,
        backend: &dyn StorageBackend,
        secure_storage: &SecureStorage,
        pack_id: &ID,
    ) -> Result<Vec<PackedBlobDescriptor>> {
        let (_id, pack_path) = repo.find(ContentIdType::Pack, &pack_id.to_hex())?;

        // We don't know a priori if this pack contains metadata, so we cannot use a StorageHint.
        let handle = Handle::new(&pack_path);
        let footer_length_bytes: [u8; 4] = backend.read(&handle, -4, 4)?.as_slice().try_into()?;
        let encoded_footer_length = u32::from_le_bytes(footer_length_bytes) as usize;

        let footer_data = backend.read(
            &handle,
            -(4 + encoded_footer_length as isize),
            4 + encoded_footer_length,
        )?;

        Self::parse_footer(secure_storage, &footer_data)
    }

    /// Parses a pack footer data from a sliice of bytes. `footer_data` must contain the footer and
    /// the length field. Since this function reads the length field, other bytes before the footer
    /// can be still passed and they will be ignored.
    fn parse_footer(
        secure_storage: &SecureStorage,
        footer_data: &[u8],
    ) -> Result<Vec<PackedBlobDescriptor>> {
        if footer_data.len() < 4 {
            bail!(
                "Pack footer is invalid: data too short for footer length (got {} bytes, need at least 4).",
                footer_data.len()
            );
        }

        let footer_length_bytes: [u8; 4] = footer_data[(footer_data.len() - 4)..]
            .try_into()
            .context("Could not read pack footer length bytes.")?;
        let encoded_footer_length = u32::from_le_bytes(footer_length_bytes) as usize;

        if footer_data.len() < encoded_footer_length {
            bail!(
                "Pack footer is invalid: declared footer_length ({}) exceeds total data length ({}).",
                encoded_footer_length,
                footer_data.len()
            );
        }

        let footer_blob_info = secure_storage.decode(
            &footer_data[(footer_data.len() - encoded_footer_length - 4)..footer_data.len() - 4],
        )?;
        let footer_len = footer_blob_info.len();

        let footer_blob_info_actual_len = footer_len;
        if footer_blob_info_actual_len % FOOTER_BLOB_LEN != 0 {
            bail!(
                "Pack footer is invalid: footer blob info length ({footer_blob_info_actual_len}) is not a multiple of expected blob descriptor size ({FOOTER_BLOB_LEN})."
            );
        }

        let num_blobs = (footer_len) / FOOTER_BLOB_LEN;

        let mut blob_descriptors = Vec::new();
        let mut offset: u32 = 0;
        for i in 0..num_blobs {
            let blob_info = &footer_blob_info[(i * FOOTER_BLOB_LEN)..((i + 1) * FOOTER_BLOB_LEN)];

            let blob_type: BlobType = blob_info[FOOTER_BLOB_TYPE_OFFSET].into();
            if matches!(blob_type, BlobType::Padding) {
                // Ignore padding blobs. They "don't exist".
                continue;
            }

            let blob_id_bytes: [u8; 32] = blob_info[FOOTER_ID_OFFSET..FOOTER_ID_OFFSET + 32]
                .try_into()
                .unwrap();
            let id = ID::from_bytes(blob_id_bytes);

            let length_bytes: [u8; 4] = blob_info
                [FOOTER_BLOB_LENGTH_OFFSET..FOOTER_BLOB_LENGTH_OFFSET + 4]
                .try_into()
                .unwrap();
            let length = u32::from_le_bytes(length_bytes);

            let raw_length_bytes: [u8; 4] = blob_info
                [FOOTER_BLOB_RAW_LENGTH_OFFSET..FOOTER_BLOB_RAW_LENGTH_OFFSET + 4]
                .try_into()
                .unwrap();
            let raw_length = u32::from_le_bytes(raw_length_bytes);

            let blob_descriptor = PackedBlobDescriptor {
                id,
                blob_type,
                offset,
                length,
                raw_length,
            };
            blob_descriptors.push(blob_descriptor);

            offset += length;
        }

        Ok(blob_descriptors)
    }
}

/// PackSaver is a dedicated worker thread manager responsible for asynchronously writing
/// fully constructed pack files (`FlushedPack` data) to the repository's storage backend.
pub struct PackSaver {
    tx: Sender<(Vec<u8>, ID, BlobType)>,
    join_handle: JoinHandle<()>,
}

impl PackSaver {
    /// Creates a new pack saver.
    pub fn new<F>(concurrency: usize, queue_fn: F) -> Self
    where
        F: Fn(Vec<u8>, ID, BlobType) + Send + Sync + 'static,
    {
        let (tx, rx) = crossbeam_channel::bounded(concurrency);
        let join_handle = std::thread::spawn(move || {
            let pool = rayon::ThreadPoolBuilder::new()
                .num_threads(concurrency)
                .build()
                .expect("Failed to build thread pool");

            pool.install(|| {
                rx.into_iter()
                    .par_bridge()
                    .for_each(|(data, id, blob_type)| {
                        queue_fn(data, id, blob_type);
                    });
            });
        });

        Self { tx, join_handle }
    }

    /// Queue a pack into the pack saver for upload.
    pub fn save_pack(
        &self,
        packer_data: Vec<u8>,
        blob_type: BlobType,
        save_id: SaveID,
    ) -> Result<ID> {
        let pack_id = match save_id {
            SaveID::CalculateID => ID::from_content(&packer_data),
            SaveID::WithID(id) => id,
        };

        self.tx
            .send((packer_data, pack_id, blob_type))
            .context("Failed to send pack data to PackSaver channel")?;

        Ok(pack_id)
    }

    /// Finalize the pack saver worker.
    pub fn finish(self) {
        drop(self.tx);
        self.join_handle.join().expect("PackSaver thread panicked");
    }
}

#[cfg(test)]
mod tests {
    use crate::repository::keys::KeyManager;

    use super::*;

    fn add_blob(packer: &mut Packer, data: &[u8], secure_storage: &SecureStorage) -> Result<()> {
        let raw_size = data.len() as u64;
        let encoded_data = secure_storage.encode(&data)?;
        let encoded_size = encoded_data.len() as u64;

        packer.add_blob(
            ID::from_content(&encoded_data),
            BlobType::Data,
            encoded_data,
            raw_size,
            encoded_size,
        );

        Ok(())
    }

    #[test]
    fn test_pack_flush() -> Result<()> {
        let mut packer = Packer::new();

        let key = KeyManager::generate_new_master_key();
        let secure_storage = SecureStorage::build()
            .with_compression(zstd::DEFAULT_COMPRESSION_LEVEL)
            .with_key(&key);

        let blobs = vec![b"mapache".to_vec(), b"backup".to_vec(), b"rust".to_vec()];

        add_blob(&mut packer, &blobs[0], &secure_storage)?;
        add_blob(&mut packer, &blobs[1], &secure_storage)?;
        add_blob(&mut packer, &blobs[2], &secure_storage)?;

        assert_eq!(packer.size(), 128);
        assert!(!packer.is_empty());

        let flushed_pack = packer
            .flush(&secure_storage)
            .expect("Failed to flush packer")
            .expect("Flushed pack data must be Some");

        assert_eq!(flushed_pack.data.len(), 2794);

        let footer_descriptors = Packer::parse_footer(&secure_storage, &flushed_pack.data)?;
        assert_eq!(flushed_pack.descriptors.len(), 64);
        assert_eq!(footer_descriptors.len(), 3);
        assert_ne!(flushed_pack.descriptors, footer_descriptors);

        // Due to obfuscation we cannot make assumptions about the hash, but we
        // can decode the content of every blob.
        for (i, descriptor) in footer_descriptors.iter().enumerate() {
            let offset = descriptor.offset as usize;
            let len = descriptor.length as usize;
            let data = &flushed_pack.data[offset..(offset + len)];
            let decoded_data = secure_storage.decode(data)?;

            assert_eq!(blobs[i], decoded_data);
        }

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
