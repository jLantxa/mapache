use std::{
    io::Write,
    sync::{Arc, Weak, atomic::Ordering},
    thread::JoinHandle,
};

use anyhow::{Context, Result, bail};
use crossbeam_channel::Sender;
use parking_lot::Mutex;
use rand::Rng;

use crate::{
    backend::{Handle, StorageBackend, StorageHint},
    mapache::{BlobType, ContentIdType, ID, SaveID, defaults::FOOTER_BLOB_MULTIPLE},
    repository::{
        repo::Repository,
        storage::{EncodingContext, SecureStorage},
    },
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
    /// The unique ID of the blob.
    pub id: ID,
    /// The type of data stored (e.g., Data, Tree, or Padding).
    pub blob_type: BlobType,
    /// The byte offset from the start of the pack file where this blob's data begins.
    pub offset: u32,
    /// The length of the blob data as stored in the pack (after encoding/encryption).
    pub length: u32,
    /// The original, uncompressed, and unencrypted size of the blob.
    pub raw_length: u32,
}

/// A structure representing the completely processed and flushed contents of a `Packer`.
#[derive(Debug)]
pub struct FlushedPack {
    /// The full binary content of the pack, including blobs, encrypted footer, and trailer.
    pub data: Vec<u8>,
    /// The list of descriptors for all blobs contained within this pack.
    pub descriptors: Vec<PackedBlobDescriptor>,
    /// Raw size of all blobs before encoding
    pub raw_size: u64,
    /// The total size of the metadata section (encrypted footer + 4-byte trailer).
    pub meta_size: u64,
    /// The unique ID of the entire pack, generated from a hash of `data`.
    pub id: ID,
}

/// The `Packer` is an in-memory buffer designed to efficiently accumulate
/// multiple blob objects. When `flush` is called, it appends an encrypted
/// footer and releases the combined data as a single `FlushedPack`.
pub struct Packer {
    buffer: Vec<u8>,
    descriptors: Vec<PackedBlobDescriptor>,
    max_capacity: usize,
    raw_size: u64,
    secure_storage: Arc<SecureStorage>,
    encoding_context: EncodingContext,
}

impl Packer {
    /// Creates a new `Packer` with a specified initial buffer capacity.
    pub fn new(capacity: usize, secure_storage: Arc<SecureStorage>) -> Result<Self> {
        let encoding_context = secure_storage.get_encoding_context()?;

        Ok(Self {
            buffer: Vec::with_capacity(capacity),
            descriptors: Vec::new(),
            max_capacity: capacity,
            raw_size: 0,
            secure_storage,
            encoding_context,
        })
    }

    /// Returns the current size of the accumulated data in bytes.
    #[inline]
    pub fn size(&self) -> u64 {
        self.buffer.len() as u64
    }

    /// Returns true if no data have been added to the packer.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Returns the number of blobs currently staged in the packer.
    #[inline]
    pub fn num_objects(&self) -> usize {
        self.descriptors.len()
    }

    /// Appends a new blob's data to the packer and records its corresponding descriptor.
    ///
    /// # Arguments
    /// * `id` - The unique ID of the blob.
    /// * `blob_type` - The category of the blob (e.g., Data or Tree).
    /// * `encoded_data` - The already encrypted/compressed blob bytes.
    /// * `raw_size` - The original size of the data before encoding.
    pub fn add_blob(&mut self, id: ID, blob_type: BlobType, encoded_data: &[u8], raw_size: u64) {
        let offset = self.buffer.len() as u32;
        let length = encoded_data.len() as u32;

        self.raw_size += raw_size;
        self.buffer.extend_from_slice(encoded_data);

        self.descriptors.push(PackedBlobDescriptor {
            id,
            blob_type,
            offset,
            length,
            raw_length: raw_size as u32,
        });
    }

    /// Finalizes the pack, returning the combined data and descriptors.
    ///
    /// This process appends an encrypted footer and a 4-byte footer length trailer.
    /// If the packer is empty, it returns `Ok(None)`.
    pub fn flush(&mut self) -> Result<Option<FlushedPack>> {
        if self.buffer.is_empty() {
            return Ok(None);
        }

        let mut data = std::mem::replace(&mut self.buffer, Vec::with_capacity(self.max_capacity));
        let mut descriptors = std::mem::take(&mut self.descriptors);
        let raw_size = std::mem::replace(&mut self.raw_size, 0);

        let footer = Self::generate_footer(&mut descriptors);

        let encoded_footer = self
            .secure_storage
            .encode_managed(&mut self.encoding_context, &footer)?;
        let footer_len_bytes = (encoded_footer.len() as u32).to_le_bytes();

        data.extend_from_slice(&encoded_footer);
        data.extend_from_slice(&footer_len_bytes);

        let hash = utils::calculate_hash(&data);

        Ok(Some(FlushedPack {
            data,
            descriptors,
            raw_size,
            meta_size: (encoded_footer.len() + 4) as u64,
            id: ID::from_bytes(hash),
        }))
    }

    /// Generates the raw binary footer from a list of descriptors.
    ///
    /// It automatically inserts random "Padding" blobs to ensure the footer
    /// length is a multiple of `FOOTER_BLOB_MULTIPLE`, hindering size analysis.
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

    /// Locates and parses the footer of a pack file from the backend.
    ///
    /// This function performs two reads: one for the trailing 4-byte length
    /// and a second to retrieve the encrypted footer data.
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

    /// Decodes a slice of footer bytes into a list of descriptors.
    ///
    /// This function validates the trailer length, decrypts the footer,
    /// and filters out any "Padding" blobs used during the packing process.
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

/// A background worker orchestrator that manages multiple `Packer` instances
/// and parallelizes the uploading of finished packs.
pub(crate) struct PackSaver {
    /// Receiver for incoming blob storage requests.
    rx: crossbeam_channel::Receiver<PackSaverRequest>,
    /// Weak reference to the repository for indexing and stats updates.
    repo: Weak<Repository>,
    /// Packer used for standard data blobs.
    data_packer: Packer,
    /// Packer used for directory tree metadata.
    tree_packer: Packer,
    /// Threshold size at which a packer will be automatically flushed.
    max_packer_size: u64,

    /// Channel to send completed packs to worker threads.
    worker_tx: Sender<(Vec<u8>, ID, BlobType)>,
    /// Handle to the worker management thread.
    worker_handle: JoinHandle<Result<()>>,
}

/// Types of requests the `PackSaver` can handle.
pub(crate) enum PackSaverRequest {
    /// Request to save a specific blob into an appropriate pack.
    SaveBlob {
        id: ID,
        blob_type: BlobType,
        data: Vec<u8>,
        raw_length: u64,
    },
}

impl PackSaver {
    /// Creates a new `PackSaver` and initializes the background worker pool.
    ///
    /// # Arguments
    /// * `upload_fn` - A closure or function that defines how a finalized pack is written to storage.
    pub(crate) fn new(
        rx: crossbeam_channel::Receiver<PackSaverRequest>,
        repo_weak: Weak<Repository>,
        secure_storage: Arc<SecureStorage>,
        max_packer_size: u64,
        write_concurrency: usize,
    ) -> Result<Self> {
        let (worker_tx, worker_rx) =
            crossbeam_channel::bounded::<(Vec<u8>, ID, BlobType)>(write_concurrency);
        let first_err = Arc::new(Mutex::new(None));
        let worker_repo_weak_clone = repo_weak.clone();

        let worker_handle = std::thread::spawn(move || -> Result<()> {
            let mut workers = Vec::with_capacity(write_concurrency);
            for _ in 0..write_concurrency {
                let rx = worker_rx.clone();
                let err_ptr = Arc::clone(&first_err);

                let repo_weak_clone = worker_repo_weak_clone.clone();

                workers.push(std::thread::spawn(move || -> Result<()> {
                    while let Ok((data, id, b_type)) = rx.recv() {
                        if err_ptr.lock().is_some() {
                            return Ok(());
                        }

                        let repo = repo_weak_clone.upgrade().context("Repository dropped")?;
                        let save_id = SaveID::WithID(id);

                        if let Err(e) = repo.save_file(
                            &save_id,
                            &data,
                            StorageHint {
                                file_type: ContentIdType::Pack,
                                is_metadata: b_type == BlobType::Tree,
                            },
                            None,
                        ) {
                            let mut lock = err_ptr.lock();
                            if lock.is_none() {
                                *lock = Some(e.context("Upload failed"));
                            }
                            return Ok(());
                        }
                    }

                    Ok(())
                }));
            }
            for t in workers {
                let _ = t.join();
            }
            first_err.lock().take().map_or(Ok(()), Err)
        });

        Ok(Self {
            rx,
            repo: repo_weak,
            data_packer: Packer::new(max_packer_size as usize, secure_storage.clone())?,
            tree_packer: Packer::new(max_packer_size as usize, secure_storage)?,
            max_packer_size,
            worker_tx,
            worker_handle,
        })
    }

    /// Starts the main event loop, processing blob requests until the channel is closed.
    pub fn run(mut self) -> Result<()> {
        while let Ok(request) = self.rx.recv() {
            match request {
                PackSaverRequest::SaveBlob {
                    id,
                    blob_type,
                    data,
                    raw_length,
                } => {
                    let packer = match blob_type {
                        BlobType::Data => &mut self.data_packer,
                        BlobType::Tree => &mut self.tree_packer,
                        _ => continue,
                    };

                    packer.add_blob(id, blob_type, &data, raw_length);

                    if packer.size() >= self.max_packer_size {
                        self.flush_and_dispatch(blob_type)?;
                    }
                }
            }
        }

        // Final flushes
        self.flush_and_dispatch(BlobType::Data)?;
        self.flush_and_dispatch(BlobType::Tree)?;

        // Shutdown workers
        drop(self.worker_tx);
        self.worker_handle
            .join()
            .map_err(|_| anyhow::anyhow!("Worker panicked"))?
    }

    /// Flushes the specified packer and dispatches the data to the worker pool.
    /// It also updates the repository index and global statistics.
    fn flush_and_dispatch(&mut self, blob_type: BlobType) -> Result<()> {
        let packer = match blob_type {
            BlobType::Data => &mut self.data_packer,
            BlobType::Tree => &mut self.tree_packer,
            _ => return Ok(()),
        };

        if let Some(flushed) = packer.flush()? {
            let num_blobs = flushed.descriptors.len() as u64;
            let pack_id = flushed.id;
            let data_len = flushed.data.len() as u64;
            let meta_len = flushed.meta_size;

            // Dispatch to internal pool
            self.worker_tx
                .send((flushed.data, pack_id, blob_type))
                .context("Failed to dispatch pack to workers")?;

            // Update Repository index and stats
            let repo = self.repo.upgrade().context("Repository dropped")?;
            repo.index()
                .add_pack(&repo, &pack_id, flushed.descriptors)?;

            match blob_type {
                BlobType::Data => {
                    repo.stats
                        .raw_bytes
                        .fetch_add(flushed.raw_size, Ordering::Relaxed);
                    repo.stats
                        .encoded_bytes
                        .fetch_add(data_len, Ordering::Relaxed);
                    repo.stats
                        .meta_raw_bytes
                        .fetch_add(meta_len, Ordering::Relaxed);
                    repo.stats
                        .meta_encoded_bytes
                        .fetch_add(meta_len, Ordering::Relaxed);
                    repo.stats
                        .data_blobs
                        .fetch_add(num_blobs, Ordering::Relaxed);
                }
                BlobType::Tree => {
                    repo.stats
                        .meta_raw_bytes
                        .fetch_add(flushed.raw_size + meta_len, Ordering::Relaxed);
                    repo.stats
                        .meta_encoded_bytes
                        .fetch_add(meta_len, Ordering::Relaxed);
                    repo.stats
                        .meta_blobs
                        .fetch_add(num_blobs, Ordering::Relaxed);
                }
                _ => {}
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::repository::keys::KeyManager;

    use super::*;

    fn add_blob(packer: &mut Packer, data: &[u8], secure_storage: &SecureStorage) -> Result<()> {
        let raw_size = data.len() as u64;
        let encoded_data = secure_storage.encode(&data)?;

        packer.add_blob(
            ID::from_content(&encoded_data),
            BlobType::Data,
            &encoded_data,
            raw_size,
        );

        Ok(())
    }

    #[test]
    fn test_pack_flush() -> Result<()> {
        let key = KeyManager::generate_new_master_key();
        let secure_storage = Arc::new(
            SecureStorage::new()
                .with_compression(zstd::DEFAULT_COMPRESSION_LEVEL)
                .with_key(&key),
        );

        let mut packer = Packer::new(0, secure_storage.clone())?;

        let blobs = vec![b"mapache".to_vec(), b"backup".to_vec(), b"rust".to_vec()];

        add_blob(&mut packer, &blobs[0], &secure_storage)?;
        add_blob(&mut packer, &blobs[1], &secure_storage)?;
        add_blob(&mut packer, &blobs[2], &secure_storage)?;

        assert_eq!(packer.size(), 128);
        assert!(!packer.is_empty());

        let flushed_pack = packer
            .flush()
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
        // We cannot test with encryption enabled because the NONCE is randomized every time.
        let secure_storage = Arc::new(SecureStorage::new());

        let mut packer = Packer::new(0, secure_storage)?;

        assert_eq!(packer.size(), 0);
        assert!(packer.is_empty());

        let flushed_pack_data = packer.flush()?;
        assert!(flushed_pack_data.is_none());

        Ok(())
    }
}
