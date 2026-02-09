use std::{
    io::Write,
    sync::{Arc, Weak, atomic::Ordering},
    thread::JoinHandle,
};

use anyhow::{Context, Result, bail};
use crossbeam_channel::{Receiver, Sender};
use parking_lot::Mutex;
use rand::RngExt;

use crate::{
    backend::{Handle, StorageBackend, StorageHint},
    mapache::{BlobType, ContentIdType, ID, SaveID, defaults::FOOTER_BLOB_MULTIPLE},
    repository::{
        repo::{Repository, SizePair},
        storage::{EncodingContext, SecureStorage},
    },
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
#[derive(Debug, Clone, PartialEq)]
pub struct PackedBlobDescriptor {
    pub id: ID,
    pub blob_type: BlobType,
    pub offset: u32,
    pub length: u32,
    pub raw_length: u32,
}

/// A structure representing the completely processed and flushed contents of a `Packer`.
#[derive(Debug)]
pub struct FlushedPack {
    pub id: ID,
    pub data: Vec<u8>,
    pub descriptors: Vec<PackedBlobDescriptor>,
    pub raw_size: u64,
    pub meta_size: u64,
}

/// Internal struct to pass summary data back from the heavy-lifting function
struct PackFinalizationResult {
    id: ID,
    data: Vec<u8>,
    descriptors: Vec<PackedBlobDescriptor>,
    raw_size: u64,
    meta_size: u64,
    encoded_size: u64,
}

/// The `Packer` is an in-memory buffer designed to efficiently accumulate multiple blob objects.
///
/// Note: This struct is designed to be REUSED. Do not drop it.
/// Pass it back to the `PackSaver` via the empty channel to recycle the internal heap allocation.
pub struct Packer {
    buffer: Vec<u8>,
    descriptors: Vec<PackedBlobDescriptor>,
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

    /// Appends a new blob's data to the packer.
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

    /// Performs the CPU-intensive work of finalizing the pack.
    ///
    /// This includes:
    /// 1. Generating the footer.
    /// 2. Encrypting the footer.
    /// 3. Hashing the entire 20MB+ buffer (BLAKE3/SHA256).
    ///
    /// Returns the data and metadata required for upload and indexing.
    /// The internal buffer is essentially "stolen" by the result and must be
    /// returned via `recycle_buffer` later.
    fn finalize_and_extract(&mut self) -> Result<Option<PackFinalizationResult>> {
        if self.buffer.is_empty() {
            return Ok(None);
        }

        // Take descriptors out to process them
        let mut descriptors = std::mem::take(&mut self.descriptors);
        let raw_size = self.raw_size;

        let footer = Self::generate_footer(&mut descriptors);

        let encoded_footer = self
            .secure_storage
            .encode_managed(&mut self.encoding_context, &footer)?;
        let footer_len_bytes = (encoded_footer.len() as u32).to_le_bytes();

        self.buffer.extend_from_slice(&encoded_footer);
        self.buffer.extend_from_slice(&footer_len_bytes);

        let id = ID::from_content(&self.buffer);

        let encoded_size = self.buffer.len() as u64;
        let meta_size = (encoded_footer.len() + 4) as u64;

        // Steal the buffer to avoid copying.
        // We will put a new empty capacity vector here temporarily,
        // but the intention is for the caller to return the buffer later.
        let data = std::mem::take(&mut self.buffer);

        // Reset state
        self.raw_size = 0;

        Ok(Some(PackFinalizationResult {
            id,
            data,
            descriptors,
            raw_size,
            encoded_size,
            meta_size,
        }))
    }

    /// Restore the internal buffer from a processed operation.
    /// This allows us to reuse the heap allocation (zero-copy / zero-alloc).
    pub fn recycle_buffer(&mut self, mut old_buffer: Vec<u8>) {
        old_buffer.clear();
        self.buffer = old_buffer;
        // Ensure descriptors are clear (they should be from finalize, but just in case)
        self.descriptors.clear();
        self.raw_size = 0;
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
            bail!("Pack footer too short");
        }
        let footer_length_bytes: [u8; 4] = footer_data[(footer_data.len() - 4)..]
            .try_into()
            .context("Could not read footer length")?;
        let encoded_footer_length = u32::from_le_bytes(footer_length_bytes) as usize;

        let footer_blob_info = secure_storage.decode(
            &footer_data[(footer_data.len() - encoded_footer_length - 4)..footer_data.len() - 4],
        )?;
        let num_blobs = footer_blob_info.len() / FOOTER_BLOB_LEN;
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
            let length = u32::from_le_bytes(
                blob_info[FOOTER_BLOB_LENGTH_OFFSET..FOOTER_BLOB_LENGTH_OFFSET + 4]
                    .try_into()
                    .unwrap(),
            );
            let raw_length = u32::from_le_bytes(
                blob_info[FOOTER_BLOB_RAW_LENGTH_OFFSET..FOOTER_BLOB_RAW_LENGTH_OFFSET + 4]
                    .try_into()
                    .unwrap(),
            );

            blob_descriptors.push(PackedBlobDescriptor {
                id,
                blob_type,
                offset,
                length,
                raw_length,
            });
            offset += length;
        }

        Ok(blob_descriptors)
    }
}

/// A background worker orchestrator that manages multiple `Packer` instances
/// and parallelizes the uploading of finished packs.
pub(crate) struct PackSaver {
    /// Receiver for incoming blob storage requests.
    rx: Receiver<PackSaverRequest>,

    // Internal state
    data_packer: Packer,
    tree_packer: Packer,
    max_packer_size: u64,

    // WORKER POOL CHANNELS
    // We send full packers to be processed
    full_packer_tx: Sender<(Packer, BlobType)>,
    // We receive empty, recycled packers to avoid allocation
    empty_packer_rx: Receiver<Packer>,

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
    pub(crate) fn new(
        rx: Receiver<PackSaverRequest>,
        repo_weak: Weak<Repository>,
        secure_storage: Arc<SecureStorage>,
        max_packer_size: u64,
        num_packers: usize,
    ) -> Result<Self> {
        // Channels for the worker pool
        // 'full' carries work to do. 'empty' carries recycled buffers.
        let (full_tx, full_rx) = crossbeam_channel::bounded::<(Packer, BlobType)>(num_packers);
        let (empty_tx, empty_rx) = crossbeam_channel::bounded::<Packer>(num_packers + 2);

        // Pre-fill the empty pool.
        // We need 'num_packers' for the workers + 2 for the active slots (data/tree) in the main thread.
        for _ in 0..(num_packers + 2) {
            let p = Packer::new(max_packer_size as usize, secure_storage.clone())?;
            empty_tx.send(p).unwrap();
        }

        let first_err = Arc::new(Mutex::new(None));
        let worker_repo_weak = repo_weak.clone();

        // Spawn Coordinator Thread for Workers
        let worker_handle = std::thread::spawn(move || -> Result<()> {
            let mut worker_threads = Vec::with_capacity(num_packers);

            for _ in 0..num_packers {
                let rx = full_rx.clone();
                let tx = empty_tx.clone(); // Path to return empty packers
                let err_ptr = Arc::clone(&first_err);
                let repo_ptr = worker_repo_weak.clone();

                worker_threads.push(std::thread::spawn(move || -> Result<()> {
                    while let Ok((mut packer, blob_type)) = rx.recv() {
                        if err_ptr.lock().is_some() {
                            return Ok(());
                        }

                        let result = match packer.finalize_and_extract() {
                            Ok(Some(res)) => res,
                            Ok(None) => {
                                let _ = tx.send(packer);
                                continue;
                            }
                            Err(e) => {
                                *err_ptr.lock() = Some(e);
                                return Ok(());
                            }
                        };

                        let stats_raw = result.raw_size;
                        let stats_enc = result.encoded_size;
                        let stats_meta = result.meta_size;
                        let stats_blobs = result.descriptors.len() as u64;

                        let repo = match repo_ptr.upgrade() {
                            Some(r) => r,
                            None => return Ok(()),
                        };

                        let save_id = SaveID::WithID(result.id);

                        // Upload
                        if let Err(e) = repo.save_file(
                            &save_id,
                            &result.data,
                            StorageHint {
                                file_type: ContentIdType::Pack,
                                is_metadata: blob_type == BlobType::Tree,
                            },
                            None,
                        ) {
                            *err_ptr.lock() = Some(e.context("Upload failed"));
                            return Ok(());
                        }

                        // Index Update
                        let index_size =
                            match repo.index().add_pack(&repo, &result.id, result.descriptors) {
                                Ok(size) => size,
                                Err(e) => {
                                    *err_ptr.lock() = Some(e.context("Index update failed"));
                                    return Ok(());
                                }
                            };

                        Self::update_stats(
                            &repo,
                            stats_raw,
                            stats_enc,
                            stats_meta,
                            stats_blobs,
                            index_size,
                            blob_type,
                        );

                        packer.recycle_buffer(result.data);
                        if tx.send(packer).is_err() {
                            return Ok(());
                        }
                    }
                    Ok(())
                }));
            }

            for t in worker_threads {
                let _ = t.join();
            }
            first_err.lock().take().map_or(Ok(()), Err)
        });

        // Get initial packers for the main thread
        let data_packer = empty_rx
            .recv()
            .context("Failed to initialize data packer")?;
        let tree_packer = empty_rx
            .recv()
            .context("Failed to initialize tree packer")?;

        Ok(Self {
            rx,
            data_packer,
            tree_packer,
            max_packer_size,
            full_packer_tx: full_tx,
            empty_packer_rx: empty_rx,
            worker_handle,
        })
    }

    /// Starts the main event loop.
    /// This loop is now extremely lightweight. It purely moves pointers.
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
                        self.dispatch_packer(blob_type)?;
                    }
                }
            }
        }

        // Final flushes
        self.dispatch_packer(BlobType::Data)?;
        self.dispatch_packer(BlobType::Tree)?;

        // Close channel to signal workers to finish
        drop(self.full_packer_tx);

        self.worker_handle
            .join()
            .map_err(|_| anyhow::anyhow!("Worker panicked"))?
    }

    /// Swaps the current packer with a fresh one from the recycle pool
    /// and sends the full one to the workers.
    fn dispatch_packer(&mut self, blob_type: BlobType) -> Result<()> {
        let packer_ref = match blob_type {
            BlobType::Data => &mut self.data_packer,
            BlobType::Tree => &mut self.tree_packer,
            _ => return Ok(()),
        };

        if packer_ref.is_empty() {
            return Ok(());
        }

        // Get an empty packer from the pool (blocks if workers are too slow, creating backpressure)
        let mut new_packer = self
            .empty_packer_rx
            .recv()
            .context("Worker pool died or no free packers")?;

        std::mem::swap(packer_ref, &mut new_packer);

        // Send the full packer to workers
        self.full_packer_tx
            .send((new_packer, blob_type))
            .context("Failed to dispatch pack to workers")?;

        Ok(())
    }

    fn update_stats(
        repo: &Repository,
        raw_size: u64,
        encoded_size: u64,
        meta_size: u64,
        num_blobs: u64,
        index: SizePair,
        blob_type: BlobType,
    ) {
        match blob_type {
            BlobType::Data => {
                repo.stats.raw_bytes.fetch_add(raw_size, Ordering::Relaxed);
                repo.stats
                    .encoded_bytes
                    .fetch_add(encoded_size, Ordering::Relaxed);
                repo.stats
                    .meta_raw_bytes
                    .fetch_add(meta_size, Ordering::Relaxed);
                repo.stats
                    .meta_encoded_bytes
                    .fetch_add(meta_size, Ordering::Relaxed);
                repo.stats
                    .data_blobs
                    .fetch_add(num_blobs, Ordering::Relaxed);
            }
            BlobType::Tree => {
                // For Tree packs, the "data" itself is metadata
                repo.stats
                    .meta_raw_bytes
                    .fetch_add(raw_size + meta_size, Ordering::Relaxed);
                repo.stats
                    .meta_encoded_bytes
                    .fetch_add(meta_size, Ordering::Relaxed);
                repo.stats
                    .meta_blobs
                    .fetch_add(num_blobs, Ordering::Relaxed);
            }
            _ => {}
        }

        repo.stats
            .index_raw_bytes
            .fetch_add(index.raw, Ordering::Relaxed);
        repo.stats
            .index_meta_bytes
            .fetch_add(index.encoded, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{mapache::defaults::DEFAULT_COMPRESSION, repository::keys::KeyManager};
    use std::sync::Arc;

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
    fn test_pack_finalize_and_recycle() -> Result<()> {
        let key = KeyManager::generate_new_master_key();
        let secure_storage = Arc::new(
            SecureStorage::new()
                .with_compression(DEFAULT_COMPRESSION.to_level())
                .with_key(&key),
        );

        let mut packer = Packer::new(1024, secure_storage.clone())?;
        let blobs = vec![b"test1".to_vec(), b"test2".to_vec()];

        // Add Data
        add_blob(&mut packer, &blobs[0], &secure_storage)?;
        add_blob(&mut packer, &blobs[1], &secure_storage)?;

        assert!(!packer.is_empty());

        // Finalize (Simulating worker action)
        let result = packer
            .finalize_and_extract()
            .expect("Finalize failed")
            .expect("Should return result");

        // CHANGE THIS:
        // result.descriptors includes the padding blobs added for obfuscation
        assert_eq!(result.descriptors.len(), 64);

        // But we can verify that the first 2 are our actual data
        assert_eq!(result.descriptors[0].blob_type, BlobType::Data);
        assert_eq!(result.descriptors[1].blob_type, BlobType::Data);

        assert!(result.data.len() > 0);

        // If you want to verify the "round-trip" through the parser:
        let parsed_descriptors = Packer::parse_footer(&secure_storage, &result.data)?;
        // The parser filters out Padding, so we should get exactly 2 back
        assert_eq!(parsed_descriptors.len(), 2);

        Ok(())
    }
}
