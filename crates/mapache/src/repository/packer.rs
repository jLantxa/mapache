use std::{
    sync::{
        Arc, Weak,
        atomic::{AtomicU64, Ordering},
    },
    thread::JoinHandle,
};

use crossbeam_channel::{Receiver, Sender};
use parking_lot::Mutex;
use rand::{RngExt, rng};

use crate::{
    backend::{Handle, StorageBackend, StorageHint},
    common::{
        BlobType, ContentIdType, ID, SaveID,
        defaults::FOOTER_BLOB_MULTIPLE,
        error::{MapacheError, Result},
    },
    repository::{
        repo::{Repository, SizePair},
        storage::{EncodingContext, SecureStorage},
    },
    utils::binary::{get_array, get_u8, get_u32, put_bytes, put_u8, put_u32},
};

//   Pack footer format:
//
//   The pack footer consists of a variable-length list of metadata blob entries, each
//   `FOOTER_BLOB_LEN` (41) bytes long, followed by a fixed-size trailer. Each entry
//   contains an ID (32 bytes), a blob type byte (u8) whose high bit doubles as the
//   compression marker (0 = stored uncompressed, 1 = zstd-compressed), and both the
//   encoded length and raw length (u32) of the associated data blob. The trailer is a
//   single u32 field which stores the total length of the entire pack footer, allowing a
//   parser to efficiently skip directly to the file's data section. All data except the
//   footer length field is zstd-compressed then encrypted.
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
//   │ Type + Compression     │    1   │   32   │  Low 7 bits: blob type; high bit:
//   │ (u8)                   │        │        │  0 = uncompressed, 1 = compressed (v2+ marker)
//   │────────────────────────┼────────┼────────│
//   │ Encoded length (u32)   │    4   │   33   │  Length of the encoded blob (in-pack)
//   │────────────────────────┼────────┼────────│
//   │ Raw length (u32)       │    4   │   37   │
//   └────────────────────────┴────────┴────────┘
//     ^ (41 bytes)
//
//   The compression algorithm is repo-wide (declared in the manifest), so a single
//   bit suffices per blob. Legacy v1 entries also use this 41-byte layout, but the
//   high bit is not meaningful there: v1 blobs are always zstd-compressed.

static NEXT_PACKER_ID: AtomicU64 = AtomicU64::new(1);

pub const FOOTER_BLOB_LEN: usize = 41;

/// Maximum descriptors per pack before flushing. Guards against unbounded
/// descriptor accumulation when a pack contains only zero blobs (which don't
/// grow `buffer` but still consume descriptor slots).
const MAX_DESCRIPTORS_PER_PACK: usize = 4096;

/// Describes a single blob's location and size within a packed file.
#[derive(Debug, Clone, PartialEq)]
pub struct PackedBlobDescriptor {
    pub id: ID,
    pub blob_type: BlobType,
    pub offset: u32,
    pub length: u32,
    pub raw_length: u32,
    /// Whether the blob's encoded payload is zstd-compressed (v2+ marker).
    pub compressed: bool,
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
    instance_id: u64,
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
            instance_id: NEXT_PACKER_ID.fetch_add(1, Ordering::Relaxed),
            buffer: Vec::with_capacity(capacity),
            descriptors: Vec::with_capacity(FOOTER_BLOB_MULTIPLE),
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

    /// Returns true if no blobs (including zero-length) have been added to the packer.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.descriptors.is_empty()
    }

    /// Returns the number of blobs currently staged in the packer.
    #[inline]
    pub fn num_objects(&self) -> usize {
        self.descriptors.len()
    }

    /// Appends a new blob's data to the packer.
    pub fn add_blob(
        &mut self,
        id: ID,
        blob_type: BlobType,
        encoded_data: &[u8],
        raw_size: u64,
        compressed: bool,
    ) -> Result<()> {
        let offset = u32::try_from(self.buffer.len()).map_err(|e| {
            MapacheError::Integrity(format!("pack buffer offset exceeds u32::MAX: {e}"))
        })?;
        let length = u32::try_from(encoded_data.len()).map_err(|e| {
            MapacheError::Integrity(format!("blob encoded length exceeds u32::MAX: {e}"))
        })?;
        let raw_length = u32::try_from(raw_size)
            .map_err(|e| MapacheError::Integrity(format!("blob raw size exceeds u32::MAX: {e}")))?;

        if blob_type != BlobType::Zero {
            self.buffer.extend_from_slice(encoded_data);
        }

        self.descriptors.push(PackedBlobDescriptor {
            id,
            blob_type,
            offset,
            length,
            raw_length,
            compressed,
        });

        self.raw_size += raw_size;

        tracing::trace!(target: "packer", "Packer #{} added blob {} ({} -> {} bytes)", self.instance_id, id.to_short_hex(8), raw_size, length);

        Ok(())
    }
    /// Performs the CPU-intensive work of finalizing the pack.
    ///
    /// This includes:
    /// 1. Generating the footer.
    /// 2. Encrypting the footer.
    /// 3. Hashing the entire pack buffer.
    ///
    /// Returns the data and metadata required for upload and indexing.
    /// The internal buffer is essentially "stolen" by the result and must be
    /// returned via `recycle_buffer` later.
    fn finalize_and_extract(&mut self) -> Result<Option<PackFinalizationResult>> {
        if self.descriptors.is_empty() {
            return Ok(None);
        }

        // Take descriptors out to process them
        let mut descriptors = std::mem::take(&mut self.descriptors);
        let raw_size = self.raw_size;

        let footer = Self::generate_footer(&mut descriptors);

        let encoded_footer = self
            .secure_storage
            .encode_managed(&mut self.encoding_context, &footer)?;
        let footer_len: u32 = encoded_footer.len().try_into().map_err(|_| {
            MapacheError::Internal(format!(
                "pack footer too large ({} bytes)",
                encoded_footer.len()
            ))
        })?;
        let footer_len_bytes = footer_len.to_le_bytes();

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

    /// Finalize the pack and return the result (public for migration use).
    pub(crate) fn finalize(&mut self) -> Result<Option<FlushedPack>> {
        self.finalize_and_extract().map(|opt| {
            opt.map(|r| FlushedPack {
                id: r.id,
                data: r.data,
                descriptors: r.descriptors,
                raw_size: r.raw_size,
                meta_size: r.meta_size,
            })
        })
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
    ///
    /// Each entry is `FOOTER_BLOB_LEN` (41) bytes; the high bit of the type
    /// byte carries the per-blob compression marker. Padding entries carry no
    /// payload, so their compression bit is meaningless noise.
    pub(crate) fn generate_footer(descriptors: &mut Vec<PackedBlobDescriptor>) -> Vec<u8> {
        let mut rng = rng();

        if !descriptors.len().is_multiple_of(FOOTER_BLOB_MULTIPLE) {
            let num_padding_blobs =
                FOOTER_BLOB_MULTIPLE - (descriptors.len() % FOOTER_BLOB_MULTIPLE);
            for _ in 0..num_padding_blobs {
                descriptors.push(PackedBlobDescriptor {
                    id: ID::new_random(),
                    blob_type: BlobType::Padding,
                    offset: rng.random::<u32>(),
                    length: rng.random::<u32>(),
                    raw_length: rng.random::<u32>(),
                    compressed: rng.random::<bool>(),
                });
            }
        }

        let mut pack_footer = Vec::with_capacity(FOOTER_BLOB_LEN * descriptors.len());

        for blob in descriptors {
            put_bytes(&mut pack_footer, blob.id.as_slice());
            put_u8(&mut pack_footer, blob.blob_type.to_byte(blob.compressed));
            put_u32(&mut pack_footer, blob.length);
            put_u32(&mut pack_footer, blob.raw_length);
        }
        pack_footer
    }

    /// Locates and parses the footer of a pack file from the backend.
    ///
    /// This function performs two reads: one for the trailing 4-byte length
    /// and a second to retrieve the encrypted footer data.
    pub async fn parse_pack_footer(
        repo: &Repository,
        backend: &dyn StorageBackend,
        secure_storage: &SecureStorage,
        pack_id: &ID,
        nonce_at_end: bool,
    ) -> Result<Vec<PackedBlobDescriptor>> {
        let pack_path = repo.get_path(ContentIdType::Pack, pack_id);

        // We don't know a priori if this pack contains metadata, so we cannot use a StorageHint.
        let handle = Handle::new(&pack_path);
        let footer_length_bytes: [u8; 4] = backend
            .read(&handle, -4, 4)
            .await?
            .as_slice()
            .try_into()
            .map_err(|e: std::array::TryFromSliceError| {
                MapacheError::Format(format!("invalid footer length bytes: {e}"))
            })?;
        let encoded_footer_length = u32::from_le_bytes(footer_length_bytes) as usize;

        let footer_data = backend
            .read(
                &handle,
                -(4 + encoded_footer_length as isize),
                4 + encoded_footer_length,
            )
            .await?;

        Self::parse_footer(
            secure_storage,
            &footer_data,
            nonce_at_end,
            repo.repo_version(),
        )
    }

    /// Decodes a slice of footer bytes into a list of descriptors.
    ///
    /// This function validates the trailer length, decrypts the footer,
    /// and filters out any "Padding" blobs used during the packing process.
    ///
    /// `repo_version` decides whether the high bit of the type byte is a
    /// meaningful compression marker: v2 honors it, while v1 entries always
    /// decode as zstd-compressed (the bit is not meaningful there).
    // TODO(v1-removal): Remove repo_version parameter, always honor the marker.
    pub fn parse_footer(
        secure_storage: &SecureStorage,
        footer_data: &[u8],
        nonce_at_end: bool,
        repo_version: u32,
    ) -> Result<Vec<PackedBlobDescriptor>> {
        if footer_data.len() < 4 {
            return Err(MapacheError::Format("pack footer too short".to_string()));
        }
        let footer_length_bytes: [u8; 4] = footer_data[(footer_data.len() - 4)..]
            .try_into()
            .map_err(|e| MapacheError::Format(format!("could not read footer length: {e}")))?;
        let encoded_footer_length = u32::from_le_bytes(footer_length_bytes) as usize;

        if encoded_footer_length + 4 > footer_data.len() {
            return Err(MapacheError::Integrity(format!(
                "pack footer length {} exceeds data length {}",
                encoded_footer_length,
                footer_data.len()
            )));
        }

        let footer_slice =
            &footer_data[(footer_data.len() - encoded_footer_length - 4)..footer_data.len() - 4];
        let decrypted = match secure_storage.decrypt_inner(footer_slice, nonce_at_end)? {
            crate::backend::WriteContents::Owned(v) => v,
            crate::backend::WriteContents::Borrowed(b) => b.to_vec(),
        };
        let footer_blob_info = secure_storage.decompress(&decrypted)?;
        let mut cur = footer_blob_info.as_slice();
        let mut blob_descriptors = Vec::new();
        let mut offset: u32 = 0;
        let has_compression_marker = repo_version >= 2; // TODO(v1-removal): always true

        while !cur.is_empty() {
            let id = ID::from_bytes(get_array::<32>(&mut cur)?);
            let (blob_type, compressed) = BlobType::from_byte(get_u8(&mut cur)?)?;
            let compressed = if has_compression_marker {
                compressed
            } else {
                true
            };
            let length = get_u32(&mut cur)?;
            let raw_length = get_u32(&mut cur)?;

            if !matches!(blob_type, BlobType::Padding) {
                blob_descriptors.push(PackedBlobDescriptor {
                    id,
                    blob_type,
                    offset,
                    length,
                    raw_length,
                    compressed,
                });
                offset = offset.checked_add(length).ok_or_else(|| {
                    MapacheError::Integrity(format!(
                        "pack offset overflow while parsing footer (offset {offset} + length {length})"
                    ))
                })?;
            }
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
        compressed: bool,
    },
}

impl PackSaver {
    /// Creates a new `PackSaver` and initializes the background worker pool.
    pub(crate) fn new(
        rx: Receiver<PackSaverRequest>,
        rt_handle: tokio::runtime::Handle,
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
            empty_tx.send(p).map_err(|_| {
                MapacheError::Repo("failed to send packer to channel during initialization".into())
            })?;
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
                let rt = rt_handle.clone();

                worker_threads.push(std::thread::spawn(move || -> Result<()> {
                    while let Ok((mut packer, blob_type)) = rx.recv() {
                        if err_ptr.lock().is_some() {
                            return Ok(());
                        }

                        // Perform CPU-intensive encryption/hashing
                        let result = match packer.finalize_and_extract() {
                            Ok(Some(res)) => res,
                            Ok(None) => {
                                if tx.send(packer).is_err() {
                                    tracing::warn!(target: "packer", "Failed to return empty packer to pool (channel closed)");
                                }
                                continue;
                            }
                            Err(e) => {
                                *err_ptr.lock() = Some(e);
                                if tx.send(packer).is_err() {
                                    tracing::warn!(target: "packer", "Failed to return packer to pool after error (channel closed)");
                                }
                                return Ok(());
                            }
                        };

                        let repo = match repo_ptr.upgrade() {
                            Some(r) => r,
                            None => {
                                if tx.send(packer).is_err() {
                                    tracing::warn!(target: "packer", "Failed to return packer to pool (repo dropped, channel closed)");
                                }
                                return Ok(());
                            }
                        };

                        // Capture stats before moving descriptors into the async block
                        let stats_raw = result.raw_size;
                        let stats_enc = result.encoded_size;
                        let stats_meta = result.meta_size;
                        let stats_blobs = result.descriptors.len() as u64;
                        let pack_id = result.id;
                        let pack_data = result.data;
                        let descriptors = result.descriptors;

                        tracing::debug!(
                            target: "packer",
                            "Worker uploading pack {} ({} blobs, {} bytes)",
                            pack_id.to_short_hex(8),
                            stats_blobs,
                            stats_enc
                        );

                        // Bridge to Async Backend:
                        // We use block_on to wait for the I/O to complete. This enforces
                        // backpressure; if the upload is slow, the worker stays busy
                        // and cannot recycle its buffer yet.
                        let upload_and_index = rt.block_on(async {
                            let save_id = SaveID::WithID(pack_id);

                            // Upload the pack file
                            repo.save_file(
                                &save_id,
                                &pack_data,
                                StorageHint {
                                    file_type: ContentIdType::Pack,
                                    is_metadata: blob_type == BlobType::Tree,
                                },
                                None,
                            )
                            .await
                            .map_err(|e| {
                                MapacheError::Backend(format!("upload failed: {}", e.inner()))
                            })?;

                            // Register with the index
                            repo.index()
                                .add_pack(&repo, &pack_id, descriptors)
                                .await
                                .map_err(|e| MapacheError::Repo(format!("index update failed: {e}")))
                        });

                        let index_size = match upload_and_index {
                            Ok(size) => {
                                tracing::debug!(target: "packer", "Pack {} uploaded and indexed", pack_id.to_short_hex(8));
                                size
                            }
                            Err(e) => {
                                tracing::error!(target: "packer", "Worker failed to save pack {}: {e}", pack_id.to_short_hex(8));
                                *err_ptr.lock() = Some(e);
                                packer.recycle_buffer(pack_data);
                                if tx.send(packer).is_err() {
                                    tracing::warn!(target: "packer", "Failed to return packer to pool after upload error (channel closed)");
                                }
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

                        // Restore buffer to the packer and return it to the empty pool for reuse
                        packer.recycle_buffer(pack_data);
                        if tx.send(packer).is_err() {
                            return Ok(());
                        }
                    }
                    Ok(())
                }));
            }

            for t in worker_threads {
                if let Err(e) = t.join() {
                    let mut err = first_err.lock();
                    if err.is_none() {
                        *err = Some(MapacheError::Repo(format!("packer thread panicked: {e:?}")));
                    }
                }
            }
            first_err.lock().take().map_or(Ok(()), Err)
        });

        // Get initial packers for the main thread active slots
        let data_packer = empty_rx
            .recv()
            .map_err(|e| MapacheError::Repo(format!("failed to initialize data packer: {e}")))?;
        let tree_packer = empty_rx
            .recv()
            .map_err(|e| MapacheError::Repo(format!("failed to initialize tree packer: {e}")))?;

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

    fn packer_for(&mut self, blob_type: BlobType) -> Option<&mut Packer> {
        match blob_type {
            BlobType::Data | BlobType::Zero => Some(&mut self.data_packer),
            BlobType::Tree => Some(&mut self.tree_packer),
            _ => None,
        }
    }

    /// Starts the main event loop.
    /// This loop is now extremely lightweight. It purely moves pointers.
    pub fn run(mut self) -> Result<()> {
        tracing::info!(target: "packer", "Pack saver loop started");
        while let Ok(request) = self.rx.recv() {
            match request {
                PackSaverRequest::SaveBlob {
                    id,
                    blob_type,
                    data,
                    raw_length,
                    compressed,
                } => {
                    let max_size = self.max_packer_size;
                    let Some(packer) = self.packer_for(blob_type) else {
                        continue;
                    };

                    packer.add_blob(id, blob_type, &data, raw_length, compressed)?;

                    if packer.size() >= max_size || packer.num_objects() >= MAX_DESCRIPTORS_PER_PACK
                    {
                        self.dispatch_packer(blob_type)?;
                    }
                }
            }
        }

        // Final flushes
        tracing::info!(target: "packer", "Flushing remaining packs");
        self.dispatch_packer(BlobType::Data)?;
        self.dispatch_packer(BlobType::Tree)?;

        // Close channel to signal workers to finish
        drop(self.full_packer_tx);

        let res = self
            .worker_handle
            .join()
            .map_err(|_| MapacheError::Internal("worker panicked".to_string()));
        tracing::info!(target: "packer", "Pack saver loop finished");
        res?
    }

    /// Swaps the current packer with a fresh one from the recycle pool
    /// and sends the full one to the workers.
    fn dispatch_packer(&mut self, blob_type: BlobType) -> Result<()> {
        let packer_ref = match blob_type {
            BlobType::Data | BlobType::Zero => &mut self.data_packer,
            BlobType::Tree => &mut self.tree_packer,
            _ => return Ok(()),
        };

        if packer_ref.is_empty() {
            return Ok(());
        }

        tracing::debug!(target: "packer", "Dispatching {blob_type:?} pack ({} bytes, {} blobs) to workers", packer_ref.size(), packer_ref.num_objects());
        // Get an empty packer from the pool (blocks if workers are too slow, creating backpressure)
        let mut new_packer = self
            .empty_packer_rx
            .recv()
            .map_err(|e| MapacheError::Repo(format!("worker pool died or no free packers: {e}")))?;

        std::mem::swap(packer_ref, &mut new_packer);

        // Send the full packer to workers
        self.full_packer_tx
            .send((new_packer, blob_type))
            .map_err(|e| MapacheError::Repo(format!("failed to dispatch pack to workers: {e}")))?;

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
            BlobType::Data | BlobType::Zero => {
                // For Data packs, 'raw' and 'encoded' sizes correspond to the blob data.
                // The 'meta' sizes correspond to the pack footer.
                // Note: the raw footer size is not available here; we use the encoded
                // footer size as a proxy. It will be slightly lower than the true raw
                // size due to compression, but accounts for the space consumed.
                repo.stats.raw_bytes.fetch_add(raw_size, Ordering::Relaxed);
                repo.stats
                    .encoded_bytes
                    .fetch_add(encoded_size.saturating_sub(meta_size), Ordering::Relaxed);
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
                // For Tree packs, everything (blobs + footer) is considered repository metadata.
                repo.stats
                    .meta_raw_bytes
                    .fetch_add(raw_size + meta_size, Ordering::Relaxed);
                repo.stats
                    .meta_encoded_bytes
                    .fetch_add(encoded_size, Ordering::Relaxed);
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
    use std::sync::Arc;

    use super::*;
    use crate::{common::defaults::DEFAULT_COMPRESSION, repository::keys::KeyManager};

    fn add_blob(packer: &mut Packer, data: &[u8], secure_storage: &SecureStorage) -> Result<()> {
        let raw_size = data.len() as u64;
        let encoded_data = secure_storage.encode(data)?;
        packer.add_blob(
            ID::from_content(&encoded_data),
            BlobType::Data,
            &encoded_data,
            raw_size,
            true,
        )?;
        Ok(())
    }

    #[test]
    fn test_pack_finalize_and_recycle() -> Result<()> {
        let key = KeyManager::generate_new_master_key();
        let secure_storage = Arc::new(
            SecureStorage::new()
                .with_compression(DEFAULT_COMPRESSION.to_level())
                .with_key(&key)
                .expect("valid 32-byte key"),
        );

        let mut packer = Packer::new(1024, secure_storage.clone())?;
        let blobs = [b"test1".to_vec(), b"test2".to_vec()];

        // Add Data
        add_blob(&mut packer, &blobs[0], &secure_storage)?;
        add_blob(&mut packer, &blobs[1], &secure_storage)?;

        assert!(!packer.is_empty());

        // Finalize (Simulating worker action)
        let result = packer
            .finalize_and_extract()
            .map_err(|e| MapacheError::Repo(format!("finalize failed: {e}")))?
            .ok_or_else(|| MapacheError::Integrity("should return result".to_string()))?;

        // CHANGE THIS:
        // result.descriptors includes the padding blobs added for obfuscation
        assert_eq!(result.descriptors.len(), 64);

        // But we can verify that the first 2 are our actual data
        assert_eq!(result.descriptors[0].blob_type, BlobType::Data);
        assert_eq!(result.descriptors[1].blob_type, BlobType::Data);

        assert!(!result.data.is_empty());

        // If you want to verify the "round-trip" through the parser:
        let parsed_descriptors = Packer::parse_footer(&secure_storage, &result.data, true, 2)?;
        // The parser filters out Padding, so we should get exactly 2 back
        assert_eq!(parsed_descriptors.len(), 2);

        Ok(())
    }

    #[test]
    fn test_packer_size_accumulation() -> Result<()> {
        let key = KeyManager::generate_new_master_key();
        let secure_storage = Arc::new(
            SecureStorage::new()
                .with_compression(DEFAULT_COMPRESSION.to_level())
                .with_key(&key)
                .expect("valid 32-byte key"),
        );

        let mut packer = Packer::new(1024, secure_storage.clone())?;

        let data1 = b"some data".to_vec();
        let encoded1 = secure_storage.encode(&data1)?;
        packer.add_blob(
            ID::from_content(&encoded1),
            BlobType::Data,
            &encoded1,
            data1.len() as u64,
            true,
        )?;

        assert_eq!(packer.size(), encoded1.len() as u64);
        assert_eq!(packer.num_objects(), 1);

        let data2 = b"more data".to_vec();
        let encoded2 = secure_storage.encode(&data2)?;
        packer.add_blob(
            ID::from_content(&encoded2),
            BlobType::Data,
            &encoded2,
            data2.len() as u64,
            true,
        )?;

        assert_eq!(packer.size(), (encoded1.len() + encoded2.len()) as u64);
        assert_eq!(packer.num_objects(), 2);

        Ok(())
    }
}
