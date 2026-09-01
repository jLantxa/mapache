use std::{
    collections::HashSet,
    fs::{File, OpenOptions},
    io::{Read, Seek, Write},
    path::Path,
    thread::{self, JoinHandle},
};

use crossbeam_channel::Sender;
use parking_lot::Mutex as ParkMutex;

use crate::{
    backend::WriteContents,
    bundle::format::{
        BUNDLE_HEADER_SIZE, BUNDLE_KEY_LEN, BUNDLE_MAGIC_END, BUNDLE_MAGIC_START, BUNDLE_SALT_LEN,
        BundleHeader, BundleIndex, BundleIndexEntry, BundleTrailer,
    },
    common::{
        BlobType, ID, SaveID,
        error::{MapacheError, Result},
        kdf,
        traits::BlobSaver,
    },
    ecc,
    repository::{
        manifest::{EccConfig, Manifest},
        storage::SecureStorage,
    },
};

/// Message sent from `save_blob` to the background writer thread.
struct EncodedBlob {
    data: Vec<u8>,
    id: ID,
    blob_type: BlobType,
    raw_length: u32,
}

/// State owned by the background writer thread.
struct WriterInner {
    file: File,
    index: BundleIndex,
}

pub struct BundleWriter {
    storage: SecureStorage,
    compress: bool,
    format_version: u16,
    ecc_config: Option<EccConfig>,
    /// Concurrent dedup set — checked in `save_blob` before sending to channel.
    seen: ParkMutex<HashSet<ID>>,
    tx: ParkMutex<Option<Sender<EncodedBlob>>>,
    writer_handle: ParkMutex<Option<JoinHandle<Result<WriterInner>>>>,
}

impl BlobSaver for BundleWriter {
    fn save_blob(
        &self,
        blob_type: BlobType,
        data: WriteContents<'_>,
        save_id: SaveID,
    ) -> Result<ID> {
        self.save_blob_internal(blob_type, data, save_id)
    }
}

impl BundleWriter {
    pub fn new<P: AsRef<Path>>(
        path: P,
        password: &str,
        compression_level: i32,
        compress: bool,
        format_version: u16,
        ecc_config: Option<EccConfig>,
        calibrate_kdf: bool,
    ) -> Result<Self> {
        let salt = SecureStorage::generate_salt::<BUNDLE_SALT_LEN>();
        let params = if calibrate_kdf {
            kdf::calibrate_params(kdf::CALIBRATE_TARGET, None)
        } else {
            kdf::default_params()
        };
        let key = SecureStorage::derive_key::<BUNDLE_KEY_LEN>(password, &salt, params.clone())?;

        let storage = if compress {
            SecureStorage::new()
                .with_compression(compression_level)
                .with_key(&*key)?
        } else {
            SecureStorage::new().with_key(&*key)?
        };

        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(path)?;

        let header = BundleHeader {
            magic: *BUNDLE_MAGIC_START,
            version: format_version,
            salt,
            argon2_t: params.t_cost(),
            argon2_m: params.m_cost(),
            argon2_p: params.p_cost(),
        };

        let header_bytes = header.to_binary();
        file.write_all(&header_bytes)?;

        let (tx, rx) = crossbeam_channel::bounded::<EncodedBlob>(256);

        let writer_handle = thread::spawn(move || {
            let mut inner = WriterInner {
                file,
                index: BundleIndex::default(),
            };

            while let Ok(blob) = rx.recv() {
                let offset = inner.file.stream_position()?;
                inner.file.write_all(&blob.data)?;

                let data_len_u64 = blob.data.len() as u64;
                let length = u32::try_from(blob.data.len()).map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "encoded blob of {data_len_u64} bytes exceeds the bundle format's maximum length of {} bytes",
                            u32::MAX
                        ),
                    )
                })?;

                inner.index.entries.push(BundleIndexEntry {
                    id: blob.id,
                    blob_type: blob.blob_type,
                    compressed: compress,
                    offset,
                    length,
                    raw_length: blob.raw_length,
                });
            }

            Ok(inner)
        });

        Ok(Self {
            storage,
            compress,
            format_version,
            ecc_config,
            seen: ParkMutex::new(HashSet::new()),
            tx: ParkMutex::new(Some(tx)),
            writer_handle: ParkMutex::new(Some(writer_handle)),
        })
    }

    fn save_blob_internal(
        &self,
        blob_type: BlobType,
        data: WriteContents<'_>,
        save_id: SaveID,
    ) -> Result<ID> {
        let id = match save_id {
            SaveID::CalculateID => ID::from_content(&data),
            SaveID::WithID(id) => id,
        };

        if self.seen.lock().contains(&id) {
            return Ok(id);
        }

        let raw_length_u64 = data.len() as u64;
        let raw_length = u32::try_from(data.len()).map_err(|_| {
            MapacheError::Format(format!(
                "blob of {} bytes exceeds the bundle format's maximum raw length of {} bytes",
                raw_length_u64,
                u32::MAX
            ))
        })?;
        let encoded_data = if self.compress {
            self.storage
                .encode(data.as_ref())
                .map_err(|e| MapacheError::Crypto(format!("failed to encode blob data: {e}")))?
        } else {
            self.storage
                .encrypt(data.as_ref())
                .map_err(|e| MapacheError::Crypto(format!("failed to encrypt blob data: {e}")))?
        };

        if !self.seen.lock().insert(id) {
            return Ok(id);
        }

        let tx_lock = self.tx.lock();
        let tx = tx_lock
            .as_ref()
            .ok_or_else(|| MapacheError::Internal("bundle writer already finalized".into()))?;

        tx.send(EncodedBlob {
            data: encoded_data,
            id,
            blob_type,
            raw_length,
        })
        .map_err(|_| MapacheError::Internal("bundle writer channel closed".into()))?;

        Ok(id)
    }

    pub fn finalize(&self, root_tree_id: ID) -> Result<()> {
        drop(self.tx.lock().take());

        let writer_inner = {
            let mut handle_guard = self.writer_handle.lock();
            handle_guard
                .take()
                .ok_or_else(|| MapacheError::Internal("bundle writer already finalized".into()))?
                .join()
                .map_err(|_| MapacheError::Internal("bundle writer thread panicked".into()))?
                .map_err(|e| MapacheError::Internal(format!("bundle writer I/O error: {e}")))?
        };

        let mut file = writer_inner.file;
        let index = writer_inner.index;

        let has_ecc = self.ecc_config.is_some();
        let mut ecc_offset: u64 = 0;
        let mut ecc_len: u32 = 0;

        if let Some(ecc_cfg) = &self.ecc_config {
            let data_end = file.stream_position()?;
            let data_start = BUNDLE_HEADER_SIZE as u64;
            let data_len = (data_end - data_start) as usize;

            let k = ecc_cfg.data_shards as usize;
            let p = ecc_cfg.parity_shards as usize;

            let layouts = ecc::calculate_stripe_layouts(data_len, k, p);
            let mut ecc_payload = Vec::with_capacity(
                ecc::HEADER_SIZE + layouts.len() * ecc::stripe_payload_size(k, p),
            );
            ecc_payload.extend_from_slice(&ecc::MAGIC);
            ecc_payload.push(ecc::VERSION);
            ecc_payload.push(0);
            ecc_payload.extend_from_slice(&(k as u16).to_le_bytes());
            ecc_payload.extend_from_slice(&(p as u16).to_le_bytes());
            ecc_payload.extend_from_slice(&(data_len as u64).to_le_bytes());
            ecc_payload.extend_from_slice(&(layouts.len() as u32).to_le_bytes());

            let rs = ecc::reed_solomon::ReedSolomon::new(k, p).map_err(|_| {
                MapacheError::Crypto(format!("invalid ECC shard count: k={k}, p={p}"))
            })?;

            file.seek(std::io::SeekFrom::Start(data_start))?;
            for stripe in &layouts {
                let sk = stripe.data_shards;
                let sp = stripe.parity_shards;
                let shard_bytes = sk * ecc::SHARD_SIZE;

                let mut shard_buf = vec![0u8; shard_bytes];
                let copy_len = stripe.data_bytes.min(shard_bytes);
                file.read_exact(&mut shard_buf[..copy_len])?;

                let data_refs: Vec<&[u8]> = shard_buf.chunks(ecc::SHARD_SIZE).take(sk).collect();

                let parity_bytes = sp * ecc::SHARD_SIZE;
                let mut parity_buf = vec![0u8; parity_bytes];
                {
                    let mut parity_refs: Vec<&mut [u8]> =
                        parity_buf.chunks_mut(ecc::SHARD_SIZE).take(sp).collect();
                    rs.encode_into(&data_refs, &mut parity_refs).map_err(|e| {
                        MapacheError::Crypto(format!("ECC encode failed for stripe: {e}"))
                    })?;
                }

                for i in 0..sk {
                    let start = i * ecc::SHARD_SIZE;
                    let crc = ecc::crc32_ieee(&shard_buf[start..start + ecc::SHARD_SIZE]);
                    ecc_payload.extend_from_slice(&crc.to_le_bytes());
                }
                for i in 0..sp {
                    let start = i * ecc::SHARD_SIZE;
                    let crc = ecc::crc32_ieee(&parity_buf[start..start + ecc::SHARD_SIZE]);
                    ecc_payload.extend_from_slice(&crc.to_le_bytes());
                }
                ecc_payload.extend_from_slice(&parity_buf);
            }

            let encrypted_ecc = self
                .storage
                .encrypt(&ecc_payload)
                .map_err(|e| MapacheError::Crypto(format!("failed to encrypt ECC section: {e}")))?;

            ecc_offset = file.stream_position()?;
            ecc_len = encrypted_ecc.len() as u32;
            file.write_all(&encrypted_ecc)?;
        }

        let index_offset = file.stream_position()?;
        let index_bytes = index.to_binary();
        let encrypted_index = self
            .storage
            .encrypt(&index_bytes)
            .map_err(|e| MapacheError::Crypto(format!("failed to encrypt index: {e}")))?;
        let index_len = encrypted_index.len() as u32;
        file.write_all(&encrypted_index)?;

        let manifest_offset = file.stream_position()?;
        let mut manifest = Manifest::new(self.format_version as u32);
        if let Some(ecc_cfg) = &self.ecc_config {
            manifest.set_ecc(Some(ecc_cfg.clone()));
        }
        let manifest_bytes = serde_json::to_vec(&manifest).map_err(MapacheError::Serialization)?;
        let encrypted_manifest = self
            .storage
            .encrypt(&manifest_bytes)
            .map_err(|e| MapacheError::Crypto(format!("failed to encrypt manifest: {e}")))?;
        let manifest_len = encrypted_manifest.len() as u32;
        file.write_all(&encrypted_manifest)?;

        let trailer = BundleTrailer {
            root_tree: root_tree_id,
            index_offset,
            index_len,
            manifest_offset,
            manifest_len,
            ecc_offset,
            ecc_len,
            magic_end: *BUNDLE_MAGIC_END,
        };

        let trailer_bytes = trailer.to_binary(has_ecc);
        let encrypted_trailer = self
            .storage
            .encrypt(&trailer_bytes)
            .map_err(|e| MapacheError::Crypto(format!("failed to encrypt trailer: {e}")))?;
        let trailer_size = encrypted_trailer.len() as u32;

        file.write_all(&encrypted_trailer)?;
        file.write_all(&trailer_size.to_le_bytes())?;

        Ok(())
    }
}
