use std::{
    collections::HashSet,
    fs::{File, OpenOptions},
    io::{Read, Seek, Write},
    path::Path,
};

use argon2::Params;
use parking_lot::Mutex;

use crate::{
    backend::WriteContents,
    bundle::format::{
        BUNDLE_KEY_LEN, BUNDLE_MAGIC_END, BUNDLE_MAGIC_START, BUNDLE_SALT_LEN, BundleHeader,
        BundleIndex, BundleIndexEntry, BundleTrailer,
    },
    common::{
        BlobType, ID, SaveID,
        error::{MapacheError, Result},
        traits::BlobSaver,
    },
    ecc,
    repository::{
        manifest::{EccConfig, Manifest},
        storage::SecureStorage,
    },
};

pub struct BundleWriter {
    storage: SecureStorage,
    compress: bool,
    format_version: u16,
    ecc_config: Option<EccConfig>,
    inner: Mutex<BundleWriterInner>,
}

struct BundleWriterInner {
    file: File,
    data_start: u64,
    index: BundleIndex,
    seen: HashSet<ID>,
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
    ) -> Result<Self> {
        let salt = SecureStorage::generate_salt::<BUNDLE_SALT_LEN>();
        let params = Params::default();
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
        let data_start = header_bytes.len() as u64;
        file.write_all(&header_bytes)?;

        Ok(Self {
            storage,
            compress,
            format_version,
            ecc_config,
            inner: Mutex::new(BundleWriterInner {
                file,
                data_start,
                index: BundleIndex::default(),
                seen: HashSet::new(),
            }),
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

        let raw_length = data.len() as u32;
        let encoded_data = if self.compress {
            self.storage
                .encode(data.as_ref())
                .map_err(|e| MapacheError::Crypto(format!("failed to encode blob data: {e}")))?
        } else {
            self.storage
                .encrypt(data.as_ref())
                .map_err(|e| MapacheError::Crypto(format!("failed to encrypt blob data: {e}")))?
        };
        let length = encoded_data.len() as u32;

        let mut inner = self.inner.lock();

        if !inner.seen.insert(id) {
            return Ok(id);
        }

        let offset = inner.file.stream_position()?;
        inner.file.write_all(&encoded_data)?;

        inner.index.entries.push(BundleIndexEntry {
            id,
            blob_type,
            compressed: self.compress,
            offset,
            length,
            raw_length,
        });

        Ok(id)
    }

    pub fn finalize(&self, root_tree_id: ID) -> Result<()> {
        let mut inner = self.inner.lock();

        // --- ECC section (between blob data and index) ---
        let has_ecc = self.ecc_config.is_some();
        let mut ecc_offset: u64 = 0;
        let mut ecc_len: u32 = 0;

        if let Some(ecc_cfg) = &self.ecc_config {
            let data_end = inner.file.stream_position()?;
            let data_start = inner.data_start;
            let data_len = (data_end - data_start) as usize;

            let k = ecc_cfg.data_shards as usize;
            let p = ecc_cfg.parity_shards as usize;

            // Build ECC header.
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

            // Encode stripe-by-stripe: read each stripe's data from the file,
            // encode parity, and append to ecc_payload. Peak memory is bounded
            // by one stripe's data+parity instead of the full data section.
            let rs = ecc::reed_solomon::ReedSolomon::new(k, p).map_err(|_| {
                MapacheError::Crypto(format!("invalid ECC shard count: k={k}, p={p}"))
            })?;

            inner.file.seek(std::io::SeekFrom::Start(data_start))?;
            for stripe in &layouts {
                let sk = stripe.data_shards;
                let sp = stripe.parity_shards;
                let shard_bytes = sk * ecc::SHARD_SIZE;

                let mut shard_buf = vec![0u8; shard_bytes];
                let copy_len = stripe.data_bytes.min(shard_bytes);
                inner.file.read_exact(&mut shard_buf[..copy_len])?;

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

            ecc_offset = inner.file.stream_position()?;
            ecc_len = encrypted_ecc.len() as u32;
            inner.file.write_all(&encrypted_ecc)?;
        }

        // --- Index ---
        let index_offset = inner.file.stream_position()?;
        let index_bytes = inner.index.to_binary();
        let encrypted_index = self
            .storage
            .encrypt(&index_bytes)
            .map_err(|e| MapacheError::Crypto(format!("failed to encrypt index: {e}")))?;
        let index_len = encrypted_index.len() as u32;
        inner.file.write_all(&encrypted_index)?;

        // --- Manifest ---
        let manifest_offset = inner.file.stream_position()?;
        let mut manifest = Manifest::new(self.format_version as u32);
        if let Some(ecc_cfg) = &self.ecc_config {
            manifest.set_ecc(Some(ecc_cfg.clone()));
        }
        let manifest_bytes = manifest.to_binary();
        let encrypted_manifest = self
            .storage
            .encrypt(&manifest_bytes)
            .map_err(|e| MapacheError::Crypto(format!("failed to encrypt manifest: {e}")))?;
        let manifest_len = encrypted_manifest.len() as u32;
        inner.file.write_all(&encrypted_manifest)?;

        // --- Trailer ---
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

        inner.file.write_all(&encrypted_trailer)?;
        inner.file.write_all(&trailer_size.to_le_bytes())?;

        Ok(())
    }
}
