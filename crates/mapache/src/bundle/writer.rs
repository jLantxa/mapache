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
    common::error::{MapacheError, Result},
    common::{BlobType, ID, SaveID, traits::BlobSaver},
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
            // Read back the blob data section to compute ECC parity.
            let data_end = inner.file.stream_position()?;
            let data_start = inner.data_start;
            let data_len = (data_end - data_start) as usize;

            let mut data_section = vec![0u8; data_len];
            inner.file.seek(std::io::SeekFrom::Start(data_start))?;
            inner.file.read_exact(&mut data_section)?;

            let k = ecc_cfg.data_shards as usize;
            let p = ecc_cfg.parity_shards as usize;
            let ecc_payload = ecc::ecc_encode(&data_section, k, p)
                .map_err(|e| MapacheError::Crypto(format!("failed to encode ECC section: {e}")))?;

            // ECC payload is encrypted before writing so parity is also protected.
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
