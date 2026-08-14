use std::{
    collections::HashSet,
    fs::File,
    io::{Seek, Write},
    path::Path,
};

use argon2::Params;
use parking_lot::Mutex;

use crate::{
    backend::WriteContents,
    bundle::format::{
        BUNDLE_KEY_LEN, BUNDLE_MAGIC_END, BUNDLE_MAGIC_START, BUNDLE_SALT_LEN, BUNDLE_VERSION,
        BundleHeader, BundleIndex, BundleIndexEntry, BundleTrailer,
    },
    common::error::{MapacheError, Result},
    common::{BlobType, ID, SaveID, traits::BlobSaver},
    repository::{manifest::Manifest, storage::SecureStorage},
};

pub struct BundleWriter {
    storage: SecureStorage,
    inner: Mutex<BundleWriterInner>,
}

struct BundleWriterInner {
    file: File,
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
    pub fn new<P: AsRef<Path>>(path: P, password: &str, compression_level: i32) -> Result<Self> {
        let salt = SecureStorage::generate_salt::<BUNDLE_SALT_LEN>();
        let params = Params::default();
        let key = SecureStorage::derive_key::<BUNDLE_KEY_LEN>(password, &salt, params.clone())?;

        let storage = SecureStorage::new()
            .with_compression(compression_level)
            .with_key(&*key)?;

        let mut file = File::create(path)?;

        let header = BundleHeader {
            magic: *BUNDLE_MAGIC_START,
            version: BUNDLE_VERSION,
            salt,
            argon2_t: params.t_cost(),
            argon2_m: params.m_cost(),
            argon2_p: params.p_cost(),
        };

        let header_bytes = header.to_binary();
        file.write_all(&header_bytes)?;

        Ok(Self {
            storage,
            inner: Mutex::new(BundleWriterInner {
                file,
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
        let encoded_data = self
            .storage
            .encode(data.as_ref())
            .map_err(|e| MapacheError::Crypto(format!("failed to encode blob data: {e}")))?;
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
            compressed: true,
            offset,
            length,
            raw_length,
        });

        Ok(id)
    }

    pub fn finalize(&self, root_tree_id: ID) -> Result<()> {
        let mut inner = self.inner.lock();

        let index_offset = inner.file.stream_position()?;
        let index_bytes = inner.index.to_binary();
        let encrypted_index = self
            .storage
            .encrypt(&index_bytes)
            .map_err(|e| MapacheError::Crypto(format!("failed to encrypt index: {e}")))?;
        let index_len = encrypted_index.len() as u32;
        inner.file.write_all(&encrypted_index)?;

        let manifest_offset = inner.file.stream_position()?;
        let manifest = Manifest::new(BUNDLE_VERSION as u32);
        let manifest_bytes = manifest.to_binary();
        let encrypted_manifest = self
            .storage
            .encrypt(&manifest_bytes)
            .map_err(|e| MapacheError::Crypto(format!("failed to encrypt manifest: {e}")))?;
        let manifest_len = encrypted_manifest.len() as u32;
        inner.file.write_all(&encrypted_manifest)?;

        let trailer = BundleTrailer {
            root_tree: root_tree_id,
            index_offset,
            index_len,
            manifest_offset,
            manifest_len,
            magic_end: *BUNDLE_MAGIC_END,
        };

        let trailer_bytes = trailer.to_binary();
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
