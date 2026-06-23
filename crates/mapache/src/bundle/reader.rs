use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::Path,
};

use anyhow::{Context, Result, bail};
use argon2::ParamsBuilder;
use async_trait::async_trait;
use parking_lot::Mutex;

use crate::{
    bundle::format::{
        BUNDLE_HEADER_SIZE, BUNDLE_KEY_LEN, BUNDLE_MAGIC_END, BUNDLE_MAGIC_START,
        BUNDLE_TRAILER_SIZE_LEN, BundleHeader, BundleIndex, BundleTrailer,
    },
    common::{ID, traits::BlobLoader},
    repository::storage::SecureStorage,
};

pub struct BundleReader {
    file: Mutex<File>,
    storage: SecureStorage,
    index: BundleIndex,
    index_map: HashMap<ID, usize>,
    pub trailer: BundleTrailer,
}

#[async_trait]
impl BlobLoader for BundleReader {
    async fn load_blob(&self, id: &ID) -> Result<Vec<u8>> {
        let idx = self
            .index_map
            .get(id)
            .context("Blob not found in bundle index")?;
        let entry = &self.index.entries[*idx];

        let mut file = self.file.lock();
        file.seek(SeekFrom::Start(entry.offset))?;
        let mut encoded_data = vec![0u8; entry.length as usize];
        file.read_exact(&mut encoded_data)?;

        let data = self
            .storage
            .decode(&encoded_data)
            .context("Failed to decode blob data")?;

        if data.len() != entry.raw_length as usize {
            bail!(
                "Decoded blob length mismatch: expected {}, got {}",
                entry.raw_length,
                data.len()
            );
        }

        Ok(data)
    }
}

impl BundleReader {
    pub fn open<P: AsRef<Path>>(path: P, password: &str) -> Result<Self> {
        let mut file = File::open(path)?;

        let mut header_bytes = vec![0u8; BUNDLE_HEADER_SIZE];
        file.read_exact(&mut header_bytes)
            .context("Failed to read header bytes from bundle")?;
        let header = BundleHeader::from_binary(&header_bytes)
            .context("Invalid bundle format: failed to parse header")?;

        if header.magic != *BUNDLE_MAGIC_START {
            bail!("Invalid bundle format: invalid magic start (not a mapache bundle)");
        }

        let params = ParamsBuilder::new()
            .m_cost(header.argon2_m)
            .t_cost(header.argon2_t)
            .p_cost(header.argon2_p)
            .build()
            .map_err(|e| {
                anyhow::anyhow!(
                    "Invalid bundle format: Argon2 parameters are invalid: {}",
                    e
                )
            })?;

        let key = SecureStorage::derive_key::<BUNDLE_KEY_LEN>(password, &header.salt, params)
            .context("Failed to derive key from password")?;
        let storage = SecureStorage::new().with_key(&*key);

        file.seek(SeekFrom::End(-(BUNDLE_TRAILER_SIZE_LEN as i64)))?;
        let mut size_bytes = [0u8; BUNDLE_TRAILER_SIZE_LEN];
        file.read_exact(&mut size_bytes)
            .context("Failed to read trailer size from bundle")?;
        let encrypted_trailer_size = u32::from_le_bytes(size_bytes);

        file.seek(SeekFrom::End(
            -(BUNDLE_TRAILER_SIZE_LEN as i64) - encrypted_trailer_size as i64,
        ))?;
        let mut encrypted_trailer = vec![0u8; encrypted_trailer_size as usize];
        file.read_exact(&mut encrypted_trailer)
            .context("Failed to read encrypted trailer from bundle")?;
        let decrypted_trailer = storage
            .decrypt(&encrypted_trailer)
            .context("Failed to decrypt bundle trailer: incorrect password or corrupted data")?;
        let trailer = BundleTrailer::from_binary(&decrypted_trailer)
            .context("Invalid bundle format: failed to parse trailer")?;

        if trailer.magic_end != *BUNDLE_MAGIC_END {
            bail!("Invalid bundle format: invalid magic end");
        }

        file.seek(SeekFrom::Start(trailer.index_offset))?;
        let mut encrypted_index = vec![0u8; trailer.index_len as usize];
        file.read_exact(&mut encrypted_index)
            .context("Failed to read encrypted index from bundle")?;
        let decrypted_index = storage
            .decrypt(&encrypted_index)
            .context("Failed to decrypt bundle index: incorrect password or corrupted data")?;
        let index = BundleIndex::from_binary(decrypted_index.as_ref())
            .context("Invalid bundle format: failed to parse index")?;

        let mut index_map = HashMap::new();
        for (i, entry) in index.entries.iter().enumerate() {
            index_map.insert(entry.id, i);
        }

        Ok(Self {
            file: Mutex::new(file),
            storage,
            index,
            index_map,
            trailer,
        })
    }

    pub fn index(&self) -> &BundleIndex {
        &self.index
    }
}
