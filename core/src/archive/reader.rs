use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::Path,
};

use anyhow::{Context, Result, bail};
use argon2::ParamsBuilder;
use parking_lot::Mutex;

use crate::{
    archive::format::{
        ARCHIVE_HEADER_SIZE, ARCHIVE_KEY_LEN, ARCHIVE_MAGIC_END, ARCHIVE_MAGIC_START,
        ARCHIVE_TRAILER_SIZE_LEN, ArchiveHeader, ArchiveIndex, ArchiveTrailer,
    },
    mapache::{ID, traits::BlobLoader},
    repository::storage::SecureStorage,
};

pub struct ArchiveReader {
    file: Mutex<File>,
    storage: SecureStorage,
    index: ArchiveIndex,
    index_map: HashMap<ID, usize>,
    pub trailer: ArchiveTrailer,
}

#[async_trait::async_trait]
impl BlobLoader for ArchiveReader {
    async fn load_blob(&self, id: &ID) -> Result<Vec<u8>> {
        self.load_blob_internal(id)
    }
}

impl ArchiveReader {
    pub fn open<P: AsRef<Path>>(path: P, password: &str) -> Result<Self> {
        let mut file = File::open(path)?;

        // Read header
        let mut header_bytes = vec![0u8; ARCHIVE_HEADER_SIZE];
        file.read_exact(&mut header_bytes)
            .context("Failed to read header bytes")?;
        let header = ArchiveHeader::from_binary(&header_bytes)
            .context("Failed to deserialize archive header")?;

        if header.magic != *ARCHIVE_MAGIC_START {
            bail!("Invalid archive magic start");
        }

        let params = ParamsBuilder::new()
            .m_cost(header.argon2_m)
            .t_cost(header.argon2_t)
            .p_cost(header.argon2_p)
            .build()
            .map_err(|e| anyhow::anyhow!("Invalid Argon2 parameters: {}", e))?;

        let key = SecureStorage::derive_key::<ARCHIVE_KEY_LEN>(password, &header.salt, params)?;
        let storage = SecureStorage::new().with_key(&*key);

        // Read and decrypt trailer from the end
        file.seek(SeekFrom::End(-(ARCHIVE_TRAILER_SIZE_LEN as i64)))?;
        let mut size_bytes = [0u8; ARCHIVE_TRAILER_SIZE_LEN];
        file.read_exact(&mut size_bytes)?;
        let encrypted_trailer_size = u32::from_le_bytes(size_bytes);

        file.seek(SeekFrom::End(
            -(ARCHIVE_TRAILER_SIZE_LEN as i64) - encrypted_trailer_size as i64,
        ))?;
        let mut encrypted_trailer = vec![0u8; encrypted_trailer_size as usize];
        file.read_exact(&mut encrypted_trailer)?;
        let decrypted_trailer = storage
            .decrypt(&encrypted_trailer)
            .context("Failed to decrypt archive trailer")?;
        let trailer = ArchiveTrailer::from_binary(&decrypted_trailer)
            .context("Failed to deserialize archive trailer")?;

        if trailer.magic_end != *ARCHIVE_MAGIC_END {
            bail!("Invalid archive magic end");
        }

        // Read and decrypt index
        file.seek(SeekFrom::Start(trailer.index_offset))?;
        let mut encrypted_index = vec![0u8; trailer.index_len as usize];
        file.read_exact(&mut encrypted_index)?;
        let decrypted_index = storage
            .decrypt(&encrypted_index)
            .context("Failed to decrypt archive index")?;
        let index = ArchiveIndex::from_binary(decrypted_index.as_ref())
            .context("Failed to deserialize archive index")?;

        // Build index map for O(1) lookups
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

    pub fn load_blob_internal(&self, id: &ID) -> Result<Vec<u8>> {
        let idx = self
            .index_map
            .get(id)
            .context("Blob not found in archive index")?;
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

    pub fn index(&self) -> &ArchiveIndex {
        &self.index
    }
}
