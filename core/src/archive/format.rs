use serde::{Deserialize, Serialize};

use crate::mapache::{BlobType, ID};

pub const ARCHIVE_MAGIC_LEN: usize = 12;
pub const ARCHIVE_MAGIC_START: &[u8; ARCHIVE_MAGIC_LEN] = b"MAPACHE_ARC\0";
pub const ARCHIVE_MAGIC_END: &[u8; ARCHIVE_MAGIC_LEN] = b"MAPACHE_END\0";
pub const ARCHIVE_VERSION: u16 = 2;
pub const ARCHIVE_SALT_LEN: usize = 32;
pub const ARCHIVE_KEY_LEN: usize = 32;
pub const ARCHIVE_TRAILER_SIZE_LEN: usize = 4;
pub const ARCHIVE_HEADER_SIZE: usize = ARCHIVE_MAGIC_LEN
    + std::mem::size_of::<u16>()
    + ARCHIVE_SALT_LEN
    + std::mem::size_of::<u32>() * 3;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[repr(C)]
pub struct ArchiveHeader {
    pub magic: [u8; ARCHIVE_MAGIC_LEN],
    pub version: u16,
    pub salt: [u8; ARCHIVE_SALT_LEN],
    pub argon2_t: u32,
    pub argon2_m: u32,
    pub argon2_p: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[repr(C)]
pub struct ArchiveTrailer {
    pub root_tree: ID,
    pub index_offset: u64,
    pub index_len: u32,
    pub manifest_offset: u64,
    pub manifest_len: u32,
    pub magic_end: [u8; ARCHIVE_MAGIC_LEN],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveIndexEntry {
    pub id: ID,
    pub blob_type: BlobType, // 1 byte
    pub offset: u64,         // 8 bytes
    pub length: u32,         // 4 bytes
    pub raw_length: u32,     // 4 bytes
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ArchiveIndex {
    pub entries: Vec<ArchiveIndexEntry>,
}

impl ArchiveIndex {
    /// Binary serialization using bincode
    pub fn to_binary(&self) -> Vec<u8> {
        bincode::serialize(self).expect("Failed to serialize ArchiveIndex")
    }

    pub fn from_binary(bytes: &[u8]) -> anyhow::Result<Self> {
        bincode::deserialize(bytes).map_err(Into::into)
    }
}
