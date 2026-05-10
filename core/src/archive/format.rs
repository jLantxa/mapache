use crate::{
    mapache::{BlobType, ID},
    utils::binary::{
        get_array, get_u8, get_u16, get_u32, get_u64, put_bytes, put_u8, put_u16, put_u32, put_u64,
    },
};

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

#[derive(Debug, Clone)]
#[repr(C)]
pub struct ArchiveHeader {
    pub magic: [u8; ARCHIVE_MAGIC_LEN],
    pub version: u16,
    pub salt: [u8; ARCHIVE_SALT_LEN],
    pub argon2_t: u32,
    pub argon2_m: u32,
    pub argon2_p: u32,
}

#[derive(Debug, Clone)]
#[repr(C)]
pub struct ArchiveTrailer {
    pub root_tree: ID,
    pub index_offset: u64,
    pub index_len: u32,
    pub manifest_offset: u64,
    pub manifest_len: u32,
    pub magic_end: [u8; ARCHIVE_MAGIC_LEN],
}

#[derive(Debug, Clone)]
pub struct ArchiveIndexEntry {
    pub id: ID,
    pub blob_type: BlobType,
    pub offset: u64,
    pub length: u32,
    pub raw_length: u32,
}

#[derive(Debug, Clone, Default)]
pub struct ArchiveIndex {
    pub entries: Vec<ArchiveIndexEntry>,
}

impl ArchiveHeader {
    pub fn to_binary(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        put_bytes(&mut buf, &self.magic);
        put_u16(&mut buf, self.version);
        put_bytes(&mut buf, &self.salt);
        put_u32(&mut buf, self.argon2_t);
        put_u32(&mut buf, self.argon2_m);
        put_u32(&mut buf, self.argon2_p);
        buf
    }

    pub fn from_binary(bytes: &[u8]) -> anyhow::Result<Self> {
        let mut cur = bytes;
        Ok(Self {
            magic: get_array(&mut cur)?,
            version: get_u16(&mut cur)?,
            salt: get_array(&mut cur)?,
            argon2_t: get_u32(&mut cur)?,
            argon2_m: get_u32(&mut cur)?,
            argon2_p: get_u32(&mut cur)?,
        })
    }
}

impl ArchiveTrailer {
    pub fn to_binary(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        put_bytes(&mut buf, self.root_tree.as_slice());
        put_u64(&mut buf, self.index_offset);
        put_u32(&mut buf, self.index_len);
        put_u64(&mut buf, self.manifest_offset);
        put_u32(&mut buf, self.manifest_len);
        put_bytes(&mut buf, &self.magic_end);
        buf
    }

    pub fn from_binary(bytes: &[u8]) -> anyhow::Result<Self> {
        let mut cur = bytes;
        Ok(Self {
            root_tree: ID::from_bytes(get_array(&mut cur)?),
            index_offset: get_u64(&mut cur)?,
            index_len: get_u32(&mut cur)?,
            manifest_offset: get_u64(&mut cur)?,
            manifest_len: get_u32(&mut cur)?,
            magic_end: get_array(&mut cur)?,
        })
    }
}

impl ArchiveIndexEntry {
    pub fn to_binary(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        put_bytes(&mut buf, self.id.as_slice());
        put_u8(&mut buf, self.blob_type as u8);
        put_u64(&mut buf, self.offset);
        put_u32(&mut buf, self.length);
        put_u32(&mut buf, self.raw_length);
        buf
    }

    pub fn from_binary(bytes: &[u8]) -> anyhow::Result<Self> {
        let mut cur = bytes;
        Ok(Self {
            id: ID::from_bytes(get_array(&mut cur)?),
            blob_type: blob_type_from_u8(get_u8(&mut cur)?)?,
            offset: get_u64(&mut cur)?,
            length: get_u32(&mut cur)?,
            raw_length: get_u32(&mut cur)?,
        })
    }
}

impl ArchiveIndex {
    pub fn to_binary(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        put_u64(&mut buf, self.entries.len() as u64);
        for entry in &self.entries {
            buf.extend_from_slice(&entry.to_binary());
        }
        buf
    }

    pub fn from_binary(bytes: &[u8]) -> anyhow::Result<Self> {
        let mut cur = bytes;
        let len = get_u64(&mut cur)? as usize;
        let mut entries = Vec::with_capacity(len);
        for _ in 0..len {
            entries.push(ArchiveIndexEntry::from_binary(
                &cur[..ArchiveIndexEntry::BINARY_SIZE],
            )?);
            cur = &cur[ArchiveIndexEntry::BINARY_SIZE..];
        }
        Ok(Self { entries })
    }
}

impl ArchiveIndexEntry {
    const BINARY_SIZE: usize = 32 + 1 + 8 + 4 + 4;
}

fn blob_type_from_u8(v: u8) -> anyhow::Result<BlobType> {
    match v {
        0x00 => Ok(BlobType::Data),
        0x01 => Ok(BlobType::Tree),
        0xff => Ok(BlobType::Padding),
        other => anyhow::bail!("invalid blob type byte: {other}"),
    }
}
