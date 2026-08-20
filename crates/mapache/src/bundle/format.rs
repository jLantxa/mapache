use crate::{
    common::error::{MapacheError, Result},
    common::{BlobType, ID},
    utils::binary::{
        get_array, get_u8, get_u16, get_u32, get_u64, put_bytes, put_u8, put_u16, put_u32, put_u64,
    },
};

pub const BUNDLE_MAGIC_LEN: usize = 12;
pub const BUNDLE_MAGIC_START: &[u8; BUNDLE_MAGIC_LEN] = b"MAPACHE_ARC\0";
pub const BUNDLE_MAGIC_END: &[u8; BUNDLE_MAGIC_LEN] = b"MAPACHE_END\0";
pub const BUNDLE_SALT_LEN: usize = 32;
pub const BUNDLE_KEY_LEN: usize = 32;
pub const BUNDLE_TRAILER_SIZE_LEN: usize = 4;
pub const BUNDLE_HEADER_SIZE: usize = BUNDLE_MAGIC_LEN
    + std::mem::size_of::<u16>()
    + BUNDLE_SALT_LEN
    + std::mem::size_of::<u32>() * 3;

/// Decrypted trailer size without ECC fields (v2 base).
pub const BUNDLE_TRAILER_BASE_SIZE: usize = 32 + 8 + 4 + 8 + 4 + 12; // 68 bytes
/// Additional bytes for ECC fields in the trailer.
pub const BUNDLE_TRAILER_ECC_EXTRA: usize = 8 + 4; // ecc_offset (u64) + ecc_len (u32)

#[derive(Debug, Clone)]
#[repr(C)]
pub struct BundleHeader {
    pub magic: [u8; BUNDLE_MAGIC_LEN],
    pub version: u16,
    pub salt: [u8; BUNDLE_SALT_LEN],
    pub argon2_t: u32,
    pub argon2_m: u32,
    pub argon2_p: u32,
}

#[derive(Debug, Clone)]
#[repr(C)]
pub struct BundleTrailer {
    pub root_tree: ID,
    pub index_offset: u64,
    pub index_len: u32,
    pub manifest_offset: u64,
    pub manifest_len: u32,
    pub ecc_offset: u64,
    pub ecc_len: u32,
    pub magic_end: [u8; BUNDLE_MAGIC_LEN],
}

#[derive(Debug, Clone)]
pub struct BundleIndexEntry {
    pub id: ID,
    pub blob_type: BlobType,
    /// Whether the blob payload is zstd-compressed. Encoded in the high bit of
    /// the type byte on disk (same as pack footers and the repository index).
    pub compressed: bool,
    pub offset: u64,
    pub length: u32,
    pub raw_length: u32,
}

#[derive(Debug, Clone, Default)]
pub struct BundleIndex {
    pub entries: Vec<BundleIndexEntry>,
}

impl BundleHeader {
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

    pub fn from_binary(bytes: &[u8]) -> Result<Self> {
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

impl BundleTrailer {
    /// Serialize the trailer. If `has_ecc` is true, includes ecc_offset and ecc_len.
    pub fn to_binary(&self, has_ecc: bool) -> Vec<u8> {
        let mut buf = Vec::new();
        put_bytes(&mut buf, self.root_tree.as_slice());
        put_u64(&mut buf, self.index_offset);
        put_u32(&mut buf, self.index_len);
        put_u64(&mut buf, self.manifest_offset);
        put_u32(&mut buf, self.manifest_len);
        if has_ecc {
            put_u64(&mut buf, self.ecc_offset);
            put_u32(&mut buf, self.ecc_len);
        }
        put_bytes(&mut buf, &self.magic_end);
        buf
    }

    /// Parse a trailer without ECC fields (68-byte decrypted payload).
    pub fn from_binary_no_ecc(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < BUNDLE_TRAILER_BASE_SIZE {
            return Err(MapacheError::Format("bundle trailer too short".to_string()));
        }
        let mut cur = bytes;
        Ok(Self {
            root_tree: ID::from_bytes(get_array(&mut cur)?),
            index_offset: get_u64(&mut cur)?,
            index_len: get_u32(&mut cur)?,
            manifest_offset: get_u64(&mut cur)?,
            manifest_len: get_u32(&mut cur)?,
            ecc_offset: 0,
            ecc_len: 0,
            magic_end: get_array(&mut cur)?,
        })
    }

    /// Parse a trailer with ECC fields (80-byte decrypted payload).
    pub fn from_binary_with_ecc(bytes: &[u8]) -> Result<Self> {
        let expected = BUNDLE_TRAILER_BASE_SIZE + BUNDLE_TRAILER_ECC_EXTRA;
        if bytes.len() < expected {
            return Err(MapacheError::Format(
                "bundle v2 trailer with ECC too short".to_string(),
            ));
        }
        let mut cur = bytes;
        Ok(Self {
            root_tree: ID::from_bytes(get_array(&mut cur)?),
            index_offset: get_u64(&mut cur)?,
            index_len: get_u32(&mut cur)?,
            manifest_offset: get_u64(&mut cur)?,
            manifest_len: get_u32(&mut cur)?,
            ecc_offset: get_u64(&mut cur)?,
            ecc_len: get_u32(&mut cur)?,
            magic_end: get_array(&mut cur)?,
        })
    }

    /// Parse a trailer, auto-detecting ECC by decrypted size.
    pub fn from_binary_auto(bytes: &[u8]) -> Result<Self> {
        let with_ecc_size = BUNDLE_TRAILER_BASE_SIZE + BUNDLE_TRAILER_ECC_EXTRA;
        if bytes.len() >= with_ecc_size {
            // Try ECC version first — the extra bytes could be part of the
            // magic_end if there's no ECC, so we validate magic.
            let trailer = Self::from_binary_with_ecc(bytes)?;
            if trailer.magic_end == *BUNDLE_MAGIC_END {
                return Ok(trailer);
            }
        }
        // Fall back to no-ECC parsing.
        Self::from_binary_no_ecc(bytes)
    }
}

impl BundleIndexEntry {
    pub fn to_binary(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        put_bytes(&mut buf, self.id.as_slice());
        put_u8(&mut buf, self.blob_type.to_byte(self.compressed));
        put_u64(&mut buf, self.offset);
        put_u32(&mut buf, self.length);
        put_u32(&mut buf, self.raw_length);
        buf
    }

    pub fn from_binary(bytes: &[u8]) -> Result<Self> {
        let mut cur = bytes;
        let id = ID::from_bytes(get_array(&mut cur)?);
        let (blob_type, compressed) = BlobType::from_byte(get_u8(&mut cur)?)?;
        let offset = get_u64(&mut cur)?;
        let length = get_u32(&mut cur)?;
        let raw_length = get_u32(&mut cur)?;
        Ok(Self {
            id,
            blob_type,
            compressed,
            offset,
            length,
            raw_length,
        })
    }
}

impl BundleIndex {
    pub fn to_binary(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        put_u64(&mut buf, self.entries.len() as u64);
        for entry in &self.entries {
            buf.extend_from_slice(&entry.to_binary());
        }
        buf
    }

    pub fn from_binary(bytes: &[u8]) -> Result<Self> {
        let mut cur = bytes;
        let len = get_u64(&mut cur)? as usize;
        let mut entries = Vec::with_capacity(len);
        for _ in 0..len {
            if cur.len() < BundleIndexEntry::BINARY_SIZE {
                return Err(MapacheError::Format(format!(
                    "truncated bundle index: need {} bytes, have {}",
                    BundleIndexEntry::BINARY_SIZE,
                    cur.len()
                )));
            }
            entries.push(BundleIndexEntry::from_binary(
                &cur[..BundleIndexEntry::BINARY_SIZE],
            )?);
            cur = &cur[BundleIndexEntry::BINARY_SIZE..];
        }
        Ok(Self { entries })
    }
}

impl BundleIndexEntry {
    const BINARY_SIZE: usize = 32 + 1 + 8 + 4 + 4;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_entry_round_trip_all_blob_types() {
        for blob_type in [
            BlobType::Data,
            BlobType::Tree,
            BlobType::Zero,
            BlobType::Padding,
        ] {
            let entry = BundleIndexEntry {
                id: ID::new_random(),
                blob_type,
                compressed: true,
                offset: 12345,
                length: 500,
                raw_length: 480,
            };
            let bytes = entry.to_binary();
            assert_eq!(bytes.len(), BundleIndexEntry::BINARY_SIZE);
            let restored = BundleIndexEntry::from_binary(&bytes).unwrap();
            assert_eq!(restored.id, entry.id);
            assert_eq!(restored.blob_type, entry.blob_type);
            assert_eq!(restored.compressed, entry.compressed);
            assert_eq!(restored.offset, entry.offset);
            assert_eq!(restored.length, entry.length);
            assert_eq!(restored.raw_length, entry.raw_length);
        }
    }

    #[test]
    fn index_entry_invalid_blob_type_byte() {
        let mut bytes = [0u8; BundleIndexEntry::BINARY_SIZE];
        bytes[32] = 0x42; // invalid blob type
        let result = BundleIndexEntry::from_binary(&bytes);
        assert!(result.is_err());
    }

    #[test]
    fn index_from_binary_truncated_input() {
        let mut buf = Vec::new();
        put_u64(&mut buf, 1); // len = 1
        // No entry bytes follow — should return an error, not panic.
        let result = BundleIndex::from_binary(&buf);
        assert!(result.is_err());
    }

    #[test]
    fn index_from_binary_truncated_middle_of_entry() {
        let mut buf = Vec::new();
        put_u64(&mut buf, 2); // len = 2
        // Full first entry
        let entry = BundleIndexEntry {
            id: ID::new_random(),
            blob_type: BlobType::Data,
            compressed: true,
            offset: 0,
            length: 100,
            raw_length: 90,
        };
        buf.extend_from_slice(&entry.to_binary());
        // Only 5 bytes of the second entry
        buf.extend_from_slice(&[0u8; 5]);
        let result = BundleIndex::from_binary(&buf);
        assert!(result.is_err());
    }

    #[test]
    fn trailer_round_trip_no_ecc() {
        let trailer = BundleTrailer {
            root_tree: ID::new_random(),
            index_offset: 1024,
            index_len: 512,
            manifest_offset: 2048,
            manifest_len: 256,
            ecc_offset: 0,
            ecc_len: 0,
            magic_end: *BUNDLE_MAGIC_END,
        };
        let bytes = trailer.to_binary(false);
        let restored = BundleTrailer::from_binary_no_ecc(&bytes).unwrap();
        assert_eq!(trailer.root_tree, restored.root_tree);
        assert_eq!(trailer.index_offset, restored.index_offset);
        assert_eq!(trailer.index_len, restored.index_len);
        assert_eq!(trailer.manifest_offset, restored.manifest_offset);
        assert_eq!(trailer.manifest_len, restored.manifest_len);
        assert_eq!(restored.ecc_offset, 0);
        assert_eq!(restored.ecc_len, 0);
        assert_eq!(trailer.magic_end, restored.magic_end);
    }

    #[test]
    fn trailer_round_trip_with_ecc() {
        let trailer = BundleTrailer {
            root_tree: ID::new_random(),
            index_offset: 2048,
            index_len: 1024,
            manifest_offset: 4096,
            manifest_len: 512,
            ecc_offset: 1024,
            ecc_len: 768,
            magic_end: *BUNDLE_MAGIC_END,
        };
        let bytes = trailer.to_binary(true);
        let restored = BundleTrailer::from_binary_with_ecc(&bytes).unwrap();
        assert_eq!(trailer.root_tree, restored.root_tree);
        assert_eq!(trailer.index_offset, restored.index_offset);
        assert_eq!(trailer.index_len, restored.index_len);
        assert_eq!(trailer.manifest_offset, restored.manifest_offset);
        assert_eq!(trailer.manifest_len, restored.manifest_len);
        assert_eq!(trailer.ecc_offset, restored.ecc_offset);
        assert_eq!(trailer.ecc_len, restored.ecc_len);
        assert_eq!(trailer.magic_end, restored.magic_end);
    }

    #[test]
    fn trailer_auto_detect_no_ecc() {
        let trailer = BundleTrailer {
            root_tree: ID::new_random(),
            index_offset: 1024,
            index_len: 512,
            manifest_offset: 2048,
            manifest_len: 256,
            ecc_offset: 0,
            ecc_len: 0,
            magic_end: *BUNDLE_MAGIC_END,
        };
        let bytes = trailer.to_binary(false);
        // auto_detect should parse as no-ECC
        let restored = BundleTrailer::from_binary_auto(&bytes).unwrap();
        assert_eq!(trailer.root_tree, restored.root_tree);
        assert_eq!(trailer.index_offset, restored.index_offset);
        assert_eq!(restored.ecc_offset, 0);
        assert_eq!(restored.ecc_len, 0);
    }

    #[test]
    fn trailer_auto_detect_with_ecc() {
        let trailer = BundleTrailer {
            root_tree: ID::new_random(),
            index_offset: 2048,
            index_len: 1024,
            manifest_offset: 4096,
            manifest_len: 512,
            ecc_offset: 1024,
            ecc_len: 768,
            magic_end: *BUNDLE_MAGIC_END,
        };
        let bytes = trailer.to_binary(true);
        // auto_detect should detect the ECC version
        let restored = BundleTrailer::from_binary_auto(&bytes).unwrap();
        assert_eq!(trailer.root_tree, restored.root_tree);
        assert_eq!(trailer.ecc_offset, restored.ecc_offset);
        assert_eq!(trailer.ecc_len, restored.ecc_len);
    }

    #[test]
    fn trailer_no_ecc_too_short() {
        let result = BundleTrailer::from_binary_no_ecc(&[0u8; 10]);
        assert!(result.is_err());
    }

    #[test]
    fn trailer_with_ecc_too_short() {
        let result = BundleTrailer::from_binary_with_ecc(&[0u8; 10]);
        assert!(result.is_err());
    }

    #[test]
    fn trailer_auto_detect_falls_back_to_no_ecc() {
        // Build a no-ECC trailer that happens to be >= with_ecc_size
        // by padding the magic_end bytes into the "extra" field position.
        let trailer = BundleTrailer {
            root_tree: ID::new_random(),
            index_offset: 1024,
            index_len: 512,
            manifest_offset: 2048,
            manifest_len: 256,
            ecc_offset: 0,
            ecc_len: 0,
            magic_end: *BUNDLE_MAGIC_END,
        };
        let mut bytes = trailer.to_binary(false);
        // Pad with zeros to exceed the ECC trailer size so auto_detect tries
        // ECC first. The magic_end won't match, so it falls back to no_ecc.
        bytes.extend_from_slice(&[0u8; 20]);
        let restored = BundleTrailer::from_binary_auto(&bytes).unwrap();
        assert_eq!(trailer.root_tree, restored.root_tree);
    }
}
