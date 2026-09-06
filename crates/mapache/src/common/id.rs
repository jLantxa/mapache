use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
    common::error::{MapacheError, Result},
    utils,
};

pub const ID_LENGTH: usize = 32;
pub type Hash256 = [u8; ID_LENGTH];

/// This is an ID that identifies object by its content.
#[derive(Default, Hash, Clone, Copy, Eq, PartialEq, PartialOrd, Ord)]
pub struct ID(pub(crate) Hash256);

impl ID {
    /// Creates a new, random ID.
    pub fn new_random() -> Self {
        Self(rand::random())
    }

    /// Constructs an ID from a slice.
    pub fn from_bytes(bytes: Hash256) -> Self {
        Self(bytes)
    }

    pub fn from_content<T: AsRef<[u8]>>(data: T) -> Self {
        super::hash::hash(data)
    }

    /// Verifies that the given content hashes to this ID.
    pub fn verify_content<T: AsRef<[u8]>>(&self, data: T) -> Result<()> {
        if Self::from_content(data) == *self {
            Ok(())
        } else {
            Err(MapacheError::Integrity(format!(
                "blob content hash does not match requested ID {}",
                self.to_hex()
            )))
        }
    }

    /// Converts the ID to a hex String.
    pub fn to_hex(&self) -> String {
        utils::bytes_to_hex(&self.0)
    }

    /// Convert to hex String with `len` bytes
    pub fn to_short_hex(&self, len: usize) -> String {
        utils::bytes_to_hex(&self.0[..len])
    }

    /// Helper function to convert a hex char into a byte.
    fn hex_char_to_byte(c: char) -> Option<u8> {
        match c {
            '0'..='9' => Some(c as u8 - b'0'),
            'a'..='f' => Some(c as u8 - b'a' + 10),
            'A'..='F' => Some(c as u8 - b'A' + 10),
            _ => None,
        }
    }

    /// Converts a hex string into an ID.
    /// Returns an `Err` if the string is not valid hex or not the correct length.
    pub fn from_hex(hex_str: &str) -> Result<Self> {
        let expected_len = ID_LENGTH * 2;
        let hex_len = hex_str.len();
        if hex_len != expected_len {
            return Err(MapacheError::Format(format!(
                "invalid ID length: expected {} hex characters ({} bytes), found {} hex characters ({} bytes)",
                expected_len,
                expected_len / 2,
                hex_len,
                hex_len / 2
            )));
        }

        let mut bytes = [0; ID_LENGTH];
        let mut chars = hex_str.chars();

        for byte in bytes.iter_mut() {
            let high_nibble_char = chars
                .next()
                .ok_or_else(|| MapacheError::Format("unexpected end of hex string".to_string()))?;
            let low_nibble_char = chars
                .next()
                .ok_or_else(|| MapacheError::Format("unexpected end of hex string".to_string()))?;

            let high_nibble = Self::hex_char_to_byte(high_nibble_char).ok_or_else(|| {
                MapacheError::Format(format!(
                    "invalid hexadecimal character: '{high_nibble_char}'"
                ))
            })?;
            let low_nibble = Self::hex_char_to_byte(low_nibble_char).ok_or_else(|| {
                MapacheError::Format(format!(
                    "invalid hexadecimal character: '{low_nibble_char}'"
                ))
            })?;

            *byte = (high_nibble << 4) | low_nibble;
        }

        Ok(Self(bytes))
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

/// Implementation of the Display trait for ID.
impl std::fmt::Display for ID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

/// Implementation of Debug for ID.
impl std::fmt::Debug for ID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

/// Implement serde `Serialize` for ID.
impl Serialize for ID {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_hex())
    }
}

/// Implement serde `Deserialize` for `ID` to deserialize it from a hex String.
impl<'de> Deserialize<'de> for ID {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        ID::from_hex(&s).map_err(serde::de::Error::custom)
    }
}

/// Type of objects that can be stored in a repository.
#[derive(Debug, Default, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[repr(u8)]
pub enum BlobType {
    Data = 0x00,
    Tree = 0x01,

    /// A padding blob descriptor used for obfuscation. This blob is fake and must be ignored.
    #[default]
    Padding = 0x02,

    /// A zero-filled blob. Used to deduplicate all-zero regions across files
    /// without storing actual data in packs.
    Zero = 0x03,
}

impl BlobType {
    pub fn is_pack_stored(self) -> bool {
        matches!(self, BlobType::Data | BlobType::Tree)
    }

    /// Encodes this type into its on-disk byte, setting the high bit when the
    /// blob's payload is zstd-compressed.
    ///
    /// The compression *algorithm* is repo-wide (declared in the manifest), so
    /// a single compressed/uncompressed bit is enough here.
    #[inline]
    pub(crate) fn to_byte(self, compressed: bool) -> u8 {
        self as u8 | ((compressed as u8) << 7)
    }

    /// Decodes a type byte written by [`BlobType::to_byte`], returning the
    /// type and whether the payload is compressed.
    #[inline]
    pub(crate) fn from_byte(byte: u8) -> Result<(Self, bool)> {
        // TODO(v1-removal): legacy alias — v1 footers wrote Padding as 0xff.
        // Remove this branch together with all v1 support.
        if byte == 0xff {
            return Ok((BlobType::Padding, false));
        }
        let compressed = byte & 0x80 != 0;
        Ok((Self::try_from(byte & 0x7f)?, compressed))
    }
}

impl TryFrom<u8> for BlobType {
    type Error = MapacheError;

    fn try_from(v: u8) -> std::result::Result<Self, Self::Error> {
        match v {
            0x00 => Ok(BlobType::Data),
            0x01 => Ok(BlobType::Tree),
            0x02 => Ok(BlobType::Padding),
            0x03 => Ok(BlobType::Zero),
            other => Err(MapacheError::Format(format!(
                "invalid blob type byte: {other}"
            ))),
        }
    }
}

/// Type of content-addressable objects that can be stored in a Repository
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum ContentIdType {
    Pack,
    Snapshot,
    Index,
    Key,
    Lock,
}

// Implement the Display trait for ContentIdType
impl std::fmt::Display for ContentIdType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ContentIdType::Pack => write!(f, "pack"),
            ContentIdType::Snapshot => write!(f, "snapshot"),
            ContentIdType::Index => write!(f, "index"),
            ContentIdType::Key => write!(f, "key"),
            ContentIdType::Lock => write!(f, "lock"),
        }
    }
}

pub enum SaveID {
    /// Let the callee calculate the ID
    CalculateID,
    /// Use a precalculated ID
    WithID(ID),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_id_new_random() {
        let id1 = ID::new_random();
        let id2 = ID::new_random();
        assert_ne!(id1, id2, "Random IDs should be different");
        assert_eq!(id1.0.len(), ID_LENGTH);
    }

    #[test]
    fn test_id_from_bytes() {
        let bytes = [0x01; ID_LENGTH];
        let id = ID::from_bytes(bytes);
        assert_eq!(id.0, bytes);
    }

    #[test]
    fn test_id_to_hex() {
        let bytes = [
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
            0xee, 0xff, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98,
            0x76, 0x54, 0x32, 0x10,
        ];
        let id = ID::from_bytes(bytes);
        let expected_hex = "00112233445566778899aabbccddeeff0123456789abcdeffedcba9876543210";
        assert_eq!(id.to_hex(), expected_hex);
    }

    #[test]
    fn test_id_to_short_hex() {
        let bytes = [
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
            0xee, 0xff, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98,
            0x76, 0x54, 0x32, 0x10,
        ];
        let id = ID::from_bytes(bytes);
        let expected_hex = "00112233445566778899aabbccddeeff0123456789abcdeffedcba9876543210";
        assert_eq!(id.to_short_hex(4), expected_hex[0..2 * 4]);
        assert_eq!(id.to_short_hex(5), expected_hex[0..2 * 5]);
        assert_eq!(id.to_short_hex(9), expected_hex[0..2 * 9]);
        assert_eq!(id.to_short_hex(12), expected_hex[0..2 * 12]);
    }

    #[test]
    fn test_id_from_hex_valid() {
        let hex_str = "00112233445566778899aabbccddeeff0123456789abcdeffedcba9876543210";
        let id = ID::from_hex(hex_str).unwrap();
        let expected_bytes = [
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
            0xee, 0xff, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98,
            0x76, 0x54, 0x32, 0x10,
        ];
        assert_eq!(id.0, expected_bytes);
    }

    #[test]
    fn test_id_from_hex_roundtrip() {
        let original_id = ID::new_random();
        let hex_str = original_id.to_hex();
        let parsed_id = ID::from_hex(&hex_str).unwrap();
        assert_eq!(original_id, parsed_id);
    }

    #[test]
    fn test_id_from_hex_invalid_length_too_short() {
        let hex_str = "001122"; // Too short (expected 64 chars)
        let id_result = ID::from_hex(hex_str);
        assert!(id_result.is_err());
    }

    #[test]
    fn test_id_from_hex_invalid_length_too_long() {
        let hex_str = "00112233445566778899aabbccddeeff0123456789abcdefedcba9876543210AA"; // Too long
        let id_result = ID::from_hex(hex_str);
        assert!(id_result.is_err());
    }

    #[test]
    fn test_id_from_hex_odd_length() {
        let hex_str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcde"; // 63 chars
        let id_result = ID::from_hex(hex_str);
        assert!(id_result.is_err());
    }

    #[test]
    fn test_id_from_hex_invalid_character() {
        let hex_str = "00112233445566778899aabbccddeeff0123456789abcdefedcba987654321G"; // 'G' is invalid
        let id_result = ID::from_hex(hex_str);
        assert!(id_result.is_err());
    }

    #[test]
    fn test_id_from_hex_empty_string() {
        let id_result = ID::from_hex("");
        assert!(id_result.is_err());
    }

    #[test]
    fn test_id_from_hex_boundary_lengths() {
        // Exactly 63 chars (1 byte short)
        let short = "00".repeat(31) + "0";
        assert!(ID::from_hex(&short).is_err());

        // Exactly 65 chars (1 byte over)
        let long = "00".repeat(32) + "00";
        assert!(ID::from_hex(&long).is_err());
    }

    #[test]
    fn test_id_all_zeros() {
        let hex = "0".repeat(64);
        let id = ID::from_hex(&hex).unwrap();
        assert_eq!(id.0, [0x00; 32]);
        assert_eq!(id.to_hex(), hex);
    }

    #[test]
    fn test_id_all_ff() {
        let hex = "f".repeat(64);
        let id = ID::from_hex(&hex).unwrap();
        assert_eq!(id.0, [0xFF; 32]);
        assert_eq!(id.to_hex(), hex);
    }

    #[test]
    fn test_id_uppercase_hex_roundtrip() {
        let lower = "00112233445566778899aabbccddeeff0123456789abcdeffedcba9876543210";
        let upper = lower.to_uppercase();
        let id = ID::from_hex(&upper).unwrap();
        assert_eq!(id.to_hex(), lower);
    }

    #[test]
    fn test_id_partial_uppercase() {
        let lower = "00112233445566778899aabbccddeeff0123456789abcdeffedcba9876543210";
        let mixed = lower[..20].to_uppercase() + &lower[20..];
        let id = ID::from_hex(&mixed).unwrap();
        assert_eq!(id.to_hex(), lower);
    }

    #[test]
    fn test_id_from_bytes_zeroes_and_max() {
        let zeroes = ID::from_bytes([0x00; 32]);
        assert_eq!(zeroes.to_hex(), "0".repeat(64));

        let maxes = ID::from_bytes([0xFF; 32]);
        assert_eq!(maxes.to_hex(), "f".repeat(64));
    }

    #[test]
    fn test_id_verify_content_rejects_mismatch() {
        let id = ID::from_content(b"expected content");
        let err = id.verify_content(b"different content").unwrap_err();
        assert!(matches!(err, MapacheError::Integrity(_)));
    }

    #[test]
    fn test_id_verify_content_zero_blob() {
        let data = vec![0u8; 4096];
        let id = ID::from_content(&data);
        assert!(id.verify_content(&data).is_ok());
        let wrong = vec![0u8; 4095];
        assert!(id.verify_content(&wrong).is_err());
    }

    #[test]
    fn test_id_serialization() {
        let id = ID::new_random();
        let json = serde_json::to_string(&id).unwrap();
        // ID should be serialized as a hex string
        assert_eq!(json, format!("\"{}\"", id.to_hex()));

        let deserialized: ID = serde_json::from_str(&json).unwrap();
        assert_eq!(id, deserialized);
    }
}
