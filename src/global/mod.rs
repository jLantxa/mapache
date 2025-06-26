// [backup] is an incremental backup tool
// Copyright (C) 2025  Javier Lancha Vázquez <javier.lancha@gmail.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

pub mod defaults;

use std::sync::LazyLock;

use aes_gcm::aead::{OsRng, rand_core::RngCore};
use anyhow::{Context, Result, bail};
use parking_lot::{RwLock, RwLockReadGuard};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{commands::GlobalArgs, global::defaults::DEFAULT_VERBOSITY, utils};

pub const ID_LENGTH: usize = 32;
pub type Hash256 = [u8; ID_LENGTH];

pub struct GlobalOpts {
    pub verbosity: u32,
}

impl Default for GlobalOpts {
    fn default() -> Self {
        Self {
            verbosity: DEFAULT_VERBOSITY,
        }
    }
}

pub static GLOBAL_OPTS: LazyLock<RwLock<Option<GlobalOpts>>> =
    LazyLock::new(|| RwLock::new(Some(GlobalOpts::default())));

pub fn set_global_opts_with_args(global_args: &GlobalArgs) {
    let verbosity = if global_args.quiet {
        0
    } else if let Some(v) = global_args.verbosity {
        v
    } else {
        DEFAULT_VERBOSITY
    };

    let new_opts = GlobalOpts { verbosity };

    let mut opts_guard = GLOBAL_OPTS.write();
    *opts_guard = Some(new_opts);
}

pub fn global_opts() -> RwLockReadGuard<'static, Option<GlobalOpts>> {
    GLOBAL_OPTS.read()
}

/// This is an ID that identifies object by its content.
#[derive(Hash, Clone, Eq, PartialEq)]
pub struct ID(Hash256);

impl ID {
    /// Creates a new, random ID.
    pub fn new_random() -> Self {
        let mut random_bytes: Hash256 = Default::default();
        OsRng.fill_bytes(&mut random_bytes);
        Self(random_bytes)
    }

    /// Constructs an ID from a slice.
    pub fn from_bytes(bytes: [u8; ID_LENGTH]) -> Self {
        Self(bytes)
    }

    pub fn from_content<T: AsRef<[u8]>>(data: T) -> Self {
        Self(utils::calculate_hash(data))
    }

    /// Converts the ID to a hex String.
    pub fn to_hex(&self) -> String {
        utils::bytes_to_hex(&self.0)
    }

    /// Convert to hex String with `len` bytes
    pub fn to_short_hex(&self, len: usize) -> String {
        utils::bytes_to_hex(&self.0[0..(len)]).to_string()
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
        let expected_len = ID_LENGTH * 2; // Each byte is 2 hex characters
        let hex_len = hex_str.len();
        if hex_len != expected_len {
            bail!(format!(
                "Invalid ID length: expected {} hex characters ({} bytes), found {} hex characters ({} bytes)",
                expected_len,
                expected_len / 2,
                hex_len,
                hex_len / 2
            ));
        }

        if hex_len % 2 != 0 {
            bail!("Hex string has an odd length");
        }

        let mut bytes = [0; ID_LENGTH];
        let mut chars = hex_str.chars();

        for byte in bytes.iter_mut().take(ID_LENGTH) {
            let high_nibble_char = chars.next().unwrap(); // Should be OK due to length check
            let low_nibble_char = chars.next().unwrap(); // Should be OK due to length check

            let high_nibble = Self::hex_char_to_byte(high_nibble_char).with_context(|| {
                format!("Invalid hexadecimal character: '{}'", high_nibble_char)
            })?;
            let low_nibble = Self::hex_char_to_byte(low_nibble_char)
                .with_context(|| format!("Invalid hexadecimal character: '{}'", low_nibble_char))?;

            *byte = (high_nibble << 4) | low_nibble;
        }

        Ok(Self(bytes))
    }
}

/// Implementation of the Display trait for ID.
impl std::fmt::Display for ID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

/// Implementation  of Debug for ID.
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ObjectType {
    Data,
    Tree,
}

/// Type of objects that can be stored in a Repository
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum FileType {
    Object,
    Snapshot,
    Index,
    Key,
    Manifest,
}

// Implement the Display trait for FileType
impl std::fmt::Display for FileType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileType::Object => write!(f, "object"),
            FileType::Snapshot => write!(f, "snapshot"),
            FileType::Index => write!(f, "index"),
            FileType::Key => write!(f, "key"),
            FileType::Manifest => write!(f, "manifest"),
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
}
