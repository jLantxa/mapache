pub mod defaults;
pub mod global;
pub mod vars;

use std::{collections::HashMap, path::PathBuf, sync::Arc};

use anyhow::{Context, Result, bail};
use num_enum::FromPrimitive;
use rand::{TryRngCore, rngs::OsRng};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
    archiver::{processor::chunk_and_store_file, tree_serializer::TreeSerializer},
    fs::{
        node::Node,
        tree::{NodeDiff, SerializedNodeDataReader, SerializedNodeStream},
    },
    repository::{repo::Repository, snapshot::Snapshot},
    ui::snapshot_progress::SnapshotProgressReporter,
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
        let mut random_bytes: Hash256 = Default::default();
        if let Err(e) = OsRng.try_fill_bytes(&mut random_bytes) {
            panic!("Error: {e}");
        }

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

        if !hex_len.is_multiple_of(2) {
            bail!("Hex string has an odd length");
        }

        let mut bytes = [0; ID_LENGTH];
        let mut chars = hex_str.chars();

        for byte in bytes.iter_mut().take(ID_LENGTH) {
            let high_nibble_char = chars.next().unwrap(); // Should be OK due to length check
            let low_nibble_char = chars.next().unwrap(); // Should be OK due to length check

            let high_nibble = Self::hex_char_to_byte(high_nibble_char)
                .with_context(|| format!("Invalid hexadecimal character: '{high_nibble_char}'"))?;
            let low_nibble = Self::hex_char_to_byte(low_nibble_char)
                .with_context(|| format!("Invalid hexadecimal character: '{low_nibble_char}'"))?;

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
#[derive(Debug, Default, Clone, Copy, PartialEq, Serialize, Deserialize, FromPrimitive)]
#[repr(u8)]
pub enum BlobType {
    Data = 0x00,
    Tree = 0x01,

    /// A padding blob descriptor used for obfuscation. This blob is fake and must be ignored.
    #[default]
    Padding = 0xff,
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

/// Rewrite a snapshot tree. This function can remove exclude paths or rechunk
/// files from already existing snapshots.
pub(crate) fn rewrite_snapshot_tree(
    repo: Arc<Repository>,
    snapshot: &mut Snapshot,
    excludes: Option<Vec<PathBuf>>,
    rechunk: bool,
    mut rechunked_blobs_list_map: Option<&mut HashMap<Vec<ID>, Vec<ID>>>,
    progress_reporter: Arc<SnapshotProgressReporter>,
) -> Result<(u64, u64)> {
    let (mut raw_bytes, mut encoded_bytes) = (0, 0);

    // Cannonicalize the exclude paths and filter the source paths using the excludes
    // This is a simulated cannonical path, since we don't refer to a path in the host,
    // but rather a relative path in the snapshot tree. We can just append the relative path
    // to the snapshot root.
    let cannonical_excludes: Option<Vec<PathBuf>> = if let Some(exclude_paths) = &excludes {
        let mut canonicalized_vec = Vec::new();
        for path in exclude_paths {
            canonicalized_vec.push(snapshot.root.join(path));
        }
        Some(canonicalized_vec)
    } else {
        None
    };

    let mut paths = snapshot.paths.clone();
    paths.retain(|p| utils::filter_path(p, None, cannonical_excludes.as_ref()));

    let mut tree_serializer = TreeSerializer::new(repo.clone(), snapshot.root.clone(), &paths);
    let node_streamer = SerializedNodeStream::new(
        repo.clone(),
        Some(snapshot.tree),
        snapshot.root.clone(),
        None,
        cannonical_excludes.clone(),
    )?;

    snapshot.summary.processed_items_count = 0;
    snapshot.summary.processed_bytes = 0;

    for (path, mut stream_node) in node_streamer.flatten() {
        progress_reporter.processing_node(&path, NodeDiff::Unchanged);

        if stream_node.node.is_file() {
            if !rechunk {
                // If not rechunking, just report processed bytes
                progress_reporter.processed_bytes(stream_node.node.metadata.size);
            } else {
                let blobs = stream_node
                    .node
                    .blobs
                    .as_ref()
                    .with_context(|| "File Node must have contents (even if empty)")?;

                let rechunk_node = |node: &Node| -> Result<Vec<ID>> {
                    let mut blob_data_reader = SerializedNodeDataReader::new(repo.clone(), node)?;
                    chunk_and_store_file(
                        repo.clone(),
                        &mut blob_data_reader,
                        &stream_node.node,
                        progress_reporter.clone(),
                    )
                };

                let rechunked_blobs = if let Some(map) = rechunked_blobs_list_map.as_deref_mut() {
                    match map.entry(blobs.clone()) {
                        std::collections::hash_map::Entry::Occupied(entry) => {
                            // The file was already rechunked, so we can skip.
                            progress_reporter.processed_bytes(stream_node.node.metadata.size);
                            entry.get().clone()
                        }
                        std::collections::hash_map::Entry::Vacant(entry) => {
                            // The file was not rechunked yet, so we do it and insert the lists to the map.
                            let rechunked = rechunk_node(&stream_node.node)?;
                            entry.insert(rechunked.clone());
                            rechunked
                        }
                    }
                } else {
                    rechunk_node(&stream_node.node)?
                };

                // Finally rewrite blob list
                stream_node.node.blobs = Some(rechunked_blobs);
            } // Finished rechunking

            snapshot.summary.processed_bytes += stream_node.node.metadata.size;
        }

        progress_reporter.processed_node(&path);
        snapshot.summary.processed_items_count += 1;

        // The path is not excluded, so we add the node to the pending trees map.
        let (raw, encoded) = tree_serializer.handle_processed_item((&path, stream_node))?;
        raw_bytes += raw;
        encoded_bytes += encoded;
    }

    let _ = tree_serializer.finalize_root()?;
    let (raw_meta, encoded_meta) = repo.flush()?;
    raw_bytes += raw_meta;
    encoded_bytes += encoded_meta;

    // Increase meta counters in snapshot summary
    snapshot.summary.meta_raw_bytes += raw_bytes;
    snapshot.summary.meta_encoded_bytes += encoded_bytes;
    snapshot.summary.total_raw_bytes += raw_bytes;
    snapshot.summary.total_encoded_bytes += encoded_bytes;

    let root_tree_id = tree_serializer.root_tree();
    match root_tree_id {
        Some(new_tree_id) => snapshot.tree = new_tree_id,
        None => bail!("Failed to serialize new snapshot tree"),
    }

    Ok((raw_bytes, encoded_bytes))
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
