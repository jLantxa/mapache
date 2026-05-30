pub(crate) mod config;
pub mod defaults;
pub mod global;
pub mod hash;
pub mod traits;
pub mod vars;

use std::{
    collections::HashMap,
    io::Read,
    path::{Path, PathBuf},
    sync::{Arc, atomic::AtomicBool},
};

use anyhow::{Context, Result, bail};
use futures::StreamExt;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tokio::io::AsyncReadExt;

use crate::{
    archiver::{
        processor::chunk_and_store_file, progress::SnapshotProgress,
        tree_serializer::TreeSerializer,
    },
    fs::{
        filter::{GlobRule, PathFilter},
        node::Node,
        tree::{NodeDiff, SerializedNodeDataReader, SerializedNodeStream},
    },
    repository::{repo::Repository, snapshot::Snapshot},
    ui::SnapshotProgressReporter,
    utils::{self},
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
        hash::hash(data)
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
            let high_nibble_char = chars
                .next()
                .expect("valid hex string length guarantees enough chars");
            let low_nibble_char = chars
                .next()
                .expect("valid hex string length guarantees enough chars");

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
    Padding = 0xff,
}

impl TryFrom<u8> for BlobType {
    type Error = anyhow::Error;

    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            0x00 => Ok(BlobType::Data),
            0x01 => Ok(BlobType::Tree),
            0xff => Ok(BlobType::Padding),
            other => anyhow::bail!("invalid blob type byte: {other}"),
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

/// Finds a terminal node in a snapshot tree by name or glob.
pub async fn find_in_snapshot(
    repo: Arc<Repository>,
    snapshot: &Snapshot,
    pattern: &str,
) -> Result<Vec<(PathBuf, Node)>> {
    let root_tree_id = snapshot.tree;
    let glob_rule = GlobRule::new(Path::new(pattern));
    let mut stream =
        SerializedNodeStream::new(repo, Some(root_tree_id), PathBuf::new(), None, None).await?;
    let mut results = Vec::new();

    while let Some(res) = stream.next().await {
        let (node_path, stream_node_res) = res?;
        let stream_node = stream_node_res?;

        if glob_rule.is_strict_match(&node_path) {
            results.push((node_path, stream_node.node));
        }
    }

    Ok(results)
}

/// A bridge to convert AsyncRead into std::io::Read by blocking the thread.
/// This must only be used inside spawn_blocking.
struct BlockingBridge<R: tokio::io::AsyncRead + Unpin> {
    inner: R,
}

impl<R: tokio::io::AsyncRead + Unpin> Read for BlockingBridge<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // Use the futures executor to block on the async read operation
        futures::executor::block_on(async { self.inner.read(buf).await })
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn rewrite_snapshot_tree(
    repo: Arc<Repository>,
    snapshot: &mut Snapshot,
    excludes: Option<&Vec<PathBuf>>,
    rechunk: bool,
    mut rechunked_blobs_list_map: Option<&mut HashMap<Vec<ID>, Vec<ID>>>,
    progress: Arc<SnapshotProgress>,
    progress_reporter: Arc<dyn SnapshotProgressReporter>,
    shutdown_signal: Arc<AtomicBool>,
) -> Result<()> {
    // Canonicalize exclude paths relative to snapshot root
    let canonical_excludes: Option<Vec<PathBuf>> = excludes.map(|exclude_paths| {
        exclude_paths
            .iter()
            .map(|path| snapshot.root.join(path))
            .collect()
    });

    let path_filter = PathFilter::new(None, canonical_excludes.clone());

    // Filter paths to retain only those allowed
    let mut paths = snapshot.paths.clone();
    paths.retain(|p| path_filter.allow(p));

    let mut tree_serializer = TreeSerializer::new(repo.clone(), snapshot.root.clone(), &paths);

    // Initialize the stream of nodes from the existing snapshot
    let mut node_stream = SerializedNodeStream::new(
        repo.clone(),
        Some(snapshot.tree),
        snapshot.root.clone(),
        None,
        canonical_excludes,
    )
    .await?;

    snapshot.summary.processed_items_count = 0;
    snapshot.summary.processed_bytes = 0;

    // Iterate through the nodes in the tree
    while let Some(res) = node_stream.next().await {
        let (path, stream_node_res) = res?;
        let mut stream_node = stream_node_res?;

        let size_hint = Some(stream_node.node.metadata.size);
        progress_reporter.processing_node(&path, NodeDiff::Unchanged, size_hint);

        if stream_node.node.is_file() {
            if !rechunk {
                // Skip rechunking: just update progress
                progress.processed_bytes(stream_node.node.metadata.size);
                progress_reporter.processed_bytes(stream_node.node.metadata.size);
            } else {
                let blobs = stream_node
                    .node
                    .blobs
                    .as_ref()
                    .context("File Node must have contents")?;

                // Check if this file (set of blobs) has already been rechunked
                let rechunked_blobs = if let Some(map) = rechunked_blobs_list_map.as_deref_mut() {
                    if let Some(rechunked) = map.get(blobs) {
                        progress.processed_bytes(stream_node.node.metadata.size);
                        progress_reporter.processed_bytes(stream_node.node.metadata.size);
                        rechunked.clone()
                    } else {
                        let rechunked = run_rechunk_task(
                            repo.clone(),
                            stream_node.node.clone(),
                            progress.clone(),
                            progress_reporter.clone(),
                            shutdown_signal.clone(),
                        )
                        .await?;
                        map.insert(blobs.clone(), rechunked.clone());
                        rechunked
                    }
                } else {
                    run_rechunk_task(
                        repo.clone(),
                        stream_node.node.clone(),
                        progress.clone(),
                        progress_reporter.clone(),
                        shutdown_signal.clone(),
                    )
                    .await?
                };

                // Update the node with the new rechunked blob IDs
                stream_node.node.blobs = Some(rechunked_blobs);
            }

            snapshot.summary.processed_bytes += stream_node.node.metadata.size;
        }

        tree_serializer
            .handle_processed_item((&path, stream_node))
            .await?;

        progress_reporter.processed_node(&path, NodeDiff::Unchanged, size_hint);
        snapshot.summary.processed_items_count += 1;
    }

    tree_serializer.finalize_root().await?;
    snapshot.tree = tree_serializer
        .root_tree()
        .context("Failed to serialize root tree")?;

    Ok(())
}

/// Bridge helper to run the synchronous chunker in a background thread pool.
async fn run_rechunk_task(
    repo: Arc<Repository>,
    node: Node,
    progress: Arc<SnapshotProgress>,
    progress_reporter: Arc<dyn SnapshotProgressReporter>,
    shutdown_signal: Arc<AtomicBool>,
) -> Result<Vec<ID>> {
    let reader = SerializedNodeDataReader::new(repo.clone(), &node).await?;
    let sync_reader = BlockingBridge { inner: reader };

    let size = node.metadata.size;
    tokio::task::spawn_blocking(move || {
        chunk_and_store_file(
            repo.as_ref(),
            sync_reader,
            size,
            progress.as_ref(),
            progress_reporter.as_ref(),
            shutdown_signal.as_ref(),
        )
    })
    .await
    .map_err(|e| anyhow::anyhow!("Rechunk task panicked: {}", e))?
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
    fn test_id_serialization() {
        let id = ID::new_random();
        let json = serde_json::to_string(&id).unwrap();
        // ID should be serialized as a hex string
        assert_eq!(json, format!("\"{}\"", id.to_hex()));

        let deserialized: ID = serde_json::from_str(&json).unwrap();
        assert_eq!(id, deserialized);
    }
}
