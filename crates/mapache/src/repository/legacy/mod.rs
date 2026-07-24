use crate::{
    common::error::{MapacheError, Result},
    fs::tree::Tree,
    repository::index::IndexFile,
};

/// Serialize an `IndexFile` to JSON (repository format v1).
pub fn serialize_index_json(index_file: &IndexFile) -> Result<Vec<u8>> {
    serde_json::to_vec(index_file).map_err(MapacheError::Serialization)
}

/// Deserialize an `IndexFile` from JSON (repository format v1).
pub fn deserialize_index_json(data: &[u8]) -> Result<IndexFile> {
    serde_json::from_slice(data)
        .map_err(|e| MapacheError::Format(format!("failed to deserialize index: {e}")))
}

/// Serialize a `Tree` to JSON (repository format v1).
pub fn serialize_tree_json(tree: &Tree) -> Result<Vec<u8>> {
    serde_json::to_vec(tree).map_err(MapacheError::Serialization)
}

/// Deserialize a `Tree` from JSON (repository format v1).
pub fn deserialize_tree_json(bytes: &[u8]) -> Result<Tree> {
    serde_json::from_slice(bytes)
        .map_err(|e| MapacheError::Format(format!("failed to deserialize tree: {e}")))
}
