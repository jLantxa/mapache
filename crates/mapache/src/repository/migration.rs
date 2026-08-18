//! Migration utilities for upgrading repository format versions.
//!
//! All items in this module are temporary and should be removed when v1 is deprecated.
// TODO(v1-removal): Remove this entire module.

use std::collections::{HashMap, HashSet};

use crate::archiver::processor::is_all_zero;
use crate::backend::{Handle, StorageBackend};
use crate::common::{
    BlobType, ContentIdType, ID,
    error::{MapacheError, Result},
};
use crate::fs::tree::Tree;
use crate::repository::packer::{PackedBlobDescriptor, Packer};
use crate::repository::repo::Repository;
use crate::repository::storage::SecureStorage;

/// Re-encrypt a single pack from `old_nonce_at_end` to `new_nonce_at_end` position.
///
/// Tree blobs are NOT re-serialized here (JSON→binary is handled separately in
/// `update_tree_hierarchy`) because re-serialization changes blob IDs, which
/// cascades through the tree hierarchy. Instead, tree plaintext data is collected
/// in the returned HashMap for later processing.
pub async fn re_encrypt_pack(
    repo: &Repository,
    backend: &dyn StorageBackend,
    secure_storage: &SecureStorage,
    old_pack_id: &ID,
    old_nonce_at_end: bool,
    new_nonce_at_end: bool,
) -> Result<(ID, Vec<PackedBlobDescriptor>, HashMap<ID, Vec<u8>>)> {
    let old_path = repo.get_path(ContentIdType::Pack, old_pack_id);
    let old_handle = Handle::new(&old_path);

    let pack_data = backend.read(&old_handle, 0, 0).await?;

    let footer_len_bytes: [u8; 4] = pack_data[pack_data.len() - 4..].try_into().map_err(
        |e: std::array::TryFromSliceError| {
            MapacheError::Format(format!("invalid footer length bytes: {e}"))
        },
    )?;
    let encoded_footer_length = u32::from_le_bytes(footer_len_bytes) as usize;

    let total_len = pack_data.len();
    let data_section_end = total_len - 4 - encoded_footer_length;

    let mut descriptors = Packer::parse_footer(secure_storage, &pack_data, old_nonce_at_end, 1)?;

    tracing::debug!(target: "migrate", "Pack {}: {} blobs, data_section={} bytes, footer={} bytes",
        old_pack_id.to_short_hex(8), descriptors.len(), data_section_end, encoded_footer_length);

    let mut new_data = Vec::with_capacity(data_section_end);
    let mut new_offset = 0u32;
    let mut tree_plaintexts: HashMap<ID, Vec<u8>> = HashMap::new();

    for desc in &mut descriptors {
        if matches!(desc.blob_type, BlobType::Padding) {
            continue;
        }

        let start = desc.offset as usize;
        let end = start + desc.length as usize;
        let blob_encrypted = &pack_data[start..end];

        let plaintext = secure_storage
            .decrypt_inner(blob_encrypted, old_nonce_at_end)?
            .into_owned();

        if is_all_zero(&plaintext) {
            desc.blob_type = BlobType::Zero;
            desc.offset = 0;
            desc.length = 0;
        } else {
            if matches!(desc.blob_type, BlobType::Tree) {
                let decompressed = secure_storage.decompress(&plaintext)?;
                tree_plaintexts.insert(desc.id, decompressed);
            }

            let re_encrypted =
                secure_storage.re_encrypt(blob_encrypted, old_nonce_at_end, new_nonce_at_end)?;
            desc.offset = new_offset;
            desc.length = re_encrypted.len() as u32;
            new_offset += desc.length;
            new_data.extend_from_slice(&re_encrypted);
        }
    }

    let mut footer_descriptors: Vec<_> = descriptors
        .iter()
        .filter(|d| !matches!(d.blob_type, BlobType::Padding))
        .cloned()
        .collect();
    let footer_bytes = Packer::generate_footer(&mut footer_descriptors);
    let mut ctx = secure_storage.get_encoding_context()?;
    let new_footer =
        secure_storage.encode_with_nonce_position(&mut ctx, &footer_bytes, new_nonce_at_end)?;
    let new_footer_len = u32::try_from(new_footer.len()).map_err(|_| {
        MapacheError::Internal(format!(
            "rebuilt pack footer too large ({} bytes)",
            new_footer.len()
        ))
    })?;
    let footer_len_bytes = new_footer_len.to_le_bytes();

    let mut new_pack = new_data;
    new_pack.extend_from_slice(&new_footer);
    new_pack.extend_from_slice(&footer_len_bytes);

    let new_id = ID::from_content(&new_pack);

    let new_path = repo.get_path(ContentIdType::Pack, &new_id);
    let new_handle = Handle::new(&new_path);
    backend.write(&new_handle, new_pack.into()).await?;

    Ok((new_id, descriptors, tree_plaintexts))
}

/// Validate that a pack can be read and decrypted.
pub async fn validate_pack(
    repo: &Repository,
    backend: &dyn StorageBackend,
    secure_storage: &SecureStorage,
    pack_id: &ID,
    nonce_at_end: bool,
) -> Result<usize> {
    let descriptors =
        Packer::parse_pack_footer(repo, backend, secure_storage, pack_id, nonce_at_end).await?;
    Ok(descriptors.len())
}

/// Re-encrypt a standalone file (snapshot, index, etc.).
pub async fn re_encrypt_file(
    repo: &Repository,
    backend: &dyn StorageBackend,
    secure_storage: &SecureStorage,
    file_type: ContentIdType,
    old_id: &ID,
    old_nonce_at_end: bool,
    new_nonce_at_end: bool,
) -> Result<ID> {
    let old_path = repo.get_path(file_type, old_id);
    let data = backend.read(&Handle::new(&old_path), 0, 0).await?;
    let re_encrypted = secure_storage.re_encrypt(&data, old_nonce_at_end, new_nonce_at_end)?;
    let new_id = ID::from_content(&re_encrypted);
    let new_path = repo.get_path(file_type, &new_id);
    let new_handle = Handle::new(&new_path);
    backend.write(&new_handle, re_encrypted.into()).await?;
    Ok(new_id)
}

/// Re-encrypt a snapshot and update its root tree ID.
#[allow(clippy::too_many_arguments)]
pub async fn re_encrypt_snapshot(
    repo: &Repository,
    backend: &dyn StorageBackend,
    secure_storage: &SecureStorage,
    old_id: &ID,
    new_root_tree_id: ID,
    old_nonce_at_end: bool,
    new_nonce_at_end: bool,
) -> Result<ID> {
    let old_path = repo.get_path(ContentIdType::Snapshot, old_id);
    let data = backend.read(&Handle::new(&old_path), 0, 0).await?;

    let decrypted = secure_storage
        .decrypt_inner(&data, old_nonce_at_end)?
        .into_owned();
    let decompressed = secure_storage.decompress(&decrypted)?;
    let mut snapshot: crate::repository::snapshot::Snapshot =
        serde_json::from_slice(&decompressed)?;
    snapshot.tree = new_root_tree_id;

    let reserialized = serde_json::to_vec(&snapshot)?;
    let mut ctx = secure_storage.get_encoding_context()?;
    let re_encrypted =
        secure_storage.encode_with_nonce_position(&mut ctx, &reserialized, new_nonce_at_end)?;

    let new_id = ID::from_content(&re_encrypted);
    let new_path = repo.get_path(ContentIdType::Snapshot, &new_id);
    let new_handle = Handle::new(&new_path);
    backend.write(&new_handle, re_encrypted.into()).await?;
    Ok(new_id)
}

/// Re-serialize a tree hierarchy from JSON to binary.
///
/// Uses DFS post-order traversal: children are always re-serialized before
/// their parents, so sub-tree references can be updated in a single pass.
///
/// Returns:
/// - `root_map`: old root tree ID → new root tree ID (both are plaintext-hash IDs)
/// - `trees`: (new_id, binary_data) for each re-serialized tree
#[allow(clippy::type_complexity)]
pub fn update_tree_hierarchy(
    tree_plaintexts: &HashMap<ID, Vec<u8>>,
    root_tree_ids: &[ID],
) -> Result<(HashMap<ID, ID>, Vec<(ID, Vec<u8>)>)> {
    let mut id_map: HashMap<ID, ID> = HashMap::new();
    let mut new_trees: Vec<(ID, Vec<u8>)> = Vec::new();
    let mut visited: HashSet<ID> = HashSet::new();

    fn dfs(
        tree_id: ID,
        tree_plaintexts: &HashMap<ID, Vec<u8>>,
        id_map: &mut HashMap<ID, ID>,
        new_trees: &mut Vec<(ID, Vec<u8>)>,
        visited: &mut HashSet<ID>,
    ) -> Result<()> {
        if !visited.insert(tree_id) {
            return Ok(());
        }
        let Some(plaintext) = tree_plaintexts.get(&tree_id) else {
            return Ok(());
        };
        let mut tree: Tree = serde_json::from_slice(plaintext)?;

        for node in &tree.nodes {
            if let Some(sub_id) = node.tree {
                dfs(sub_id, tree_plaintexts, id_map, new_trees, visited)?;
            }
        }

        for node in &mut tree.nodes {
            if let Some(sub_id) = &node.tree
                && let Some(&new_sub_id) = id_map.get(sub_id)
            {
                node.tree = Some(new_sub_id);
            }
        }

        let binary = tree.to_binary()?;
        let new_id = ID::from_content(&binary);
        id_map.insert(tree_id, new_id);
        new_trees.push((new_id, binary));
        Ok(())
    }

    for &root_id in root_tree_ids {
        if let Err(e) = dfs(
            root_id,
            tree_plaintexts,
            &mut id_map,
            &mut new_trees,
            &mut visited,
        ) {
            tracing::warn!(target: "migrate", "Failed to re-serialize tree {}: {e}", root_id.to_short_hex(8));
        }
    }

    let mut root_map = HashMap::new();
    for &root_id in root_tree_ids {
        if let Some(&new_id) = id_map.get(&root_id) {
            root_map.insert(root_id, new_id);
        }
    }

    new_trees.sort_by_key(|(id, _)| *id);
    Ok((root_map, new_trees))
}

/// Create a pack from pre-encoded blobs, write it to the backend, and return
/// the pack id + descriptors for index registration.
pub async fn create_pack_from_blobs(
    repo: &Repository,
    backend: &dyn StorageBackend,
    secure_storage: &std::sync::Arc<SecureStorage>,
    blobs: &[(ID, BlobType, Vec<u8>, u64)],
) -> Result<Option<(ID, Vec<PackedBlobDescriptor>)>> {
    let mut packer = Packer::new(16 * 1024 * 1024, secure_storage.clone())?;
    for (id, blob_type, encoded, raw_size) in blobs {
        packer.add_blob(*id, *blob_type, encoded, *raw_size, true)?;
    }
    let flushed = match packer.finalize()? {
        Some(f) => f,
        None => return Ok(None),
    };
    let path = repo.get_path(ContentIdType::Pack, &flushed.id);
    backend
        .write(&Handle::new(&path), flushed.data.into())
        .await?;
    Ok(Some((flushed.id, flushed.descriptors)))
}
