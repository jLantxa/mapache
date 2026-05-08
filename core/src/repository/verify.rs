use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};

use crate::{
    backend::StorageBackend,
    mapache::{ContentIdType, ID, defaults::DEFAULT_RESTORE_PACK_SEGMENT_MAX_SIZE},
    repository::{packer::Packer, repo::Repository, storage::SecureStorage},
    utils::collections::IdSet,
};

pub struct PackStats {
    pub dangling: usize,
    pub verified_blobs: usize,
    pub corrupt_blobs: Vec<ID>,
    pub bytes_processed: u64,
    pub bit_rot: bool,
}

/// Verify the checksum and contents of a pack.
///
/// This performs a "Physical Verification" with constant memory usage.
pub async fn verify_pack(
    repo: Arc<Repository>,
    backend: Arc<dyn StorageBackend>,
    secure_storage: Arc<SecureStorage>,
    pack_id: ID,
) -> Result<PackStats> {
    let pack_path = repo.get_path(ContentIdType::Pack, &pack_id);

    // Get footer first to know blob locations.
    let pack_header =
        Packer::parse_pack_footer(repo.as_ref(), backend.as_ref(), &secure_storage, &pack_id)
            .await?;

    // Get pack size
    let attr = backend.lstat(&pack_path).await?;
    let pack_size = attr.size.ok_or_else(|| anyhow!("Pack size unknown"))?;

    let mut bit_rot_hasher = crate::mapache::hash::Hasher::new();
    let mut current_file_offset: u64 = 0;
    const CHUNK_SIZE: usize = DEFAULT_RESTORE_PACK_SEGMENT_MAX_SIZE as usize;

    let mut verified_blobs = 0;
    let mut corrupt_blobs = Vec::new();
    let mut bytes_processed = 0;

    let mut next_blob_idx = 0;
    let mut current_blob_data = Vec::with_capacity(CHUNK_SIZE); // Reuse buffer for blobs

    // Process the pack file sequentially
    while current_file_offset < pack_size {
        let to_read = (pack_size - current_file_offset).min(CHUNK_SIZE as u64) as usize;
        let chunk = backend
            .read(
                &crate::backend::Handle::new(&pack_path),
                current_file_offset as isize,
                to_read,
            )
            .await
            .context("Failed to read pack chunk during verification")?;

        bit_rot_hasher.update(&chunk);
        let chunk_start = current_file_offset;
        let chunk_end = current_file_offset + to_read as u64;

        // Process all blobs that intersect with this chunk
        while next_blob_idx < pack_header.len() {
            let desc = &pack_header[next_blob_idx];
            let blob_start = desc.offset as u64;
            let blob_end = blob_start + desc.length as u64;

            // If the current blob starts after this chunk, we are done with this chunk
            if blob_start >= chunk_end {
                break;
            }

            // Calculate the intersection of the blob and the current chunk
            let intersect_start = blob_start.max(chunk_start);
            let intersect_end = blob_end.min(chunk_end);

            if intersect_start < intersect_end {
                let start_in_chunk = (intersect_start - chunk_start) as usize;
                let end_in_chunk = (intersect_end - chunk_start) as usize;
                current_blob_data.extend_from_slice(&chunk[start_in_chunk..end_in_chunk]);
            }

            // If we have collected the full blob, verify it
            if current_blob_data.len() == desc.length as usize {
                let plaintext_res = secure_storage.decode(&current_blob_data);
                match plaintext_res {
                    Ok(plaintext) => {
                        if ID::from_content(&plaintext) != desc.id {
                            corrupt_blobs.push(desc.id);
                        } else {
                            verified_blobs += 1;
                        }
                    }
                    Err(_) => {
                        corrupt_blobs.push(desc.id);
                    }
                }
                bytes_processed += desc.length as u64;

                // CRITICAL: Reset the blob buffer for the next one
                current_blob_data.clear();
                next_blob_idx += 1;
            } else {
                // Blob continues in the next chunk
                break;
            }
        }

        current_file_offset = chunk_end;
    }

    let bit_rot = bit_rot_hasher.finalize() != pack_id;
    let index = repo.index();
    let mut num_dangling = 0;
    for blob in &pack_header {
        if !index.contains(&blob.id) {
            num_dangling += 1;
        }
    }

    Ok(PackStats {
        dangling: num_dangling,
        verified_blobs,
        corrupt_blobs,
        bytes_processed,
        bit_rot,
    })
}

/// Verify that all blobs referenced by a snapshot are indexed.
pub async fn verify_snapshot_refs(
    repo: Arc<Repository>,
    snapshot_id: &ID,
    existing_packs: &IdSet<ID>,
    verified_trees: Arc<crate::utils::collections::ShardedIdSet>,
) -> Result<usize> {
    let snapshot = repo.load_snapshot(snapshot_id, None).await?;
    let tree_id = snapshot.tree;

    // Validate the root tree exists first
    if repo.index().get(&tree_id).is_none() {
        bail!("Snapshot root tree {} is missing from index", tree_id);
    }

    let mut stack = vec![tree_id];
    let index = repo.index();

    while let Some(current_tree_id) = stack.pop() {
        // Global Deduplication: Skip if this tree has already been verified
        if !verified_trees.insert(current_tree_id) {
            continue;
        }

        let tree = crate::fs::tree::Tree::load_from_repo(repo.as_ref(), &current_tree_id).await?;
        for node in tree.nodes {
            let referenced_ids = node.blobs.iter().flatten().chain(node.tree.as_ref());

            for id in referenced_ids {
                match index.get(id) {
                    Some(blob_locator) => {
                        if !existing_packs.contains(&blob_locator.pack_id) {
                            bail!(
                                "Broken Reference: Pack {} referenced by blob {} is missing from storage",
                                blob_locator.pack_id,
                                id
                            );
                        }
                    }
                    None => {
                        bail!("Broken Reference: Blob {} is missing from index", id);
                    }
                }
            }

            if let Some(subtree_id) = node.tree {
                stack.push(subtree_id);
            }
        }
    }

    Ok(0)
}
