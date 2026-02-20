use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result, bail};
use futures::StreamExt;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

use crate::{
    backend::StorageBackend,
    fs::tree::SerializedNodeStream,
    mapache::ID,
    repository::{packer::Packer, repo::Repository, storage::SecureStorage},
    utils::collections::IdSet,
};

pub struct PackStats {
    pub dangling: usize,
    pub verified_blobs: usize,
    pub bytes_processed: u64,
}

/// Verify the checksum and contents of a pack.
///
/// This performs a "Physical Verification":
/// 1. Reads the ENTIRE file from the backend to verify the file-level ID (bit-rot check).
/// 2. Parses the pack footer from the memory-resident data.
/// 3. Decrypts and verifies EVERY blob in the pack in parallel (CPU-bound).
/// 4. Hashes the plaintext and compares it to the ID.
pub async fn verify_pack(
    repo: &Repository,
    backend: &dyn StorageBackend,
    secure_storage: &SecureStorage,
    pack_id: &ID,
) -> Result<PackStats> {
    let pack_path = repo.get_path(crate::mapache::ContentIdType::Pack, pack_id);

    // Bit-rot check: Verify full-file hash matches the filename (ID)
    // By reading the entire file once, we avoid subsequent I/O during blob verification.
    let raw_data = backend
        .read(&crate::backend::Handle::new(&pack_path), 0, 0)
        .await
        .context("Failed to read pack file for bit-rot check")?;

    let file_hash = ID::from_content(&raw_data);
    if file_hash != *pack_id {
        bail!(
            "Pack ID Mismatch (Bit-rot)! Filename: {} | Calculated: {}",
            pack_id,
            file_hash
        );
    }

    // Verify Footer / Metadata
    let pack_header = Packer::parse_footer(secure_storage, &raw_data)
        .context("Failed to parse pack footer")?;

    let index = repo.index();

    // Verify Data Integrity (Parallel)
    // This is now purely CPU bound (decryption + hashing) since we have raw_data in memory.
    let (verified_blobs, bytes_processed) = pack_header
        .par_iter()
        .try_fold(
            || (0, 0),
            |acc, blob_desc| {
                let start = blob_desc.offset as usize;
                let end = (blob_desc.offset + blob_desc.length) as usize;

                if end > raw_data.len() {
                    bail!("Blob offset out of bounds for pack {}", pack_id);
                }

                let data = secure_storage
                    .decode(&raw_data[start..end])
                    .with_context(|| format!("Failed to decrypt blob {}", blob_desc.id))?;

                // Integrity Check: Hash(Plaintext) == ID
                let checksum = ID::from_content(&data);
                if checksum != blob_desc.id {
                    bail!(
                        "Checksum Mismatch! Blob: {:?} | Pack: {:?} | Calculated: {} | Expected: {}",
                        blob_desc.id,
                        pack_id,
                        checksum,
                        blob_desc.id
                    );
                }

                Ok((acc.0 + 1, acc.1 + blob_desc.length as u64))
            },
        )
        .try_reduce(|| (0, 0), |a, b| Ok((a.0 + b.0, a.1 + b.1)))?;

    // Check for Dangling Blobs
    let mut num_dangling = 0;
    for blob in &pack_header {
        if !index.contains(&blob.id) {
            num_dangling += 1;
        }
    }

    Ok(PackStats {
        dangling: num_dangling,
        verified_blobs,
        bytes_processed,
    })
}

/// Verify that all blobs referenced by a snapshot are indexed.
pub async fn verify_snapshot_refs(
    repo: Arc<Repository>,
    snapshot_id: &ID,
    existing_packs: &IdSet<ID>,
) -> Result<usize> {
    let snapshot = repo.load_snapshot(snapshot_id, None).await?;
    let tree_id = snapshot.tree;

    // Validate the root tree exists first
    if repo.index().get(&tree_id).is_none() {
        bail!("Snapshot root tree {} is missing from index", tree_id);
    }

    let mut stream =
        SerializedNodeStream::new(repo.clone(), Some(tree_id), PathBuf::new(), None, None).await?;
    let index = repo.index();

    while let Some(node_res) = stream.next().await {
        let (path, stream_node) =
            node_res.with_context(|| "Error streaming snapshot nodes during verification")?;
        let node = stream_node.node;

        let referenced_ids = node.blobs.iter().flatten().chain(node.tree.as_ref());

        for id in referenced_ids {
            match index.get(id) {
                Some(blob_locator) => {
                    if !existing_packs.contains(&blob_locator.pack_id) {
                        bail!(
                            "Broken Reference at {:?}: Pack {} referenced by blob {} is missing from storage",
                            path,
                            blob_locator.pack_id,
                            id
                        );
                    }
                }
                None => {
                    bail!(
                        "Broken Reference at {:?}: Blob {} is missing from index",
                        path,
                        id
                    );
                }
            }
        }
    }

    Ok(0) // 0 errors
}
