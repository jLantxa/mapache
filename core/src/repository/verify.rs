use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result, bail};
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
/// 1. Reads the pack footer (decrypts metadata).
/// 2. Reads EVERY blob in the pack (decrypts data).
/// 3. Hashes the plaintext and compares it to the ID.
pub fn verify_pack(
    repo: &Repository,
    backend: &dyn StorageBackend,
    secure_storage: &SecureStorage,
    pack_id: &ID,
) -> Result<PackStats> {
    // Verify Footer / Metadata
    let pack_header = Packer::parse_pack_footer(repo, backend, secure_storage, pack_id)
        .context("Failed to parse pack footer")?;

    let index = repo.index();

    // Verify Data Integrity (Parallel)
    // We map-reduce to get stats and bubble up the first error encountered.
    let (verified_blobs, bytes_processed) = pack_header.par_iter().try_fold(
        || (0, 0),
        |acc, blob_desc| {
            let data = repo.load_from_pack(
                pack_id,
                blob_desc.blob_type,
                blob_desc.offset,
                blob_desc.length,
            ).with_context(|| format!("Failed to load/decrypt blob {}", blob_desc.id))?;

            // Integrity Check: Hash(Plaintext) == ID
            let checksum = ID::from_content(&data);
            if checksum != blob_desc.id {
                bail!(
                    "Checksum Mismatch! Blob: {:?} | Pack: {:?} | Calculated: {} | Expected: {}",
                    blob_desc.id,
                    pack_id,
                    checksum.to_hex(),
                    blob_desc.id
                );
            }

            Ok((acc.0 + 1, acc.1 + blob_desc.length as u64))
        }
    ).try_reduce(
        || (0, 0),
        |a, b| Ok((a.0 + b.0, a.1 + b.1))
    )?;

    // Check for Dangling Blobs (Garbage Collection hints)
    let num_dangling = pack_header
        .iter()
        .filter(|blob| !index.contains(&blob.id))
        .count();

    Ok(PackStats {
        dangling: num_dangling,
        verified_blobs,
        bytes_processed,
    })
}

/// Verify that all blobs referenced by a snapshot are indexed.
pub fn verify_snapshot_refs(
    repo: Arc<Repository>,
    snapshot_id: &ID,
    existing_packs: &IdSet<ID>,
) -> Result<usize> {
    let snapshot = repo.load_snapshot(snapshot_id, None)?;
    let tree_id = snapshot.tree;

    // Validate the root tree exists first
    if repo.index().get(&tree_id).is_none() {
        bail!("Snapshot root tree {} is missing from index", tree_id);
    }

    let stream =
        SerializedNodeStream::new(repo.clone(), Some(tree_id), PathBuf::new(), None, None)?;
    let index = repo.index();
    let mut missing_blobs = 0;

    for (_path, stream_node) in stream.flatten() {
        let node = stream_node.node;

        let referenced_ids = node.blobs.iter().flatten().chain(node.tree.as_ref());

        for id in referenced_ids {
            match index.get(id) {
                Some(blob_locator) => {
                    if !existing_packs.contains(&blob_locator.pack_id) {
                        bail!(
                            "Pack {} referenced by blob {} is missing from storage",
                            blob_locator.pack_id,
                            id
                        );
                    }
                }
                None => missing_blobs += 1,
            }
        }
    }

    if missing_blobs > 0 {
        bail!(
            "Snapshot contains {} missing references (index entries not found)",
            missing_blobs
        );
    }

    Ok(0) // 0 errors
}
