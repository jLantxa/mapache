use std::sync::Arc;

use anyhow::{Context, Result, bail};
use futures::StreamExt;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

use crate::{
    backend::StorageBackend,
    mapache::ID,
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
    let bit_rot = file_hash != *pack_id;

    // Verify Footer / Metadata
    let pack_header =
        Packer::parse_footer(secure_storage, &raw_data).context("Failed to parse pack footer")?;

    let index = repo.index();

    // Verify Data Integrity (Parallel)
    // This is now purely CPU bound (decryption + hashing) since we have raw_data in memory.
    let (verified_blobs, corrupt_blobs, bytes_processed) = pack_header
        .par_iter()
        .fold(
            || (0, Vec::new(), 0),
            |(mut v_count, mut corrupt, mut bytes), blob_desc| {
                let start = blob_desc.offset as usize;
                let end = (blob_desc.offset + blob_desc.length) as usize;

                if end > raw_data.len() {
                    corrupt.push(blob_desc.id);
                    return (v_count, corrupt, bytes);
                }

                let data_res = secure_storage.decode(&raw_data[start..end]);

                match data_res {
                    Ok(data) => {
                        let checksum = ID::from_content(&data);
                        if checksum != blob_desc.id {
                            corrupt.push(blob_desc.id);
                        } else {
                            v_count += 1;
                        }
                    }
                    Err(_) => {
                        corrupt.push(blob_desc.id);
                    }
                }

                bytes += blob_desc.length as u64;
                (v_count, corrupt, bytes)
            },
        )
        .reduce(
            || (0, Vec::new(), 0),
            |mut a, mut b| {
                a.1.append(&mut b.1);
                (a.0 + b.0, a.1, a.2 + b.2)
            },
        );

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
    verified_trees: Arc<parking_lot::Mutex<IdSet<ID>>>,
) -> Result<usize> {
    let snapshot = repo.load_snapshot(snapshot_id, None).await?;
    let root_tree_id = snapshot.tree;

    let index = repo.index();

    // Check root tree exists
    if index.get(&root_tree_id).is_none() {
        bail!("Snapshot root tree {} is missing from index", root_tree_id);
    }

    let mut pending_trees = vec![root_tree_id];
    let mut loading_trees = futures::stream::FuturesUnordered::new();
    const CONCURRENCY_LIMIT: usize = 8;

    loop {
        // Pop from stack and load if not verified
        while !pending_trees.is_empty() && loading_trees.len() < CONCURRENCY_LIMIT {
            let tree_id = pending_trees.pop().unwrap();

            // Atomic check+insert using our shared mutex
            {
                let mut seen = verified_trees.lock();
                if seen.contains(&tree_id) {
                    continue;
                }
                seen.insert(tree_id);
            }

            let repo_clone = repo.clone();
            loading_trees.push(async move {
                let res = crate::fs::tree::Tree::load_from_repo(&repo_clone, &tree_id).await;
                (tree_id, res)
            });
        }

        if loading_trees.is_empty() {
            break;
        }

        if let Some((tree_id, tree_res)) = loading_trees.next().await {
            let tree = tree_res.with_context(|| format!("Failed to load tree {tree_id}"))?;

            for node in tree.nodes {
                let referenced_ids = node.blobs.iter().flatten().chain(node.tree.as_ref());

                for id in referenced_ids {
                    match index.get(id) {
                        Some(blob_locator) => {
                            if !existing_packs.contains(&blob_locator.pack_id) {
                                bail!(
                                    "Broken Reference: Pack {} referenced by blob {} is missing from storage (found in node '{}')",
                                    blob_locator.pack_id,
                                    id,
                                    node.name
                                );
                            }
                        }
                        None => {
                            bail!(
                                "Broken Reference: Blob {} is missing from index (found in node '{}')",
                                id,
                                node.name
                            );
                        }
                    }
                }

                if let Some(subtree_id) = node.tree {
                    pending_trees.push(subtree_id);
                }
            }
        }
    }

    Ok(0)
}
