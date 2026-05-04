use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result, bail};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

use crate::{
    backend::StorageBackend,
    mapache::{ContentIdType, ID},
    repository::{packer::Packer, repo::Repository, storage::SecureStorage},
    utils::collections::IdSet,
};

use futures::{FutureExt, StreamExt};

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
    repo: Arc<Repository>,
    backend: Arc<dyn StorageBackend>,
    secure_storage: Arc<SecureStorage>,
    pack_id: ID,
) -> Result<PackStats> {
    let pack_path = repo.get_path(ContentIdType::Pack, &pack_id);

    // Bit-rot check: Verify full-file hash matches the filename (ID)
    // By reading the entire file once, we avoid subsequent I/O during blob verification.
    let raw_data = backend
        .read(&crate::backend::Handle::new(&pack_path), 0, 0)
        .await
        .context("Failed to read pack file for bit-rot check")?;

    let index = repo.index();

    // Move CPU intensive work (hashing, decryption) to blocking thread pool
    tokio::task::spawn_blocking(move || {
        let file_hash = ID::from_content(&raw_data);
        let bit_rot = file_hash != pack_id;

        // Verify Footer / Metadata
        let pack_header = Packer::parse_footer(&secure_storage, &raw_data)
            .context("Failed to parse pack footer")?;

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
    })
    .await?
}

/// Verify that all blobs referenced by a snapshot are indexed.
pub async fn verify_snapshot_refs(
    repo: Arc<Repository>,
    snapshot_id: &ID,
    existing_packs: &IdSet<ID>,
    verified_trees: Arc<parking_lot::Mutex<IdSet<ID>>>,
) -> Result<usize> {
    let snapshot = repo.load_snapshot(snapshot_id, None).await?;
    let tree_id = snapshot.tree;

    // Validate the root tree exists first
    if repo.index().get(&tree_id).is_none() {
        bail!("Snapshot root tree {} is missing from index", tree_id);
    }

    let mut stack = vec![(PathBuf::new(), tree_id)];
    let index = repo.index();

    while !stack.is_empty() {
        // Drain current stack and start fetching subtrees in parallel with a limit
        let to_fetch: Vec<_> = std::mem::take(&mut stack);
        let mut fetch_stream = futures::stream::iter(to_fetch)
            .map(|(path, current_tree_id)| {
                // Global Deduplication: Skip if this tree has already been verified
                let mut seen = verified_trees.lock();
                if seen.contains(&current_tree_id) {
                    return futures::future::ready(Ok(None)).left_future();
                }
                seen.insert(current_tree_id);
                drop(seen);

                let repo = repo.clone();
                async move {
                    let tree =
                        crate::fs::tree::Tree::load_from_repo(repo.as_ref(), &current_tree_id)
                            .await?;
                    Ok::<_, anyhow::Error>(Some((path, tree)))
                }
                .right_future()
            })
            .buffer_unordered(8); // Limit parallel tree loads

        while let Some(res) = fetch_stream.next().await {
            let maybe_tree = res?;
            if let Some((path, tree)) = maybe_tree {
                for node in tree.nodes {
                    let node_path = path.join(&node.name);
                    let referenced_ids = node.blobs.iter().flatten().chain(node.tree.as_ref());

                    for id in referenced_ids {
                        match index.get(id) {
                            Some(blob_locator) => {
                                if !existing_packs.contains(&blob_locator.pack_id) {
                                    bail!(
                                        "Broken Reference at {:?}: Pack {} referenced by blob {} is missing from storage",
                                        node_path,
                                        blob_locator.pack_id,
                                        id
                                    );
                                }
                            }
                            None => {
                                bail!(
                                    "Broken Reference at {:?}: Blob {} is missing from index",
                                    node_path,
                                    id
                                );
                            }
                        }
                    }

                    // If it's a directory, push to stack for next round of parallel fetching
                    if let Some(subtree_id) = node.tree {
                        stack.push((node_path, subtree_id));
                    }
                }
            }
        }
    }

    Ok(0) // 0 errors
}
