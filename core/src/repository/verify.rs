use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use futures::stream::{self, StreamExt};
use rayon::prelude::*;
use tokio::sync::mpsc;

use crate::{
    backend::{Handle, StorageBackend},
    mapache::{self, ContentIdType, ID},
    repository::{packer::Packer, repo::Repository, storage::SecureStorage},
    utils::{collections::IdSet, size},
};

pub struct PackStats {
    pub dangling: usize,
    pub verified_blobs: usize,
    pub corrupt_blobs: Vec<ID>,
    pub bytes_processed: u64,
    pub bit_rot: bool,
}

const CHUNK_SIZE: usize = 64 * size::MiB as usize;
const MAX_VERIFICATION_BATCH_SIZE: usize = 64 * size::MiB as usize;

/// Verify the checksum and contents of a pack.
///
/// If the pack fits in one chunk, reads it in one shot and verifies
/// all blobs in parallel. For larger packs, streams in chunks
/// with batched parallel verification (constant memory).
pub async fn verify_pack(
    repo: Arc<Repository>,
    backend: Arc<dyn StorageBackend>,
    secure_storage: Arc<SecureStorage>,
    pack_id: ID,
) -> Result<PackStats> {
    tracing::debug!(target: "verify", "Verifying pack {}", pack_id.to_short_hex(8));
    let pack_path = repo.get_path(ContentIdType::Pack, &pack_id);
    let attr = backend.lstat(&pack_path).await?;
    let pack_size = attr.size.ok_or_else(|| anyhow!("Pack size unknown"))?;

    if pack_size <= CHUNK_SIZE as u64 {
        tracing::trace!(target: "verify", "Using inline verification for pack {}", pack_id.to_short_hex(8));
        verify_pack_inline(repo, backend, secure_storage, pack_id, pack_path).await
    } else {
        tracing::trace!(target: "verify", "Using streaming verification for pack {}", pack_id.to_short_hex(8));
        verify_pack_streaming(repo, backend, secure_storage, pack_id, pack_path).await
    }
}

/// Fast path for packs ≤ CHUNK_SIZE: one read, parallel verify.
async fn verify_pack_inline(
    repo: Arc<Repository>,
    backend: Arc<dyn StorageBackend>,
    secure_storage: Arc<SecureStorage>,
    pack_id: ID,
    pack_path: std::path::PathBuf,
) -> Result<PackStats> {
    let handle = Handle::new(&pack_path);
    let raw_data = backend
        .read(&handle, 0, 0)
        .await
        .context("Failed to read pack file for verification")?;

    tokio::task::spawn_blocking(move || {
        let file_hash = ID::from_content(&raw_data);
        let bit_rot = file_hash != pack_id;

        let pack_header = Packer::parse_footer(&secure_storage, &raw_data)
            .context("Failed to parse pack footer")?;

        let (verified_blobs, corrupt_blobs, bytes_processed) = pack_header
            .par_iter()
            .fold(
                || (0usize, Vec::new(), 0u64),
                |(mut v, mut corrupt, mut bytes), desc| {
                    let start = desc.offset as usize;
                    let end = start + desc.length as usize;

                    if end > raw_data.len() {
                        corrupt.push(desc.id);
                        return (v, corrupt, bytes);
                    }

                    match secure_storage.decode(&raw_data[start..end]) {
                        Ok(plaintext) => {
                            if ID::from_content(&plaintext) != desc.id {
                                corrupt.push(desc.id);
                            } else {
                                v += 1;
                            }
                        }
                        Err(_) => corrupt.push(desc.id),
                    }
                    bytes += desc.length as u64;
                    (v, corrupt, bytes)
                },
            )
            .reduce(
                || (0, Vec::new(), 0),
                |(v1, mut c1, b1), (v2, c2, b2)| {
                    c1.extend(c2);
                    (v1 + v2, c1, b1 + b2)
                },
            );

        let index = repo.index();
        let num_dangling = pack_header
            .iter()
            .filter(|b| !index.contains(&b.id))
            .count();

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

/// Streaming path for packs > CHUNK_SIZE: read footer, then stream data in chunks,
/// verify blobs in batches via spawn_blocking + rayon.
async fn verify_pack_streaming(
    repo: Arc<Repository>,
    backend: Arc<dyn StorageBackend>,
    secure_storage: Arc<SecureStorage>,
    pack_id: ID,
    pack_path: std::path::PathBuf,
) -> Result<PackStats> {
    let handle = Handle::new(&pack_path);

    // Read the pack footer: last 4 bytes = footer length, preceding = encrypted footer.
    let footer_len_bytes = backend
        .read(&handle, -4, 4)
        .await
        .context("Failed to read pack footer length")?;
    let footer_len = u32::from_le_bytes(footer_len_bytes.as_slice().try_into()?) as usize;
    let footer_raw = backend
        .read(&handle, -(4 + footer_len as isize), 4 + footer_len)
        .await
        .context("Failed to read pack footer")?;

    let pack_header = Packer::parse_footer(&secure_storage, &footer_raw)
        .context("Failed to parse pack footer")?;

    let data_end = pack_header
        .last()
        .map(|desc| desc.offset as u64 + desc.length as u64)
        .unwrap_or(0);

    let mut bit_rot_hasher = mapache::hash::Hasher::new();

    let (batch_tx, mut batch_rx) = mpsc::channel::<Vec<(ID, Vec<u8>)>>(8);

    let ss = secure_storage.clone();
    let verifier_handle = tokio::spawn(async move {
        let mut v_total = 0;
        let mut c_total = Vec::new();
        let mut b_total = 0;

        let mut pending_verifications = futures::stream::FuturesUnordered::new();

        loop {
            tokio::select! {
                Some(batch) = batch_rx.recv(), if pending_verifications.len() < 4 => {
                    let ss = ss.clone();
                    pending_verifications.push(tokio::task::spawn_blocking(move || batch_verify(&batch, &ss)));
                }
                Some(res) = pending_verifications.next() => {
                    let (v, c, b) = res.context("Verification task panicked")?;
                    v_total += v;
                    c_total.extend(c);
                    b_total += b;
                }
                else => {
                    if pending_verifications.is_empty() { break; }
                }
            }
        }
        Ok::<(usize, Vec<ID>, u64), anyhow::Error>((v_total, c_total, b_total))
    });

    let chunk_size = CHUNK_SIZE;
    let offsets: Vec<u64> = (0..data_end).step_by(chunk_size).collect();

    let mut chunk_stream = stream::iter(offsets)
        .map(move |offset| {
            let to_read = (data_end - offset).min(chunk_size as u64) as usize;
            let backend = backend.clone();
            let pack_path = pack_path.clone();
            async move {
                let handle = Handle::new(&pack_path);
                let data = backend
                    .read(&handle, offset as isize, to_read)
                    .await
                    .context("Failed to read pack chunk during verification")?;
                Ok::<(u64, Vec<u8>), anyhow::Error>((offset, data))
            }
        })
        .buffered(4);

    let mut next_blob = 0usize;
    let mut blob_acc = Vec::with_capacity(CHUNK_SIZE);
    let mut pending_blobs = Vec::new();
    let mut pending_blobs_size = 0usize;

    while let Some(chunk_res) = chunk_stream.next().await {
        let (cs, chunk) = chunk_res?;
        let ce = cs + chunk.len() as u64;

        bit_rot_hasher.update(&chunk);

        while next_blob < pack_header.len() {
            let desc = &pack_header[next_blob];
            let bs = desc.offset as u64;
            let be = bs + desc.length as u64;

            if bs >= ce {
                break;
            }

            let blob_len = desc.length as usize;

            if blob_acc.is_empty() && bs >= cs && be <= ce {
                // Fast path: blob is entirely in this chunk, bypass accumulator.
                let start = (bs - cs) as usize;
                let end = (be - cs) as usize;
                let data = chunk[start..end].to_vec();
                pending_blobs_size += data.len();
                pending_blobs.push((desc.id, data));
                next_blob += 1;
            } else {
                let is = bs.max(cs);
                let ie = be.min(ce);
                if is < ie {
                    blob_acc.extend_from_slice(&chunk[(is - cs) as usize..(ie - cs) as usize]);
                }

                if blob_acc.len() == blob_len {
                    pending_blobs_size += blob_acc.len();
                    pending_blobs.push((desc.id, std::mem::take(&mut blob_acc)));
                    // Pre-allocate for next potentially multi-chunk blob
                    blob_acc = Vec::with_capacity(CHUNK_SIZE.min(size::MiB as usize));
                    next_blob += 1;
                } else {
                    break;
                }
            }

            if pending_blobs_size >= MAX_VERIFICATION_BATCH_SIZE {
                batch_tx
                    .send(std::mem::take(&mut pending_blobs))
                    .await
                    .context("Failed to send batch to verifier")?;
                pending_blobs_size = 0;
            }
        }
    }

    // Include footer in the bit-rot hash (covers the entire file).
    bit_rot_hasher.update(&footer_raw);

    if !pending_blobs.is_empty() {
        batch_tx
            .send(pending_blobs)
            .await
            .context("Failed to send final batch to verifier")?;
    }
    drop(batch_tx);

    let (verified_blobs, corrupt_blobs, bytes_processed) =
        verifier_handle.await.context("Verifier task panicked")??;

    let bit_rot = bit_rot_hasher.finalize() != pack_id;
    let index = repo.index();
    let num_dangling = pack_header
        .iter()
        .filter(|b| !index.contains(&b.id))
        .count();

    Ok(PackStats {
        dangling: num_dangling,
        verified_blobs,
        corrupt_blobs,
        bytes_processed,
        bit_rot,
    })
}

fn batch_verify(batch: &[(ID, Vec<u8>)], secure_storage: &SecureStorage) -> (usize, Vec<ID>, u64) {
    batch
        .par_iter()
        .fold(
            || (0usize, Vec::new(), 0u64),
            |(mut v, mut corrupt, mut bytes), (id, data)| {
                match secure_storage.decode(data) {
                    Ok(plain) => {
                        if ID::from_content(&plain) != *id {
                            corrupt.push(*id);
                        } else {
                            v += 1;
                        }
                    }
                    Err(_) => corrupt.push(*id),
                }
                bytes += data.len() as u64;
                (v, corrupt, bytes)
            },
        )
        .reduce(
            || (0, Vec::new(), 0),
            |(v1, mut c1, b1), (v2, c2, b2)| {
                c1.extend(c2);
                (v1 + v2, c1, b1 + b2)
            },
        )
}

/// Verify that all blobs referenced by a snapshot are indexed.
pub async fn verify_snapshot_refs(
    repo: Arc<Repository>,
    snapshot_id: &ID,
    existing_packs: &IdSet<ID>,
    verified_trees: Arc<crate::utils::collections::ShardedIdSet>,
) -> Result<usize> {
    tracing::info!(target: "verify", "Verifying references for snapshot {}", snapshot_id.to_short_hex(8));
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

    tracing::info!(target: "verify", "Snapshot {} references verified successfully", snapshot_id.to_short_hex(8));
    Ok(0)
}
