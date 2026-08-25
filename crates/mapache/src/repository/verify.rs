use std::sync::Arc;

use futures::stream::{self, StreamExt};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use tokio::sync::mpsc;

use crate::{
    backend::{Handle, StorageBackend, WriteContents},
    common::{
        self, BlobType, ContentIdType, ID,
        error::{MapacheError, Result},
    },
    ecc,
    fs::tree::Tree,
    repository::{
        index::IndexFile,
        packer::Packer,
        repo::{REPO_ECC_EXTENSION, Repository},
        snapshot::Snapshot,
        storage::SecureStorage,
    },
    utils::{
        collections::{IdSet, ShardedIdSet},
        size,
    },
};

pub struct PackStats {
    pub dangling: usize,
    pub verified_blobs: usize,
    pub corrupt_blobs: Vec<ID>,
    pub bytes_processed: u64,
    pub bit_rot: bool,
    pub repaired: bool,
}

const CHUNK_SIZE: usize = 64 * size::MiB as usize;
const MAX_VERIFICATION_BATCH_SIZE: usize = 64 * size::MiB as usize;
/// Concurrency for verification batches (channel buffer and in-flight limit).
const VERIFY_BATCH_CONCURRENCY: usize = 4;

/// Verify the checksum and contents of a pack.
///
/// If the pack fits in one chunk, reads it in one shot and verifies
/// all blobs in parallel. For larger packs, streams in chunks
/// with batched parallel verification.
///
/// When `repair` is true and bit-rot is detected, attempts to repair the pack
/// using the ECC sidecar (requires the repo to have ECC enabled).
pub async fn verify_pack(
    repo: Arc<Repository>,
    backend: Arc<dyn StorageBackend>,
    secure_storage: Arc<SecureStorage>,
    pack_id: ID,
    repair: bool,
) -> Result<PackStats> {
    tracing::debug!(target: "verify", "Verifying pack {}", pack_id.to_short_hex(8));
    let pack_path = repo.get_path(ContentIdType::Pack, &pack_id);
    let attr = backend.lstat(&pack_path).await?;
    let pack_size = attr
        .size
        .ok_or_else(|| MapacheError::Internal("pack size unknown".to_string()))?;

    let mut stats = if pack_size <= CHUNK_SIZE as u64 {
        tracing::trace!(target: "verify", "Using inline verification for pack {}", pack_id.to_short_hex(8));
        verify_pack_inline(
            repo.clone(),
            backend.clone(),
            secure_storage.clone(),
            pack_id,
            pack_path.clone(),
        )
        .await?
    } else {
        tracing::trace!(target: "verify", "Using streaming verification for pack {}", pack_id.to_short_hex(8));
        verify_pack_streaming(
            repo.clone(),
            backend.clone(),
            secure_storage.clone(),
            pack_id,
            pack_path.clone(),
        )
        .await?
    };

    // Attempt ECC repair when bit-rot is detected and repair is requested.
    if repair && stats.bit_rot {
        match repo.manifest().ecc() {
            Some(_ecc_config) => {
                tracing::info!(target: "verify", "Bit-rot detected in pack {}, attempting ECC repair", pack_id.to_short_hex(8));
                let sidecar_path = pack_path.with_extension(REPO_ECC_EXTENSION);
                match repair_pack_with_ecc(
                    &repo,
                    &backend,
                    &secure_storage,
                    pack_id,
                    &pack_path,
                    &sidecar_path,
                )
                .await
                {
                    Ok(()) => {
                        tracing::info!(target: "verify", "ECC repair succeeded for pack {}, re-verifying", pack_id.to_short_hex(8));
                        // Re-verify the repaired pack to get correct blob stats.
                        stats = if pack_size <= CHUNK_SIZE as u64 {
                            verify_pack_inline(repo, backend, secure_storage, pack_id, pack_path)
                                .await?
                        } else {
                            verify_pack_streaming(repo, backend, secure_storage, pack_id, pack_path)
                                .await?
                        };
                        stats.repaired = true;
                    }
                    Err(e) => {
                        tracing::warn!(target: "verify", "ECC repair failed for pack {}: {}", pack_id.to_short_hex(8), e);
                    }
                }
            }
            None => {
                tracing::warn!(target: "verify", "Bit-rot detected in pack {} but ECC is not enabled, cannot repair", pack_id.to_short_hex(8));
            }
        }
    }

    Ok(stats)
}

/// Read the pack + sidecar, run ECC decode (which verifies parity and repairs),
/// then write the repaired data back to disk.
async fn repair_pack_with_ecc(
    _repo: &Repository,
    backend: &Arc<dyn StorageBackend>,
    secure_storage: &SecureStorage,
    pack_id: ID,
    pack_path: &std::path::Path,
    sidecar_path: &std::path::Path,
) -> Result<()> {
    // Read the full pack data.
    let handle = Handle::new(pack_path);
    let attr = backend.lstat(pack_path).await?;
    let pack_size = attr
        .size
        .ok_or_else(|| MapacheError::Internal("pack size unknown for repair".to_string()))?;
    let raw_data = backend
        .read(&handle, 0, pack_size as usize)
        .await
        .map_err(|e| {
            MapacheError::Backend(format!("failed to read pack for ECC repair: {}", e.inner()))
        })?;

    // Read and decode the ECC sidecar (encoded = compressed + encrypted).
    let sidecar_handle = Handle::new(sidecar_path);
    let sidecar_encoded = backend
        .read(&sidecar_handle, 0, 0)
        .await
        .map_err(|e| MapacheError::Backend(format!("failed to read ECC sidecar: {}", e.inner())))?;
    let sidecar_data = secure_storage
        .decode(&sidecar_encoded)
        .map_err(|e| MapacheError::Repo(format!("failed to decode ECC sidecar: {}", e)))?;

    // Run ECC decode (verifies + repairs in-place) on a blocking thread.
    let (decoded, was_repaired) = tokio::task::spawn_blocking(move || {
        let decoded = ecc::ecc_decode(&raw_data, &sidecar_data)?;
        let repaired = decoded != raw_data;
        if repaired {
            ecc::validate_crc(&decoded, &sidecar_data)
                .map_err(ecc::EccDecodeError::CrcValidationFailed)?;
        }
        Ok::<_, ecc::EccDecodeError>((decoded, repaired))
    })
    .await
    .map_err(|e| MapacheError::Internal(format!("ECC decode task failed: {e}")))?
    .map_err(|e| {
        MapacheError::Repo(format!(
            "ECC decode failed for pack {}: {}",
            pack_id.to_short_hex(8),
            e
        ))
    })?;

    if was_repaired {
        let tmp_path = pack_path.with_extension("tmp");
        let tmp_handle = Handle::new(&tmp_path);
        backend
            .write(&tmp_handle, WriteContents::Borrowed(&decoded))
            .await
            .map_err(|e| {
                MapacheError::Backend(format!("failed to write repaired pack: {}", e.inner()))
            })?;
        backend.rename(&tmp_path, pack_path).await.map_err(|e| {
            MapacheError::Backend(format!("failed to rename repaired pack: {}", e.inner()))
        })?;
        tracing::info!(target: "verify", "Repaired pack {} ({} bytes)", pack_id.to_short_hex(8), decoded.len());
    }
    Ok(())
}

/// Result of verifying a non-pack metadata file (index, snapshot, manifest).
pub struct MetadataFileStats {
    pub file_type: ContentIdType,
    pub file_id: Option<ID>,
    pub file_path: std::path::PathBuf,
    pub readable: bool,
    pub bit_rot: bool,
    pub repaired: bool,
}

/// Verify a non-pack metadata file by reading it, decrypting, and validating
/// its content structure. If an ECC sidecar exists, also verifies via ECC
/// and optionally repairs.
pub async fn verify_metadata_file(
    backend: Arc<dyn StorageBackend>,
    secure_storage: Arc<SecureStorage>,
    repo_version: u32,
    file_type: ContentIdType,
    file_id: Option<ID>,
    file_path: std::path::PathBuf,
    repair: bool,
) -> Result<MetadataFileStats> {
    let mut stats = MetadataFileStats {
        file_type,
        file_id,
        file_path: file_path.clone(),
        readable: false,
        bit_rot: false,
        repaired: false,
    };

    // Read the file from disk.
    let raw_data = match backend.read(&Handle::new(&file_path), 0, 0).await {
        Ok(d) => d,
        Err(_) => {
            // File unreadable — not necessarily bit-rot, could be missing.
            return Ok(stats);
        }
    };

    // Decrypt and validate content structure on a blocking thread.
    let ss = secure_storage.clone();
    let ft = file_type;
    let raw_data_clone = raw_data.clone();
    let decode_result = tokio::task::spawn_blocking(move || match ft {
        ContentIdType::Pack => Err(MapacheError::Internal(
            "use verify_pack for packs".to_string(),
        )),
        ContentIdType::Key => {
            ss.decompress(&raw_data_clone)?;
            Ok(())
        }
        ContentIdType::Snapshot => {
            let plaintext = ss.decode_owned(raw_data_clone)?;
            let _snapshot: Snapshot = serde_json::from_slice(&plaintext)
                .map_err(|e| MapacheError::Format(format!("corrupt snapshot structure: {e}")))?;
            Ok(())
        }
        ContentIdType::Index => {
            let plaintext = ss.decode_owned(raw_data_clone)?;
            IndexFile::deserialize(&plaintext, repo_version)
                .map_err(|e| MapacheError::Format(format!("corrupt index structure: {e}")))?;
            Ok(())
        }
        ContentIdType::Lock => Ok(()),
    })
    .await
    .map_err(|e| MapacheError::task_panicked("metadata verification", e))?;

    match decode_result {
        Ok(()) => stats.readable = true,
        Err(_) => {
            // File is corrupt or unreadable. Fall through to ECC check if repair is enabled.
            if !repair {
                return Ok(stats);
            }
        }
    }

    // Check ECC sidecar only when repair is requested.
    // ECC is only supported on v2+ repos with ECC enabled, so this branch
    // is only reached when both conditions hold.
    if !repair {
        return Ok(stats);
    }

    let sidecar_path = file_path.with_extension(REPO_ECC_EXTENSION);
    if !backend.path_exists(&sidecar_path).await {
        return Ok(stats);
    }

    let sidecar_encoded = match backend.read(&Handle::new(&sidecar_path), 0, 0).await {
        Ok(d) => d,
        Err(e) => {
            tracing::warn!(target: "verify", "Failed to read ECC sidecar {}: {e}", sidecar_path.display());
            return Ok(stats);
        }
    };

    let sidecar_data = match secure_storage.decode(&sidecar_encoded) {
        Ok(d) => d,
        Err(e) => {
            tracing::warn!(target: "verify", "Failed to decode ECC sidecar {}: {e}", sidecar_path.display());
            return Ok(stats);
        }
    };

    // Run ECC decode on a blocking thread.
    let file_data_clone = raw_data.clone();
    let sidecar_data_clone = sidecar_data.clone();
    let ecc_result = tokio::task::spawn_blocking(move || {
        let decoded = ecc::ecc_decode(&file_data_clone, &sidecar_data_clone)?;
        let repaired = decoded != file_data_clone;
        if repaired {
            ecc::validate_crc(&decoded, &sidecar_data_clone)
                .map_err(ecc::EccDecodeError::CrcValidationFailed)?;
        }
        Ok::<_, ecc::EccDecodeError>((decoded, repaired))
    })
    .await
    .map_err(|e| MapacheError::Internal(format!("ECC decode task failed: {e}")))?
    .map_err(|e| {
        MapacheError::Repo(format!(
            "ECC decode failed for {}: {}",
            file_path.display(),
            e
        ))
    })?;

    let (decoded, was_repaired) = ecc_result;
    if was_repaired {
        stats.bit_rot = true;

        // Write repaired data back.
        let tmp_path = file_path.with_extension("ecc.tmp");
        let tmp_handle = Handle::new(&tmp_path);
        backend
            .write(&tmp_handle, WriteContents::Borrowed(&decoded))
            .await
            .map_err(|e| {
                MapacheError::Backend(format!(
                    "failed to write repaired {}: {}",
                    file_type,
                    e.inner()
                ))
            })?;
        backend.rename(&tmp_path, &file_path).await.map_err(|e| {
            MapacheError::Backend(format!("failed to rename repaired {}: {}", file_type, e))
        })?;
        stats.repaired = true;
        tracing::info!(target: "verify", "Repaired {} at {}", file_type, file_path.display());
    }

    Ok(stats)
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
    let raw_data = backend.read(&handle, 0, 0).await.map_err(|e| {
        MapacheError::Backend(format!(
            "failed to read pack file for verification: {}",
            e.inner()
        ))
    })?;

    tokio::task::spawn_blocking(move || {
        let file_hash = ID::from_content(&raw_data);
        let bit_rot = file_hash != pack_id;

        let pack_header = Packer::parse_footer(
            &secure_storage,
            &raw_data,
            secure_storage.nonce_at_end(),
            repo.repo_version(),
        )?;

        let (verified_blobs, corrupt_blobs, bytes_processed) = pack_header
            .par_iter()
            .fold(
                || (0usize, Vec::new(), 0u64),
                |(mut v, mut corrupt, mut bytes), desc| {
                    // Zero blobs have no data in the pack (length=0); skip physical verification.
                    if matches!(desc.blob_type, BlobType::Zero) {
                        v += 1;
                        return (v, corrupt, bytes);
                    }

                    let start = desc.offset as usize;
                    let end = start + desc.length as usize;

                    if end > raw_data.len() {
                        corrupt.push(desc.id);
                        return (v, corrupt, bytes);
                    }

                    match secure_storage.decode_blob(&raw_data[start..end], desc.compressed) {
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
            repaired: false,
        })
    })
    .await
    .map_err(|e| MapacheError::task_panicked("verification", e))?
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
    let footer_len_bytes = backend.read(&handle, -4, 4).await.map_err(|e| {
        MapacheError::Backend(format!("failed to read pack footer length: {}", e.inner()))
    })?;
    let footer_len = u32::from_le_bytes(footer_len_bytes.as_slice().try_into().map_err(
        |e: std::array::TryFromSliceError| {
            MapacheError::Format(format!("invalid footer length bytes: {}", e))
        },
    )?) as usize;
    let footer_raw = backend
        .read(&handle, -(4 + footer_len as isize), 4 + footer_len)
        .await
        .map_err(|e| MapacheError::Backend(format!("failed to read pack footer: {}", e.inner())))?;

    let pack_header = Packer::parse_footer(
        &secure_storage,
        &footer_raw,
        secure_storage.nonce_at_end(),
        repo.repo_version(),
    )?;

    let data_end = pack_header
        .last()
        .map(|desc| desc.offset as u64 + desc.length as u64)
        .unwrap_or(0);

    let mut bit_rot_hasher = common::hash::Hasher::new();

    let (batch_tx, mut batch_rx) =
        mpsc::channel::<Vec<(ID, Vec<u8>, bool)>>(VERIFY_BATCH_CONCURRENCY);

    let ss = secure_storage.clone();
    let verifier_handle = tokio::spawn(async move {
        let mut v_total = 0;
        let mut c_total = Vec::new();
        let mut b_total = 0;

        let mut pending_verifications = futures::stream::FuturesUnordered::new();

        loop {
            tokio::select! {
                Some(batch) = batch_rx.recv(), if pending_verifications.len() < VERIFY_BATCH_CONCURRENCY => {
                    let ss = ss.clone();
                    pending_verifications.push(tokio::task::spawn_blocking(move || batch_verify(&batch, &ss)));
                }
                Some(res) = pending_verifications.next() => {
                    let (v, c, b) = res
                        .map_err(|e| MapacheError::task_panicked("verification", e))?;
                    v_total += v;
                    c_total.extend(c);
                    b_total += b;
                }
                else => {
                    if pending_verifications.is_empty() { break; }
                }
            }
        }
        Ok::<(usize, Vec<ID>, u64), MapacheError>((v_total, c_total, b_total))
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
                let read_offset = i64::try_from(offset).map_err(|_| {
                    MapacheError::Backend(format!(
                        "pack offset too large for signed read: {offset}"
                    ))
                })?;
                let data = backend
                    .read(&handle, read_offset as isize, to_read)
                    .await
                    .map_err(|e| {
                        MapacheError::Backend(format!(
                            "failed to read pack chunk during verification: {}",
                            e.inner()
                        ))
                    })?;
                Ok::<(u64, Vec<u8>), MapacheError>((offset, data))
            }
        })
        .buffered(VERIFY_BATCH_CONCURRENCY);

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

            // Zero blobs have no data in the pack (length=0); skip physical verification.
            if matches!(desc.blob_type, BlobType::Zero) {
                next_blob += 1;
                continue;
            }

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
                pending_blobs.push((desc.id, data, desc.compressed));
                next_blob += 1;
            } else {
                let is = bs.max(cs);
                let ie = be.min(ce);
                if is < ie {
                    blob_acc.extend_from_slice(&chunk[(is - cs) as usize..(ie - cs) as usize]);
                }

                if blob_acc.len() == blob_len {
                    pending_blobs_size += blob_acc.len();
                    pending_blobs.push((desc.id, std::mem::take(&mut blob_acc), desc.compressed));
                    // Pre-allocate for next potentially multi-chunk blob
                    blob_acc = Vec::with_capacity(size::MiB as usize);
                    next_blob += 1;
                } else {
                    break;
                }
            }

            if pending_blobs_size >= MAX_VERIFICATION_BATCH_SIZE {
                batch_tx
                    .send(std::mem::take(&mut pending_blobs))
                    .await
                    .map_err(|e| {
                        MapacheError::Repo(format!("failed to send batch to verifier: {}", e))
                    })?;
                pending_blobs_size = 0;
            }
        }
    }

    // Include footer in the bit-rot hash (covers the entire file).
    bit_rot_hasher.update(&footer_raw);

    if !pending_blobs.is_empty() {
        batch_tx.send(pending_blobs).await.map_err(|e| {
            MapacheError::Repo(format!("failed to send final batch to verifier: {}", e))
        })?;
    }
    drop(batch_tx);

    let (verified_blobs, corrupt_blobs, bytes_processed) = verifier_handle
        .await
        .map_err(|e| MapacheError::task_panicked("verifier", e))??;

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
        repaired: false,
    })
}

fn batch_verify(
    batch: &[(ID, Vec<u8>, bool)],
    secure_storage: &SecureStorage,
) -> (usize, Vec<ID>, u64) {
    batch
        .par_iter()
        .fold(
            || (0usize, Vec::new(), 0u64),
            |(mut v, mut corrupt, mut bytes), (id, data, compressed)| {
                match secure_storage.decode_blob(data, *compressed) {
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
    verified_trees: Arc<ShardedIdSet>,
) -> Result<usize> {
    tracing::info!(target: "verify", "Verifying references for snapshot {}", snapshot_id.to_short_hex(8));
    let snapshot = repo.load_snapshot(snapshot_id, None).await?;
    let tree_id = snapshot.tree;
    let index = repo.index();

    // Validate the root tree exists first
    if index.get(&tree_id).await.is_none() {
        return Err(MapacheError::Integrity(format!(
            "snapshot root tree {} is missing from index",
            tree_id
        )));
    }

    let mut stack = vec![tree_id];
    let mut verified_count: usize = 0;

    while let Some(current_tree_id) = stack.pop() {
        // Global Deduplication: Skip if this tree has already been verified
        if !verified_trees.insert(current_tree_id) {
            continue;
        }

        let tree = Tree::load_from_repo(repo.as_ref(), &current_tree_id).await?;
        for node in tree.nodes {
            let referenced_ids = node.blobs.iter().flatten().chain(node.tree.as_ref());

            for id in referenced_ids {
                match index.get(id).await {
                    Some(blob_locator) => {
                        if blob_locator.blob_type != BlobType::Zero
                            && !existing_packs.contains(&blob_locator.pack_id)
                        {
                            return Err(MapacheError::Integrity(format!(
                                "broken reference: Pack {} referenced by blob {} is missing from storage",
                                blob_locator.pack_id, id
                            )));
                        }
                    }
                    None => {
                        return Err(MapacheError::Integrity(format!(
                            "broken reference: Blob {} is missing from index",
                            id
                        )));
                    }
                }
                verified_count += 1;
            }

            if let Some(subtree_id) = node.tree {
                stack.push(subtree_id);
            }
        }
    }

    tracing::info!(target: "verify", "Snapshot {} references verified successfully", snapshot_id.to_short_hex(8));
    Ok(verified_count)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn storage_with_key() -> SecureStorage {
        let key = [0x42u8; 32];
        SecureStorage::new().with_key(&key).unwrap()
    }

    #[test]
    fn batch_verify_empty() {
        let storage = storage_with_key();
        let (verified, corrupt, bytes) = batch_verify(&[], &storage);
        assert_eq!(verified, 0);
        assert!(corrupt.is_empty());
        assert_eq!(bytes, 0);
    }

    #[test]
    fn batch_verify_all_valid() {
        let storage = storage_with_key();
        let plaintexts: Vec<Vec<u8>> = (0..5).map(|i| format!("blob-{i}").into_bytes()).collect();
        let batch: Vec<(ID, Vec<u8>, bool)> = plaintexts
            .iter()
            .map(|p| (ID::from_content(p), storage.encode(p).unwrap(), true))
            .collect();

        let (verified, corrupt, bytes) = batch_verify(&batch, &storage);
        assert_eq!(verified, 5);
        assert!(corrupt.is_empty());
        assert!(bytes > 0);
    }

    #[test]
    fn batch_verify_all_corrupt_wrong_id() {
        let storage = storage_with_key();
        let plaintexts: Vec<Vec<u8>> = (0..3).map(|i| format!("blob-{i}").into_bytes()).collect();
        let batch: Vec<(ID, Vec<u8>, bool)> = plaintexts
            .iter()
            .map(|p| {
                // Use a wrong ID (hash of different content)
                let wrong_id = ID::from_content(b"wrong");
                (wrong_id, storage.encode(p).unwrap(), true)
            })
            .collect();

        let (verified, corrupt, bytes) = batch_verify(&batch, &storage);
        assert_eq!(verified, 0);
        assert_eq!(corrupt.len(), 3);
        assert!(bytes > 0);
    }

    #[test]
    fn batch_verify_all_corrupt_garbage_data() {
        let storage = storage_with_key();
        let batch: Vec<(ID, Vec<u8>, bool)> = (0..3)
            .map(|i| {
                let id = ID::from_content(format!("blob-{i}").as_bytes());
                // Random garbage that won't decrypt
                (id, vec![0xff; 100], false)
            })
            .collect();

        let (verified, corrupt, bytes) = batch_verify(&batch, &storage);
        assert_eq!(verified, 0);
        assert_eq!(corrupt.len(), 3);
        assert_eq!(bytes, 300);
    }

    #[test]
    fn batch_verify_mixed_valid_and_corrupt() {
        let storage = storage_with_key();
        let valid_plain = b"valid-data";
        let corrupt_plain = b"corrupt-data";

        let batch: Vec<(ID, Vec<u8>, bool)> = vec![
            (
                ID::from_content(valid_plain),
                storage.encode(valid_plain).unwrap(),
                true,
            ),
            (
                ID::from_content(b"wrong"),
                storage.encode(corrupt_plain).unwrap(),
                true,
            ),
            (
                ID::from_content(valid_plain),
                storage.encode(valid_plain).unwrap(),
                true,
            ),
            // garbage data
            (
                ID::from_content(b"another"),
                vec![0xde, 0xad, 0xbe, 0xef],
                false,
            ),
        ];

        let (verified, corrupt, bytes) = batch_verify(&batch, &storage);
        assert_eq!(verified, 2);
        assert_eq!(corrupt.len(), 2);
        assert!(bytes > 0);
    }

    #[test]
    fn batch_verify_bytes_processed_counts_encoded_size() {
        let storage = storage_with_key();
        let data = vec![0xaa; 1000];
        let encoded = storage.encode(&data).unwrap();
        let batch = vec![(ID::from_content(&data), encoded.clone(), true)];

        let (_, _, bytes) = batch_verify(&batch, &storage);
        assert_eq!(bytes, encoded.len() as u64);
    }
}
