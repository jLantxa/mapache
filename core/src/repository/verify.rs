use std::{collections::BTreeSet, path::PathBuf, sync::Arc};

use anyhow::{Result, bail};

use crate::{
    backend::StorageBackend,
    fs::{node::NodeType, tree::SerializedNodeStream},
    mapache::ID,
    repository::{packer::Packer, repo::Repository, storage::SecureStorage},
    utils,
};

/// Verify the checksum and contents of a blob with a known ID in the repository.
pub fn verify_blob(repo: &Repository, id: &ID) -> Result<(u64, u64)> {
    let index = repo.index();
    let index_guard = index.read();
    let blob_entry = index_guard.get(id);
    match blob_entry {
        Some(locator) => {
            // The ID of a blob is the hash of its plaintext content.
            let blob_data = repo.read_from_pack_and_decode(
                locator.blob_type,
                &locator.pack_id,
                locator.offset as u64,
                locator.length as u64,
            )?;
            let checksum = utils::calculate_hash(&blob_data);
            if checksum != id.0[..] {
                bail!("Invalid blob checksum");
            }

            Ok((locator.raw_length as u64, locator.length as u64))
        }
        None => bail!("Could not find blob {id:?} in index"),
    }
}

// No significant changes needed here, it's already concise.
pub fn verify_data(id: &ID, data: &[u8], expected_len: Option<u32>) -> Result<u64> {
    let checksum = utils::calculate_hash(data);
    if checksum != id.0[..] {
        bail!("Invalid blob checksum");
    }
    if let Some(some_len) = expected_len
        && data.len() != some_len as usize
    {
        bail!("Invalid blob length");
    }

    Ok(data.len() as u64)
}

/// Verify the checksum and contents of a pack  with a known ID in the repository.
pub fn verify_pack(
    repo: &Repository,
    backend: &dyn StorageBackend,
    secure_storage: &SecureStorage,
    id: &ID,
    visited_blobs: &mut BTreeSet<ID>,
) -> Result<usize> {
    let pack_data = repo.load_pack(id)?;
    let checksum = utils::calculate_hash(&pack_data);
    if checksum != id.0[..] {
        bail!("Invalid pack checksum");
    }

    let pack_header = Packer::parse_pack_header(repo, backend, secure_storage, id)?;
    let mut num_dangling_blobs = 0;

    let index = repo.index();
    let index_guard = index.read();

    for blob_descriptor in pack_header {
        if !visited_blobs.contains(&blob_descriptor.id) {
            // Only verify blobs referenced by the master index
            if index_guard.contains(&blob_descriptor.id) {
                // The blob ID is verified inside verify_blob
                verify_blob(repo, &blob_descriptor.id)?;
                visited_blobs.insert(blob_descriptor.id);
            } else {
                num_dangling_blobs += 1;
            }
        }
    }

    Ok(num_dangling_blobs)
}

/// Verify that all blobs referenced by a snapshot are indexed.
///
/// This function only verifies that all IDs referenced in a snapshot are listed in the  master
/// index, but it doesn't check the actual data. The blobs or packs could actually not exist
/// or be corrupted.
pub fn verify_snapshot_refs(repo: Arc<Repository>, snapshot_id: &ID) -> Result<()> {
    let snapshot = repo.load_snapshot(snapshot_id, None)?;
    let tree_id = snapshot.tree;

    let stream =
        SerializedNodeStream::new(repo.clone(), Some(tree_id), PathBuf::new(), None, None)?;

    let index = repo.index();
    let index_guard = index.read();

    let mut error_counter = 0;
    for (_path, stream_node) in stream.flatten() {
        let node = stream_node.node;
        match node.node_type {
            NodeType::File => {
                if let Some(blobs) = node.blobs {
                    for blob_id in &blobs {
                        if index_guard.get(blob_id).is_none() {
                            error_counter += 1;
                        }
                    }
                }
            }
            NodeType::Directory => {
                if let Some(tree_id) = &node.tree
                    && index_guard.get(tree_id).is_none()
                {
                    error_counter += 1;
                }
            }
            NodeType::Symlink
            | NodeType::BlockDevice
            | NodeType::CharDevice
            | NodeType::Fifo
            | NodeType::Socket => (),
        }
    }

    if error_counter > 0 {
        bail!("Snapshot has {error_counter} corrupt blobs");
    }

    Ok(())
}
