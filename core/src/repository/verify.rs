use std::{path::PathBuf, sync::Arc};

use anyhow::{Result, bail};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

use crate::{
    backend::StorageBackend,
    fs::{node::NodeType, tree::SerializedNodeStream},
    mapache::ID,
    repository::{packer::Packer, repo::Repository, storage::SecureStorage},
    utils,
};

/// Verify the checksum and contents of a pack with a known ID in the repository.
pub fn verify_pack(
    repo: &Repository,
    backend: &dyn StorageBackend,
    secure_storage: &SecureStorage,
    pack_id: &ID,
) -> Result<usize> {
    let pack_header = Packer::parse_pack_footer(repo, backend, secure_storage, pack_id)?;

    let index = repo.index();

    pack_header.par_iter().try_for_each(|blob_desc| {
        let data = repo.load_blob(&blob_desc.id)?;
        let checksum = utils::calculate_hash(&data);

        if checksum != blob_desc.id.0[..] {
            bail!(
                "Invalid checksum for blob {:?} in pack {:?}",
                blob_desc.id,
                pack_id
            );
        }

        Ok(())
    })?;

    let num_dangling = pack_header
        .iter()
        .filter(|blob| !index.contains(&blob.id))
        .count();

    Ok(num_dangling)
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

    let mut error_counter = 0;
    for (_path, stream_node) in stream.flatten() {
        let node = stream_node.node;
        match node.node_type {
            NodeType::File => {
                if let Some(blobs) = node.blobs {
                    for blob_id in &blobs {
                        if index.get(blob_id).is_none() {
                            error_counter += 1;
                        }
                    }
                }
            }
            NodeType::Directory => {
                if let Some(tree_id) = &node.tree
                    && index.get(tree_id).is_none()
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
