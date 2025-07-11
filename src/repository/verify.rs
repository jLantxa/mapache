// mapache is an incremental backup tool
// Copyright (C) 2025  Javier Lancha Vázquez <javier.lancha@gmail.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use std::{collections::BTreeSet, path::PathBuf, sync::Arc};

use anyhow::{Result, bail};

use crate::{
    global::ID,
    repository::{
        RepositoryBackend, packer::Packer, storage::SecureStorage,
        streamers::SerializedNodeStreamer, tree::NodeType,
    },
    utils,
};

pub fn verify_blob(repo: &dyn RepositoryBackend, id: &ID, len: Option<u32>) -> Result<()> {
    let blob_data = repo.load_blob(id)?;
    let checksum = utils::calculate_hash(&blob_data);
    if checksum != id.0[..] {
        bail!("Invalid blob checksum");
    }
    if let Some(some_len) = len
        && blob_data.len() != some_len as usize
    {
        bail!("Invalid blob length");
    }

    Ok(())
}

pub fn verify_pack(
    repo: &dyn RepositoryBackend,
    secure_storage: &SecureStorage,
    id: &ID,
    visited_blobs: &mut BTreeSet<ID>,
) -> Result<()> {
    let pack_data = repo.load_object(id)?;
    let checksum = utils::calculate_hash(&pack_data);
    if checksum != id.0[..] {
        bail!("Invalid pack checksum");
    }

    let pack_header = Packer::read_header(secure_storage, &pack_data)?;
    for blob_descriptor in pack_header {
        if !visited_blobs.contains(&blob_descriptor.id) {
            verify_blob(repo, &blob_descriptor.id, Some(blob_descriptor.length))?;
            visited_blobs.insert(blob_descriptor.id.clone());
        }
    }

    Ok(())
}

pub fn verify_snapshot(
    repo: Arc<dyn RepositoryBackend>,
    snapshot_id: &ID,
    visited_blobs: &mut BTreeSet<ID>,
) -> Result<()> {
    let snapshot_data = repo.load_file(crate::global::FileType::Snapshot, snapshot_id)?;
    let checksum = utils::calculate_hash(snapshot_data);
    if checksum != snapshot_id.0[..] {
        bail!("Invalid snapshot checksum");
    }

    let snapshot = repo.load_snapshot(snapshot_id)?;
    let tree_id = snapshot.tree;
    let streamer =
        SerializedNodeStreamer::new(repo.clone(), Some(tree_id), PathBuf::new(), None, None)?;
    for (_path, stream_node) in streamer.flatten() {
        let node = stream_node.node;
        match node.node_type {
            NodeType::File => {
                if let Some(blobs) = node.blobs {
                    for blob in blobs {
                        if !visited_blobs.contains(&blob) {
                            verify_blob(repo.as_ref(), &blob, None)?;
                            visited_blobs.insert(blob);
                        }
                    }
                }
            }
            NodeType::Symlink
            | NodeType::Directory
            | NodeType::BlockDevice
            | NodeType::CharDevice
            | NodeType::Fifo
            | NodeType::Socket => (),
        }
    }

    Ok(())
}
