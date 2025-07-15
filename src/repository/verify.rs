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

use std::{collections::BTreeSet, sync::Arc};

use anyhow::{Result, bail};
use parking_lot::Mutex;

use crate::{
    backend::StorageBackend,
    global::ID,
    repository::{RepositoryBackend, packer::Packer, storage::SecureStorage},
    utils,
};

/// Verify the checksum and contents of a blob with a known ID in the repository.
pub fn verify_blob(repo: &dyn RepositoryBackend, id: &ID) -> Result<u64> {
    let blob_data = repo.load_blob(id)?;
    let checksum = utils::calculate_hash(&blob_data);
    if checksum != id.0[..] {
        bail!("Invalid blob checksum");
    }

    Ok(blob_data.len() as u64)
}

pub fn verify_data(id: &ID, data: &[u8], expected_len: Option<u32>) -> Result<u64> {
    let checksum = utils::calculate_hash(&data);
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
    repo: &dyn RepositoryBackend,
    backend: &dyn StorageBackend,
    secure_storage: &SecureStorage,
    id: &ID,
    visited_blobs: Arc<Mutex<BTreeSet<ID>>>,
) -> Result<usize> {
    let pack_data = repo.load_object(id)?;
    let checksum = utils::calculate_hash(&pack_data);
    if checksum != id.0[..] {
        bail!("Invalid pack checksum");
    }

    let pack_header = Packer::parse_pack_header(repo, backend, secure_storage, id)?;
    let mut num_dangling_blobs = 0;
    for blob_descriptor in pack_header {
        if !visited_blobs.lock().contains(&blob_descriptor.id) {
            // Only verify blobs referenced by the master index
            if repo.index().read().contains(&blob_descriptor.id) {
                verify_blob(repo, &blob_descriptor.id)?;
                visited_blobs.lock().insert(blob_descriptor.id.clone());
            } else {
                num_dangling_blobs += 1;
            }
        }
    }

    Ok(num_dangling_blobs)
}
