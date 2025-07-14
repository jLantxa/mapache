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

use std::collections::BTreeSet;

use anyhow::{Result, bail};

use crate::{
    global::ID,
    repository::{RepositoryBackend, packer::Packer, storage::SecureStorage},
    utils,
};

/// Verify the checksum and contents of a blob with a known ID in the repository.
pub fn verify_blob(repo: &dyn RepositoryBackend, id: &ID, len: Option<u32>) -> Result<u64> {
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

    Ok(blob_data.len() as u64)
}

/// Verify the checksum and contents of a pack  with a known ID in the repository.
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
