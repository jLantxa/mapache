// [backup] is an incremental backup tool
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

use std::{fs::File, io::BufReader, path::Path, sync::Arc};

use anyhow::{Context, Result};
use fastcdc::v2020::{Normalization, StreamCDC};

use crate::{
    global::{self, ID, ObjectType},
    repository::RepositoryBackend,
    ui::snapshot_progress::SnapshotProgressReporter,
};

/// Puts a file into the repository
///
/// This function will split the file into chunks for deduplication, which
/// will be compressed, encrypted and stored in the repository.
/// The content hash of each chunk is used to identify the chunk and determine
/// if the chunk already exists in the repository.
pub(crate) fn save_file(
    repo: &dyn RepositoryBackend,
    src_path: &Path,
    progress_reporter: &Arc<SnapshotProgressReporter>,
) -> Result<Vec<ID>> {
    let source = File::open(src_path)
        .with_context(|| format!("Could not open file \'{}\'", src_path.display()))?;
    let reader = BufReader::new(source);

    // The chunker parameters must remain stable across versions, otherwise
    // same contents will no longer produce same chunks and IDs.
    let chunker = StreamCDC::with_level(
        reader,
        global::defaults::MIN_CHUNK_SIZE,
        global::defaults::AVG_CHUNK_SIZE,
        global::defaults::MAX_CHUNK_SIZE,
        Normalization::Level1,
    );

    let mut chunk_hashes = Vec::with_capacity(
        1 + (global::defaults::MAX_PACK_SIZE / global::defaults::AVG_CHUNK_SIZE as u64) as usize,
    );

    for result in chunker {
        let chunk = result?;
        let processed_size = chunk.data.len() as u64;

        let (chunk_id, raw_size, encoded_size) = repo.save_blob(ObjectType::Data, chunk.data)?;

        chunk_hashes.push(chunk_id);

        // Notify reporter
        progress_reporter.written_data_bytes(raw_size, encoded_size);
        progress_reporter.processed_bytes(processed_size);
    }

    Ok(chunk_hashes)
}
