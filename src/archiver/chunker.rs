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
    repository::{RepositoryBackend, tree::Node},
    ui::snapshot_progress::SnapshotProgressReporter,
};

/// Puts a file into the repository
///
/// This function will split the file into chunks for deduplication, which will be compressed,
/// encrypted and stored in the repository. Files smaller than the minimum chunk size are stored
/// directly as blobs.
pub(crate) fn save_file(
    repo: Arc<dyn RepositoryBackend>,
    src_path: &Path,
    node: &Node,
    progress_reporter: Arc<SnapshotProgressReporter>,
) -> Result<Vec<ID>> {
    // Do not chunk if the file is smaller than the minimum chunk size
    if node.metadata.size < global::defaults::MIN_CHUNK_SIZE.into() {
        let data = std::fs::read(src_path)?;
        let (id, raw_size, encoded_size) =
            repo.save_blob(ObjectType::Data, data, global::SaveID::CalculateID)?;
        progress_reporter.written_data_bytes(raw_size, encoded_size);
        progress_reporter.processed_bytes(node.metadata.size);

        Ok(vec![id])
    } else {
        chunk_and_save_blobs(repo, src_path, progress_reporter)
    }
}

// Chunks the file and saves the blobs in the repository.
// This function completes the pipeline with two more stages:
// 1. Stage 1 produces chunks.
// 2. Stage 2 receives and saves the chunks. To be efficient, the second
//    stage must not stall the chunker, for example, by blocking when a packer
//    must be flushed.
//
// This is extremely useful when the only work remaining is processing a single
// big file, which is only handled by a single worker upstream. We want to make
// sure that our thoughput is only limited by the available I/O bandwidth, not
// the rate at which the chunker can produce blobs.
fn chunk_and_save_blobs(
    repo: Arc<dyn RepositoryBackend>,
    src_path: &Path,
    progress_reporter: Arc<SnapshotProgressReporter>,
) -> Result<Vec<ID>> {
    let source = File::open(src_path)
        .with_context(|| format!("Could not open file \'{}\'", src_path.display()))?;
    let reader = BufReader::new(source);

    let mut chunk_ids = Vec::new();

    // The chunker parameters must remain stable across versions, otherwise
    // same contents will no longer produce same chunks and IDs.
    let chunker = StreamCDC::with_level(
        reader,
        global::defaults::MIN_CHUNK_SIZE,
        global::defaults::AVG_CHUNK_SIZE,
        global::defaults::MAX_CHUNK_SIZE,
        Normalization::Level1,
    );

    for result in chunker {
        let chunk = result.with_context(|| "Failed to chunk file")?;

        let processed_size = chunk.data.len() as u64;
        let (chunk_id, raw_size, encoded_size) = repo
            .save_blob(ObjectType::Data, chunk.data, global::SaveID::CalculateID)
            .with_context(|| "Failed to save blob to repository")?;

        chunk_ids.push(chunk_id);

        progress_reporter.written_data_bytes(raw_size, encoded_size);
        progress_reporter.processed_bytes(processed_size);
    }

    Ok(chunk_ids)
}
