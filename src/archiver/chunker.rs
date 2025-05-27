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

use anyhow::{Context, Result, anyhow};
use crossbeam_channel::{self};
use fastcdc::v2020::{Normalization, StreamCDC};
use rayon::iter::{ParallelBridge, ParallelIterator};

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
        let (id, raw_size, encoded_size) = repo.save_blob(ObjectType::Data, data)?;
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

    // The chunker parameters must remain stable across versions, otherwise
    // same contents will no longer produce same chunks and IDs.
    let chunker = StreamCDC::with_level(
        reader,
        global::defaults::MIN_CHUNK_SIZE,
        global::defaults::AVG_CHUNK_SIZE,
        global::defaults::MAX_CHUNK_SIZE,
        Normalization::Level1,
    );

    // Two workers should be enough to keep the pipeline running in case one blocks flushing a packer.
    const NUM_SAVE_WORKERS: usize = 2;
    let (tx, rx) = crossbeam_channel::bounded::<Vec<u8>>(NUM_SAVE_WORKERS);

    let chunker_thread = std::thread::spawn(move || -> Result<()> {
        for result in chunker {
            let chunk = result.with_context(|| "Failed to chunk file")?;

            tx.send(chunk.data)
                .with_context(|| "Chunker thread failed to send data to next stage")?;
        }

        Ok(())
    });

    // Note: The par_bridge preserves the order, so as long as the chunks are sent in order though
    // the channels, they will be processed and collected in order.
    let chunk_hashes: Vec<ID> = rx
        .into_iter()
        .par_bridge()
        .map(|data| {
            let processed_size = data.len() as u64;
            let (chunk_id, raw_size, encoded_size) = repo
                .save_blob(ObjectType::Data, data)
                .with_context(|| "Failed to save blob to repository")?;

            progress_reporter.written_data_bytes(raw_size, encoded_size);
            progress_reporter.processed_bytes(processed_size);

            Ok(chunk_id) // Return the ID for collection
        })
        .collect::<Result<Vec<ID>>>()?;
    chunker_thread
        .join()
        .map_err(|e| anyhow!("Chunker thread panicked: {:?}", e))??;

    Ok(chunk_hashes)
}
