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

use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
};

use anyhow::{Result, bail};
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};

use crate::{
    global::{self, ID, defaults::MAX_PACK_SIZE},
    repository::{
        RepositoryBackend, snapshot::SnapshotStreamer, streamers::SerializedNodeStreamer,
    },
    ui,
};

pub struct Plan {
    pub repo: Arc<dyn RepositoryBackend>,
    pub referenced_blobs: HashSet<ID>, // Blobs referenced by existing snapshots
    pub referenced_packs: HashSet<ID>, // Packs referenced by the referenced blobs
    pub obsolete_packs: HashSet<ID>,   // Packs containing non-referenced blobs
    pub tolerated_packs: HashSet<ID>,  // Packs containing garbage, but keep due to tolerance
    pub unused_packs: HashSet<ID>,     // Packs not referenced by any snapshot or index
    pub index_ids: HashSet<ID>,        // Current index IDs
}

pub fn scan(repo: Arc<dyn RepositoryBackend>, tolerance: f32) -> Result<Plan> {
    let (referenced_blobs, referenced_packs) = get_referenced_blobs_and_packs(repo.clone())?;

    let mut unused_packs: HashSet<ID> = repo.list_objects()?;
    unused_packs.retain(|id| !referenced_packs.contains(id));

    let mut plan = Plan {
        repo: repo.clone(),
        referenced_blobs,
        referenced_packs,
        obsolete_packs: HashSet::new(),
        tolerated_packs: HashSet::new(),
        unused_packs,
        index_ids: repo.index().read().ids(),
    };

    // Count garbage bytes in each pack
    let mut pack_garbage: HashMap<ID, u64> = HashMap::new();

    // Find obsolete packs and blobs in index
    for (id, locator) in repo.index().read().iter_ids() {
        if !plan.referenced_blobs.contains(&id) {
            pack_garbage
                .entry(locator.pack_id)
                .and_modify(|size| *size += locator.length)
                .or_insert(locator.length);
        }
    }

    // Check garbage levels
    for (pack_id, garbage_bytes) in pack_garbage.into_iter() {
        if (garbage_bytes as f32 / MAX_PACK_SIZE as f32) > tolerance {
            plan.obsolete_packs.insert(pack_id);
        } else {
            plan.tolerated_packs.insert(pack_id);
        }
    }

    Ok(plan)
}

impl Plan {
    /// Execute the plan. Calling this method consumes the plan so it cannot be
    /// executed more than once.
    pub fn execute(mut self) -> Result<()> {
        self.repo
            .init_pack_saver(global::defaults::DEFAULT_WRITE_CONCURRENCY);

        for id in &self.unused_packs {
            self.repo.delete_file(global::FileType::Object, id)?;
        }

        // Collect information about the blobs to repack. Since we will rewrite the index, we will
        // lose this information.
        let mut repack_blob_info = HashMap::new();
        for referenced_blob_id in &self.referenced_blobs {
            if let Some((pack_id, blob_type, offset, length)) =
                self.repo.index().read().get(&referenced_blob_id)
            {
                if self.obsolete_packs.contains(&pack_id) {
                    repack_blob_info
                        .insert(referenced_blob_id, (pack_id, blob_type, offset, length));
                }
            }
        }

        // Rewrite index (remove obsolete packs) and repack.
        // We read the blobs we need to repack and pass them to the repository.
        // Since they are no longer in the index, this is like doing a backup of those blobs,
        // without creating the snapshot.
        self.repo.index().write().rewrite(&self.obsolete_packs);

        const REPACK_CONCURRENCY: usize = 4;
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(REPACK_CONCURRENCY)
            .build()
            .expect("Failed to build thread pool");
        let process_result: Result<()> = pool.install(|| {
            repack_blob_info.into_par_iter().try_for_each(
                |(blob_id, (pack_id, blob_type, offset, length))| {
                    let data = self.repo.read_from_file(
                        global::FileType::Object,
                        &pack_id,
                        offset,
                        length,
                    )?;
                    self.repo.save_blob(
                        blob_type,
                        data,
                        global::SaveID::WithID(blob_id.clone()),
                    )?;
                    Ok(())
                },
            )
        });

        if let Err(e) = process_result {
            bail!("An error occurred during repacking: {}", e);
        }

        self.repo.flush()?;
        self.repo.finalize_pack_saver();

        // Delete obsolete index files
        // Make sure that the new index files don't overlap the files to delete.
        // This can happen if an index did not change while repacking.
        let new_index_ids = self.repo.index().read().ids();
        self.index_ids.retain(|id| !new_index_ids.contains(id));
        self.index_ids.par_iter().for_each(|id| {
            if let Err(_) = self.repo.delete_file(crate::global::FileType::Index, id) {
                ui::cli::warning!("Could not delete index file {}", id);
            }
        });

        // Delete obsolete pack files
        self.obsolete_packs.par_iter().for_each(|id| {
            if let Err(_) = self.repo.delete_file(crate::global::FileType::Object, id) {
                ui::cli::warning!("Could not delete pack file {}", id);
            }
        });

        Ok(())
    }
}

/// Returns all blobs and packs referenced by all exising snapshots in the repository.
fn get_referenced_blobs_and_packs(
    repo: Arc<dyn RepositoryBackend>,
) -> Result<(HashSet<ID>, HashSet<ID>)> {
    let mut referenced_blobs = HashSet::new();
    let mut referenced_packs = HashSet::new();

    let snapshot_streamer = SnapshotStreamer::new(repo.clone())?;

    for (_snapshot_id, snapshot) in snapshot_streamer {
        let tree_id = snapshot.tree;
        let (pack_id, _, _, _) = repo.index().read().get(&tree_id).unwrap();
        referenced_blobs.insert(tree_id.clone());
        referenced_packs.insert(pack_id);

        let node_streamer =
            SerializedNodeStreamer::new(repo.clone(), Some(tree_id), PathBuf::new(), None, None)?;

        for node_res in node_streamer {
            match node_res {
                Ok((_path, stream_node)) => {
                    // Tree blob
                    if let Some(tree) = stream_node.node.tree {
                        referenced_blobs.insert(tree.clone());

                        match repo.index().read().get(&tree) {
                            None => {
                                ui::cli::warning!(
                                    "Tree referenced in snapshot is not contained in index"
                                );
                            }
                            Some((pack_id, _, _, _)) => {
                                referenced_packs.insert(pack_id);
                            }
                        }

                    // Data blobs
                    } else if let Some(blobs) = stream_node.node.blobs {
                        for blob_id in blobs {
                            referenced_blobs.insert(blob_id.clone());

                            match repo.index().read().get(&blob_id) {
                                None => {
                                    ui::cli::warning!(
                                        "Blob referenced in snapshot is not contained in index"
                                    );
                                }
                                Some((pack_id, _, _, _)) => {
                                    referenced_packs.insert(pack_id);
                                }
                            }
                        }
                    }
                }
                Err(e) => ui::cli::warning!("Error parsing node: {}", e.to_string()),
            }
        }
    }

    Ok((referenced_blobs, referenced_packs))
}
