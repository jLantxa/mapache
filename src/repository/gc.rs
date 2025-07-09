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

use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use anyhow::{Result, bail};
use colored::Colorize;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};

use crate::{
    global::{
        self, ID,
        defaults::{DEFAULT_MIN_PACK_SIZE_FACTOR, MAX_PACK_SIZE},
    },
    repository::{
        RepositoryBackend, packer::Packer, snapshot::SnapshotStreamer,
        streamers::SerializedNodeStreamer,
    },
    ui::{self, PROGRESS_REFRESH_RATE_HZ, SPINNER_TICK_CHARS, default_bar_draw_target},
};

pub struct Plan {
    pub repo: Arc<dyn RepositoryBackend>,
    pub total_packs: usize, // Total number of blobs in the repository
    pub referenced_blobs: HashSet<ID>, // Blobs referenced by existing snapshots
    pub referenced_packs: HashSet<ID>, // Packs referenced by the referenced blobs
    pub obsolete_packs: HashSet<ID>, // Packs containing non-referenced blobs
    pub tolerated_packs: HashSet<ID>, // Packs containing garbage, but keep due to tolerance
    pub unused_packs: HashSet<ID>, // Packs not referenced by any snapshot or index
    pub index_ids: HashSet<ID>, // Current index IDs
}

pub fn scan(repo: Arc<dyn RepositoryBackend>, tolerance: f32) -> Result<Plan> {
    let (referenced_blobs, referenced_packs) = get_referenced_blobs_and_packs(repo.clone())?;

    let mut keep_packs: HashSet<ID> = repo.list_objects()?;
    let mut unused_packs = keep_packs.clone();

    keep_packs.retain(|id| referenced_packs.contains(id));
    unused_packs.retain(|id| !referenced_packs.contains(id));

    let mut plan = Plan {
        repo: repo.clone(),
        total_packs: keep_packs.len(),
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
    let spinner = ProgressBar::new_spinner();
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.cyan} Finding obsolete blobs: {pos}")
            .unwrap()
            .tick_chars(SPINNER_TICK_CHARS),
    );
    spinner.enable_steady_tick(Duration::from_millis(
        (1000.0f32 / PROGRESS_REFRESH_RATE_HZ as f32) as u64,
    ));
    for (id, locator) in repo.index().read().iter_ids() {
        if !plan.referenced_blobs.contains(id) {
            pack_garbage
                .entry(locator.pack_id)
                .and_modify(|size| *size += locator.length as u64)
                .or_insert(locator.length as u64);
            spinner.inc(1);
        }
    }
    spinner.finish_and_clear();
    ui::cli::log!(
        "Found {} obsolete blobs in {} packs",
        spinner.position(),
        pack_garbage.len()
    );

    // Check garbage levels
    let spinner = ProgressBar::new_spinner();
    spinner.set_length(pack_garbage.len() as u64);
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.cyan} Checking garbage levels ({pos} / {len} packs)")
            .unwrap()
            .tick_chars(SPINNER_TICK_CHARS),
    );
    spinner.enable_steady_tick(Duration::from_millis(
        (1000.0f32 / PROGRESS_REFRESH_RATE_HZ as f32) as u64,
    ));
    for (pack_id, garbage_bytes) in pack_garbage.into_iter() {
        if (garbage_bytes as f32 / MAX_PACK_SIZE as f32) > tolerance {
            keep_packs.remove(&pack_id);
            plan.obsolete_packs.insert(pack_id);
        } else {
            plan.tolerated_packs.insert(pack_id);
        }
        spinner.inc(1);
    }
    spinner.finish_and_clear();

    // Determine small packs to repack
    let mut repack_small_packs = HashSet::new();
    for pack_id in &keep_packs {
        let pack = repo.load_object(pack_id)?;
        let blob_descriptors = Packer::read_header(&pack)?;
        let pack_size: u32 = blob_descriptors.iter().map(|d| d.length).sum();

        if DEFAULT_MIN_PACK_SIZE_FACTOR
            > (pack_size as f32 / global::defaults::MAX_PACK_SIZE as f32)
        {
            repack_small_packs.insert(pack_id.clone());
        }
    }
    if repack_small_packs.len() > 1 && !plan.obsolete_packs.is_empty() {
        for id in repack_small_packs.into_iter() {
            plan.obsolete_packs.insert(id);
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

        let unused_pack_delete_bar = ProgressBar::with_draw_target(
            Some(self.unused_packs.len() as u64),
            default_bar_draw_target(),
        )
        .with_style(
            ProgressStyle::default_bar()
                .template("[{bar:25.cyan/white}] Deleting unused packs: {pos}/{len}")
                .unwrap()
                .progress_chars("=> "),
        );

        for id in &self.unused_packs {
            self.repo.delete_file(global::FileType::Object, id)?;
            unused_pack_delete_bar.inc(1);
        }
        unused_pack_delete_bar.finish_and_clear();
        ui::cli::log!("Deleted {} unused packs", unused_pack_delete_bar.position());

        // Collect information about the blobs to repack. Since we will rewrite the index, we will
        // lose this information.
        let mut repack_blob_info = HashMap::new();
        for referenced_blob_id in &self.referenced_blobs {
            if let Some((pack_id, blob_type, offset, length)) =
                self.repo.index().read().get(referenced_blob_id)
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
        self.repo
            .index()
            .write()
            .cleanup(Some(&self.obsolete_packs));

        let repack_bar = ProgressBar::with_draw_target(
            Some(repack_blob_info.len() as u64),
            default_bar_draw_target(),
        )
        .with_style(
            ProgressStyle::default_bar()
                .template("[{bar:25.cyan/white}] Repacking blobs: {pos}/{len}")
                .unwrap()
                .progress_chars("=> "),
        );

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
                        offset as u64,
                        length as u64,
                    )?;
                    self.repo.save_blob(
                        blob_type,
                        data,
                        global::SaveID::WithID(blob_id.clone()),
                    )?;
                    repack_bar.inc(1);
                    Ok(())
                },
            )
        });
        repack_bar.finish_and_clear();
        ui::cli::log!("Repacked {} blobs", repack_bar.position());

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

        let index_delete_bar = ProgressBar::with_draw_target(
            Some(self.index_ids.len() as u64),
            default_bar_draw_target(),
        )
        .with_style(
            ProgressStyle::default_bar()
                .template("[{bar:25.cyan/white}] Deleting old index files: {pos}/{len}")
                .unwrap()
                .progress_chars("=> "),
        );

        self.index_ids.par_iter().for_each(|id| {
            let _ = self.repo.delete_file(crate::global::FileType::Index, id);
            index_delete_bar.inc(1);
        });
        index_delete_bar.finish_and_clear();
        ui::cli::log!(
            "Deleted {} obsolete index files",
            index_delete_bar.position()
        );

        // Delete obsolete pack files
        let obsolete_pack_delete_bar = ProgressBar::with_draw_target(
            Some(self.obsolete_packs.len() as u64),
            default_bar_draw_target(),
        )
        .with_style(
            ProgressStyle::default_bar()
                .template("[{bar:25.cyan/white}] Deleting obsolete pack files: {pos}/{len}")
                .unwrap()
                .progress_chars("=> "),
        );

        self.obsolete_packs.par_iter().for_each(|id| {
            let _ = self.repo.delete_file(crate::global::FileType::Object, id);
            obsolete_pack_delete_bar.inc(1);
        });
        obsolete_pack_delete_bar.finish_and_clear();
        ui::cli::log!(
            "Deleted {} obsolete packs",
            obsolete_pack_delete_bar.position()
        );

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

    let spinner = ProgressBar::new_spinner();
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.cyan} Searching referenced blobs: {pos}")
            .unwrap()
            .tick_chars(SPINNER_TICK_CHARS),
    );
    spinner.enable_steady_tick(Duration::from_millis(
        (1000.0f32 / PROGRESS_REFRESH_RATE_HZ as f32) as u64,
    ));

    for (_snapshot_id, snapshot) in snapshot_streamer {
        let tree_id = snapshot.tree;
        let (pack_id, _, _, _) = repo.index().read().get(&tree_id).unwrap();
        referenced_blobs.insert(tree_id.clone());
        referenced_packs.insert(pack_id);
        spinner.inc(1);

        let node_streamer =
            SerializedNodeStreamer::new(repo.clone(), Some(tree_id), PathBuf::new(), None, None)?;

        let mut dangling_tree_blobs: usize = 0;
        let mut dangling_data_blobs: usize = 0;
        for node_res in node_streamer {
            match node_res {
                Ok((_path, stream_node)) => {
                    // Tree blob
                    if let Some(tree) = stream_node.node.tree {
                        if referenced_blobs.insert(tree.clone()) {
                            spinner.set_position(referenced_blobs.len() as u64);
                        }

                        match repo.index().read().get(&tree) {
                            None => {
                                dangling_tree_blobs += 1;
                            }
                            Some((pack_id, _, _, _)) => {
                                referenced_packs.insert(pack_id);
                            }
                        }

                    // Data blobs
                    } else if let Some(blobs) = stream_node.node.blobs {
                        for blob_id in blobs {
                            if referenced_blobs.insert(blob_id.clone()) {
                                spinner.set_position(referenced_blobs.len() as u64);
                            }

                            match repo.index().read().get(&blob_id) {
                                None => {
                                    dangling_data_blobs += 1;
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

        if dangling_tree_blobs > 0 {
            ui::cli::warning!(
                "{} tree blobs referenced in snapshots are not contained in index",
                dangling_tree_blobs.to_string().bold()
            );
        }
        if dangling_data_blobs > 0 {
            ui::cli::warning!(
                "{} data blobs referenced in snapshots are not contained in index",
                dangling_data_blobs.to_string().bold()
            );
        }
    }

    spinner.finish_and_clear();
    ui::cli::log!("Found {} referenced blobs", referenced_blobs.len());

    Ok((referenced_blobs, referenced_packs))
}
