use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use anyhow::{Result, bail};
use colored::Colorize;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};

use crate::{
    backend::{StorageBackend, read_backend_dir},
    fs::tree::SerializedNodeStream,
    mapache::{
        self, ContentIdType, ID, SaveID,
        defaults::{DEFAULT_MIN_PACK_SIZE_FACTOR, DEFAULT_PACK_SIZE},
        global::GlobalOpts,
    },
    repository::{
        repo::{REPO_DROPPED_EXTENSION, REPO_TMP_EXTENSION, Repository},
        snapshot::SnapshotStream,
    },
    ui::{self, SPINNER_TICK_CHARS, default_bar_draw_target},
    utils::{
        self,
        collections::{IdMap, IdSet},
    },
};

/// The cleanup plan. This struct contains lists of items that are valid, unused or need some work.
/// A plan can be executed to complete the garbage collection process. Once executed, the plan
/// object is consumed and cannot be used again. This is an intended safety measure.
pub struct Plan {
    pub repo: Arc<Repository>,
    pub total_packs: usize,          // Total number of packs in the repository
    pub referenced_blobs: IdSet<ID>, // Blobs referenced by existing snapshots
    pub referenced_packs: IdSet<ID>, // Packs referenced by the referenced blobs
    pub obsolete_packs: IdSet<ID>, // Packs containing non-referenced blobs or are small/duplicate sources
    pub small_packs: IdSet<ID>,    // Small packs marked to be repacked (to merge)
    pub tolerated_packs: IdSet<ID>, // Packs containing garbage, but keep due to tolerance
    pub unused_packs: IdSet<ID>,   // Packs not referenced by any snapshot or index
    pub index_ids: IdSet<ID>,      // Current index IDs
}

/// Scan the repository and make a plan of what needs to be cleaned.
pub fn scan(repo: Arc<Repository>, tolerance: f32) -> Result<Plan> {
    let (referenced_blobs, referenced_packs) = get_referenced_blobs_and_packs(repo.clone())?;

    let mut keep_packs: IdSet<ID> = repo.list_packs()?;
    let mut unused_packs = keep_packs.clone();

    keep_packs.retain(|id| referenced_packs.contains(id));
    unused_packs.retain(|id| !referenced_packs.contains(id));

    let mut plan = Plan {
        repo: repo.clone(),
        total_packs: keep_packs.len(),
        referenced_blobs,
        referenced_packs,
        obsolete_packs: IdSet::default(),
        tolerated_packs: IdSet::default(),
        unused_packs,
        index_ids: repo.index().ids(),
        small_packs: IdSet::default(),
    };

    // Count garbage bytes in each pack
    let mut kept_pack_size: IdMap<ID, u64> = IdMap::default();
    let mut pack_garbage: IdMap<ID, u64> = IdMap::default();

    // Find obsolete packs and blobs in index
    let spinner = ProgressBar::new_spinner();
    spinner.set_draw_target(default_bar_draw_target());
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.cyan} Finding obsolete blobs: {pos}")
            .unwrap()
            .tick_chars(SPINNER_TICK_CHARS),
    );
    spinner.enable_steady_tick(GlobalOpts::progress_refresh_interval());
    // for (id, locator) in repo.index().read().iter_ids() {

    repo.index().for_each_id(|id, locator| {
        kept_pack_size
            .entry(locator.pack_id)
            .and_modify(|size| {
                *size += locator.length as u64;
            })
            .or_default();

        if !plan.referenced_blobs.contains(id) {
            pack_garbage
                .entry(locator.pack_id)
                .and_modify(|size| *size += locator.length as u64)
                .or_insert(locator.length as u64);
            spinner.inc(1);
        }
    });

    // Find small packs to repack
    let current_pack_size = repo.pack_size();
    for (pack_id, size) in kept_pack_size {
        if (size as f32 / current_pack_size as f32) < DEFAULT_MIN_PACK_SIZE_FACTOR {
            plan.small_packs.insert(pack_id);
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
    spinner.set_draw_target(default_bar_draw_target());
    spinner.set_length(pack_garbage.len() as u64);
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.cyan} Checking garbage levels ({pos} / {len} packs)")
            .unwrap()
            .tick_chars(SPINNER_TICK_CHARS),
    );
    spinner.enable_steady_tick(GlobalOpts::progress_refresh_interval());
    for (pack_id, garbage_bytes) in pack_garbage.into_iter() {
        if (garbage_bytes as f32 / DEFAULT_PACK_SIZE as f32) > tolerance {
            keep_packs.remove(&pack_id);
            plan.obsolete_packs.insert(pack_id);
        } else {
            plan.tolerated_packs.insert(pack_id);
        }
        spinner.inc(1);
    }
    spinner.finish_and_clear();

    Ok(plan)
}

impl Plan {
    /// Execute the plan. Calling this method consumes the plan so it cannot be
    /// executed more than once.
    pub fn execute(mut self) -> Result<i64> {
        let mut deleted_size: i64 = 0;
        let mut added_size: i64 = 0;

        // Delete all expired locks first. This operation is independent of all others,
        // as the expired locks are not useful anymore.
        deleted_size += remove_expired_locks(&self.repo)? as i64;

        delete_trash_files(self.repo.backend().as_ref())?;

        // Append small packs to the obsolete pack list. These will be repacked and deleted,
        // which helps consolidate storage and eliminates duplicates that might be in them.
        if self.small_packs.len() > 1 {
            self.obsolete_packs.extend(self.small_packs.drain());
        }

        deleted_size += self.delete_unused_packs()? as i64;

        // No need to repack and rewrite the indices if there are no obsolete packs
        if !self.obsolete_packs.is_empty() {
            self.repo
                .init_pack_saver(mapache::defaults::DEFAULT_SNAPSHOT_PACKERS)?;

            self.repack()?;
            let repo_stats = self.repo.flush_and_finalize_pack_saver()?;

            added_size += (repo_stats.data + repo_stats.meta).encoded as i64;

            deleted_size += self.delete_old_indices()? as i64;
            deleted_size += self.delete_obsolete_packs()? as i64;
        }

        Ok(deleted_size - added_size)
    }

    /// Delete packs that contain no referenced blobs.
    fn delete_unused_packs(&self) -> Result<u64> {
        let unused_pack_delete_bar = ProgressBar::with_draw_target(
            Some(self.unused_packs.len() as u64),
            default_bar_draw_target(),
        )
        .with_style(
            ProgressStyle::default_bar()
                .template(
                    "[{percent} %] [{bar:20.cyan/white}] Deleting unused packs: {pos} / {len}",
                )
                .unwrap()
                .progress_chars("=> "),
        );

        let mut deleted_size = 0;
        for id in &self.unused_packs {
            deleted_size += self.repo.delete_file(ContentIdType::Pack, id, None)?;
            unused_pack_delete_bar.inc(1);
        }
        unused_pack_delete_bar.finish_and_clear();
        ui::cli::log!("Deleted {} unused packs", unused_pack_delete_bar.position());

        Ok(deleted_size)
    }

    /// Repack referenced blobs from obsolete packs to new packs.
    /// This process inherently removes duplicates by using the MasterIndex merge logic.
    fn repack(&mut self) -> Result<()> {
        // Collect information about ALL referenced blobs in obsolete packs.
        // We use iter_ids to find every single record (including duplicates)
        // that points into a pack scheduled for deletion.
        let repack_bar = ProgressBar::with_draw_target(
            Some(self.referenced_blobs.len() as u64),
            default_bar_draw_target(),
        )
        .with_style(
            ProgressStyle::default_bar()
                .template("[{percent} %] [{bar:20.cyan/white}] Finding blobs to repack")
                .unwrap()
                .progress_chars("=> "),
        );
        repack_bar.tick();

        // Key: Blob ID. Value: Tuple of (Pack ID, BlobType, Offset, Raw Length, Encoded Length)
        // HashMap ensures we only get one repack instruction per unique referenced blob ID.
        let mut repack_blob_info = HashMap::<ID, (ID, mapache::BlobType, u32, u32, u32)>::new();

        self.repo
            .index()
            .for_each_id(|referenced_blob_id, locator| {
                repack_bar.inc(1);

                if self.referenced_blobs.contains(referenced_blob_id)
                    && self.obsolete_packs.contains(&locator.pack_id)
                {
                    // HashMap insertion here automatically deduplicates the *repack instruction* by Blob ID.
                    repack_blob_info.insert(
                        *referenced_blob_id,
                        (
                            locator.pack_id,
                            locator.blob_type,
                            locator.offset,
                            locator.raw_length,
                            locator.length,
                        ),
                    );
                }
            });
        repack_bar.finish_and_clear();

        // Index cleanup must happen before repacking to clear the old references,
        // otherwise the repacked blobs will be considered duplicates and the save will fail.
        // This is where the old, duplicate index entries are logically removed.
        self.repo.index().cleanup(Some(&self.obsolete_packs));

        let repack_bar = ProgressBar::with_draw_target(
            Some(repack_blob_info.len() as u64),
            default_bar_draw_target(),
        )
        .with_style(
            ProgressStyle::default_bar()
                .template("[{percent} %] [{bar:20.cyan/white}] Repacking blobs: ({pos} / {len})")
                .unwrap()
                .progress_chars("=> "),
        );
        repack_bar.tick();

        const REPACK_CONCURRENCY: usize = 4;
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(REPACK_CONCURRENCY)
            .build()
            .expect("Failed to build thread pool");

        let process_result: Result<()> = pool.install(|| {
            repack_blob_info.into_par_iter().try_for_each_init(
                || {
                    self.repo
                        .get_encoding_context()
                        .expect("Failed to create thread context")
                },
                |ctx, (blob_id, (pack_id, blob_type, offset, _raw_length, length))| {
                    // Read and decode the original data from the obsolete pack.
                    let data = self.repo.read_from_pack_and_decode(
                        blob_type,
                        &pack_id,
                        offset as u64,
                        length as u64,
                    )?;

                    // Re-encode and save the blob. SaveID::WithID ensures the same blob_id is used,
                    // and its new location is recorded in the MasterIndex.
                    // let (_id, data_size, meta_size) = self.repo.encode_and_save_blob(
                    self.repo.encode_and_save_blob(
                        ctx,
                        blob_type,
                        data,
                        SaveID::WithID(blob_id),
                    )?;

                    repack_bar.inc(1);
                    Ok(())
                },
            )
        });
        repack_bar.finish_and_clear();
        ui::cli::log!("Repacked {} unique blobs", repack_bar.position());

        if let Err(e) = process_result {
            bail!("An error occurred during repacking: {e}");
        }

        Ok(())
    }

    /// Delete old index files
    /// This operation must be performed after the master index has been cleaned up
    /// and all referenced packs have been repacked.
    fn delete_old_indices(&mut self) -> Result<u64> {
        // Delete obsolete index files
        // Make sure that the new index files don't overlap the files to delete.
        let new_index_ids = self.repo.index().ids();
        self.index_ids.retain(|id| !new_index_ids.contains(id));

        let index_delete_bar = ProgressBar::with_draw_target(
            Some(self.index_ids.len() as u64),
            default_bar_draw_target(),
        )
        .with_style(
            ProgressStyle::default_bar()
                .template(
                    "[{percent} %] [{bar:20.cyan/white}] Deleting old index files: {pos}/{len}",
                )
                .unwrap()
                .progress_chars("=> "),
        );

        let deleted_size = AtomicU64::new(0);
        self.index_ids.par_iter().for_each(|id| {
            let size_res = self.repo.delete_file(ContentIdType::Index, id, None);
            deleted_size.fetch_add(size_res.unwrap_or(0), Ordering::AcqRel);
            index_delete_bar.inc(1);
        });
        index_delete_bar.finish_and_clear();
        ui::cli::log!(
            "Deleted {} obsolete index files",
            index_delete_bar.position()
        );

        Ok(deleted_size.load(Ordering::Relaxed))
    }

    /// Delete all pack files marked as obsolete.
    fn delete_obsolete_packs(&self) -> Result<u64> {
        // Delete obsolete pack files
        let obsolete_pack_delete_bar = ProgressBar::with_draw_target(
            Some(self.obsolete_packs.len() as u64),
            default_bar_draw_target(),
        )
        .with_style(
            ProgressStyle::default_bar()
                .template("[{percent} %]  [{bar:20.cyan/white}] Deleting obsolete pack files: {pos}/{len}")
                .unwrap()
                .progress_chars("=> "),
        );

        let deleted_size = AtomicU64::new(0);
        self.obsolete_packs.par_iter().for_each(|id| {
            let size_res = self.repo.delete_file(ContentIdType::Pack, id, None);
            deleted_size.fetch_add(size_res.unwrap_or(0), Ordering::AcqRel);
            obsolete_pack_delete_bar.inc(1);
        });
        obsolete_pack_delete_bar.finish_and_clear();
        ui::cli::log!(
            "Deleted {} obsolete packs",
            obsolete_pack_delete_bar.position()
        );

        Ok(deleted_size.load(Ordering::Relaxed))
    }
}

/// Returns all blobs and packs referenced by all existing snapshots in the repository.
fn get_referenced_blobs_and_packs(repo: Arc<Repository>) -> Result<(IdSet<ID>, IdSet<ID>)> {
    let mut referenced_blobs: IdSet<ID> = IdSet::default();
    let mut referenced_packs: IdSet<ID> = IdSet::default();
    let index = repo.index();

    let snapshot_stream = SnapshotStream::new(repo.clone())?;

    let spinner = ProgressBar::new_spinner();
    spinner.set_draw_target(default_bar_draw_target());
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.cyan} Searching referenced blobs: {pos}")
            .unwrap()
            .tick_chars(SPINNER_TICK_CHARS),
    );
    spinner.enable_steady_tick(GlobalOpts::progress_refresh_interval());

    for (_snapshot_id, snapshot) in snapshot_stream {
        let tree_id = snapshot.tree;

        // Tree blob of the snapshot
        if referenced_blobs.insert(tree_id) {
            spinner.set_position(referenced_blobs.len() as u64);
        }

        match index.get(&tree_id) {
            Some(locator) => {
                referenced_packs.insert(locator.pack_id);
            }
            None => {
                ui::cli::warning!(
                    "Snapshot tree {} is referenced but not found in index",
                    tree_id
                );
            }
        }

        // Stream all nodes in the snapshot
        let node_stream =
            SerializedNodeStream::new(repo.clone(), Some(tree_id), PathBuf::new(), None, None)?;

        let mut missing_tree_blobs = 0;
        let mut missing_data_blobs = 0;

        for node_res in node_stream {
            match node_res {
                Ok((_path, stream_node)) => {
                    let node = &stream_node.node;

                    // Tree blobs
                    if let Some(tree) = &node.tree {
                        if referenced_blobs.insert(*tree) {
                            spinner.set_position(referenced_blobs.len() as u64);
                        }

                        match index.get(tree) {
                            Some(locator) => {
                                referenced_packs.insert(locator.pack_id);
                            }
                            None => {
                                missing_tree_blobs += 1;
                            }
                        }
                    }

                    // Data blobs
                    if let Some(blobs) = &node.blobs {
                        for blob_id in blobs {
                            if referenced_blobs.insert(*blob_id) {
                                spinner.set_position(referenced_blobs.len() as u64);
                            }

                            match index.get(blob_id) {
                                Some(locator) => {
                                    referenced_packs.insert(locator.pack_id);
                                }
                                None => {
                                    missing_data_blobs += 1;
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    ui::cli::warning!("Error parsing node: {e}");
                }
            }
        }

        if missing_tree_blobs > 0 {
            ui::cli::warning!(
                "{} tree blobs referenced in snapshot are missing in the index",
                missing_tree_blobs.to_string().bold()
            );
        }

        if missing_data_blobs > 0 {
            ui::cli::warning!(
                "{} data blobs referenced in snapshot are missing in the index",
                missing_data_blobs.to_string().bold()
            );
        }
    }

    spinner.finish_and_clear();
    ui::cli::log!(
        "Found {} referenced blobs and {} packs",
        referenced_blobs.len(),
        referenced_packs.len()
    );

    Ok((referenced_blobs, referenced_packs))
}

/// Remove all expired locks from the repository
fn remove_expired_locks(repo: &Arc<Repository>) -> Result<u64> {
    let locks = repo.get_locks()?;
    let mut size_freed = 0;
    let mut num_deleted_locks = 0;

    for lock in locks {
        if lock.is_expired() {
            size_freed += repo.delete_file(ContentIdType::Lock, lock.id(), None)?;
            num_deleted_locks += 1;
        }
    }

    ui::cli::log!(
        "Deleted {}",
        utils::format_count(num_deleted_locks, "expired lock", "expired locks")
    );

    Ok(size_freed)
}

/// Remove all .tmp and .dropped files in the repository.
fn delete_trash_files(backend: &dyn StorageBackend) -> Result<()> {
    let nodes = read_backend_dir(backend, Path::new(""))?;
    let tmp_nodes = nodes
        .into_iter()
        .filter(|node| match node.path().extension() {
            Some(ext) => {
                ext.to_string_lossy() == REPO_TMP_EXTENSION
                    || ext.to_string_lossy() == REPO_DROPPED_EXTENSION
            }
            None => false,
        });

    for node in tmp_nodes {
        if backend.remove(node.path()).is_err() {
            // Failing to delete one of these files is not a fatal error.
            ui::cli::warning!("Could not remove file {}", node.path().display())
        }
    }

    Ok(())
}
