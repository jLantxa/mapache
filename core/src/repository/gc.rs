use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result, anyhow};
use colored::Colorize;
use futures::{FutureExt, StreamExt, TryStreamExt, stream};
use indicatif::{ProgressBar, ProgressStyle};

use crate::{
    backend::WriteContents,
    mapache::{
        self, ContentIdType, ID, SaveID,
        defaults::{DEFAULT_MIN_PACK_SIZE_FACTOR, DEFAULT_PACK_SIZE},
        global::GlobalOpts,
    },
    repository::{
        loader,
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
    pub object_trash: Vec<PathBuf>, // Pre-collected .tmp/.dropped files in objects directory
}

#[derive(Debug, Default)]
pub struct GcSizes {
    pub added_bytes: u64,
    pub deleted_bytes: u64,
}

/// Scan the repository and make a plan of what needs to be cleaned.
pub async fn scan(repo: Arc<Repository>, tolerance: f32) -> Result<Plan> {
    let (referenced_blobs, referenced_packs) = get_referenced_blobs_and_packs(repo.clone()).await?;

    let (keep_packs, object_trash) = repo.list_packs_and_trash().await?;
    let mut keep_packs = keep_packs;
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
        object_trash,
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
        *kept_pack_size.entry(locator.pack_id).or_insert(0) += locator.length as u64;

        if !plan.referenced_blobs.contains(id) {
            pack_garbage
                .entry(locator.pack_id)
                .and_modify(|size| *size += locator.length as u64)
                .or_insert(locator.length as u64);
            spinner.inc(1);
        }
    });
    spinner.finish_and_clear();

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
    pub async fn execute(mut self) -> Result<GcSizes> {
        let mut gc_sizes = GcSizes::default();

        // Delete all expired locks first. This operation is independent of all others,
        // as the expired locks are not useful anymore.
        gc_sizes.deleted_bytes += remove_expired_locks(&self.repo).await?;
        delete_trash_files(&self.repo, Some(self.object_trash.drain(..).collect())).await?;

        if self.small_packs.len() > 1 {
            self.obsolete_packs.extend(self.small_packs.drain());
        }

        gc_sizes.deleted_bytes += self.delete_unused_packs().await?;

        // No need to repack and rewrite the indices if there are no obsolete packs
        if !self.obsolete_packs.is_empty() {
            self.repo
                .init_pack_saver(mapache::defaults::DEFAULT_SNAPSHOT_PACKERS)?;

            self.repack().await?;

            let repo_stats = self.repo.flush_and_finalize_pack_saver().await?;

            gc_sizes.added_bytes += (repo_stats.data + repo_stats.meta + repo_stats.index).encoded;
            gc_sizes.deleted_bytes += self.delete_old_indices().await?;
            gc_sizes.deleted_bytes += self.delete_obsolete_packs().await?;
        }

        Ok(gc_sizes)
    }

    /// Delete packs that contain no referenced blobs.
    async fn delete_unused_packs(&self) -> Result<u64> {
        if self.unused_packs.is_empty() {
            return Ok(0);
        }

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

        let deleted_size = stream::iter(&self.unused_packs)
            .map(|id| {
                let repo = &self.repo;
                let bar = &unused_pack_delete_bar;
                Ok(async move {
                    let size = repo.delete_file(ContentIdType::Pack, id, None).await?;
                    bar.inc(1);
                    Ok::<u64, anyhow::Error>(size)
                })
            })
            .try_buffer_unordered(16)
            .try_fold(0, |acc, size| async move { Ok(acc + size) })
            .await?;

        unused_pack_delete_bar.finish_and_clear();
        ui::cli::log!("Deleted {} unused packs", unused_pack_delete_bar.position());

        Ok(deleted_size)
    }

    /// Repack referenced blobs from obsolete packs to new packs.
    /// This process inherently removes duplicates by using the MasterIndex merge logic.
    async fn repack(&mut self) -> Result<()> {
        // Gather locators while the index is still intact.
        // We use a Vec to preserve the exact metadata we need for the loader.
        let mut locators_to_repack = Vec::new();

        self.repo.index().for_each_id(|id, locator| {
            if self.referenced_blobs.contains(id) && self.obsolete_packs.contains(&locator.pack_id)
            {
                locators_to_repack.push((*id, locator));
            }
        });

        if locators_to_repack.is_empty() {
            return Ok(());
        }

        // Clear old references.
        // This prevents the saver from seeing the blobs as "already existing"
        // and ensures our new pack file becomes the primary source.
        self.repo.index().cleanup(Some(&self.obsolete_packs));

        let repack_bar = ProgressBar::with_draw_target(
            Some(locators_to_repack.len() as u64),
            default_bar_draw_target(),
        )
        .with_style(
            ProgressStyle::default_bar()
                .template("[{percent} %] [{bar:20.cyan/white}] Repacking blobs: {pos} / {len}")
                .unwrap()
                .progress_chars("=> "),
        );

        let loader = loader::BlobLoader::new(self.repo.clone());

        // We chunk the locators to keep memory usage predictable during the repack.
        for chunk in locators_to_repack.chunks(100) {
            let loaded_blobs = loader.load_with_locators(chunk.to_vec()).await?;

            for (id, data) in loaded_blobs {
                // We find the original blob_type from our chunked locators
                let blob_type = chunk
                    .iter()
                    .find(|(cid, _)| *cid == id)
                    .map(|(_, loc)| loc.blob_type)
                    .unwrap_or(mapache::BlobType::Data);

                let repo_clone = self.repo.clone();
                tokio::task::spawn_blocking(move || {
                    repo_clone.encode_and_save_blob(
                        blob_type,
                        WriteContents::Owned(data),
                        SaveID::WithID(id),
                    )
                })
                .await
                .context("Repack task panicked")??;

                repack_bar.inc(1);
            }
        }

        repack_bar.finish_and_clear();
        Ok(())
    }

    /// Delete old index files
    /// This operation must be performed after the master index has been cleaned up
    /// and all referenced packs have been repacked.
    async fn delete_old_indices(&mut self) -> Result<u64> {
        // Make sure that the new index files don't overlap the files to delete.
        let new_index_ids = self.repo.index().ids();
        self.index_ids.retain(|id| !new_index_ids.contains(id));

        if self.index_ids.is_empty() {
            return Ok(0);
        }

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

        let deleted_size = stream::iter(&self.index_ids)
            .map(|id| {
                let repo = &self.repo;
                let bar = &index_delete_bar;
                async move {
                    let size = repo
                        .delete_file(ContentIdType::Index, id, None)
                        .await
                        .unwrap_or(0);
                    bar.inc(1);
                    size
                }
            })
            .buffer_unordered(8)
            .fold(0u64, |acc, size| async move { acc + size })
            .await;

        index_delete_bar.finish_and_clear();

        ui::cli::log!(
            "Deleted {} obsolete index files",
            index_delete_bar.position()
        );

        Ok(deleted_size)
    }

    /// Delete all pack files marked as obsolete.
    async fn delete_obsolete_packs(&self) -> Result<u64> {
        if self.obsolete_packs.is_empty() {
            return Ok(0);
        }

        // Initialize progress bar
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

        // Convert the IdSet into an async stream.
        // We map each ID to an async delete operation and process them in parallel.
        let deleted_size = stream::iter(&self.obsolete_packs)
            .map(|id| {
                let repo = &self.repo;
                let bar = &obsolete_pack_delete_bar;
                async move {
                    // Perform the async delete
                    let size = repo
                        .delete_file(ContentIdType::Pack, id, None)
                        .await
                        .unwrap_or(0);

                    bar.inc(1);
                    size
                }
            })
            .buffer_unordered(8)
            .fold(0u64, |acc, size| async move { acc + size })
            .await;

        obsolete_pack_delete_bar.finish_and_clear();

        ui::cli::log!(
            "Deleted {} obsolete packs",
            obsolete_pack_delete_bar.position()
        );

        Ok(deleted_size)
    }
}

/// Returns all blobs and packs referenced by all existing snapshots in the repository.
async fn get_referenced_blobs_and_packs(repo: Arc<Repository>) -> Result<(IdSet<ID>, IdSet<ID>)> {
    let referenced_blobs = Arc::new(parking_lot::Mutex::new(IdSet::default()));
    let referenced_packs = Arc::new(parking_lot::Mutex::new(IdSet::default()));
    let verified_trees = Arc::new(parking_lot::Mutex::new(IdSet::default()));

    let mut snapshot_stream = SnapshotStream::new(repo.clone()).await?;
    let mut snapshots = Vec::new();
    while let Some(res) = snapshot_stream.next().await {
        snapshots.push(res?);
    }

    let spinner = ProgressBar::new_spinner();
    spinner.set_draw_target(default_bar_draw_target());
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.cyan} Searching referenced blobs: {pos}")
            .unwrap()
            .tick_chars(SPINNER_TICK_CHARS),
    );
    spinner.enable_steady_tick(GlobalOpts::progress_refresh_interval());

    let results = stream::iter(snapshots)
        .map(|(_snapshot_id, snapshot)| {
            let repo = repo.clone();
            let spinner = spinner.clone();
            let referenced_blobs = referenced_blobs.clone();
            let referenced_packs = referenced_packs.clone();
            let verified_trees = verified_trees.clone();

            async move {
                let index = repo.index();
                let mut stack = vec![snapshot.tree];

                while !stack.is_empty() {
                    let to_fetch: Vec<_> = std::mem::take(&mut stack);
                    let mut fetch_stream = futures::stream::iter(to_fetch)
                        .map(|tree_id| {
                            // Global Deduplication
                            let mut seen = verified_trees.lock();
                            if seen.contains(&tree_id) {
                                return futures::future::ready(Ok(None)).left_future();
                            }
                            seen.insert(tree_id);
                            drop(seen);

                            // Mark tree blob as referenced
                            {
                                let mut blobs = referenced_blobs.lock();
                                if blobs.insert(tree_id) {
                                    spinner.set_position(blobs.len() as u64);
                                }
                            }

                            match index.get(&tree_id) {
                                Some(locator) => {
                                    referenced_packs.lock().insert(locator.pack_id);
                                }
                                None => {
                                    ui::cli::warning!(
                                        "Snapshot tree {} is referenced but not found in index",
                                        tree_id
                                    );
                                }
                            }

                            let repo = repo.clone();
                            async move {
                                let tree =
                                    crate::fs::tree::Tree::load_from_repo(repo.as_ref(), &tree_id)
                                        .await?;
                                Ok::<_, anyhow::Error>(Some(tree))
                            }
                            .right_future()
                        })
                        .buffer_unordered(8);

                    while let Some(tree_res) = fetch_stream.next().await {
                        let maybe_tree = tree_res?;
                        if let Some(tree) = maybe_tree {
                            for node in tree.nodes {
                                // Tree blobs (subdirectories)
                                if let Some(subtree_id) = node.tree {
                                    stack.push(subtree_id);
                                }

                                // Data blobs
                                if let Some(blobs) = node.blobs {
                                    let mut ref_blobs = referenced_blobs.lock();
                                    let mut ref_packs = referenced_packs.lock();

                                    for blob_id in blobs {
                                        if ref_blobs.insert(blob_id) {
                                            spinner.set_position(ref_blobs.len() as u64);
                                        }

                                        match index.get(&blob_id) {
                                            Some(locator) => {
                                                ref_packs.insert(locator.pack_id);
                                            }
                                            None => {
                                                ui::cli::warning!(
                                                    "Data blob {} is referenced but not found in index",
                                                    blob_id
                                                );
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                Ok::<(), anyhow::Error>(())
            }
        })
        .buffer_unordered(4) // Parallelism factor for snapshots
        .collect::<Vec<_>>()
        .await;

    for res in results {
        res?;
    }

    spinner.finish_and_clear();

    let final_blobs = Arc::try_unwrap(referenced_blobs)
        .map_err(|_| anyhow!("Internal error: could not unwrap referenced_blobs Arc"))?
        .into_inner();
    let final_packs = Arc::try_unwrap(referenced_packs)
        .map_err(|_| anyhow!("Internal error: could not unwrap referenced_packs Arc"))?
        .into_inner();

    ui::cli::log!(
        "Found {} referenced blobs and {} packs",
        final_blobs.len(),
        final_packs.len()
    );

    Ok((final_blobs, final_packs))
}

/// Remove all expired locks from the repository
async fn remove_expired_locks(repo: &Arc<Repository>) -> Result<u64> {
    let locks = repo.get_locks().await?;
    let mut size_freed = 0;
    let mut num_deleted_locks = 0;

    for lock in locks {
        if lock.is_stale() {
            size_freed += repo
                .delete_file(ContentIdType::Lock, lock.id(), None)
                .await?;
            num_deleted_locks += 1;
        }
    }

    ui::cli::log!(
        "Deleted {}",
        utils::format_count(num_deleted_locks, "stale lock", "stale locks")
    );

    Ok(size_freed)
}

/// Remove all .tmp and .dropped files in the repository.
///
/// This function avoids a full recursive crawl by targeting only the specific
/// directories where temporary files are created (index, snapshots, keys, locks, and data fanout).
/// If `object_trash` is provided, it skips scanning the object directories.
async fn delete_trash_files(
    repo: &Arc<Repository>,
    object_trash: Option<Vec<PathBuf>>,
) -> Result<()> {
    let target_dirs = vec![
        repo.snapshot_path().to_path_buf(),
        repo.index_path().to_path_buf(),
        repo.keys_path().to_path_buf(),
        repo.locks_path().to_path_buf(),
    ];

    let backend = repo.backend();

    // Use a stream with limited concurrency to avoid saturating the runtime
    // and preventing deadlocks in block_on contexts.
    let trash_files: Vec<PathBuf> = stream::iter(target_dirs)
        .map(|dir| {
            let backend = backend.clone();
            async move {
                let mut found = Vec::new();
                if let Ok(entries) = backend.list_dir(&dir).await {
                    for node in entries {
                        let entry = node.into_path();
                        if let Some(ext) = entry.extension() {
                            let ext_str = ext.to_string_lossy();
                            if ext_str == REPO_TMP_EXTENSION || ext_str == REPO_DROPPED_EXTENSION {
                                found.push(entry);
                            }
                        }
                    }
                }
                Ok::<Vec<PathBuf>, anyhow::Error>(found)
            }
        })
        .buffer_unordered(4) // Use a small concurrency factor to stay safe
        .try_collect::<Vec<Vec<PathBuf>>>()
        .await?
        .into_iter()
        .flatten()
        .chain(object_trash.into_iter().flatten())
        .collect();

    // Delete found trash files
    for path in trash_files {
        if backend.remove(&path).await.is_err() {
            ui::cli::warning!("Could not remove trash file {}", path.display());
        }
    }

    Ok(())
}
