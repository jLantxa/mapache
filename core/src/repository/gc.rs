use std::{collections::HashMap, path::Path, sync::Arc};

use anyhow::Result;
use futures::{
    StreamExt,
    stream::{self, FuturesUnordered},
};
use indicatif::{ProgressBar, ProgressStyle};

use crate::{
    backend::{StorageBackend, WriteContents, read_backend_dir},
    fs::tree::Tree,
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

#[derive(Debug, Default)]
pub struct GcSizes {
    pub added_bytes: u64,
    pub deleted_bytes: u64,
}

/// Scan the repository and make a plan of what needs to be cleaned.
pub async fn scan(repo: Arc<Repository>, tolerance: f32) -> Result<Plan> {
    let (referenced_blobs, referenced_packs) = get_referenced_blobs_and_packs(repo.clone()).await?;

    let mut keep_packs: IdSet<ID> = repo.list_packs().await?;
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
        delete_trash_files(self.repo.backend().as_ref()).await?;

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
            deleted_size += self.repo.delete_file(ContentIdType::Pack, id, None).await?;
            unused_pack_delete_bar.inc(1);
        }
        unused_pack_delete_bar.finish_and_clear();
        ui::cli::log!("Deleted {} unused packs", unused_pack_delete_bar.position());

        Ok(deleted_size)
    }

    /// Repack referenced blobs from obsolete packs to new packs.
    /// This process inherently removes duplicates by using the MasterIndex merge logic.
    async fn repack(&mut self) -> Result<()> {
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

        // Key: Blob ID. Value: Tuple of (Pack ID, BlobType, Offset, Encoded Length)
        // HashMap ensures we only get one repack instruction per unique referenced blob ID.
        let mut repack_blob_info = HashMap::<ID, (ID, mapache::BlobType, u32, u32)>::new();

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

        let rt_handle = tokio::runtime::Handle::current();
        let (tx, rx) = crossbeam_channel::unbounded();

        let pool = rayon::ThreadPoolBuilder::new().num_threads(4).build()?;

        pool.scope(|s| {
            for (blob_id, (pack_id, blob_type, offset, length)) in repack_blob_info {
                let repo = self.repo.clone();
                let rt = rt_handle.clone();
                let tx = tx.clone();

                s.spawn(move |_| {
                    let res = rt.block_on(async {
                        let data = repo
                            .read_from_pack_and_decode(
                                blob_type,
                                &pack_id,
                                offset as u64,
                                length as u64,
                            )
                            .await?;

                        // Re-pack via the PackSaver (which is already handling its own threads)
                        repo.encode_and_save_blob(
                            blob_type,
                            WriteContents::Owned(data),
                            SaveID::WithID(blob_id),
                        )
                        .await
                    });
                    let _ = tx.send(res);
                });
            }
        });

        // Check for errors in the channel
        drop(tx);
        for res in rx {
            res?;
        }

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
    let mut referenced_blobs: IdSet<ID> = IdSet::default();
    let mut referenced_packs: IdSet<ID> = IdSet::default();
    let index = repo.index();

    let mut snapshot_stream = SnapshotStream::new(repo.clone()).await?;

    let spinner = ProgressBar::new_spinner();
    spinner.set_draw_target(default_bar_draw_target());
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.cyan} Searching referenced blobs: {pos}")
            .unwrap()
            .tick_chars(SPINNER_TICK_CHARS),
    );
    spinner.enable_steady_tick(GlobalOpts::progress_refresh_interval());

    // Track which trees we've already traversed to avoid redundant work.
    let mut visited_trees = IdSet::default();
    let mut pending_trees = Vec::new();
    let mut loading_trees = FuturesUnordered::new();

    // Concurrency limit for tree loading
    const CONCURRENCY_LIMIT: usize = 8;

    loop {
        // Fill loading_trees up to the limit from pending_trees
        while !pending_trees.is_empty() && loading_trees.len() < CONCURRENCY_LIMIT {
            let tree_id = pending_trees.pop().unwrap();
            let repo = repo.clone();
            loading_trees.push(async move {
                let tree_res = Tree::load_from_repo(&repo, &tree_id).await;
                (tree_id, tree_res)
            });
        }

        // Try to get more snapshots if we have capacity in loading_trees
        if loading_trees.len() < CONCURRENCY_LIMIT
            && let Some((_snapshot_id, snapshot)) = snapshot_stream.next().await
        {
            let root_tree_id = snapshot.tree;
            if visited_trees.insert(root_tree_id) {
                pending_trees.push(root_tree_id);
            }
            // Continue to fill loading_trees in the next iteration
            continue;
        }

        // If nothing is loading and nothing is pending, and no more snapshots, we are done
        if loading_trees.is_empty() {
            break;
        }

        // Wait for at least one tree to finish loading
        if let Some((tree_id, tree_res)) = loading_trees.next().await {
            // Tree blob itself is referenced
            referenced_blobs.insert(tree_id);
            if let Some(locator) = index.get(&tree_id) {
                referenced_packs.insert(locator.pack_id);
            } else {
                ui::cli::warning!("Tree blob {} is referenced but not found in index", tree_id);
            }

            let tree = match tree_res {
                Ok(t) => t,
                Err(e) => {
                    ui::cli::warning!("Failed to load tree {}: {}", tree_id, e);
                    continue;
                }
            };

            for node in tree.nodes {
                // If this is a directory, add its tree ID if not visited
                if let Some(subtree_id) = node.tree
                    && visited_trees.insert(subtree_id)
                {
                    pending_trees.push(subtree_id);
                }

                // Add all data blobs referenced by this node
                if let Some(blobs) = node.blobs {
                    for blob_id in blobs {
                        if referenced_blobs.insert(blob_id) {
                            spinner.set_position(referenced_blobs.len() as u64);
                        }

                        if let Some(locator) = index.get(&blob_id) {
                            referenced_packs.insert(locator.pack_id);
                        }
                    }
                }
            }
            spinner.set_position(referenced_blobs.len() as u64);
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
async fn remove_expired_locks(repo: &Arc<Repository>) -> Result<u64> {
    let locks = repo.get_locks().await?;
    let mut size_freed = 0;
    let mut num_deleted_locks = 0;

    for lock in locks {
        if lock.is_expired() {
            size_freed += repo
                .delete_file(ContentIdType::Lock, lock.id(), None)
                .await?;
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
async fn delete_trash_files(backend: &dyn StorageBackend) -> Result<()> {
    let nodes = read_backend_dir(backend, Path::new("")).await?;
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
        if backend.remove(node.path()).await.is_err() {
            // Failing to delete one of these files is not a fatal error.
            ui::cli::warning!("Could not remove file {}", node.path().display())
        }
    }

    Ok(())
}
