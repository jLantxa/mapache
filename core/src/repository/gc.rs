use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result, anyhow};
use futures::{FutureExt, StreamExt, TryStreamExt, stream};

use crate::{
    backend::WriteContents,
    mapache::{
        self, ContentIdType, ID, SaveID,
        defaults::{self},
    },
    repository::{
        loader,
        repo::{REPO_DROPPED_EXTENSION, REPO_TMP_EXTENSION, Repository},
        snapshot::SnapshotStream,
    },
    ui,
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
pub async fn scan(
    repo: Arc<Repository>,
    tolerance: f32,
    reporter: Arc<dyn ui::GcProgressReporter>,
) -> Result<Plan> {
    tracing::info!(target: "gc", "Starting garbage collection scan (tolerance={:.1}%)", tolerance * 100.0);
    let (referenced_blobs, referenced_packs) =
        get_referenced_blobs_and_packs(repo.clone(), reporter.clone()).await?;

    let (keep_packs, object_trash) = repo.list_packs_and_trash().await?;
    let mut keep_packs = keep_packs;
    let mut unused_packs = keep_packs.clone();

    keep_packs.retain(|id| referenced_packs.contains(id));
    unused_packs.retain(|id| !referenced_packs.contains(id));
    tracing::debug!(target: "gc", "Found {} referenced packs and {} unused packs", keep_packs.len(), unused_packs.len());

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
    reporter.start_task(ui::GcTask::FindingObsoleteBlobs, None);
    let mut obsolete_blobs_count = 0;

    repo.index().for_each_id(|id, locator| {
        *kept_pack_size.entry(locator.pack_id).or_insert(0) += locator.length as u64;

        if !plan.referenced_blobs.contains(id) {
            pack_garbage
                .entry(locator.pack_id)
                .and_modify(|size| *size += locator.length as u64)
                .or_insert(locator.length as u64);
            obsolete_blobs_count += 1;
            reporter.update_task(ui::GcTask::FindingObsoleteBlobs, obsolete_blobs_count);
        }
    });
    reporter.finish_task(ui::GcTask::FindingObsoleteBlobs);

    // Find small packs to repack
    let current_pack_size = repo.pack_size();
    let min_pack_size_factor = defaults::runtime().min_pack_size_factor;
    for (pack_id, size) in kept_pack_size {
        if (size as f32 / current_pack_size as f32) < min_pack_size_factor {
            plan.small_packs.insert(pack_id);
        }
    }

    tracing::debug!(target: "gc", "Found {} obsolete blobs in {} packs", obsolete_blobs_count, pack_garbage.len());
    reporter.log(format!(
        "Found {} obsolete blobs in {} packs",
        obsolete_blobs_count,
        pack_garbage.len()
    ));

    // Check garbage levels
    reporter.start_task(
        ui::GcTask::CheckingGarbageLevels,
        Some(pack_garbage.len() as u64),
    );
    let mut checked_packs_count = 0;
    for (pack_id, garbage_bytes) in pack_garbage.into_iter() {
        if (garbage_bytes as f32 / current_pack_size as f32) > tolerance {
            tracing::trace!(target: "gc", "Pack {} is obsolete (garbage bytes: {})", pack_id.to_short_hex(8), garbage_bytes);
            keep_packs.remove(&pack_id);
            plan.obsolete_packs.insert(pack_id);
        } else {
            plan.tolerated_packs.insert(pack_id);
        }
        checked_packs_count += 1;
        reporter.update_task(ui::GcTask::CheckingGarbageLevels, checked_packs_count);
    }
    reporter.finish_task(ui::GcTask::CheckingGarbageLevels);
    tracing::info!(target: "gc", "Scan completed: {} obsolete, {} small, {} tolerated, {} unused packs", plan.obsolete_packs.len(), plan.small_packs.len(), plan.tolerated_packs.len(), plan.unused_packs.len());

    Ok(plan)
}

impl Plan {
    /// Execute the plan. Calling this method consumes the plan so it cannot be
    /// executed more than once.
    pub async fn execute(mut self, reporter: Arc<dyn ui::GcProgressReporter>) -> Result<GcSizes> {
        tracing::info!(target: "gc", "Executing garbage collection plan");
        let mut gc_sizes = GcSizes::default();

        // Delete all expired locks first. This operation is independent of all others,
        // as the expired locks are not useful anymore.
        gc_sizes.deleted_bytes += remove_expired_locks(&self.repo, reporter.clone()).await?;
        delete_trash_files(
            &self.repo,
            Some(self.object_trash.drain(..).collect()),
            reporter.clone(),
        )
        .await?;

        if self.small_packs.len() > 1 {
            tracing::debug!(target: "gc", "Marking {} small packs as obsolete for repacking", self.small_packs.len());
            self.obsolete_packs.extend(self.small_packs.drain());
        }

        gc_sizes.deleted_bytes += self.delete_unused_packs(reporter.clone()).await?;

        // No need to repack and rewrite the indices if there are no obsolete packs
        if !self.obsolete_packs.is_empty() {
            tracing::info!(target: "gc", "Repacking {} obsolete packs", self.obsolete_packs.len());
            self.repo
                .init_pack_saver(mapache::defaults::DEFAULT_SNAPSHOT_PACKERS)?;

            self.repack(reporter.clone()).await?;

            let repo_stats = self.repo.flush_and_finalize_pack_saver().await?;

            gc_sizes.added_bytes += (repo_stats.data + repo_stats.meta + repo_stats.index).encoded;
            gc_sizes.deleted_bytes += self.delete_old_indices(reporter.clone()).await?;
            gc_sizes.deleted_bytes += self.delete_obsolete_packs(reporter.clone()).await?;
        }

        tracing::info!(target: "gc", "Garbage collection execution finished");
        Ok(gc_sizes)
    }

    /// Delete packs that contain no referenced blobs.
    async fn delete_unused_packs(&self, reporter: Arc<dyn ui::GcProgressReporter>) -> Result<u64> {
        if self.unused_packs.is_empty() {
            return Ok(0);
        }

        reporter.start_task(
            ui::GcTask::DeletingUnusedPacks,
            Some(self.unused_packs.len() as u64),
        );
        let pos = Arc::new(std::sync::atomic::AtomicU64::new(0));

        let deleted_size = stream::iter(&self.unused_packs)
            .map(|id| {
                let repo = &self.repo;
                let reporter = reporter.clone();
                let pos = pos.clone();
                Ok(async move {
                    let size = repo.delete_file(ContentIdType::Pack, id, None).await?;
                    let current = pos.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                    reporter.update_task(ui::GcTask::DeletingUnusedPacks, current);
                    Ok::<u64, anyhow::Error>(size)
                })
            })
            .try_buffer_unordered(16)
            .try_fold(0, |acc, size| async move { Ok(acc + size) })
            .await?;

        reporter.finish_task(ui::GcTask::DeletingUnusedPacks);
        reporter.log(format!(
            "Deleted {} unused packs",
            pos.load(std::sync::atomic::Ordering::Relaxed)
        ));

        Ok(deleted_size)
    }

    /// Repack referenced blobs from obsolete packs to new packs.
    /// This process inherently removes duplicates by using the MasterIndex merge logic.
    async fn repack(&mut self, reporter: Arc<dyn ui::GcProgressReporter>) -> Result<()> {
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
            tracing::debug!(target: "gc", "No blobs to repack");
            return Ok(());
        }

        tracing::info!(target: "gc", "Repacking {} blobs", locators_to_repack.len());

        // Clear old references.
        // This prevents the saver from seeing the blobs as "already existing"
        // and ensures our new pack file becomes the primary source.
        self.repo.index().cleanup(Some(&self.obsolete_packs));

        reporter.start_task(
            ui::GcTask::RepackingBlobs,
            Some(locators_to_repack.len() as u64),
        );
        let pos = Arc::new(std::sync::atomic::AtomicU64::new(0));

        // Process chunks in parallel with pipelining
        const CHUNK_SIZE: usize = 100;
        const MAX_CONCURRENT_CHUNKS: usize = 4;

        let chunks: Vec<_> = locators_to_repack.chunks(CHUNK_SIZE).collect();

        stream::iter(chunks)
            .map(|chunk| {
                let repo = self.repo.clone();
                let reporter = reporter.clone();
                let pos = pos.clone();

                async move {
                    let loader = loader::BlobLoader::new(repo.clone());
                    let loaded_blobs = loader.load_with_locators(chunk.to_vec()).await?;

                    // Process blobs in parallel within each chunk
                    let tasks: Vec<_> = loaded_blobs
                        .into_iter()
                        .map(|(id, data)| {
                            let blob_type = chunk
                                .iter()
                                .find(|(cid, _)| *cid == id)
                                .map(|(_, loc)| loc.blob_type)
                                .unwrap_or(mapache::BlobType::Data);

                            let repo_clone = repo.clone();
                            let reporter_clone = reporter.clone();
                            let pos_clone = pos.clone();

                            tokio::task::spawn_blocking(move || {
                                let result = repo_clone.encode_and_save_blob(
                                    blob_type,
                                    WriteContents::Owned(data),
                                    SaveID::WithID(id),
                                );
                                let current = pos_clone
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                                    + 1;
                                reporter_clone.update_task(ui::GcTask::RepackingBlobs, current);
                                result
                            })
                        })
                        .collect();

                    // Await all tasks in parallel
                    let results = futures::future::join_all(tasks).await;

                    for result in results {
                        result
                            .context("Repack task panicked")?
                            .context("Repack blob failed")?;
                    }

                    Ok::<(), anyhow::Error>(())
                }
            })
            .buffer_unordered(MAX_CONCURRENT_CHUNKS)
            .try_collect::<Vec<_>>()
            .await?;

        reporter.finish_task(ui::GcTask::RepackingBlobs);
        Ok(())
    }

    /// Delete old index files
    /// This operation must be performed after the master index has been cleaned up
    /// and all referenced packs have been repacked.
    async fn delete_old_indices(
        &mut self,
        reporter: Arc<dyn ui::GcProgressReporter>,
    ) -> Result<u64> {
        // Make sure that the new index files don't overlap the files to delete.
        let new_index_ids = self.repo.index().ids();
        self.index_ids.retain(|id| !new_index_ids.contains(id));

        if self.index_ids.is_empty() {
            return Ok(0);
        }

        reporter.start_task(
            ui::GcTask::DeletingOldIndices,
            Some(self.index_ids.len() as u64),
        );
        let pos = Arc::new(std::sync::atomic::AtomicU64::new(0));

        let deleted_size = stream::iter(&self.index_ids)
            .map(|id| {
                let repo = &self.repo;
                let reporter = reporter.clone();
                let pos = pos.clone();
                async move {
                    let size = repo
                        .delete_file(ContentIdType::Index, id, None)
                        .await
                        .unwrap_or(0);
                    let current = pos.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                    reporter.update_task(ui::GcTask::DeletingOldIndices, current);
                    size
                }
            })
            .buffer_unordered(8)
            .fold(0u64, |acc, size| async move { acc + size })
            .await;

        reporter.finish_task(ui::GcTask::DeletingOldIndices);

        reporter.log(format!(
            "Deleted {} obsolete index files",
            pos.load(std::sync::atomic::Ordering::Relaxed)
        ));

        Ok(deleted_size)
    }

    /// Delete all pack files marked as obsolete.
    async fn delete_obsolete_packs(
        &self,
        reporter: Arc<dyn ui::GcProgressReporter>,
    ) -> Result<u64> {
        if self.obsolete_packs.is_empty() {
            return Ok(0);
        }

        reporter.start_task(
            ui::GcTask::DeletingObsoletePacks,
            Some(self.obsolete_packs.len() as u64),
        );
        let pos = Arc::new(std::sync::atomic::AtomicU64::new(0));

        // Convert the IdSet into an async stream.
        // We map each ID to an async delete operation and process them in parallel.
        let deleted_size = stream::iter(&self.obsolete_packs)
            .map(|id| {
                let repo = &self.repo;
                let reporter = reporter.clone();
                let pos = pos.clone();
                async move {
                    // Perform the async delete
                    let size = repo
                        .delete_file(ContentIdType::Pack, id, None)
                        .await
                        .unwrap_or(0);

                    let current = pos.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                    reporter.update_task(ui::GcTask::DeletingObsoletePacks, current);
                    size
                }
            })
            .buffer_unordered(8)
            .fold(0u64, |acc, size| async move { acc + size })
            .await;

        reporter.finish_task(ui::GcTask::DeletingObsoletePacks);

        reporter.log(format!(
            "Deleted {} obsolete packs",
            pos.load(std::sync::atomic::Ordering::Relaxed)
        ));

        Ok(deleted_size)
    }
}

/// Returns all blobs and packs referenced by all existing snapshots in the repository.
async fn get_referenced_blobs_and_packs(
    repo: Arc<Repository>,
    reporter: Arc<dyn ui::GcProgressReporter>,
) -> Result<(IdSet<ID>, IdSet<ID>)> {
    let referenced_blobs = Arc::new(parking_lot::Mutex::new(IdSet::default()));
    let referenced_packs = Arc::new(parking_lot::Mutex::new(IdSet::default()));
    let verified_trees = Arc::new(parking_lot::Mutex::new(IdSet::default()));

    let snapshot_stream = SnapshotStream::new(repo.clone()).await?;

    reporter.start_task(ui::GcTask::SearchingReferencedBlobs, None);

    snapshot_stream
        .map(|res| {
            let repo = repo.clone();
            let reporter = reporter.clone();
            let referenced_blobs = referenced_blobs.clone();
            let referenced_packs = referenced_packs.clone();
            let verified_trees = verified_trees.clone();

            async move {
                let (_snapshot_id, snapshot) = res?;
                let index = repo.index();
                let mut stack = vec![snapshot.tree];

                while !stack.is_empty() {
                    let to_fetch: Vec<_> = std::mem::take(&mut stack);
                    let mut fetch_stream = futures::stream::iter(to_fetch)
                        .map(|tree_id| {
                            let mut seen = verified_trees.lock();
                            if seen.contains(&tree_id) {
                                return futures::future::ready(Ok(None)).left_future();
                            }
                            seen.insert(tree_id);
                            drop(seen);

                            {
                                let mut blobs = referenced_blobs.lock();
                                if blobs.insert(tree_id) {
                                    reporter.update_task(
                                        ui::GcTask::SearchingReferencedBlobs,
                                        blobs.len() as u64,
                                    );
                                }
                            }

                            match index.get(&tree_id) {
                                Some(locator) => {
                                    referenced_packs.lock().insert(locator.pack_id);
                                }
                                None => {
                                    reporter.warning(format!(
                                        "Snapshot tree {} is referenced but not found in index",
                                        tree_id
                                    ));
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
                        let tree = match tree_res? {
                            Some(t) => t,
                            None => continue,
                        };

                        for node in tree.nodes {
                            if let Some(subtree_id) = node.tree {
                                stack.push(subtree_id);
                            }

                            let blobs = match node.blobs {
                                Some(b) => b,
                                None => continue,
                            };

                            let mut ref_blobs = referenced_blobs.lock();
                            let mut ref_packs = referenced_packs.lock();

                            for blob_id in blobs {
                                if ref_blobs.insert(blob_id) {
                                    reporter.update_task(
                                        ui::GcTask::SearchingReferencedBlobs,
                                        ref_blobs.len() as u64,
                                    );
                                }

                                if let Some(locator) = index.get(&blob_id) {
                                    ref_packs.insert(locator.pack_id);
                                } else {
                                    reporter.warning(format!(
                                        "Data blob {} is referenced but not found in index",
                                        blob_id
                                    ));
                                }
                            }
                        }
                    }
                }
                Ok::<(), anyhow::Error>(())
            }
        })
        .buffer_unordered(4)
        .try_collect::<Vec<_>>()
        .await?;

    reporter.finish_task(ui::GcTask::SearchingReferencedBlobs);

    let final_blobs = Arc::try_unwrap(referenced_blobs)
        .map_err(|_| anyhow!("Internal error: could not unwrap referenced_blobs Arc"))?
        .into_inner();
    let final_packs = Arc::try_unwrap(referenced_packs)
        .map_err(|_| anyhow!("Internal error: could not unwrap referenced_packs Arc"))?
        .into_inner();

    reporter.log(format!(
        "Found {} referenced blobs and {} packs",
        final_blobs.len(),
        final_packs.len()
    ));

    Ok((final_blobs, final_packs))
}

/// Remove all expired locks from the repository
async fn remove_expired_locks(
    repo: &Arc<Repository>,
    reporter: Arc<dyn ui::GcProgressReporter>,
) -> Result<u64> {
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

    reporter.log(format!(
        "Deleted {}",
        utils::format_count(num_deleted_locks, "stale lock", "stale locks")
    ));

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
    reporter: Arc<dyn ui::GcProgressReporter>,
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
            reporter.warning(format!("Could not remove trash file {}", path.display()));
        }
    }

    Ok(())
}
