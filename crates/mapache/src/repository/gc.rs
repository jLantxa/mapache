use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use crate::common::error::{MapacheError, Result};
use futures::{StreamExt, TryStreamExt, stream};

use crate::{
    backend::WriteContents,
    common::{
        self, ContentIdType, ID, SaveID,
        defaults::{self},
    },
    fs::tree::Tree,
    repository::{
        index::BlobLocator,
        loader,
        repo::{REPO_DROPPED_EXTENSION, REPO_ECC_EXTENSION, REPO_TMP_EXTENSION, Repository},
        snapshot::SnapshotStream,
    },
    ui::events::{Event, EventSender, GcEvent, GcTaskKind},
    utils::{
        self,
        collections::{IdMap, IdSet},
    },
};

#[derive(Clone)]
struct GcReporter(EventSender);

impl GcReporter {
    fn start_task(&self, kind: GcTaskKind, total: Option<u64>) {
        (self.0)(Event::Gc(GcEvent::TaskProgress {
            kind,
            pos: 0,
            total,
        }));
    }

    fn update_task(&self, kind: GcTaskKind, pos: u64) {
        (self.0)(Event::Gc(GcEvent::TaskProgress {
            kind,
            pos,
            total: None,
        }));
    }

    fn finish_task(&self, kind: GcTaskKind) {
        (self.0)(Event::Gc(GcEvent::TaskFinished { kind }));
    }

    fn log(&self, msg: String) {
        (self.0)(Event::Gc(GcEvent::Log(msg)));
    }

    fn warning(&self, msg: String) {
        (self.0)(Event::Gc(GcEvent::Warning(msg)));
    }
}

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
    /// Flag that signals GC to abort at the next safe checkpoint. Checked
    /// between phases only, so an interrupted GC leaves on-disk state intact.
    pub shutdown_signal: Arc<AtomicBool>,
}

/// Bails out if the shutdown signal has been raised.
fn check_shutdown(signal: &AtomicBool) -> Result<()> {
    if signal.load(Ordering::Relaxed) {
        return Err(MapacheError::Interrupted);
    }
    Ok(())
}

#[derive(Debug, Default)]
pub struct GcSizes {
    pub added_bytes: u64,
    pub deleted_bytes: u64,
}

/// Scan the repository and make a plan of what needs to be cleaned.
///
/// `shutdown_signal` is polled at safe checkpoints; if set, scanning aborts
/// without mutating any repository state. It is stored on the returned `Plan`
/// so subsequent execution can honour the same signal.
pub async fn scan(
    repo: Arc<Repository>,
    tolerance: f32,
    event_sender: EventSender,
    shutdown_signal: Arc<AtomicBool>,
) -> Result<Plan> {
    let reporter = GcReporter(event_sender);
    check_shutdown(&shutdown_signal)?;
    tracing::info!(target: "gc", "Starting garbage collection scan (tolerance={:.1}%)", tolerance * 100.0);
    let (referenced_blobs, referenced_packs) =
        get_referenced_blobs_and_packs(repo.clone(), reporter.0.clone(), shutdown_signal.clone())
            .await?;

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
        shutdown_signal: shutdown_signal.clone(),
    };

    // Count garbage bytes in each pack
    let mut kept_pack_size: IdMap<ID, u64> = IdMap::default();
    let mut pack_garbage: IdMap<ID, u64> = IdMap::default();

    // Find obsolete packs and blobs in index
    reporter.start_task(GcTaskKind::FindingObsoleteBlobs, None);
    let mut obsolete_blobs_count = 0;

    repo.index().for_each_id(|id, locator| {
        *kept_pack_size.entry(locator.pack_id).or_insert(0) += locator.length as u64;

        if !plan.referenced_blobs.contains(id) {
            pack_garbage
                .entry(locator.pack_id)
                .and_modify(|size| *size += locator.length as u64)
                .or_insert(locator.length as u64);
            obsolete_blobs_count += 1;
            reporter.update_task(GcTaskKind::FindingObsoleteBlobs, obsolete_blobs_count);
        }
    });

    // Also iterate cold indices, loading one at a time (memory bounded)
    let repo_clone = repo.clone();
    repo.index()
        .for_each_cold_index(
            |file_id| {
                let repo = repo_clone.clone();
                Box::pin(async move { repo.load_index_from_file_public(file_id).await })
            },
            |index| {
                for (id, locator) in index.iter_ids() {
                    *kept_pack_size.entry(locator.pack_id).or_insert(0) += locator.length as u64;

                    if !plan.referenced_blobs.contains(id) {
                        pack_garbage
                            .entry(locator.pack_id)
                            .and_modify(|size| *size += locator.length as u64)
                            .or_insert(locator.length as u64);
                        obsolete_blobs_count += 1;
                        reporter
                            .update_task(GcTaskKind::FindingObsoleteBlobs, obsolete_blobs_count);
                    }
                }
            },
        )
        .await;

    reporter.finish_task(GcTaskKind::FindingObsoleteBlobs);

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
        GcTaskKind::CheckingGarbageLevels,
        Some(pack_garbage.len() as u64),
    );
    let mut checked_packs_count = 0;
    for (pack_id, garbage_bytes) in pack_garbage.into_iter() {
        check_shutdown(&shutdown_signal)?;
        if (garbage_bytes as f32 / current_pack_size as f32) > tolerance {
            tracing::trace!(target: "gc", "Pack {} is obsolete (garbage bytes: {})", pack_id.to_short_hex(8), garbage_bytes);
            keep_packs.remove(&pack_id);
            plan.obsolete_packs.insert(pack_id);
        } else {
            plan.tolerated_packs.insert(pack_id);
        }
        checked_packs_count += 1;
        reporter.update_task(GcTaskKind::CheckingGarbageLevels, checked_packs_count);
    }
    reporter.finish_task(GcTaskKind::CheckingGarbageLevels);
    tracing::info!(target: "gc", "Scan completed: {} obsolete, {} small, {} tolerated, {} unused packs", plan.obsolete_packs.len(), plan.small_packs.len(), plan.tolerated_packs.len(), plan.unused_packs.len());

    Ok(plan)
}

/// Delete a set of objects from the repository in parallel, reporting
/// progress through the given reporter. Returns the total bytes freed.
async fn delete_objects(
    repo: &Repository,
    ids: &IdSet<ID>,
    content_type: ContentIdType,
    task_kind: GcTaskKind,
    label: &str,
    reporter: &GcReporter,
    concurrency: usize,
) -> Result<u64> {
    if ids.is_empty() {
        return Ok(0);
    }

    reporter.start_task(task_kind, Some(ids.len() as u64));
    let pos = Arc::new(std::sync::atomic::AtomicU64::new(0));

    let deleted_size = stream::iter(ids)
        .map(|id| {
            let r = reporter.clone();
            let pos = pos.clone();
            async move {
                let size = match repo.delete_file(content_type, id, None).await {
                    Ok(size) => size,
                    Err(e) => {
                        tracing::warn!(target: "gc", "failed to delete {label} {id}: {e}");
                        0
                    }
                };
                let current = pos.fetch_add(1, Ordering::Relaxed) + 1;
                r.update_task(task_kind, current);
                size
            }
        })
        .buffer_unordered(concurrency)
        .fold(0u64, |acc, size| async move { acc + size })
        .await;

    reporter.finish_task(task_kind);
    reporter.log(format!("Deleted {} {label}s", pos.load(Ordering::Relaxed)));

    Ok(deleted_size)
}

impl Plan {
    /// Execute the plan, consuming it. Checks the shutdown signal between
    /// phases only, so on-disk state stays consistent on interruption.
    pub async fn execute(mut self, event_sender: EventSender) -> Result<GcSizes> {
        let reporter = GcReporter(event_sender);
        tracing::info!(target: "gc", "Executing garbage collection plan");
        let mut gc_sizes = GcSizes::default();
        let signal = self.shutdown_signal.clone();

        check_shutdown(&signal)?;
        // Delete all expired locks first. This operation is independent of all others,
        // as the expired locks are not useful anymore.
        gc_sizes.deleted_bytes += remove_expired_locks(&self.repo, reporter.0.clone()).await?;
        delete_trash_files(
            &self.repo,
            Some(std::mem::take(&mut self.object_trash)),
            reporter.0.clone(),
        )
        .await?;

        if self.small_packs.len() > 1 {
            tracing::debug!(target: "gc", "Marking {} small packs as obsolete for repacking", self.small_packs.len());
            self.obsolete_packs.extend(self.small_packs.drain());
        }

        gc_sizes.deleted_bytes += self.delete_unused_packs(reporter.0.clone()).await?;

        // Safety checkpoint: only unreferenced items have been deleted so
        // far. On-disk state is still consistent. Below this, `repack`
        // mutates the in-memory index before the new packs are flushed, so
        // we let it run to completion (or abandon wholesale).
        check_shutdown(&signal)?;

        // Clean unused packs from the index — they were deleted from disk
        // above but the index still references them.
        if !self.obsolete_packs.is_empty() {
            if !self.unused_packs.is_empty() {
                self.repo.index().cleanup(Some(&self.unused_packs));
            }

            tracing::info!(target: "gc", "Repacking {} obsolete packs", self.obsolete_packs.len());
            self.repo
                .init_pack_saver(common::defaults::DEFAULT_SNAPSHOT_PACKERS)?;

            self.repack(reporter.0.clone()).await?;

            // New index is now on disk; subsequent deletions are safe to
            // interrupt partway.
            let repo_stats = self.repo.flush_and_finalize_pack_saver().await?;

            gc_sizes.added_bytes += (repo_stats.data + repo_stats.meta + repo_stats.index).encoded;
            gc_sizes.deleted_bytes += self.delete_old_indices(reporter.0.clone()).await?;
            gc_sizes.deleted_bytes += self.delete_obsolete_packs(reporter.0.clone()).await?;
        } else if !self.unused_packs.is_empty() {
            tracing::info!(target: "gc", "Cleaning index for {} unused packs", self.unused_packs.len());
            self.repo.index().cleanup(Some(&self.unused_packs));
            self.repo.index().persist(&self.repo).await?;
            gc_sizes.deleted_bytes += self.delete_old_indices(reporter.0.clone()).await?;
        }

        tracing::info!(target: "gc", "Garbage collection execution finished");
        Ok(gc_sizes)
    }

    /// Delete packs that contain no referenced blobs.
    async fn delete_unused_packs(&self, event_sender: EventSender) -> Result<u64> {
        let r = GcReporter(event_sender);
        delete_objects(
            &self.repo,
            &self.unused_packs,
            ContentIdType::Pack,
            GcTaskKind::DeletingUnusedPacks,
            "unused pack",
            &r,
            16,
        )
        .await
    }

    /// Repack referenced blobs from obsolete packs to new packs.
    /// This process inherently removes duplicates by using the MasterIndex merge logic.
    async fn repack(&mut self, event_sender: EventSender) -> Result<()> {
        // Gather locators while the index is still intact.
        // We use a Vec to preserve the exact metadata we need for the loader.
        let mut locators_to_repack = Vec::new();

        self.repo.index().for_each_id(|id, locator| {
            if self.referenced_blobs.contains(id) && self.obsolete_packs.contains(&locator.pack_id)
            {
                locators_to_repack.push((*id, locator));
            }
        });

        // Also gather locators from cold indices, loading one at a time
        let repo_clone = self.repo.clone();
        let referenced_blobs = self.referenced_blobs.clone();
        let obsolete_packs = self.obsolete_packs.clone();
        self.repo
            .index()
            .for_each_cold_index(
                |file_id| {
                    let repo = repo_clone.clone();
                    Box::pin(async move { repo.load_index_from_file_public(file_id).await })
                },
                |index| {
                    for (id, locator) in index.iter_ids() {
                        if referenced_blobs.contains(id)
                            && obsolete_packs.contains(&locator.pack_id)
                        {
                            locators_to_repack.push((*id, locator));
                        }
                    }
                },
            )
            .await;

        if locators_to_repack.is_empty() {
            tracing::debug!(target: "gc", "No blobs to repack");
            return Ok(());
        }

        tracing::info!(target: "gc", "Repacking {} blobs", locators_to_repack.len());

        // Clear old references so the saver doesn't treat these blobs as
        // already existing. This only mutates the in-memory index; the
        // on-disk index is not updated until the post-repack flush.
        self.repo.index().cleanup(Some(&self.obsolete_packs));

        let r = GcReporter(event_sender);
        r.start_task(
            GcTaskKind::RepackingBlobs,
            Some(locators_to_repack.len() as u64),
        );
        let pos = Arc::new(std::sync::atomic::AtomicU64::new(0));

        // Partition blobs into chunks that each fit within the decoded-byte
        // budget, so load_with_locators never exceeds it.
        let d = defaults::runtime();
        let max_concurrent = d.gc_repack_concurrency;
        let budget = d.gc_decoded_budget;
        let sem_capacity = std::cmp::min(budget as usize, u32::MAX as usize).max(1);
        let byte_semaphore = Arc::new(tokio::sync::Semaphore::new(sem_capacity));

        let chunks: Vec<&[(ID, BlobLocator)]> = {
            let mut chunks = Vec::new();
            let mut start = 0usize;
            let mut acc = 0u64;
            for (i, (_, loc)) in locators_to_repack.iter().enumerate() {
                if acc > 0 && acc + loc.raw_length as u64 > budget {
                    chunks.push(&locators_to_repack[start..i]);
                    start = i;
                    acc = 0;
                }
                acc += loc.raw_length as u64;
            }
            if start < locators_to_repack.len() {
                chunks.push(&locators_to_repack[start..]);
            }
            chunks
        };

        stream::iter(chunks)
            .map(|chunk| {
                let repo = self.repo.clone();
                let r = r.clone();
                let pos = pos.clone();
                let shutdown_signal = self.shutdown_signal.clone();
                let sem = byte_semaphore.clone();

                async move {
                    // Stay within the decoded-byte budget by acquiring permits for
                    // the total raw (decoded) size of every blob in this chunk.
                    let chunk_bytes: usize =
                        chunk.iter().map(|(_, loc)| loc.raw_length as usize).sum();
                    let needed = std::cmp::min(chunk_bytes, sem_capacity);
                    let permit = sem
                        .acquire_many(u32::try_from(needed).unwrap_or(u32::MAX))
                        .await
                        .map_err(|e| {
                            MapacheError::Repo(format!(
                                "failed to acquire GC memory budget permit: {}",
                                e
                            ))
                        })?;

                    // Build a fast lookup for blob types to avoid O(n²) linear search
                    let mut blob_types: IdMap<ID, common::BlobType> = IdMap::default();
                    for (cid, loc) in chunk.iter() {
                        blob_types.insert(*cid, loc.blob_type);
                    }

                    let loader = loader::BlobLoader::new(repo.clone());
                    let loaded_blobs = loader.load_with_locators(chunk.to_vec()).await?;

                    // Release the memory budget permit before waiting for encode+save to
                    // finish, so the next chunk can start loading decoded data in parallel.
                    drop(permit);

                    // Process blobs with bounded concurrency via a stream pipeline instead
                    // of collecting all futures upfront, so only N data Vecs are in memory
                    // at any time. The concurrency limit also prevents exhausting the
                    // blocking thread pool (default 512), which would deadlock when
                    // LocalBackend::write calls spawn_blocking internally for file I/O.
                    let max_concurrent = defaults::DEFAULT_SNAPSHOT_READERS;

                    stream::iter(loaded_blobs)
                        .map(|(id, data)| {
                            let blob_type = blob_types
                                .get(&id)
                                .copied()
                                .unwrap_or(common::BlobType::Data);
                            let repo_clone = repo.clone();
                            let r = r.clone();
                            let pos_clone = pos.clone();

                            async move {
                                tokio::task::spawn_blocking(move || {
                                    let result = repo_clone.encode_and_save_blob(
                                        blob_type,
                                        WriteContents::Owned(data),
                                        SaveID::WithID(id),
                                    );
                                    let current = pos_clone
                                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                                        + 1;
                                    r.update_task(GcTaskKind::RepackingBlobs, current);
                                    result
                                })
                                .await
                                .map_err(|e| MapacheError::task_panicked("repack", e))?
                                .map_err(|e| {
                                    MapacheError::Repo(format!("repack blob failed: {}", e))
                                })?;
                                Ok::<(), MapacheError>(())
                            }
                        })
                        .buffer_unordered(max_concurrent)
                        .try_collect::<Vec<_>>()
                        .await?;

                    // Check shutdown only after a chunk has fully finished
                    // writing. Abandoning here leaves the on-disk index
                    // untouched; partial new packs become `unused_packs` on
                    // the next GC run.
                    check_shutdown(&shutdown_signal)?;

                    Ok::<(), MapacheError>(())
                }
            })
            .buffer_unordered(max_concurrent)
            .try_collect::<Vec<_>>()
            .await?;

        r.finish_task(GcTaskKind::RepackingBlobs);
        Ok(())
    }

    /// Delete old index files.
    /// This operation must be performed after the master index has been cleaned up
    /// and all referenced packs have been repacked.
    async fn delete_old_indices(&mut self, event_sender: EventSender) -> Result<u64> {
        let new_index_ids = self.repo.index().ids();
        self.index_ids.retain(|id| !new_index_ids.contains(id));

        let r = GcReporter(event_sender);
        delete_objects(
            &self.repo,
            &self.index_ids,
            ContentIdType::Index,
            GcTaskKind::DeletingOldIndices,
            "obsolete index file",
            &r,
            8,
        )
        .await
    }

    /// Delete all pack files marked as obsolete.
    async fn delete_obsolete_packs(&self, event_sender: EventSender) -> Result<u64> {
        let r = GcReporter(event_sender);
        delete_objects(
            &self.repo,
            &self.obsolete_packs,
            ContentIdType::Pack,
            GcTaskKind::DeletingObsoletePacks,
            "obsolete pack",
            &r,
            8,
        )
        .await
    }
}

/// Returns all blobs and packs referenced by all existing snapshots in the repository.
async fn get_referenced_blobs_and_packs(
    repo: Arc<Repository>,
    event_sender: EventSender,
    shutdown_signal: Arc<AtomicBool>,
) -> Result<(IdSet<ID>, IdSet<ID>)> {
    let reporter = GcReporter(event_sender);
    let referenced_blobs = Arc::new(parking_lot::Mutex::new(IdSet::default()));
    let referenced_packs = Arc::new(parking_lot::Mutex::new(IdSet::default()));
    let verified_trees = Arc::new(parking_lot::Mutex::new(IdSet::default()));

    let snapshot_stream = SnapshotStream::new(repo.clone()).await?;

    reporter.start_task(GcTaskKind::SearchingReferencedBlobs, None);

    snapshot_stream
        .map(|res| {
            let repo = repo.clone();
            let reporter = reporter.clone();
            let referenced_blobs = referenced_blobs.clone();
            let referenced_packs = referenced_packs.clone();
            let verified_trees = verified_trees.clone();
            let shutdown_signal = shutdown_signal.clone();

            async move {
                check_shutdown(&shutdown_signal)?;
                let (_snapshot_id, snapshot) = res?;
                let index = repo.index();
                let mut stack = vec![snapshot.tree];

                while !stack.is_empty() {
                    check_shutdown(&shutdown_signal)?;
                    let to_fetch: Vec<_> = std::mem::take(&mut stack);
                    let mut fetch_stream = futures::stream::iter(to_fetch)
                        .map(|tree_id| {
                            let repo = repo.clone();
                            let reporter = reporter.clone();
                            let referenced_blobs = referenced_blobs.clone();
                            let referenced_packs = referenced_packs.clone();
                            let verified_trees = verified_trees.clone();

                            async move {
                                {
                                    let mut seen = verified_trees.lock();
                                    if seen.contains(&tree_id) {
                                        return Ok(None);
                                    }
                                    seen.insert(tree_id);
                                }

                                {
                                    let mut blobs = referenced_blobs.lock();
                                    if blobs.insert(tree_id) {
                                        reporter.update_task(
                                            GcTaskKind::SearchingReferencedBlobs,
                                            blobs.len() as u64,
                                        );
                                    }
                                }

                                let index = repo.index();
                                if let Some(locator) = index.get(&tree_id).await {
                                    referenced_packs.lock().insert(locator.pack_id);
                                } else {
                                    reporter.warning(format!(
                                        "Snapshot tree {} is referenced but not found in index",
                                        tree_id
                                    ));
                                }

                                let tree = Tree::load_from_repo(repo.as_ref(), &tree_id).await?;
                                Ok::<_, MapacheError>(Some(tree))
                            }
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

                            for blob_id in blobs {
                                {
                                    let mut ref_blobs = referenced_blobs.lock();
                                    if ref_blobs.insert(blob_id) {
                                        reporter.update_task(
                                            GcTaskKind::SearchingReferencedBlobs,
                                            ref_blobs.len() as u64,
                                        );
                                    }
                                }

                                if let Some(locator) = index.get(&blob_id).await {
                                    referenced_packs.lock().insert(locator.pack_id);
                                }
                            }
                        }
                    }
                }
                Ok::<(), MapacheError>(())
            }
        })
        .buffer_unordered(4)
        .try_collect::<Vec<_>>()
        .await?;

    reporter.finish_task(GcTaskKind::SearchingReferencedBlobs);

    let final_blobs = Arc::try_unwrap(referenced_blobs)
        .map_err(|_| {
            MapacheError::Internal(
                "internal error: could not unwrap referenced_blobs Arc".to_string(),
            )
        })?
        .into_inner();
    let final_packs = Arc::try_unwrap(referenced_packs)
        .map_err(|_| {
            MapacheError::Internal(
                "internal error: could not unwrap referenced_packs Arc".to_string(),
            )
        })?
        .into_inner();

    reporter.log(format!(
        "Found {} referenced blobs and {} packs",
        final_blobs.len(),
        final_packs.len()
    ));

    Ok((final_blobs, final_packs))
}

/// Remove all expired locks from the repository
async fn remove_expired_locks(repo: &Arc<Repository>, event_sender: EventSender) -> Result<u64> {
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

    (event_sender)(Event::Gc(GcEvent::Log(format!(
        "Deleted {}",
        utils::format_count(num_deleted_locks, "stale lock", "stale locks")
    ))));

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
    event_sender: EventSender,
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
                match backend.list_dir(&dir).await {
                    Ok(entries) => {
                        for node in entries {
                            let entry = node.into_path();
                            if let Some(ext) = entry.extension() {
                                let ext_str = ext.to_string_lossy();
                                if ext_str == REPO_TMP_EXTENSION || ext_str == REPO_DROPPED_EXTENSION {
                                    found.push(entry);
                                } else if ext_str == REPO_ECC_EXTENSION {
                                    // Orphaned .ecc: base file doesn't exist.
                                    let base = entry.with_extension("");
                                    if !backend.path_exists(&base).await {
                                        found.push(entry);
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!(target: "gc", "Failed to list trash directory {:?}: {e}", dir);
                    }
                }
                Ok::<Vec<PathBuf>, MapacheError>(found)
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
            (event_sender)(Event::Gc(GcEvent::Warning(format!(
                "Could not remove trash file {}",
                path.display()
            ))));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::Local;

    use super::*;
    use crate::{
        backend::{
            StorageBackend, StorageHint,
            mock::{BackendOp, MockBackend},
        },
        common::{BlobType, defaults::TEST_REPO_CONFIG, traits::BlobSaver},
        fs::{
            node::{Node, NodeType},
            tree::Tree,
        },
        repository::{
            repo::{Auth, THIS_REPOSITORY_VERSION},
            snapshot::Snapshot,
        },
        ui::events::noop_sender,
    };

    fn make_auth() -> Auth {
        Auth {
            username: "mapachito".to_string(),
            password: zeroize::Zeroizing::new("password".to_string()),
        }
    }

    async fn init_repo() -> Result<(Arc<Repository>, Arc<MockBackend>)> {
        let auth = make_auth();
        let backend = Arc::new(MockBackend::new());
        let backend_dyn: Arc<dyn StorageBackend> = backend.clone();
        Repository::init(
            THIS_REPOSITORY_VERSION,
            &auth,
            None,
            backend_dyn.clone(),
            None,
        )
        .await?;
        let (repo, _) =
            Repository::try_open_unlocked(&auth, None, backend_dyn, TEST_REPO_CONFIG).await?;
        Ok((repo, backend))
    }

    fn make_node(name: &str, blobs: Vec<ID>) -> Node {
        Node {
            name: name.to_string(),
            node_type: NodeType::File,
            blobs: Some(blobs),
            ..Default::default()
        }
    }

    async fn save_snapshot(repo: &Arc<Repository>, tree_id: ID) -> Result<()> {
        let snapshot = Snapshot {
            timestamp: Local::now(),
            tree: tree_id,
            root: std::path::PathBuf::from("/"),
            paths: vec![std::path::PathBuf::from("/root")],
            ..Default::default()
        };
        let snapshot_bytes = serde_json::to_vec(&snapshot)?;
        let hint = StorageHint {
            file_type: ContentIdType::Snapshot,
            is_metadata: true,
        };
        repo.save_file(&SaveID::CalculateID, &snapshot_bytes, hint, None)
            .await?;
        repo.reload_master_index().await?;
        Ok(())
    }

    /// Garbage collection: orphaned blobs removed, referenced blobs survive.
    ///
    /// Scenario: 3 data blobs saved, but only 1 is referenced by a snapshot.
    /// The 2 orphaned blobs must be deleted by GC.
    #[tokio::test]
    async fn test_garbage_collection_removes_orphaned_blobs() -> Result<()> {
        let (repo, backend) = init_repo().await?;

        // Pack 1: save A, B, C (no snapshot yet, so all three are in a pack but
        // unreferenced)
        repo.init_pack_saver(2)?;
        let blob_a = repo.encode_and_save_blob(
            BlobType::Data,
            WriteContents::Borrowed(b"aaaa"),
            SaveID::CalculateID,
        )?;
        let blob_b = repo.encode_and_save_blob(
            BlobType::Data,
            WriteContents::Borrowed(b"bbbb"),
            SaveID::CalculateID,
        )?;
        let blob_c = repo.encode_and_save_blob(
            BlobType::Data,
            WriteContents::Borrowed(b"cccc"),
            SaveID::CalculateID,
        )?;
        repo.flush_and_finalize_pack_saver().await?;

        // Pack 2: tree referencing only A → pack + snapshot make A reachable
        repo.init_pack_saver(2)?;
        let mut tree = Tree::new(vec![make_node("file_a.txt", vec![blob_a])]);
        let tree_id = tree
            .save_to_store(repo.clone() as Arc<dyn BlobSaver>, repo.repo_version())
            .await?;
        repo.flush_and_finalize_pack_saver().await?;
        save_snapshot(&repo, tree_id).await?;

        // Clear history before GC execution
        backend.clear_history();

        let plan = super::scan(
            repo.clone(),
            0.0,
            noop_sender(),
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
        )
        .await?;

        assert!(plan.referenced_blobs.contains(&blob_a));
        assert!(!plan.referenced_blobs.contains(&blob_b));
        assert!(!plan.referenced_blobs.contains(&blob_c));

        let gc_sizes = plan.execute(noop_sender()).await?;
        assert!(gc_sizes.deleted_bytes > 0);

        assert_eq!(repo.load_blob(&blob_a).await?, b"aaaa");
        assert!(repo.load_blob(&blob_b).await.is_err());
        assert!(repo.load_blob(&blob_c).await.is_err());

        // Spy: Verify that packs containing orphaned blobs were removed
        let history = backend.history();
        let removed_paths: Vec<PathBuf> = history
            .iter()
            .filter_map(|e| {
                if let BackendOp::Remove(path) = &e.op {
                    Some(path.clone())
                } else {
                    None
                }
            })
            .collect();

        // With init_pack_saver(2), blobs a, b, c were saved.
        // Pack 1: a, b, c.
        // Pack 2: tree (with a).
        // Blobs b and c are orphaned. They reside in Pack 1.
        // Pack 1 should be removed, Pack 2 should stay.

        // We expect exactly 4 removal operations:
        // - 2 index files (the old index files being replaced/cleaned up)
        // - 2 object files (orphaned packs or similar, depends on repository structure)
        assert_eq!(
            removed_paths.len(),
            4,
            "Expected exactly 4 removals, got: {:?}",
            removed_paths
        );

        // Ensure at least one was a data/object file removal
        assert!(
            removed_paths
                .iter()
                .any(|p| p.to_string_lossy().contains("objects")),
            "Expected removal of an object pack, got: {:?}",
            removed_paths
        );

        Ok(())
    }

    /// GC with all blobs referenced — no garbage-based obsolete packs, no unused packs.
    ///
    /// Packs may still be repacked if they fall below the small-pack threshold,
    /// so deleted_bytes can be non-zero. What matters is that referenced blobs survive.
    #[tokio::test]
    async fn test_gc_no_garbage() -> Result<()> {
        let (repo, _backend) = init_repo().await?;

        repo.init_pack_saver(2)?;
        let blob_a = repo.encode_and_save_blob(
            BlobType::Data,
            WriteContents::Borrowed(b"aaaa"),
            SaveID::CalculateID,
        )?;
        let blob_b = repo.encode_and_save_blob(
            BlobType::Data,
            WriteContents::Borrowed(b"bbbb"),
            SaveID::CalculateID,
        )?;
        let mut tree = Tree::new(vec![
            make_node("a.txt", vec![blob_a]),
            make_node("b.txt", vec![blob_b]),
        ]);
        let tree_id = tree
            .save_to_store(repo.clone() as Arc<dyn BlobSaver>, repo.repo_version())
            .await?;
        repo.flush_and_finalize_pack_saver().await?;
        save_snapshot(&repo, tree_id).await?;

        let plan = super::scan(
            repo.clone(),
            0.0,
            noop_sender(),
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
        )
        .await?;

        assert!(plan.referenced_blobs.contains(&blob_a));
        assert!(plan.referenced_blobs.contains(&blob_b));
        // No packs marked obsolete due to garbage
        assert!(
            plan.obsolete_packs.is_empty(),
            "no garbage-based obsolete packs"
        );
        // No completely unused packs
        assert!(plan.unused_packs.is_empty(), "no unused packs");

        let _gc_sizes = plan.execute(noop_sender()).await?;

        // Referenced blobs survive regardless of small-pack repacking
        assert_eq!(repo.load_blob(&blob_a).await?, b"aaaa");
        assert_eq!(repo.load_blob(&blob_b).await?, b"bbbb");

        Ok(())
    }

    /// GC with multiple snapshots — shared blobs survive.
    #[tokio::test]
    async fn test_gc_multiple_snapshots() -> Result<()> {
        let (repo, _backend) = init_repo().await?;

        // Pack 1: A, B + tree1 + snapshot1
        repo.init_pack_saver(2)?;
        let blob_a = repo.encode_and_save_blob(
            BlobType::Data,
            WriteContents::Borrowed(b"aaaa"),
            SaveID::CalculateID,
        )?;
        let blob_b = repo.encode_and_save_blob(
            BlobType::Data,
            WriteContents::Borrowed(b"bbbb"),
            SaveID::CalculateID,
        )?;
        let mut tree1 = Tree::new(vec![
            make_node("a.txt", vec![blob_a]),
            make_node("b.txt", vec![blob_b]),
        ]);
        let tree1_id = tree1
            .save_to_store(repo.clone() as Arc<dyn BlobSaver>, repo.repo_version())
            .await?;
        repo.flush_and_finalize_pack_saver().await?;
        save_snapshot(&repo, tree1_id).await?;

        // Pack 2: C + tree2 + snapshot2
        repo.init_pack_saver(2)?;
        let blob_c = repo.encode_and_save_blob(
            BlobType::Data,
            WriteContents::Borrowed(b"cccc"),
            SaveID::CalculateID,
        )?;
        let blob_d = repo.encode_and_save_blob(
            BlobType::Data,
            WriteContents::Borrowed(b"dddd"),
            SaveID::CalculateID,
        )?;
        let mut tree2 = Tree::new(vec![
            make_node("b.txt", vec![blob_b]), // shares blob_b with snapshot1
            make_node("c.txt", vec![blob_c]),
        ]);
        let tree2_id = tree2
            .save_to_store(repo.clone() as Arc<dyn BlobSaver>, repo.repo_version())
            .await?;
        repo.flush_and_finalize_pack_saver().await?;
        save_snapshot(&repo, tree2_id).await?;

        // blob_d is orphaned (not referenced by any snapshot)
        let plan = super::scan(
            repo.clone(),
            0.0,
            noop_sender(),
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
        )
        .await?;

        assert!(plan.referenced_blobs.contains(&blob_a));
        assert!(plan.referenced_blobs.contains(&blob_b));
        assert!(plan.referenced_blobs.contains(&blob_c));
        assert!(!plan.referenced_blobs.contains(&blob_d));

        let gc_sizes = plan.execute(noop_sender()).await?;
        assert!(gc_sizes.deleted_bytes > 0);

        assert_eq!(repo.load_blob(&blob_a).await?, b"aaaa");
        assert_eq!(repo.load_blob(&blob_b).await?, b"bbbb");
        assert_eq!(repo.load_blob(&blob_c).await?, b"cccc");
        assert!(repo.load_blob(&blob_d).await.is_err());

        Ok(())
    }

    /// When the shutdown signal is set before scanning starts, `scan` must
    /// abort with an "interrupted" error and must not mutate repository
    /// state.
    #[tokio::test]
    async fn test_scan_aborts_on_shutdown_signal() -> Result<()> {
        let (repo, _backend) = init_repo().await?;

        repo.init_pack_saver(2)?;
        let blob = repo.encode_and_save_blob(
            BlobType::Data,
            WriteContents::Borrowed(b"data"),
            SaveID::CalculateID,
        )?;
        repo.flush_and_finalize_pack_saver().await?;

        let signal = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let err = match super::scan(repo.clone(), 0.0, noop_sender(), signal.clone()).await {
            Ok(_) => panic!("scan should fail when interrupted"),
            Err(e) => e,
        };
        let msg = err.to_string();
        assert!(
            msg.to_lowercase().contains("interrupt"),
            "expected interruption error, got: {msg}"
        );

        // The blob written before the scan is still there; scan is read-only.
        assert_eq!(repo.load_blob(&blob).await?, b"data");

        Ok(())
    }

    /// When the shutdown signal is set after a plan is built, `execute` must
    /// abort at the next safe checkpoint. The on-disk repository must remain
    /// in a state where every referenced blob can still be loaded.
    #[tokio::test]
    async fn test_execute_aborts_on_shutdown_signal() -> Result<()> {
        let (repo, _backend) = init_repo().await?;

        repo.init_pack_saver(2)?;
        let blob_a = repo.encode_and_save_blob(
            BlobType::Data,
            WriteContents::Borrowed(b"aaaa"),
            SaveID::CalculateID,
        )?;
        let blob_b = repo.encode_and_save_blob(
            BlobType::Data,
            WriteContents::Borrowed(b"bbbb"),
            SaveID::CalculateID,
        )?;
        let mut tree = Tree::new(vec![make_node("a.txt", vec![blob_a, blob_b])]);
        let tree_id = tree
            .save_to_store(repo.clone() as Arc<dyn BlobSaver>, repo.repo_version())
            .await?;
        repo.flush_and_finalize_pack_saver().await?;
        save_snapshot(&repo, tree_id).await?;

        // Build a plan with no shutdown signal.
        let signal = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let plan = super::scan(repo.clone(), 0.0, noop_sender(), signal.clone()).await?;

        // Now set the signal so `execute` aborts at the first checkpoint
        // (after `delete_unused_packs`, before `repack`).
        signal.store(true, std::sync::atomic::Ordering::SeqCst);

        let err = match plan.execute(noop_sender()).await {
            Ok(_) => panic!("execute should fail when interrupted"),
            Err(e) => e,
        };
        let msg = err.to_string();
        assert!(
            msg.to_lowercase().contains("interrupt"),
            "expected interruption error, got: {msg}"
        );

        // All referenced blobs must still be loadable after interruption.
        assert_eq!(repo.load_blob(&blob_a).await?, b"aaaa");
        assert_eq!(repo.load_blob(&blob_b).await?, b"bbbb");

        Ok(())
    }
}
