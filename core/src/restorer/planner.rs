use std::{
    collections::HashMap,
    fs::{self, File},
    path::Path,
    sync::{Arc, atomic::Ordering},
};

#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;

use anyhow::{Context, Result, anyhow, bail};
use futures::StreamExt;
use parking_lot::Mutex;
use tokio::task::spawn_blocking;

use crate::{
    fs::{self as repo_fs, node::Node, tree::SerializedNodeStream},
    mapache::{ID, defaults, hash},
    repository::index::MasterIndex,
    restorer::{
        BlobRestoreRequest, FileRestorePlan, RestorePlan, Restorer, Strategy, node_restorer,
    },
    ui::events::{Event, RestoreEvent, emit_event},
    utils::{self, size},
};

impl Restorer {
    /// Builds a restoration plan by walking the snapshot tree and determining
    /// which nodes need to be restored.
    pub(crate) async fn build_plan(
        &self,
        node_stream: SerializedNodeStream,
        index: Arc<MasterIndex>,
    ) -> Result<RestorePlan> {
        let files = Arc::new(Mutex::new(Vec::new()));
        let directories = Arc::new(Mutex::new(Vec::new()));
        let skipped_item_paths = Arc::new(Mutex::new(Vec::new()));
        let packs = Arc::new(dashmap::DashMap::<ID, Vec<(ID, BlobRestoreRequest)>>::new());
        let node_count = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let total_items = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let total_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let skipped_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let hardlink_index = Arc::new(parking_lot::Mutex::new(HashMap::<(u64, u64), usize>::new()));
        let hardlinks = Arc::new(parking_lot::Mutex::new(Vec::<(usize, usize)>::new()));

        emit_event(&self.event_sender, Event::Restore(RestoreEvent::Planning));

        let d = defaults::runtime();
        let num_workers = d.restore_blob_concurrency;

        node_stream
            .for_each_concurrent(num_workers, |node_res| {
                let index = index.clone();
                let files = files.clone();
                let directories = directories.clone();
                let skipped_item_paths = skipped_item_paths.clone();
                let packs = packs.clone();
                let node_count = node_count.clone();
                let total_items = total_items.clone();
                let total_bytes = total_bytes.clone();
                let skipped_bytes = skipped_bytes.clone();
                let event_sender = self.event_sender.clone();
                let shutdown_signal = self.shutdown_signal.clone();
                let hardlink_index = hardlink_index.clone();
                let hardlinks = hardlinks.clone();

                async move {
                    if shutdown_signal.load(Ordering::Acquire) {
                        return;
                    }

                    let visited = node_count.fetch_add(1, Ordering::Relaxed) + 1;
                    emit_event(&event_sender, Event::Restore(RestoreEvent::NodeVisited(visited)));

                    let (mut path, stream_node_res) = match node_res {
                        Ok(res) => res,
                        Err(e) => {
                            emit_event(&event_sender, Event::Restore(RestoreEvent::Error(format!("Error during planning: {e}"))));
                            return;
                        }
                    };

                    let stream_node = match stream_node_res {
                        Ok(node) => node,
                        Err(e) => {
                            emit_event(&event_sender, Event::Restore(RestoreEvent::Error(format!(
                                "Error reading node {}: {}",
                                path.display(),
                                e
                            ))));
                            return;
                        }
                    };
                    let node = stream_node.node;

                    if let Some(prefix) = &self.opts.strip_prefix {
                        path = match path.strip_prefix(prefix) {
                            Ok(stripped_path) => {
                                if stripped_path.as_os_str().is_empty() {
                                    return;
                                }
                                stripped_path.to_path_buf()
                            }
                            Err(_) => return,
                        };
                    }

                    total_items.fetch_add(1, Ordering::Relaxed);
                    let mut size_to_add = 0u64;
                    if node.is_file() {
                        size_to_add = node.metadata.size;
                        total_bytes.fetch_add(size_to_add, Ordering::Relaxed);
                    }

                    let restore_path = match utils::secure_join(&self.target_path, &path) {
                        Ok(p) => p,
                        Err(e) => {
                            emit_event(&event_sender, Event::Restore(RestoreEvent::Error(format!("Secure join failed for {path:?}: {e}"))));
                            return;
                        }
                    };

                    match self
                        .should_restore_node(&node, &restore_path, index.clone())
                        .await
                    {
                        Ok(true) => {}
                        Ok(false) => {
                            if node.is_file() {
                                skipped_bytes.fetch_add(size_to_add, Ordering::Relaxed);
                            }
                            skipped_item_paths.lock().push(restore_path);
                            return;
                        }
                        Err(e) => {
                            emit_event(&event_sender, Event::Restore(RestoreEvent::Error(format!(
                                "Error checking {}: {}",
                                path.display(),
                                e
                            ))));
                            return;
                        }
                    }

                    if node.is_dir() {
                        if !self.opts.dry_run
                            && let Err(e) = fs::create_dir_all(&restore_path)
                        {
                            emit_event(&event_sender, Event::Restore(RestoreEvent::Error(format!(
                                "Failed to create directory {}: {}",
                                restore_path.display(),
                                e
                            ))));
                            return;
                        }
                        directories.lock().push((restore_path.clone(), node.metadata));
                        return;
                    }

                    if node.is_file() {
                        let mut file_blobs = Vec::new();
                        if let Some(blobs) = &node.blobs {
                            let mut offset_in_file = 0;
                            for blob_id in blobs {
                                let locator = match index.get_data(blob_id) {
                                    Some(loc) => loc,
                                    None => {
                                        let err_msg = format!("Blob {blob_id} not found in index");
                                        emit_event(&event_sender, Event::Restore(RestoreEvent::Error(err_msg)));
                                        return;
                                    }
                                };
                                file_blobs.push((*blob_id, locator, offset_in_file));
                                offset_in_file += locator.raw_length as u64;
                            }
                        }

                        let file_idx = {
                            let mut files_lock = files.lock();
                            let idx = files_lock.len();
                            files_lock.push(FileRestorePlan {
                                path: restore_path.clone(),
                                num_blobs: 0,
                                size: node.metadata.size,
                                is_hardlink: false,
                            });
                            idx
                        };

                        let is_hardlink_secondary = {
                            if let (Some(dev), Some(inode)) =
                                (node.metadata.dev, node.metadata.inode)
                            {
                                let nlink = node.metadata.nlink;
                                if nlink.unwrap_or(0) > 1 {
                                    let mut idx = hardlink_index.lock();
                                    if let Some(&primary_idx) = idx.get(&(dev, inode)) {
                                        hardlinks.lock().push((file_idx, primary_idx));
                                        files.lock()[file_idx].is_hardlink = true;
                                        true
                                    } else {
                                        idx.insert((dev, inode), file_idx);
                                        false
                                    }
                                } else {
                                    false
                                }
                            } else {
                                false
                            }
                        };

                        if is_hardlink_secondary {
                            if !self.opts.dry_run
                                && let Some(parent) = restore_path.parent()
                                && let Err(e) = fs::create_dir_all(parent) {
                                    emit_event(&event_sender, Event::Restore(RestoreEvent::Error(format!(
                                        "Failed to create parent directory for secondary hardlink {}: {}",
                                        restore_path.display(),
                                        e
                                    ))));
                            }
                        } else {
                            let num_blobs = file_blobs.len().min(u32::MAX as usize) as u32;
                            for (blob_id, locator, offset_in_file) in file_blobs {
                                packs.entry(locator.pack_id).or_default().push((
                                    blob_id,
                                    BlobRestoreRequest {
                                        file_idx,
                                        offset_in_file,
                                        blob_offset: locator.offset,
                                        blob_length: locator.length,
                                        raw_length: locator.raw_length,
                                    },
                                ));
                            }

                            files.lock()[file_idx].num_blobs = num_blobs;
                        }
                    } else if node.is_symlink() {
                        // Symlinks are restored immediately during planning.
                        if !self.opts.dry_run
                            && let Err(e) = node_restorer::restore_node_to_path(
                                self,
                                &event_sender,
                                &node,
                                &restore_path,
                                false,
                            )
                            .await
                        {
                            emit_event(&event_sender, Event::Restore(RestoreEvent::Error(e.to_string())));
                        }
                        // We must record symlinks in the plan to report them as processed items later.
                        // However, we don't want them in `files` because `restore_packs` would treat them as files.
                        // Let's reuse skipped_item_paths for now, as they only need to be reported as processed.
                        skipped_item_paths.lock().push(restore_path);
                    }
                }
            })
            .await;

        let final_visited = node_count.load(Ordering::Relaxed);
        emit_event(
            &self.event_sender,
            Event::Restore(RestoreEvent::NodeVisited(final_visited)),
        );

        let files = Arc::into_inner(files)
            .context("Internal error: multiple Arc references to files remained after planning")?
            .into_inner();
        let directories = Arc::into_inner(directories)
            .context(
                "Internal error: multiple Arc references to directories remained after planning",
            )?
            .into_inner();
        let skipped_item_paths = Arc::into_inner(skipped_item_paths)
            .context(
                "Internal error: multiple Arc references to skipped_item_paths remained after planning",
            )?
            .into_inner();
        let packs_map = Arc::into_inner(packs)
            .context("Internal error: multiple Arc references to packs remained after planning")?
            .into_iter()
            .collect();
        let hardlinks = Arc::into_inner(hardlinks)
            .context(
                "Internal error: multiple Arc references to hardlinks remained after planning",
            )?
            .into_inner();

        Ok(RestorePlan {
            files: Arc::new(files),
            packs: Arc::new(packs_map),
            directories,
            skipped_item_paths,
            total_items: total_items.load(Ordering::Relaxed),
            total_bytes: total_bytes.load(Ordering::Relaxed),
            skipped_bytes: skipped_bytes.load(Ordering::Relaxed),
            hardlinks,
        })
    }

    /// Checks if a node should be restored based on the current restoration strategy.
    async fn should_restore_node(
        &self,
        node: &Node,
        restore_path: &Path,
        index: Arc<MasterIndex>,
    ) -> Result<bool> {
        if !repo_fs::path_exists(restore_path).await {
            return Ok(true);
        }

        if node.is_file()
            && let Ok(local_metadata) = fs::symlink_metadata(restore_path)
        {
            let local_size = local_metadata.len();
            let local_mtime = local_metadata.modified().ok();

            if local_size == node.metadata.size {
                let mtime_matches = node
                    .metadata
                    .times_match(local_mtime, node.metadata.modified_time);

                if mtime_matches && !self.opts.verify {
                    return Ok(false);
                }

                let content_matches = match self
                    .verify_file_content(node, restore_path, index)
                    .await
                {
                    Ok(matches) => matches,
                    Err(e) => {
                        tracing::warn!(target: "restorer", "Could not verify file {:?}: {e}", restore_path);
                        false
                    }
                };
                if content_matches {
                    return Ok(false);
                }
            }
        }

        match self.opts.strategy {
            Strategy::Overwrite => Ok(true),
            Strategy::Skip => Ok(false),
            Strategy::Newer => {
                let local_metadata = fs::symlink_metadata(restore_path).with_context(|| {
                    format!(
                        "Failed to get metadata for local file {}",
                        restore_path.display()
                    )
                })?;

                if let Some(repo_mtime) = node.metadata.modified_time {
                    let local_mtime = local_metadata.modified().with_context(|| {
                        format!(
                            "Failed to get modified time for local file {}",
                            restore_path.display()
                        )
                    })?;

                    let local_size = local_metadata.len();
                    if local_mtime < repo_mtime {
                        return Ok(true);
                    }

                    if local_mtime == repo_mtime && local_size != node.metadata.size {
                        return Ok(true);
                    }

                    return Ok(false);
                }

                Ok(true)
            }
            Strategy::Fail => {
                if node.is_dir() {
                    return Ok(true);
                }
                bail!("Target {} exists already", restore_path.display());
            }
        }
    }

    /// Verifies that the content of a local file matches the blobs in the repository.
    async fn verify_file_content(
        &self,
        node: &Node,
        local_path: &Path,
        index: Arc<MasterIndex>,
    ) -> Result<bool> {
        let blobs = match &node.blobs {
            Some(b) => b.clone(),
            None => return Ok(true),
        };

        let local_path = local_path.to_path_buf();
        spawn_blocking(move || {
            let file = File::open(&local_path)?;
            let mut offset = 0;

            const VERIFY_BUFFER_SIZE: usize = size::MiB as usize;
            let mut buffer = vec![0u8; VERIFY_BUFFER_SIZE];

            for blob_id in blobs {
                let locator = index
                    .get_data(&blob_id)
                    .ok_or_else(|| anyhow!("Blob {} not found in index", blob_id))?;

                let mut hasher = hash::Hasher::new();
                let mut remaining = locator.raw_length as u64;
                let mut blob_offset = offset;

                while remaining > 0 {
                    let to_read = (remaining as usize).min(VERIFY_BUFFER_SIZE);
                    let chunk = &mut buffer[..to_read];

                    #[cfg(unix)]
                    file.read_exact_at(chunk, blob_offset)?;
                    #[cfg(windows)]
                    {
                        let mut read_total = 0;
                        while read_total < to_read {
                            let n = file.seek_read(
                                &mut chunk[read_total..],
                                blob_offset + read_total as u64,
                            )?;
                            if n == 0 {
                                anyhow::bail!("Unexpected EOF while reading blob for verification");
                            }
                            read_total += n;
                        }
                    }

                    hasher.update(chunk);
                    let read_bytes = to_read as u64;
                    remaining -= read_bytes;
                    blob_offset += read_bytes;
                }

                let actual_id = hasher.finalize();
                if actual_id != blob_id {
                    return Ok(false);
                }
                offset += locator.raw_length as u64;
            }
            Ok(true)
        })
        .await?
    }
}
