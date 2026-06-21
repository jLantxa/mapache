use std::{collections::HashMap, sync::Arc, sync::atomic::Ordering};

#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;

use anyhow::{Context, Result, anyhow, bail};
use futures::StreamExt;
use rayon::prelude::*;
use tokio::task::spawn_blocking;

use crate::{
    mapache::{BlobType, ContentIdType, ID, defaults},
    repository::{index::BlobLocator, loader, storage::SecureStorage},
    ui::events::{Event, EventSender, RestoreEvent, emit_event},
};

use super::{BlobRestoreRequest, FileRestorePlan, PackMap, Restorer, ShardedFileHandleCache};

struct DecodedBlob {
    data: Vec<u8>,
    targets: Vec<BlobRestoreRequest>,
}

struct RestoreContext {
    handle_cache: Arc<ShardedFileHandleCache>,
    files: Arc<Vec<FileRestorePlan>>,
    remaining: Arc<Vec<std::sync::atomic::AtomicU32>>,
    initialized: Arc<Vec<std::sync::atomic::AtomicBool>>,
    restorer: Arc<Restorer>,
    event_sender: EventSender,
    quit_on_error: bool,
}

impl Restorer {
    pub(crate) async fn restore_packs(
        &self,
        files: Arc<Vec<FileRestorePlan>>,
        packs: Arc<PackMap>,
        secure_storage: Arc<SecureStorage>,
        dry_run: bool,
    ) -> Result<()> {
        if dry_run {
            for blob_requests in packs.values() {
                for (_, request) in blob_requests.iter() {
                    emit_event(
                        &self.event_sender,
                        Event::Restore(RestoreEvent::BytesProcessed(request.raw_length as u64)),
                    );
                }
            }
            for file in files.iter() {
                emit_event(
                    &self.event_sender,
                    Event::Restore(RestoreEvent::ItemProcessed(file.path.clone())),
                );
            }
            return Ok(());
        }

        let remaining = Arc::new(
            files
                .iter()
                .map(|f| std::sync::atomic::AtomicU32::new(f.num_blobs))
                .collect::<Vec<_>>(),
        );

        let initialized = Arc::new(
            (0..files.len())
                .map(|_| std::sync::atomic::AtomicBool::new(false))
                .collect::<Vec<_>>(),
        );

        let d = defaults::runtime();
        let handle_cache = Arc::new(ShardedFileHandleCache::new(d.restore_max_open_files));

        for (idx, file) in files.iter().enumerate() {
            if file.num_blobs == 0 && !file.is_hardlink {
                if !file.path.exists() || file.size == 0 {
                    let mut cache_guard = handle_cache.get_shard(idx).lock();
                    cache_guard.get_handle(idx, &file.path, file, &initialized[idx], self)?;
                }
                emit_event(
                    &self.event_sender,
                    Event::Restore(RestoreEvent::ItemProcessed(file.path.clone())),
                );
            }
        }

        let restorer_arc = Arc::new(self.clone_for_workers());
        let ctx = Arc::new(RestoreContext {
            handle_cache,
            files,
            remaining,
            initialized,
            restorer: restorer_arc,
            event_sender: self.event_sender.clone(),
            quit_on_error: self.opts.quit_on_error,
        });

        let packs_map = Arc::try_unwrap(packs).unwrap_or_else(|arc| (*arc).clone());

        let mut download_stream = futures::stream::iter(packs_map)
            .flat_map(|(pack_id, blob_requests)| {
                let mut blob_to_targets: HashMap<ID, Vec<BlobRestoreRequest>> = HashMap::new();
                for (blob_id, req) in blob_requests {
                    blob_to_targets.entry(blob_id).or_default().push(req);
                }

                let blobs_vec: Vec<(ID, BlobLocator, Vec<BlobRestoreRequest>)> = blob_to_targets
                    .into_iter()
                    .map(|(id, targets)| {
                        let t0 = &targets[0];
                        let locator = BlobLocator {
                            pack_id,
                            offset: t0.blob_offset,
                            length: t0.blob_length,
                            raw_length: t0.raw_length,
                            blob_type: BlobType::Data,
                        };
                        (id, locator, targets)
                    })
                    .collect();

                futures::stream::iter(loader::segment_blobs(pack_id, blobs_vec))
            })
            .map(|segment| {
                let repo = self.repo.clone();
                let secure_storage = secure_storage.clone();
                let ctx = ctx.clone();
                let shutdown_signal = self.shutdown_signal.clone();
                let d = d.clone();

                async move {
                    if shutdown_signal.load(Ordering::Acquire) {
                        bail!("Interrupted");
                    }

                    let path = repo.get_path(ContentIdType::Pack, &segment.pack_id);
                    let is_tree = segment
                        .blobs
                        .iter()
                        .all(|(_, loc, _)| loc.blob_type == BlobType::Tree);

                    let segment_data = repo
                        .backend()
                        .read(
                            &crate::backend::Handle::new_with_hint(&path, ContentIdType::Pack, is_tree),
                            segment.min_offset as isize,
                            segment.source_len(),
                        )
                        .await
                        .with_context(|| format!("Failed to read pack {}", segment.pack_id))?;

                    tracing::debug!(target: "restorer", "Segment from pack {} downloaded ({} bytes)",
                        segment.pack_id.to_short_hex(8), segment_data.len());

                    let data_arc = Arc::new(segment_data);
                    let mut blobs = segment.blobs;

                    while !blobs.is_empty() {
                        if shutdown_signal.load(Ordering::Acquire) {
                            bail!("Interrupted");
                        }

                        let mut batch = Vec::new();
                        let mut batch_raw_size = 0;
                        while let Some(entry) = blobs.pop() {
                            batch_raw_size += entry.1.raw_length as u64;
                            batch.push(entry);
                            if batch_raw_size >= d.restore_decoded_budget {
                                break;
                            }
                        }

                        let secure_storage_inner = secure_storage.clone();
                        let data_arc_inner = data_arc.clone();
                        let min_offset = segment.min_offset;

                        let decoded_results: Vec<Result<DecodedBlob>> = spawn_blocking(move || {
                            batch
                                .into_par_iter()
                                .map(|(blob_id, locator, targets)| {
                                    let blob_offset = locator.offset as u64;
                                    if blob_offset < min_offset {
                                        anyhow::bail!("Blob offset {} is before segment start {}", blob_offset, min_offset);
                                    }
                                    let start = (blob_offset - min_offset) as usize;
                                    let end = start + locator.length as usize;
                                    if end > data_arc_inner.len() {
                                        anyhow::bail!("Blob end {} exceeds segment data length {}", end, data_arc_inner.len());
                                    }
                                    let encoded_blob = &data_arc_inner[start..end];

                                    let decoded_data = secure_storage_inner
                                        .decode(encoded_blob)
                                        .with_context(|| {
                                            format!("Failed to decode blob {blob_id}")
                                        })?;

                                    Ok(DecodedBlob {
                                        data: decoded_data,
                                        targets,
                                    })
                                })
                                .collect()
                        })
                        .await
                        .map_err(|e| anyhow!(e))?;

                        let mut file_batches: HashMap<usize, Vec<(Vec<u8>, u64)>> = HashMap::new();
                        for res in decoded_results {
                            let decoded = res?;
                            if decoded.targets.len() == 1 {
                                let target = &decoded.targets[0];
                                file_batches
                                    .entry(target.file_idx)
                                    .or_default()
                                    .push((decoded.data, target.offset_in_file));
                            } else {
                                for target in &decoded.targets {
                                    file_batches
                                        .entry(target.file_idx)
                                        .or_default()
                                        .push((decoded.data.clone(), target.offset_in_file));
                                }
                            }
                        }

                        Self::flush_file_batches(
                            &mut file_batches,
                            &ctx,
                            d.restore_blob_concurrency,
                        )
                        .await?;
                    }

                    Ok::<(), anyhow::Error>(())
                }
            })
            .buffer_unordered(d.restore_pack_prefetch);

        while let Some(res) = download_stream.next().await {
            if self.shutdown_signal.load(Ordering::Acquire) {
                bail!("Interrupted");
            }
            res?;
        }

        Ok(())
    }

    /// Flush accumulated file batches: write each file's blobs in a single
    /// spawn_blocking, processing files concurrently.
    async fn flush_file_batches(
        file_batches: &mut HashMap<usize, Vec<(Vec<u8>, u64)>>,
        ctx: &Arc<RestoreContext>,
        concurrency: usize,
    ) -> Result<()> {
        let batches = std::mem::take(file_batches);

        let mut batch_stream = futures::stream::iter(batches)
            .map(|(file_idx, writes)| {
                let num_blobs = writes.len().min(u32::MAX as usize) as u32;
                let total_bytes: u64 = writes.iter().map(|(d, _)| d.len() as u64).sum();
                let ctx = ctx.clone();

                async move {
                    let file_path = ctx.files[file_idx].path.clone();
                    let file_path_for_write = file_path.clone();
                    let ctx_inner = ctx.clone();
                    let write_result = spawn_blocking(move || -> Result<u64, anyhow::Error> {
                        let mut cache_guard = ctx_inner.handle_cache.get_shard(file_idx).lock();
                        let file = cache_guard.get_handle(
                            file_idx,
                            &file_path_for_write,
                            &ctx_inner.files[file_idx],
                            &ctx_inner.initialized[file_idx],
                            &ctx_inner.restorer,
                        )?;
                        let mut written = 0u64;
                        for (data, offset) in writes {
                            let mut data_remaining = data.as_slice();
                            let mut write_offset = offset;
                            while !data_remaining.is_empty() {
                                #[cfg(unix)]
                                let n = file
                                    .write_at(data_remaining, write_offset)
                                    .map_err(|e| anyhow!(e))?;
                                #[cfg(windows)]
                                let n = file
                                    .seek_write(data_remaining, write_offset)
                                    .map_err(|e| anyhow!(e))?;
                                if n == 0 {
                                    anyhow::bail!("Failed to write data: wrote 0 bytes");
                                }
                                data_remaining = &data_remaining[n..];
                                write_offset += n as u64;
                            }
                            written += data.len() as u64;
                        }
                        Ok(written)
                    })
                    .await
                    .map_err(|e| anyhow!(e))?;

                    match write_result {
                        Ok(_bytes) => {
                            emit_event(
                                &ctx.event_sender,
                                Event::Restore(RestoreEvent::BytesProcessed(total_bytes)),
                            );
                            if ctx.remaining[file_idx].fetch_sub(num_blobs, Ordering::Relaxed)
                                == num_blobs
                            {
                                emit_event(
                                    &ctx.event_sender,
                                    Event::Restore(RestoreEvent::ItemProcessed(file_path)),
                                );
                            }
                        }
                        Err(e) => {
                            let err_msg = format!("Failed to write to file index {file_idx}: {e}");
                            if ctx.quit_on_error {
                                bail!(err_msg);
                            }
                            emit_event(
                                &ctx.event_sender,
                                Event::Restore(RestoreEvent::Error(err_msg)),
                            );
                        }
                    }

                    Ok::<(), anyhow::Error>(())
                }
            })
            .buffer_unordered(concurrency);

        while let Some(res) = batch_stream.next().await {
            res?;
        }

        Ok(())
    }
}
