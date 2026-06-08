use std::{collections::HashMap, sync::Arc, sync::atomic::Ordering};

#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;

use anyhow::{Context, Result, anyhow, bail};
use futures::StreamExt;
use tokio::task::spawn_blocking;

use crate::{
    mapache::{BlobType, ContentIdType, ID, defaults},
    repository::{index::BlobLocator, loader, storage::SecureStorage},
    ui::RestoreProgressReporter,
};

use super::{BlobRestoreRequest, FileRestorePlan, PackMap, Restorer, ShardedFileHandleCache};

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
                    self.progress_reporter
                        .processed_bytes(request.raw_length as u64);
                }
            }
            for file in files.iter() {
                self.progress_reporter.processed_item(&file.path);
            }
            return Ok(());
        }

        let remaining = Arc::new(
            files
                .iter()
                .map(|f| std::sync::atomic::AtomicU32::new(f.num_blobs))
                .collect::<Vec<_>>(),
        );

        for file in files.iter() {
            if file.num_blobs == 0 {
                self.progress_reporter.processed_item(&file.path);
            }
        }

        self.progress_reporter
            .set_message("Restoring...".to_string());

        let d = defaults::runtime();
        let handle_cache = Arc::new(ShardedFileHandleCache::new(d.restore_max_open_files));
        let mut packs_iter = packs.iter();
        let quit_on_error = self.opts.quit_on_error;
        let progress_reporter = self.progress_reporter.clone();
        let shutdown_signal = self.shutdown_signal.clone();

        let mut download_stream = futures::stream::iter(std::iter::from_fn(|| {
            packs_iter
                .next()
                .map(|(pack_id, blob_requests)| (*pack_id, blob_requests.clone()))
        }))
        .map(move |(pack_id, blob_requests)| {
            let repo = self.repo.clone();
            let secure_storage = secure_storage.clone();
            let handle_cache = handle_cache.clone();
            let files = files.clone();
            let remaining = remaining.clone();
            let progress_reporter = progress_reporter.clone();
            let shutdown_signal = shutdown_signal.clone();
            let d = d.clone();

            async move {
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

                let segments = loader::segment_blobs(pack_id, blobs_vec);

                tracing::debug!(target: "restorer", "Processing {} segments from pack {} ({} bytes)", segments.len(), pack_id.to_short_hex(8),
                    segments.iter().map(|s| s.source_len() as u64).sum::<u64>());

                let total_segments = segments.len();
                for (segment_idx, segment) in segments.into_iter().enumerate() {
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

                    tracing::debug!(target: "restorer", "Segment {}/{} from pack {} downloaded ({} bytes)",
                        segment_idx + 1, total_segments, pack_id.to_short_hex(8), segment_data.len());

                    let data_arc = Arc::new(segment_data);
                    let mut file_batches: HashMap<usize, Vec<(Vec<u8>, u64)>> = HashMap::new();
                    let mut pending_decoded: u64 = 0;

                    for (blob_id, locator, targets) in segment.blobs {
                        let start = (locator.offset as u64 - segment.min_offset) as usize;
                        let end = start + locator.length as usize;
                        let encoded_blob = &data_arc[start..end];

                        let decoded_data = secure_storage
                            .decode_owned(encoded_blob.to_vec())
                            .with_context(|| format!("Failed to decode blob {blob_id}"))?;

                        let raw_len = decoded_data.len() as u64;

                        if targets.len() == 1 {
                            let target = &targets[0];
                            file_batches
                                .entry(target.file_idx)
                                .or_default()
                                .push((decoded_data, target.offset_in_file));
                        } else {
                            for target in &targets {
                                file_batches
                                    .entry(target.file_idx)
                                    .or_default()
                                    .push((decoded_data.clone(), target.offset_in_file));
                            }
                        }

                        pending_decoded += raw_len;

                        if pending_decoded >= d.restore_decoded_budget {
                            Self::flush_file_batches(
                                &mut file_batches,
                                &handle_cache,
                                &files,
                                remaining.as_ref(),
                                &progress_reporter,
                                quit_on_error,
                                d.restore_blob_concurrency,
                            )
                            .await?;
                            pending_decoded = 0;
                        }
                    }

                    Self::flush_file_batches(
                        &mut file_batches,
                        &handle_cache,
                        &files,
                        remaining.as_ref(),
                        &progress_reporter,
                        quit_on_error,
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
        handle_cache: &Arc<ShardedFileHandleCache>,
        files: &Arc<Vec<FileRestorePlan>>,
        remaining: &[std::sync::atomic::AtomicU32],
        progress_reporter: &Arc<dyn RestoreProgressReporter>,
        quit_on_error: bool,
        concurrency: usize,
    ) -> Result<()> {
        let batches = std::mem::take(file_batches);

        let mut batch_stream = futures::stream::iter(batches)
            .map(|(file_idx, writes)| {
                let num_blobs = writes.len().min(u32::MAX as usize) as u32;
                let total_bytes: u64 = writes.iter().map(|(d, _)| d.len() as u64).sum();
                let file_path = files[file_idx].path.clone();
                let handle_cache = handle_cache.clone();
                let files = files.clone();
                let progress_reporter = progress_reporter.clone();

                async move {
                    let write_result = spawn_blocking(move || -> Result<u64, anyhow::Error> {
                        let mut cache_guard = handle_cache.get_shard(file_idx).lock();
                        let file = cache_guard.get_handle(file_idx, &file_path)?;
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
                            progress_reporter.processed_bytes(total_bytes);
                            if remaining[file_idx].fetch_sub(num_blobs, Ordering::Relaxed)
                                == num_blobs
                            {
                                progress_reporter.processed_item(&files[file_idx].path);
                            }
                        }
                        Err(e) => {
                            let err_msg = format!("Failed to write to file index {file_idx}: {e}");
                            if quit_on_error {
                                bail!(err_msg);
                            }
                            progress_reporter.error(&err_msg);
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
