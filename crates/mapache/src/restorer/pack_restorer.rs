use std::{collections::HashMap, sync::Arc, sync::atomic::Ordering};

#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;

use futures::StreamExt;
use rayon::prelude::*;
use tokio::task::spawn_blocking;

use crate::{
    backend::Handle,
    common::{
        BlobType, ContentIdType, ID, defaults,
        error::{MapacheError, Result},
    },
    repository::{
        index::{BlobLocator, MasterIndex},
        loader,
        storage::SecureStorage,
    },
    restorer::{
        BlobRestoreRequest, FileRestorePlan, PackMap, Restorer, ShardedFileHandleCache,
        ZeroBatchMap,
    },
    ui::events::{Event, EventSender, RestoreEvent, emit_event},
};

/// Decoded blob data that may be owned or shared via Arc.
/// Single-target blobs are moved directly; multi-target blobs are shared.
enum BlobData {
    Owned(Vec<u8>),
    Shared(Arc<Vec<u8>>),
}

impl BlobData {
    fn as_slice(&self) -> &[u8] {
        match self {
            BlobData::Owned(v) => v.as_slice(),
            BlobData::Shared(a) => a.as_slice(),
        }
    }

    fn len(&self) -> usize {
        match self {
            BlobData::Owned(v) => v.len(),
            BlobData::Shared(a) => a.len(),
        }
    }
}

/// A batch of decoded blob data paired with its write offset.
type BlobWriteBatch = Vec<(BlobData, u64)>;

/// Pre-grouped pack data: pack_id → list of (blob_id, locator, file targets).
type PackGroup = Vec<(ID, Vec<(ID, BlobLocator, Vec<BlobRestoreRequest>)>)>;

struct DecodedBlob {
    data: Vec<u8>,
    targets: Vec<BlobRestoreRequest>,
}

struct RestoreContext {
    handle_cache: Arc<ShardedFileHandleCache>,
    files: Arc<Vec<FileRestorePlan>>,
    initialized: Arc<Vec<std::sync::atomic::AtomicBool>>,
    restorer: Arc<Restorer>,
    event_sender: EventSender,
    quit_on_error: bool,
}

/// Process a chunk of files: download pack segments, decode blobs, write to disk.
pub(crate) async fn restore_packs(
    restorer: &Restorer,
    files: Arc<Vec<FileRestorePlan>>,
    packs: PackMap,
    index: Arc<MasterIndex>,
    secure_storage: Arc<SecureStorage>,
    dry_run: bool,
) -> Result<()> {
    if dry_run {
        for blob_requests in packs.values() {
            for (blob_id, _) in blob_requests.iter() {
                if let Some(locator) = index.get(blob_id).await {
                    emit_event(
                        &restorer.event_sender,
                        Event::Restore(RestoreEvent::BytesProcessed(locator.raw_length as u64)),
                    );
                }
            }
        }
        for file in files.iter() {
            emit_event(
                &restorer.event_sender,
                Event::Restore(RestoreEvent::ItemProcessed(file.path.clone())),
            );
        }
        return Ok(());
    }

    let initialized = Arc::new(
        (0..files.len())
            .map(|_| std::sync::atomic::AtomicBool::new(false))
            .collect::<Vec<_>>(),
    );

    let defaults = defaults::runtime();
    let handle_cache = Arc::new(ShardedFileHandleCache::new(defaults.restore_max_open_files));

    for (idx, file) in files.iter().enumerate() {
        if !file.is_selective
            && ((file.num_blobs == 0 && !file.is_hardlink && !file.path.exists()) || file.size == 0)
        {
            let mut cache_guard = handle_cache.get_shard(idx).lock();
            cache_guard.get_handle(idx, &file.path, file, &initialized[idx], restorer)?;
        }
    }

    let restorer_arc = Arc::new(restorer.clone_for_workers());
    let ctx = Arc::new(RestoreContext {
        handle_cache,
        files,
        initialized,
        restorer: restorer_arc,
        event_sender: restorer.event_sender.clone(),
        quit_on_error: restorer.opts.quit_on_error,
    });

    // Flatten HashMap into a single Vec and split zero vs regular in one pass.
    // Each blob ID is looked up once in the master index and reused across all
    // its targets to avoid re-surveying cold indices repeatedly.
    let mut locator_cache: HashMap<ID, BlobLocator> = HashMap::new();
    let mut zero_file_batches: ZeroBatchMap = HashMap::new();
    let mut regular: Vec<(ID, ID, BlobRestoreRequest, BlobLocator)> = Vec::new();

    for (pack_id, blob_requests) in packs {
        for (blob_id, req) in blob_requests {
            let locator = match locator_cache.get(&blob_id) {
                Some(&locator) => locator,
                None => match index.get(&blob_id).await {
                    Some(locator) => {
                        locator_cache.insert(blob_id, locator);
                        locator
                    }
                    None => {
                        emit_event(
                            &ctx.event_sender,
                            Event::Restore(RestoreEvent::Error(format!(
                                "Blob {blob_id} not found in index"
                            ))),
                        );
                        if ctx.quit_on_error {
                            return Err(MapacheError::Internal(format!(
                                "Blob {blob_id} not found in index"
                            )));
                        }
                        continue;
                    }
                },
            };
            if matches!(locator.blob_type, BlobType::Zero) {
                zero_file_batches
                    .entry(req.file_idx)
                    .or_default()
                    .push((req.offset_in_file, locator.raw_length));
            } else {
                regular.push((pack_id, blob_id, req, locator));
            }
        }
    }

    if !zero_file_batches.is_empty() {
        flush_zero_batches(
            &mut zero_file_batches,
            &ctx,
            defaults.restore_blob_concurrency,
        )
        .await?;
    }

    // Sort by (pack_id, blob_id) so packs are contiguous and blobs within
    // each pack are grouped for dedup during decode.
    regular.sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

    // Pre-group into (pack_id, Vec<(blob_id, locator, targets)>) to avoid
    // per-pack HashMap allocations during the download stream.
    let mut pack_groups: PackGroup = Vec::new();
    {
        let mut i = 0;
        while i < regular.len() {
            let pack_id = regular[i].0;
            let pack_start = i;
            while i < regular.len() && regular[i].0 == pack_id {
                i += 1;
            }

            let pack_slice = &regular[pack_start..i];
            let mut blobs_vec: Vec<(ID, BlobLocator, Vec<BlobRestoreRequest>)> =
                Vec::with_capacity(pack_slice.len());

            // Group consecutive entries with the same blob_id.
            let mut j = 0;
            while j < pack_slice.len() {
                let blob_id = pack_slice[j].1;
                let locator = pack_slice[j].3;
                let blob_start = j;
                while j < pack_slice.len() && pack_slice[j].1 == blob_id {
                    j += 1;
                }
                let targets: Vec<BlobRestoreRequest> = pack_slice[blob_start..j]
                    .iter()
                    .map(|(_, _, req, _)| req.clone())
                    .collect();
                blobs_vec.push((blob_id, locator, targets));
            }

            pack_groups.push((pack_id, blobs_vec));
        }
    }

    let mut download_stream = futures::stream::iter(pack_groups)
        .flat_map(|(pack_id, blobs_vec)| {
            futures::stream::iter(loader::segment_blobs(pack_id, blobs_vec))
        })
        .map(|segment| {
            let repo = restorer.repo.clone();
            let secure_storage = secure_storage.clone();
            let ctx = ctx.clone();
            let shutdown_signal = restorer.shutdown_signal.clone();
            let defaults = defaults.clone();

            async move {
                if shutdown_signal.load(Ordering::Acquire) {
                    return Err(MapacheError::Interrupted);
                }

                let path = repo.get_path(ContentIdType::Pack, &segment.pack_id);
                let is_tree = segment
                    .blobs
                    .iter()
                    .all(|(_, loc, _)| loc.blob_type == BlobType::Tree);

                let segment_data = repo
                    .backend()
                    .read(
                        &Handle::new_with_hint(&path, ContentIdType::Pack, is_tree),
                        segment.min_offset as isize,
                        segment.source_len() as usize,
                    )
                    .await
                    .map_err(|e| {
                        MapacheError::Internal(format!(
                            "failed to read pack {}: {e}",
                            segment.pack_id
                        ))
                    })?;

                tracing::debug!(target: "restorer", "Segment from pack {} downloaded ({} bytes)",
                    segment.pack_id.to_short_hex(8), segment_data.len());

                let data_arc = Arc::new(segment_data);
                let mut blobs = segment.blobs;

                while !blobs.is_empty() {
                    if shutdown_signal.load(Ordering::Acquire) {
                        return Err(MapacheError::Interrupted);
                    }

                    let mut batch = Vec::new();
                    let mut batch_raw_size = 0;
                    while let Some(entry) = blobs.pop() {
                        batch_raw_size += entry.1.raw_length as u64;
                        batch.push(entry);
                        if batch_raw_size >= defaults.restore_decoded_budget {
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
                                    return Err(MapacheError::Format(format!(
                                        "blob offset {} is before segment start {}",
                                        blob_offset, min_offset
                                    )));
                                }
                                let start = (blob_offset - min_offset) as usize;
                                let end = start + locator.length as usize;
                                if end > data_arc_inner.len() {
                                    return Err(MapacheError::Format(format!(
                                        "blob end {} exceeds segment data length {}",
                                        end,
                                        data_arc_inner.len()
                                    )));
                                }
                                let encoded_blob = &data_arc_inner[start..end];

                                let decoded_data = secure_storage_inner
                                    .decode_blob(encoded_blob, locator.compressed)
                                    .map_err(|e| {
                                        MapacheError::Internal(format!(
                                            "failed to decode blob {blob_id}: {e}"
                                        ))
                                    })?;

                                if decoded_data.len() != locator.raw_length as usize {
                                    return Err(MapacheError::Format(format!(
                                        "decoded blob {blob_id} has length {} but expected {}",
                                        decoded_data.len(),
                                        locator.raw_length
                                    )));
                                }
                                blob_id.verify_content(&decoded_data)?;

                                Ok(DecodedBlob {
                                    data: decoded_data,
                                    targets,
                                })
                            })
                            .collect()
                    })
                    .await
                    .map_err(|e| MapacheError::task_panicked("pack restore", e))?;

                    let mut file_batches: HashMap<usize, BlobWriteBatch> = HashMap::new();
                    for res in decoded_results {
                        let decoded = res?;
                        if decoded.targets.len() == 1 {
                            let target = &decoded.targets[0];
                            file_batches
                                .entry(target.file_idx)
                                .or_default()
                                .push((BlobData::Owned(decoded.data), target.offset_in_file));
                        } else {
                            let shared = Arc::new(decoded.data);
                            for target in &decoded.targets {
                                file_batches.entry(target.file_idx).or_default().push((
                                    BlobData::Shared(Arc::clone(&shared)),
                                    target.offset_in_file,
                                ));
                            }
                        }
                    }

                    flush_file_batches(&mut file_batches, &ctx, defaults.restore_blob_concurrency)
                        .await?;
                }

                Ok::<(), MapacheError>(())
            }
        })
        .buffer_unordered(defaults.restore_pack_prefetch);

    while let Some(res) = download_stream.next().await {
        if restorer.shutdown_signal.load(Ordering::Acquire) {
            return Err(MapacheError::Interrupted);
        }
        res?;
    }

    Ok(())
}

/// Flush accumulated file batches: write each file's blobs in a single
/// spawn_blocking, processing files concurrently.
async fn flush_file_batches(
    file_batches: &mut HashMap<usize, BlobWriteBatch>,
    ctx: &Arc<RestoreContext>,
    concurrency: usize,
) -> Result<()> {
    let batches = std::mem::take(file_batches);

    let mut batch_stream = futures::stream::iter(batches)
        .map(|(file_idx, writes)| {
            let total_bytes: u64 = writes.iter().map(|(data, _)| data.len() as u64).sum();
            let ctx = ctx.clone();

            async move {
                let file_path = ctx.files[file_idx].path.clone();
                let file_path_for_write = file_path.clone();
                let ctx_inner = ctx.clone();
                let write_result = spawn_blocking(move || -> Result<u64> {
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
                            let n = file.write_at(data_remaining, write_offset)?;
                            #[cfg(windows)]
                            let n = file.seek_write(data_remaining, write_offset)?;
                            if n == 0 {
                                return Err(MapacheError::Internal(
                                    "failed to write data: wrote 0 bytes".to_string(),
                                ));
                            }
                            data_remaining = &data_remaining[n..];
                            write_offset += n as u64;
                        }
                        written += data.len() as u64;
                    }
                    Ok(written)
                })
                .await
                .map_err(|e| MapacheError::task_panicked("pack restore", e))?;

                match write_result {
                    Ok(_bytes) => {
                        emit_event(
                            &ctx.event_sender,
                            Event::Restore(RestoreEvent::BytesProcessed(total_bytes)),
                        );
                    }
                    Err(e) => {
                        let err_msg = format!("failed to write to file index {file_idx}: {e}");
                        if ctx.quit_on_error {
                            return Err(MapacheError::Internal(err_msg));
                        }
                        emit_event(
                            &ctx.event_sender,
                            Event::Restore(RestoreEvent::Error(err_msg)),
                        );
                    }
                }

                Ok::<(), MapacheError>(())
            }
        })
        .buffer_unordered(concurrency);

    while let Some(res) = batch_stream.next().await {
        res?;
    }

    Ok(())
}

/// Chunk size for writing zeros — avoids allocating the full zero blob in memory.
const ZERO_WRITE_CHUNK: usize = 64 * 1024;

/// Flush zero blob batches: write zeros directly to files in chunks,
/// without materializing the full zero content in memory.
async fn flush_zero_batches(
    zero_batches: &mut ZeroBatchMap,
    ctx: &Arc<RestoreContext>,
    concurrency: usize,
) -> Result<()> {
    let batches = std::mem::take(zero_batches);

    let mut batch_stream = futures::stream::iter(batches)
        .map(|(file_idx, writes)| {
            let total_bytes: u64 = writes.iter().map(|&(_, len)| len as u64).sum();
            let ctx = ctx.clone();

            async move {
                let file_path = ctx.files[file_idx].path.clone();
                let file_path_for_write = file_path.clone();
                let ctx_inner = ctx.clone();
                let write_result = spawn_blocking(move || -> Result<u64> {
                    let mut cache_guard = ctx_inner.handle_cache.get_shard(file_idx).lock();
                    let file = cache_guard.get_handle(
                        file_idx,
                        &file_path_for_write,
                        &ctx_inner.files[file_idx],
                        &ctx_inner.initialized[file_idx],
                        &ctx_inner.restorer,
                    )?;
                    let zeros = [0u8; ZERO_WRITE_CHUNK];
                    let mut written = 0u64;
                    for (offset, length) in writes {
                        let mut remaining = length as u64;
                        let mut write_offset = offset;
                        while remaining > 0 {
                            let chunk = remaining.min(ZERO_WRITE_CHUNK as u64) as usize;
                            let slice = &zeros[..chunk];
                            #[cfg(unix)]
                            let n = file.write_at(slice, write_offset)?;
                            #[cfg(windows)]
                            let n = file.seek_write(slice, write_offset)?;
                            if n == 0 {
                                return Err(MapacheError::Internal(
                                    "failed to write zeros: wrote 0 bytes".to_string(),
                                ));
                            }
                            remaining -= n as u64;
                            write_offset += n as u64;
                        }
                        written += length as u64;
                    }
                    Ok(written)
                })
                .await
                .map_err(|e| MapacheError::task_panicked("zero blob restore", e))?;

                match write_result {
                    Ok(_bytes) => {
                        emit_event(
                            &ctx.event_sender,
                            Event::Restore(RestoreEvent::BytesProcessed(total_bytes)),
                        );
                    }
                    Err(e) => {
                        let err_msg =
                            format!("failed to write zeros to file index {file_idx}: {e}");
                        if ctx.quit_on_error {
                            return Err(MapacheError::Internal(err_msg));
                        }
                        emit_event(
                            &ctx.event_sender,
                            Event::Restore(RestoreEvent::Error(err_msg)),
                        );
                    }
                }

                Ok::<(), MapacheError>(())
            }
        })
        .buffer_unordered(concurrency);

    while let Some(res) = batch_stream.next().await {
        res?;
    }

    Ok(())
}
