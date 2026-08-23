use std::{collections::HashMap, sync::Arc};

use futures::stream::{self, StreamExt};

use crate::{
    backend::Handle,
    common::error::{MapacheError, Result},
    common::{BlobType, ContentIdType, ID, defaults},
    repository::{index::BlobLocator, repo::Repository},
};

/// A segment of a pack file to be downloaded.
#[derive(Debug, Clone)]
pub struct PackSegment<T> {
    pub pack_id: ID,
    pub min_offset: u64,
    pub max_offset: u64,
    /// List of blobs in this segment: (ID, Locator, Attachment)
    pub blobs: Vec<(ID, BlobLocator, T)>,
}

impl<T> PackSegment<T> {
    pub fn source_len(&self) -> usize {
        (self.max_offset - self.min_offset) as usize
    }
}

/// Groups individual blob requests into larger pack segments to optimize downloads.
pub fn segment_blobs<T>(
    pack_id: ID,
    mut blob_locators: Vec<(ID, BlobLocator, T)>,
) -> Vec<PackSegment<T>> {
    let d = defaults::runtime();
    let merge_threshold = d.restore_pack_read_merge_threshold;
    let segment_max_size = d.restore_pack_segment_max_size;

    blob_locators.sort_by_key(|(_, loc, _)| loc.offset);

    let mut segments = Vec::new();
    let mut current_blobs = Vec::new();
    let mut segment_min = 0;
    let mut segment_max = 0;

    for (id, loc, attachment) in blob_locators {
        let blob_start = loc.offset as u64;
        let blob_end = blob_start + loc.length as u64;

        if current_blobs.is_empty() {
            segment_min = blob_start;
            segment_max = blob_end;
            current_blobs.push((id, loc, attachment));
        } else {
            let next_segment_max = segment_max.max(blob_end);
            let next_segment_size = next_segment_max - segment_min;

            if blob_start <= segment_max + merge_threshold && next_segment_size <= segment_max_size
            {
                segment_max = next_segment_max;
                current_blobs.push((id, loc, attachment));
            } else {
                segments.push(PackSegment {
                    pack_id,
                    min_offset: segment_min,
                    max_offset: segment_max,
                    blobs: std::mem::take(&mut current_blobs),
                });
                segment_min = blob_start;
                segment_max = blob_end;
                current_blobs.push((id, loc, attachment));
            }
        }
    }

    if !current_blobs.is_empty() {
        segments.push(PackSegment {
            pack_id,
            min_offset: segment_min,
            max_offset: segment_max,
            blobs: current_blobs,
        });
    }

    segments
}

/// Downloads requested pack segments from the repository.
pub async fn download_pack_segments<T: Send + 'static>(
    repo: Arc<Repository>,
    segments: Vec<PackSegment<T>>,
) -> Result<Vec<(PackSegment<T>, Vec<u8>)>> {
    let results = stream::iter(segments)
        .map(|segment| {
            let repo = repo.clone();
            async move {
                let path = repo.get_path(ContentIdType::Pack, &segment.pack_id);
                // Hint if all blobs are trees
                let is_tree = segment
                    .blobs
                    .iter()
                    .all(|(_, loc, _)| loc.blob_type == BlobType::Tree);

                let data = repo
                    .backend()
                    .read(
                        &Handle::new_with_hint(&path, ContentIdType::Pack, is_tree),
                        segment.min_offset as isize,
                        segment.source_len(),
                    )
                    .await
                    .map_err(|e| {
                        MapacheError::Backend(format!(
                            "Failed to read pack {}: {}",
                            segment.pack_id, e
                        ))
                    })?;

                Ok::<(_, Vec<u8>), MapacheError>((segment, data))
            }
        })
        .buffer_unordered(8)
        .collect::<Vec<Result<_>>>()
        .await;

    let mut final_results = Vec::with_capacity(results.len());
    for res in results {
        final_results.push(res?);
    }
    Ok(final_results)
}

pub struct BlobLoader {
    repo: Arc<Repository>,
}

impl BlobLoader {
    pub fn new(repo: Arc<Repository>) -> Self {
        Self { repo }
    }

    pub async fn load_with_id(&self, blob_ids: &[ID]) -> Result<HashMap<ID, Vec<u8>>> {
        let mut locators = Vec::with_capacity(blob_ids.len());
        for id in blob_ids {
            let loc = self
                .repo
                .index()
                .get(id)
                .await
                .ok_or(MapacheError::NotInIndex(*id))?;
            locators.push((*id, loc));
        }
        self.load_with_locators(locators).await
    }

    pub async fn load_with_locators(
        &self,
        locators: Vec<(ID, BlobLocator)>,
    ) -> Result<HashMap<ID, Vec<u8>>> {
        if locators.is_empty() {
            return Ok(HashMap::new());
        }

        // Group by Pack
        let mut pack_groups: HashMap<ID, Vec<(ID, BlobLocator, ())>> = HashMap::new();
        for (id, loc) in locators {
            pack_groups
                .entry(loc.pack_id)
                .or_default()
                .push((id, loc, ()));
        }

        // Segment
        let all_segments: Vec<_> = pack_groups
            .into_iter()
            .flat_map(|(pack_id, blobs)| segment_blobs(pack_id, blobs))
            .collect();

        // Download
        let loaded = download_pack_segments(self.repo.clone(), all_segments).await?;

        // Extract and Decode
        let mut result = HashMap::with_capacity(loaded.len());
        for (segment, data) in loaded {
            for (id, loc, _) in segment.blobs {
                let blob_offset = loc.offset as u64;
                if blob_offset < segment.min_offset {
                    return Err(MapacheError::Integrity(format!(
                        "Blob offset {} is before segment start {}",
                        blob_offset, segment.min_offset
                    )));
                }
                let start = (blob_offset - segment.min_offset) as usize;
                let end = start + loc.length as usize;
                if end > data.len() {
                    return Err(MapacheError::Integrity(format!(
                        "Blob end {} exceeds segment data length {}",
                        end,
                        data.len()
                    )));
                }

                let decoded = self
                    .repo
                    .secure_storage()
                    .decode_blob(&data[start..end], loc.compressed)?;

                result.insert(id, decoded);
            }
        }

        Ok(result)
    }
}
