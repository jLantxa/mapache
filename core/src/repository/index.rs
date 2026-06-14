use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use anyhow::{Result, anyhow, bail};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::{
    backend::StorageHint,
    mapache::{self, BlobType, ContentIdType, ID},
    repository::{
        packer::PackedBlobDescriptor,
        repo::{Repository, SizePair},
    },
    utils::collections::{BloomFilter, IdIndexSet, IdMap, IdSet, ShardedIdSet},
};

/// Internal optimized representation of a blob's location.
#[derive(Debug, Clone, Copy)]
struct BlobLocationInternal {
    /// The index into the `pack_ids` `IndexSet` for the pack containing this blob. See Index.
    pub pack_array_index: u32,
    /// The offset of the blob within its pack file.
    pub offset: u32,
    /// The length of the blob within its pack file.
    pub length: u32,
    /// The raw sized (uncompressed, unencrypted) of the blob
    pub raw_length: u32,
}

/// Full descriptor of a blob's location, including the resolved Pack ID.
#[derive(Debug, Clone, Copy)]
pub struct BlobLocator {
    pub pack_id: ID,
    pub offset: u32,
    pub length: u32,
    pub raw_length: u32,
    pub blob_type: BlobType,
}

#[derive(Debug, Copy, Clone)]
pub(crate) enum IndexStatus {
    /// The index is still accepting entries.
    Pending,
    /// The index is now read-only but not persisted to file.
    Finalized,
    /// The index is persisted to a file with an ID.
    Persisted(ID),
}

static NEXT_INSTANCE_ID: AtomicU64 = AtomicU64::new(1);

/// Manages the mapping of blob IDs to their locations within pack files.
/// An `Index` can be in a 'pending' state, indicating it's still being built.
#[derive(Debug, Clone)]
pub struct Index {
    /// Unique internal ID to identify this specific instance in memory.
    instance_id: u64,

    /// blob ID -> BlobLocationInternal map. This is the core lookup table.
    data_ids: IdMap<ID, BlobLocationInternal>,
    tree_ids: IdMap<ID, BlobLocationInternal>,

    /// The Pack IDs referenced in this index. Using an `IndexSet` allows us
    /// to store a small `usize` index in `BlobLocationInternal` instead of the full `ID`,
    /// significantly reducing memory usage.
    pack_ids: IdIndexSet<ID>,

    /// Status: Pending, finalized or serialized.
    status: IndexStatus,

    create_time: Instant,
}

impl Default for Index {
    fn default() -> Self {
        Self::new()
    }
}

impl Index {
    pub fn new() -> Self {
        Self {
            instance_id: NEXT_INSTANCE_ID.fetch_add(1, Ordering::Relaxed),
            data_ids: IdMap::default(),
            tree_ids: IdMap::default(),
            pack_ids: IdIndexSet::new_id_set(),
            status: IndexStatus::Pending,
            create_time: Instant::now(),
        }
    }

    /// Returns `true` if the index is currently pending (still receiving entries).
    #[inline]
    pub fn is_pending(&self) -> bool {
        matches!(self.status, IndexStatus::Pending)
    }

    /// Returns `true` if the index is currently finalized.
    #[inline]
    pub fn is_finalized(&self) -> bool {
        matches!(self.status, IndexStatus::Finalized)
    }

    /// Returns `true` if the index is already persisted to disk.
    #[inline]
    pub fn is_persisted(&self) -> bool {
        matches!(self.status, IndexStatus::Persisted(_))
    }

    /// Marks the index as finalized. A finalized index no longer accepts new entries
    /// and is typically ready for persistence or read-only operations.
    #[inline]
    pub fn finalize(&mut self) {
        self.set_status(IndexStatus::Finalized);
    }

    /// Marks the index as pending.
    #[inline]
    fn set_status(&mut self, status: IndexStatus) {
        self.status = status;
    }

    /// Returns the id of this index
    #[inline]
    pub fn id(&self) -> Option<ID> {
        match self.status {
            IndexStatus::Persisted(id) => Some(id),
            _ => None,
        }
    }

    /// Returns true if the index contains enough blobs to be considered full
    #[inline]
    pub fn is_full(&self) -> bool {
        self.num_blobs() >= mapache::defaults::runtime().blobs_per_index_file
    }

    /// Creates an `Index` from a serialized `IndexFile`.
    pub fn from_index_file(index_file: IndexFile, id: ID) -> Self {
        let mut index = Self::new();
        tracing::debug!(target: "index", "Loading index {} into instance #{}", id.to_short_hex(8), index.instance_id);
        index.set_status(IndexStatus::Persisted(id));

        for pack in index_file.packs {
            let pack_index = index.pack_ids.insert(pack.id) as u32;

            for blob in pack.blobs {
                if matches!(blob.blob_type, BlobType::Padding) {
                    continue;
                }

                let map = match blob.blob_type {
                    BlobType::Data => &mut index.data_ids,
                    BlobType::Tree => &mut index.tree_ids,
                    _ => continue,
                };

                map.insert(
                    blob.id,
                    BlobLocationInternal {
                        pack_array_index: pack_index,
                        offset: blob.offset,
                        length: blob.length,
                        raw_length: blob.raw_length,
                    },
                );
            }
        }
        index
    }

    /// Checks if the index contains the given object ID.
    #[inline]
    pub fn contains(&self, id: &ID) -> bool {
        self.data_ids.contains_key(id) || self.tree_ids.contains_key(id)
    }

    /// Helper to resolve internal location to a public BlobLocator.
    fn resolve_location(
        &self,
        loc: &BlobLocationInternal,
        blob_type: BlobType,
    ) -> Option<BlobLocator> {
        let pack_id = self.pack_ids.get_value(loc.pack_array_index as usize)?;

        Some(BlobLocator {
            pack_id: *pack_id,
            blob_type,
            offset: loc.offset,
            length: loc.length,
            raw_length: loc.raw_length,
        })
    }

    pub fn get(&self, id: &ID) -> Option<BlobLocator> {
        self.data_ids
            .get(id)
            .and_then(|l| self.resolve_location(l, BlobType::Data))
            .or_else(|| {
                self.tree_ids
                    .get(id)
                    .and_then(|l| self.resolve_location(l, BlobType::Tree))
            })
    }

    /// Adds all blob descriptors from a specific pack to the index.
    pub fn add_pack<I>(&mut self, pack_id: &ID, descriptors: I)
    where
        I: IntoIterator<Item = PackedBlobDescriptor>,
    {
        let pack_index = self.pack_ids.insert(*pack_id) as u32;

        for blob in descriptors {
            if matches!(blob.blob_type, BlobType::Padding) {
                continue;
            }

            let map = match blob.blob_type {
                BlobType::Data => &mut self.data_ids,
                BlobType::Tree => &mut self.tree_ids,
                _ => continue,
            };

            map.insert(
                blob.id,
                BlobLocationInternal {
                    pack_array_index: pack_index,
                    offset: blob.offset,
                    length: blob.length,
                    raw_length: blob.raw_length,
                },
            );
        }
    }

    /// Saves the index to the repository.
    /// Returns the total uncompressed and compressed sizes of the saved index files.
    pub async fn persist(&mut self, repo: &Repository) -> Result<SizePair> {
        self.finalize();

        if self.is_empty() {
            return Ok(SizePair::zero());
        }

        let mut pack_entries: Vec<IndexFilePack> = self
            .pack_ids
            .iter()
            .map(|pack_id| IndexFilePack {
                id: *pack_id,
                blobs: Vec::new(),
            })
            .collect();

        // Helper to avoid duplication
        let mut add_to_entries = |map: &IdMap<ID, BlobLocationInternal>, b_type: BlobType| {
            for (id, loc) in map {
                pack_entries[loc.pack_array_index as usize]
                    .blobs
                    .push(IndexFileBlob {
                        id: *id,
                        blob_type: b_type,
                        offset: loc.offset,
                        length: loc.length,
                        raw_length: loc.raw_length,
                    });
            }
        };

        add_to_entries(&self.data_ids, BlobType::Data);
        add_to_entries(&self.tree_ids, BlobType::Tree);

        // Sort blobs within each pack for deterministic serialization
        for pack in &mut pack_entries {
            pack.blobs.sort_unstable_by_key(|b| b.id);
        }

        // Filter out empty packs (though there shouldn't be any in a healthy index)
        pack_entries.retain(|p| !p.blobs.is_empty());

        // Sort packs themselves
        pack_entries.sort_unstable_by_key(|p| p.id);

        let serialized = serde_json::to_vec(&IndexFile {
            packs: pack_entries,
        })?;

        let (id, size) = repo
            .save_file(
                &mapache::SaveID::CalculateID,
                &serialized,
                StorageHint {
                    is_metadata: true,
                    file_type: ContentIdType::Index,
                },
                None,
            )
            .await?;

        self.set_status(IndexStatus::Persisted(id));

        Ok(size)
    }

    #[inline]
    pub fn num_blobs(&self) -> usize {
        self.data_ids.len() + self.tree_ids.len()
    }

    #[inline]
    pub fn num_packs(&self) -> usize {
        self.pack_ids.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.num_blobs() == 0
    }

    pub fn iter_ids(&self) -> impl Iterator<Item = (&ID, BlobLocator)> {
        let data = self.data_ids.iter().filter_map(move |(id, loc)| {
            self.resolve_location(loc, BlobType::Data).map(|l| (id, l))
        });
        let trees = self.tree_ids.iter().filter_map(move |(id, loc)| {
            self.resolve_location(loc, BlobType::Tree).map(|l| (id, l))
        });
        data.chain(trees)
    }

    /// Returns a map of Pack ID -> List of descriptors for all packs in this index.
    fn get_pack_descriptors(
        &self,
        obsolete_packs: Option<&IdSet<ID>>,
    ) -> IdMap<ID, Vec<PackedBlobDescriptor>> {
        let mut pack_descriptors = IdMap::default();

        let mut process_map = |map: &IdMap<ID, BlobLocationInternal>, b_type: BlobType| {
            for (id, loc) in map {
                let Some(pack_id) = self.pack_ids.get_value(loc.pack_array_index as usize) else {
                    tracing::error!(target: "index", "Corrupt index: pack_array_index {} out of bounds, skipping blob {}", loc.pack_array_index, id);
                    continue;
                };

                if let Some(obsolete) = obsolete_packs
                    && obsolete.contains(pack_id)
                {
                    continue;
                }

                pack_descriptors
                    .entry(*pack_id)
                    .or_insert_with(Vec::new)
                    .push(PackedBlobDescriptor {
                        id: *id,
                        blob_type: b_type,
                        offset: loc.offset,
                        length: loc.length,
                        raw_length: loc.raw_length,
                    });
            }
        };

        process_map(&self.data_ids, BlobType::Data);
        process_map(&self.tree_ids, BlobType::Tree);

        pack_descriptors
    }
}

/// Manages a collection of `Index` instances, providing a unified view
/// over all known blobs in the repository.
#[derive(Debug, Clone)]
pub struct MasterIndex {
    /// Internal state protected by a read-write lock.
    inner: Arc<RwLock<MasterIndexInner>>,
    /// Stores the IDs of blobs that are waiting to be serialized into a pack file.
    /// This is sharded to reduce contention during parallel snapshotting.
    pending_blobs: Arc<ShardedIdSet>,
    auto_save: bool,
}

#[derive(Debug)]
struct MasterIndexInner {
    /// A list of individual indices, some of which might be pending.
    indices: Vec<Index>,
    /// Bloom Filter for fast deduplication checks.
    bloom_filter: Option<BloomFilter>,
}

impl Default for MasterIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl MasterIndex {
    /// Creates a new, empty `MasterIndex`.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(MasterIndexInner {
                indices: Vec::with_capacity(1),
                bloom_filter: None,
            })),
            pending_blobs: Arc::new(ShardedIdSet::new()),
            auto_save: true,
        }
    }

    pub fn clear(&self) {
        let mut lock = self.inner.write();
        lock.indices.clear();
        lock.bloom_filter = None;
        self.pending_blobs.clear();
    }

    /// Returns the total number of blobs in all finalized indices.
    pub fn num_blobs(&self) -> usize {
        let lock = self.inner.read();
        lock.indices.iter().map(|idx| idx.num_blobs()).sum()
    }

    /// Returns `true` if the object ID is known either in a finalized index
    /// or is currently a pending blob.
    pub fn contains(&self, id: &ID) -> bool {
        if self.pending_blobs.contains(id) {
            return true;
        }

        let lock = self.inner.read();
        if let Some(bf) = &lock.bloom_filter
            && !bf.contains(id)
        {
            return false;
        }
        lock.indices.iter().rev().any(|idx| idx.contains(id))
    }

    /// Search backwards for Data blobs specifically.
    pub fn get_data(&self, id: &ID) -> Option<BlobLocator> {
        let lock = self.inner.read();

        lock.indices.iter().rev().find_map(|idx| {
            idx.data_ids
                .get(id)
                .and_then(|l| idx.resolve_location(l, BlobType::Data))
        })
    }

    // Search backwards for Tree blobs specifically.
    pub fn get_tree(&self, id: &ID) -> Option<BlobLocator> {
        let lock = self.inner.read();

        lock.indices.iter().rev().find_map(|idx| {
            idx.tree_ids
                .get(id)
                .and_then(|l| idx.resolve_location(l, BlobType::Tree))
        })
    }

    /// Retrieves an entry for a given blob ID by searching through finalized indices.
    /// Pending blobs (those not yet packed) cannot be retrieved via this method.
    pub fn get(&self, id: &ID) -> Option<BlobLocator> {
        let lock = self.inner.read();

        let res = lock.indices.iter().rev().find_map(|idx| idx.get(id));

        if let Some(locator) = res {
            tracing::trace!(target: "index",
                "Lookup blob {}: found in pack {}",
                id.to_short_hex(8), locator.pack_id.to_short_hex(8));
        } else {
            tracing::trace!(target: "index", "Lookup blob {}: not found", id.to_short_hex(8));
        }

        res
    }

    /// Adds a fully constructed `Index` to the master index.
    /// This is typically used for adding loaded, finalized indices.
    pub fn add_index(&self, index: Index) {
        let mut lock = self.inner.write();

        if let Some(bf) = &mut lock.bloom_filter {
            for (id, _) in index.iter_ids() {
                bf.insert(id);
            }
        }

        lock.indices.push(index);
    }

    /// Initializes a Bloom Filter for all blobs currently in the master index.
    pub fn initialize_bloom_filter(&self, total_blobs: usize) {
        const BLOOM_FILTER_FALSE_POSITIVE_RATE: f64 = 0.01;

        let mut lock = self.inner.write();
        let mut bf = BloomFilter::new(total_blobs, BLOOM_FILTER_FALSE_POSITIVE_RATE);

        for idx in &lock.indices {
            for (id, _) in idx.iter_ids() {
                bf.insert(id);
            }
        }

        lock.bloom_filter = Some(bf);
    }

    /// Adds a blob ID to the set of blobs that are waiting to be packed.
    /// Returns `true` if the ID did not exist in the set and was inserted; `false` otherwise.
    pub fn add_pending_blob(&self, id: ID) -> bool {
        // Fast path: check if it's already in pending_blobs or in the index (read-only)
        if self.pending_blobs.contains(&id) {
            return false;
        }

        {
            let lock = self.inner.read();
            if lock.indices.iter().rev().any(|idx| idx.contains(&id)) {
                return false;
            }
        }

        // Try to insert into pending_blobs. This is sharded so it's low contention.
        self.pending_blobs.insert(id)
    }

    /// Processes a newly created pack of blobs. It removes these blobs from the
    /// `pending_blobs` set and adds them to all currently pending `Index` instances.
    ///
    /// It's assumed that there is at least one pending index that should receive these blobs,
    /// or that a new one will be created as part of the overall backup process if needed.
    pub async fn add_pack(
        &self,
        repo: &Repository,
        pack_id: &ID,
        descriptors: Vec<PackedBlobDescriptor>,
    ) -> Result<SizePair> {
        let mut index_to_persist = None;
        let mut target_instance_id = 0;

        {
            let num_blobs = descriptors.len();
            let mut lock = self.inner.write();

            for blob in &descriptors {
                self.pending_blobs.remove(&blob.id);
            }

            if let Some(bf) = &mut lock.bloom_filter {
                for blob in &descriptors {
                    bf.insert(&blob.id);
                }
            }

            if !lock.indices.iter().any(|idx| idx.is_pending()) {
                lock.indices.push(Index::new());
            }

            let pending_index = lock
                .indices
                .iter_mut()
                .find(|idx| idx.is_pending())
                .ok_or_else(|| anyhow!("No pending index available to add pack {pack_id}"))?;

            tracing::debug!(target: "index", "Adding pack {} ({} blobs) to pending index #{}", pack_id.to_short_hex(8), num_blobs, pending_index.instance_id);
            pending_index.add_pack(pack_id, descriptors);

            let is_full = pending_index.is_full();
            let is_timed_out = pending_index.create_time.elapsed()
                >= mapache::defaults::runtime().index_flush_timeout;

            if self.auto_save && (is_full || is_timed_out) {
                let reason = if is_full { "full" } else { "timeout" };
                tracing::info!(target: "index", "Persisting index #{} (reason: {})", pending_index.instance_id, reason);
                // We must persist this index. To avoid holding the lock during IO,
                // we'll take it out of the list or mark it as non-pending.
                // Simplified approach: just finalize it and keep it in the list.
                pending_index.finalize();
                target_instance_id = pending_index.instance_id;
                index_to_persist = Some(pending_index.clone());
            } else if is_full {
                tracing::debug!(target: "index", "Index #{} is full, finalizing", pending_index.instance_id);
                pending_index.finalize();
            }
        }

        if let Some(mut idx) = index_to_persist {
            let size = idx.persist(repo).await?;
            // Update the status in the actual list
            let mut lock = self.inner.write();
            if let Some(actual_idx) = lock
                .indices
                .iter_mut()
                .find(|i| i.instance_id == target_instance_id)
            {
                actual_idx.status = idx.status;
            }
            Ok(size)
        } else {
            Ok(SizePair::zero())
        }
    }

    pub async fn persist(&self, repo: &Repository) -> Result<SizePair> {
        let mut total_size = SizePair::zero();

        // Collect all indices that need persisting
        let mut indices_to_persist = Vec::new();
        {
            let mut lock = self.inner.write();
            for idx in &mut lock.indices {
                if !matches!(idx.status, IndexStatus::Persisted(_)) && !idx.is_empty() {
                    tracing::debug!(target: "index", "Marking index #{} for persistence", idx.instance_id);
                    idx.finalize();
                    indices_to_persist.push(idx.clone());
                }
            }
        }

        let num_to_persist = indices_to_persist.len();
        if num_to_persist > 0 {
            tracing::info!(target: "index", "Persisting {} indices", num_to_persist);
        }

        for mut idx in indices_to_persist {
            let size = idx.persist(repo).await?;
            total_size += size;

            // Update the status in the master index
            let mut lock = self.inner.write();
            if let Some(actual_idx) = lock
                .indices
                .iter_mut()
                .find(|i| i.instance_id == idx.instance_id)
            {
                actual_idx.status = idx.status;
            }
        }

        Ok(total_size)
    }

    pub fn for_each_id<F>(&self, mut f: F)
    where
        F: FnMut(&ID, BlobLocator),
    {
        let lock = self.inner.read();

        for idx in &lock.indices {
            for (id, loc) in idx.iter_ids() {
                f(id, loc);
            }
        }
    }

    pub fn for_each_pack_id<F>(&self, mut f: F)
    where
        F: FnMut(&ID),
    {
        let lock = self.inner.read();
        let mut seen = IdSet::default();

        for idx in &lock.indices {
            for pack_id in idx.pack_ids.iter() {
                if seen.insert(*pack_id) {
                    f(pack_id);
                }
            }
        }
    }

    pub fn ids(&self) -> IdSet<ID> {
        let lock = self.inner.read();

        lock.indices
            .iter()
            .filter_map(|idx| if !idx.is_pending() { idx.id() } else { None })
            .collect()
    }

    pub fn cleanup(&self, obsolete_packs: Option<&IdSet<ID>>) {
        let mut lock = self.inner.write();
        self.merge_index(&mut lock, obsolete_packs);
    }

    /// Merges all current indices into a new collection of full indices.
    fn merge_index(&self, lock: &mut MasterIndexInner, obsolete_packs: Option<&IdSet<ID>>) {
        let num_old_indices = lock.indices.len();
        tracing::info!(target: "index", "Merging {} indices", num_old_indices);
        let mut old_indices = std::mem::take(&mut lock.indices);
        // Sort by instance_id to ensure deterministic merge order.
        old_indices.sort_by_key(|idx| idx.instance_id);

        // Gather descriptors for all packs from all indices.
        // We use a sequential fold to maintain perfect determinism.
        let all_pack_descriptors: IdMap<ID, IdMap<ID, PackedBlobDescriptor>> =
            old_indices.iter().fold(IdMap::default(), |mut acc, idx| {
                let pack_map = idx.get_pack_descriptors(obsolete_packs);
                for (pack_id, descriptors) in pack_map {
                    let entry = acc.entry(pack_id).or_default();
                    for desc in descriptors {
                        entry.insert(desc.id, desc);
                    }
                }
                acc
            });

        // Convert to sorted vector of (pack_id, descriptors) for deterministic index building
        let mut sorted_packs: Vec<_> = all_pack_descriptors.into_iter().collect();
        sorted_packs.sort_by_key(|(pack_id, _)| *pack_id);

        let mut new_indices = Vec::new();
        let mut current_index = Index::new();

        // Rebuild indices sequentially
        for (pack_id, descriptors_map) in sorted_packs {
            let mut descriptors: Vec<_> = descriptors_map.into_values().collect();
            descriptors.sort_by_key(|d| d.offset); // Deterministic blob order within pack

            current_index.add_pack(&pack_id, descriptors);

            if current_index.is_full() {
                current_index.set_status(IndexStatus::Finalized);
                new_indices.push(current_index);
                current_index = Index::new();
            }
        }

        if !current_index.is_empty() {
            current_index.set_status(IndexStatus::Pending);
            new_indices.push(current_index);
        }

        tracing::info!(target: "index", "Indices merged: {} -> {}", num_old_indices, new_indices.len());
        lock.indices = new_indices;
    }

    pub async fn search_prefix(&self, prefix: &str) -> Result<Option<ID>> {
        let mut matched = Vec::new();

        self.for_each_id(|id, _| {
            if id.to_hex().starts_with(prefix) {
                matched.push(*id);
            }
        });

        if matched.len() > 1 {
            bail!("Prefix '{}' is ambiguous", prefix);
        }

        Ok(matched.first().cloned())
    }

    pub fn set_autosave(&mut self, auto_save: bool) {
        self.auto_save = auto_save;
    }
}

/// Represents the on-disk format for an index file.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct IndexFile {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub packs: Vec<IndexFilePack>,
}

/// Represents a pack's entry within an `IndexFile`.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct IndexFilePack {
    pub id: ID,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub blobs: Vec<IndexFileBlob>,
}

/// Represents a blob's entry within an `IndexFilePack`.
#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
#[serde(default)]
pub struct IndexFileBlob {
    pub id: ID,
    #[serde(rename = "type")]
    pub blob_type: BlobType,
    pub offset: u32,
    pub length: u32,
    pub raw_length: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    // A simple deterministic ID generator for testing
    fn mock_id(s: &str) -> ID {
        ID::from_content(s.as_bytes())
    }

    // Mock PackedBlobDescriptor
    fn mock_blob_desc(
        s: &str,
        blob_type: BlobType,
        offset: u32,
        length: u32,
    ) -> PackedBlobDescriptor {
        PackedBlobDescriptor {
            id: mock_id(s),
            blob_type,
            offset,
            length,
            raw_length: length * 2, // Example raw length
        }
    }

    #[test]
    fn test_index_add_and_get() {
        let mut index = Index::new();
        let pack_id_a = mock_id("pack_A");
        let pack_id_b = mock_id("pack_B");

        let data_blob = mock_blob_desc("data1", BlobType::Data, 100, 50);
        let tree_blob = mock_blob_desc("tree1", BlobType::Tree, 200, 30);
        let padding_blob = mock_blob_desc("pad1", BlobType::Padding, 300, 10);

        // Add packs
        index.add_pack(&pack_id_a, vec![data_blob.clone(), padding_blob.clone()]);
        index.add_pack(&pack_id_b, vec![tree_blob.clone()]);

        assert_eq!(index.num_blobs(), 2, "Should count Data and Tree blobs");
        assert_eq!(index.num_packs(), 2, "Should count two unique packs");
        assert!(index.is_pending(), "New index should be pending");
        assert!(index.contains(&data_blob.id));
        assert!(index.contains(&tree_blob.id));
        assert!(
            !index.contains(&padding_blob.id),
            "Should not contain padding blob"
        );

        // Test get for Data blob
        let blob_locator = index.get(&data_blob.id).unwrap();
        assert_eq!(blob_locator.pack_id, pack_id_a);
        assert_eq!(blob_locator.blob_type, BlobType::Data);
        assert_eq!(blob_locator.offset, 100);
        assert_eq!(blob_locator.length, 50);
        assert_eq!(blob_locator.raw_length, 100);

        // Test get for Tree blob
        let blob_locator = index.get(&tree_blob.id).unwrap();
        assert_eq!(blob_locator.pack_id, pack_id_b);
        assert_eq!(blob_locator.blob_type, BlobType::Tree);
        assert_eq!(blob_locator.offset, 200);
        assert_eq!(blob_locator.length, 30);
        assert_eq!(blob_locator.raw_length, 60);

        // Test iterator
        let ids: IdSet<&ID> = index.iter_ids().map(|(id, _)| id).collect();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&data_blob.id));
        assert!(ids.contains(&tree_blob.id));
    }

    #[test]
    fn test_index_status_transitions() {
        let mut index = Index::new();
        assert!(index.is_pending());
        assert!(!index.is_finalized());
        assert!(!index.is_persisted());
        assert!(index.id().is_none());

        index.finalize();
        assert!(!index.is_pending());
        assert!(index.is_finalized());
        assert!(!index.is_persisted());

        let persisted_id = mock_id("persisted_index");
        index.set_status(IndexStatus::Persisted(persisted_id));
        assert!(!index.is_pending());
        assert!(!index.is_finalized());
        assert!(index.is_persisted());
        assert_eq!(index.id(), Some(persisted_id));
    }

    #[test]
    fn test_master_index_basic() {
        let mi = MasterIndex::new();
        let id1 = mock_id("blob1");
        let id2 = mock_id("blob2");

        assert!(!mi.contains(&id1));

        // Add pending blob
        assert!(mi.add_pending_blob(id1));
        assert!(mi.contains(&id1));
        assert!(!mi.add_pending_blob(id1)); // Already exists

        // Add an index
        let mut idx = Index::new();
        let pack_id = mock_id("pack1");
        let b2 = mock_blob_desc("blob2", BlobType::Data, 0, 100);
        idx.add_pack(&pack_id, vec![b2.clone()]);
        mi.add_index(idx);

        assert!(mi.contains(&id2));
        let loc = mi.get(&id2).unwrap();
        assert_eq!(loc.pack_id, pack_id);

        mi.clear();
        assert!(!mi.contains(&id1));
        assert!(!mi.contains(&id2));
    }

    #[test]
    fn test_master_index_cleanup_and_merge() {
        let mi = MasterIndex::new();

        // Setup: Multiple small indices with various packs
        let pack1 = mock_id("pack1");
        let b1 = mock_blob_desc("b1", BlobType::Data, 0, 100);
        let b2 = mock_blob_desc("b2", BlobType::Data, 100, 100);

        let pack2 = mock_id("pack2");
        let b3 = mock_blob_desc("b3", BlobType::Data, 0, 100);

        let pack3 = mock_id("pack3");
        let b4 = mock_blob_desc("b4", BlobType::Data, 0, 100);

        // Index A: Pack 1, Pack 2
        let mut idx_a = Index::new();
        idx_a.add_pack(&pack1, vec![b1.clone(), b2.clone()]);
        idx_a.add_pack(&pack2, vec![b3.clone()]);
        mi.add_index(idx_a);

        // Index B: Pack 3
        let mut idx_b = Index::new();
        idx_b.add_pack(&pack3, vec![b4.clone()]);
        mi.add_index(idx_b);

        // Verify initial state
        assert_eq!(mi.inner.read().indices.len(), 2);
        assert!(mi.contains(&b1.id));
        assert!(mi.contains(&b3.id));
        assert!(mi.contains(&b4.id));

        // Perform cleanup with pack2 as obsolete
        let mut obsolete = IdSet::default();
        obsolete.insert(pack2);

        mi.cleanup(Some(&obsolete));

        // Verify results
        let inner = mi.inner.read();
        // Since we merged, it should now be 1 index (they were small)
        assert_eq!(inner.indices.len(), 1);
        let merged_idx = &inner.indices[0];

        // pack1 and pack3 should remain
        assert!(merged_idx.pack_ids.contains(&pack1));
        assert!(merged_idx.pack_ids.contains(&pack3));
        // pack2 should be gone
        assert!(!merged_idx.pack_ids.contains(&pack2));

        // Blobs from pack1 and pack3 must be present
        assert!(merged_idx.contains(&b1.id));
        assert!(merged_idx.contains(&b2.id));
        assert!(merged_idx.contains(&b4.id));
        // Blob from pack2 must be gone
        assert!(!merged_idx.contains(&b3.id));

        // Verify locations are still correct
        let loc1 = merged_idx.get(&b1.id).unwrap();
        assert_eq!(loc1.pack_id, pack1);
        assert_eq!(loc1.offset, b1.offset);

        let loc4 = merged_idx.get(&b4.id).unwrap();
        assert_eq!(loc4.pack_id, pack3);
    }

    #[test]
    fn test_index_serialization() {
        let pack_id = mock_id("pack1");
        let b1 = mock_blob_desc("b1", BlobType::Data, 0, 100);

        let index_file = IndexFile {
            packs: vec![IndexFilePack {
                id: pack_id,
                blobs: vec![IndexFileBlob {
                    id: b1.id,
                    blob_type: b1.blob_type,
                    offset: b1.offset,
                    length: b1.length,
                    raw_length: b1.raw_length,
                }],
            }],
        };

        let json = serde_json::to_string(&index_file).unwrap();
        // The JSON should contain the pack ID and the blob ID
        assert!(json.contains(&pack_id.to_hex()));
        assert!(json.contains(&mock_id("b1").to_hex()));

        let deserialized: IndexFile = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.packs.len(), 1);
        assert_eq!(deserialized.packs[0].blobs.len(), 1);
        assert_eq!(deserialized.packs[0].id, pack_id);
        assert_eq!(deserialized.packs[0].blobs[0].id, mock_id("b1"));
    }

    #[test]
    fn test_master_index_bloom_filter() {
        let mi = MasterIndex::new();
        let pack1 = mock_id("pack1");
        let b1 = mock_blob_desc("b1", BlobType::Data, 0, 100);
        let b2 = mock_blob_desc("b2", BlobType::Data, 100, 100);

        let mut idx = Index::new();
        idx.add_pack(&pack1, vec![b1.clone()]);
        mi.add_index(idx);

        // Bloom filter is initially None
        assert!(mi.inner.read().bloom_filter.is_none());

        mi.initialize_bloom_filter(10);
        assert!(mi.inner.read().bloom_filter.is_some());

        assert!(mi.contains(&b1.id));
        assert!(!mi.contains(&b2.id));

        // Adding an index should update the Bloom filter
        let mut idx2 = Index::new();
        let pack2 = mock_id("pack2");
        idx2.add_pack(&pack2, vec![b2.clone()]);
        mi.add_index(idx2);

        assert!(mi.contains(&b2.id));
    }

    #[test]
    fn test_master_index_merge_deduplication() {
        let mi = MasterIndex::new();

        let pack1 = mock_id("pack1");
        let b1 = mock_blob_desc("b1", BlobType::Data, 0, 100);

        // Index A contains pack1
        let mut idx_a = Index::new();
        idx_a.add_pack(&pack1, vec![b1.clone()]);
        mi.add_index(idx_a);

        // Index B ALSO contains pack1 (e.g. from an interrupted operation or overlapping indices)
        let mut idx_b = Index::new();
        idx_b.add_pack(&pack1, vec![b1.clone()]);
        mi.add_index(idx_b);

        assert_eq!(mi.inner.read().indices.len(), 2);

        // Merge indices
        mi.cleanup(None);

        let inner = mi.inner.read();
        assert_eq!(inner.indices.len(), 1);
        let merged = &inner.indices[0];

        // Should only have pack1 ONCE
        assert_eq!(merged.num_packs(), 1);
        assert_eq!(merged.num_blobs(), 1);
        assert!(merged.contains(&b1.id));
    }
}
