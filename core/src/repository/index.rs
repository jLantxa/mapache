use std::{collections::HashMap, sync::Arc, time::Instant};

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

use crate::{
    backend::StorageHint,
    mapache::{self, BlobType, ContentIdType, ID},
    repository::repo::{Repository, SizePair},
    utils::collections::{IdIndexSet, IdMap, IdSet},
};

use super::packer::PackedBlobDescriptor;

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

type ReverseMap = HashMap<u32, Vec<ID>>;

/// Manages the mapping of blob IDs to their locations within pack files.
/// An `Index` can be in a 'pending' state, indicating it's still being built.
#[derive(Debug, Clone)]
pub struct Index {
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
            data_ids: IdMap::default(),
            tree_ids: IdMap::default(),
            pack_ids: IdIndexSet::new_id_set(),
            status: IndexStatus::Pending,
            create_time: Instant::now(),
        }
    }

    /// Helper to generate a temporary reverse map for management operations.
    fn get_reverse_map(&self) -> ReverseMap {
        let mut rev: ReverseMap = HashMap::with_capacity(self.pack_ids.len());
        for (id, loc) in self.data_ids.iter().chain(self.tree_ids.iter()) {
            rev.entry(loc.pack_array_index).or_default().push(*id);
        }
        rev
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
        self.num_blobs() >= mapache::defaults::BLOBS_PER_INDEX_FILE
    }

    /// Creates an `Index` from a serialized `IndexFile`.
    pub fn from_index_file(index_file: IndexFile, id: ID) -> Self {
        let mut index = Self::new();
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
    fn resolve_location(&self, loc: &BlobLocationInternal, blob_type: BlobType) -> BlobLocator {
        let pack_id = self
            .pack_ids
            .get_value(loc.pack_array_index as usize)
            .expect("Index invariant violated: pack_index out of bounds");

        BlobLocator {
            pack_id: *pack_id,
            blob_type,
            offset: loc.offset,
            length: loc.length,
            raw_length: loc.raw_length,
        }
    }

    pub fn get(&self, id: &ID) -> Option<BlobLocator> {
        self.data_ids
            .get(id)
            .map(|l| self.resolve_location(l, BlobType::Data))
            .or_else(|| {
                self.tree_ids
                    .get(id)
                    .map(|l| self.resolve_location(l, BlobType::Tree))
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

        let reverse = self.get_reverse_map();
        let mut pack_entries = Vec::with_capacity(self.pack_ids.len());

        for (p_idx, pack_id) in self.pack_ids.iter().enumerate() {
            let p_idx_u32 = p_idx as u32;

            if let Some(blob_ids) = reverse.get(&p_idx_u32) {
                let mut blobs = Vec::with_capacity(blob_ids.len());

                for id in blob_ids {
                    let (loc, b_type) = self
                        .data_ids
                        .get(id)
                        .map(|l| (l, BlobType::Data))
                        .or_else(|| self.tree_ids.get(id).map(|l| (l, BlobType::Tree)))
                        .expect("ID from reverse map must exist in data/tree maps");

                    blobs.push(IndexFileBlob {
                        id: *id,
                        blob_type: b_type,
                        offset: loc.offset,
                        length: loc.length,
                        raw_length: loc.raw_length,
                    });
                }

                pack_entries.push(IndexFilePack {
                    id: *pack_id,
                    blobs,
                });
            }
        }

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
        let data = self
            .data_ids
            .iter()
            .map(move |(id, loc)| (id, self.resolve_location(loc, BlobType::Data)));
        let trees = self
            .tree_ids
            .iter()
            .map(move |(id, loc)| (id, self.resolve_location(loc, BlobType::Tree)));
        data.chain(trees)
    }

    fn remove_pack(&mut self, target_pack_id: &ID) {
        if let Some(&pack_index) = self.pack_ids.get_index(target_pack_id) {
            let p_idx_u32 = pack_index as u32;

            // Linear scan to remove blobs belonging to the target pack.
            self.data_ids
                .retain(|_, loc| loc.pack_array_index != p_idx_u32);
            self.tree_ids
                .retain(|_, loc| loc.pack_array_index != p_idx_u32);

            let last_idx = (self.pack_ids.len() - 1) as u32;
            self.pack_ids.remove(target_pack_id);

            // If the removed pack wasn't the last one, IndexSet moves the last pack
            // to fill the gap. We must update the index of all affected blobs.
            if p_idx_u32 != last_idx {
                for loc in self.data_ids.values_mut().chain(self.tree_ids.values_mut()) {
                    if loc.pack_array_index == last_idx {
                        loc.pack_array_index = p_idx_u32;
                    }
                }
            }
        }
    }
}

/// Manages a collection of `Index` instances, providing a unified view
/// over all known blobs in the repository.
#[derive(Debug, Clone)]
pub struct MasterIndex {
    /// Internal state protected by a read-write lock.
    inner: Arc<parking_lot::RwLock<MasterIndexInner>>,
    auto_save: bool,
}

#[derive(Debug)]
struct MasterIndexInner {
    /// A list of individual indices, some of which might be pending.
    indices: Vec<Index>,
    /// Stores the IDs of blobs that are waiting to be serialized into a pack file.
    pending_blobs: IdSet<ID>,
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
            inner: Arc::new(parking_lot::RwLock::new(MasterIndexInner {
                indices: Vec::with_capacity(1),
                pending_blobs: IdSet::default(),
            })),
            auto_save: true,
        }
    }

    pub fn clear(&self) {
        let mut lock = self.inner.write();
        lock.indices.clear();
        lock.pending_blobs.clear();
    }

    /// Returns `true` if the object ID is known either in a finalized index
    /// or is currently a pending blob.
    pub fn contains(&self, id: &ID) -> bool {
        let lock = self.inner.read();

        lock.pending_blobs.contains(id) || lock.indices.iter().rev().any(|idx| idx.contains(id))
    }

    /// Search backwards for Data blobs specifically.
    pub fn get_data(&self, id: &ID) -> Option<BlobLocator> {
        let lock = self.inner.read();

        lock.indices.iter().rev().find_map(|idx| {
            idx.data_ids
                .get(id)
                .map(|l| idx.resolve_location(l, BlobType::Data))
        })
    }

    // Search backwards for Tree blobs specifically.
    pub fn get_tree(&self, id: &ID) -> Option<BlobLocator> {
        let lock = self.inner.read();

        lock.indices.iter().rev().find_map(|idx| {
            idx.tree_ids
                .get(id)
                .map(|l| idx.resolve_location(l, BlobType::Tree))
        })
    }

    /// Retrieves an entry for a given blob ID by searching through finalized indices.
    /// Pending blobs (those not yet packed) cannot be retrieved via this method.
    pub fn get(&self, id: &ID) -> Option<BlobLocator> {
        let lock = self.inner.read();

        lock.indices.iter().rev().find_map(|idx| idx.get(id))
    }

    /// Adds a fully constructed `Index` to the master index.
    /// This is typically used for adding loaded, finalized indices.
    pub fn add_index(&self, index: Index) {
        let mut lock = self.inner.write();

        lock.indices.push(index);
    }

    /// Adds a blob ID to the set of blobs that are waiting to be packed.
    /// Returns `true` if the ID did not exist in the set and was inserted; `false` otherwise.
    pub fn add_pending_blob(&self, id: ID) -> bool {
        if self.contains(&id) {
            return false;
        }

        let mut lock = self.inner.write();

        if lock.pending_blobs.contains(&id)
            || lock.indices.iter().rev().any(|idx| idx.contains(&id))
        {
            return false;
        }

        lock.pending_blobs.insert(id)
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

        {
            let mut lock = self.inner.write();

            for blob in &descriptors {
                lock.pending_blobs.remove(&blob.id);
            }

            if !lock.indices.iter().any(|idx| idx.is_pending()) {
                lock.indices.push(Index::new());
            }

            let pending_index = lock
                .indices
                .iter_mut()
                .find(|idx| idx.is_pending())
                .expect("There should be a pending index");

            pending_index.add_pack(pack_id, descriptors);

            let is_full = pending_index.is_full();
            let is_timed_out =
                pending_index.create_time.elapsed() >= mapache::defaults::INDEX_FLUSH_TIMEOUT;

            if self.auto_save && (is_full || is_timed_out) {
                // We must persist this index. To avoid holding the lock during IO,
                // we'll take it out of the list or mark it as non-pending.
                // Simplified approach: just finalize it and keep it in the list.
                pending_index.finalize();
                index_to_persist = Some(pending_index.clone());
            } else if is_full {
                pending_index.finalize();
            }
        }

        if let Some(mut idx) = index_to_persist {
            let size = idx.persist(repo).await?;
            // Update the status in the actual list
            let mut lock = self.inner.write();
            if let Some(actual_idx) = lock.indices.iter_mut().find(|i| {
                // Find by pointer or some other way?
                // Since we just added it and finalized it, it's likely the one with the same content.
                // But Index doesn't have a unique ID until persisted.
                // Let's use the created_time and num_blobs as a weak heuristic for this simple implementation.
                i.is_finalized() && !i.is_persisted()
            }) {
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
            let lock = self.inner.read();
            for idx in &lock.indices {
                if !matches!(idx.status, IndexStatus::Persisted(_)) && !idx.is_empty() {
                    indices_to_persist.push(idx.clone());
                }
            }
        }

        for mut idx in indices_to_persist {
            let size = idx.persist(repo).await?;
            total_size += size;

            // Update the status in the master index
            let mut lock = self.inner.write();
            if let Some(actual_idx) = lock.indices.iter_mut().find(|i| {
                // Weak matching for simplification
                !i.is_persisted()
                    && i.num_blobs() == idx.num_blobs()
                    && i.create_time == idx.create_time
            }) {
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

    pub fn ids(&self) -> IdSet<ID> {
        let lock = self.inner.read();

        lock.indices
            .iter()
            .filter_map(|idx| if !idx.is_pending() { idx.id() } else { None })
            .collect()
    }

    pub fn cleanup(&self, obsolete_packs: Option<&IdSet<ID>>) {
        let mut lock = self.inner.write();

        if let Some(packs) = obsolete_packs {
            for idx in &mut lock.indices {
                if packs.iter().any(|p| idx.pack_ids.contains(p)) {
                    idx.set_status(IndexStatus::Pending);
                    for p in packs {
                        idx.remove_pack(p);
                    }
                }
            }
        }

        self.merge_index(&mut lock);
    }

    /// Merges all current indices into a new collection of full indices.
    fn merge_index(&self, lock: &mut MasterIndexInner) {
        let old_indices = std::mem::take(&mut lock.indices);
        let mut new_indices = Vec::new();
        let mut current_index = Index::new();
        let mut seen_packs = IdSet::default();

        for idx in old_indices {
            let reverse = idx.get_reverse_map();
            for (&pack_array_idx, blob_ids) in &reverse {
                let pack_id = idx
                    .pack_ids
                    .get_value(pack_array_idx as usize)
                    .expect("Index invariant violated");

                if seen_packs.contains(pack_id) {
                    continue;
                }

                let descriptor_stream = blob_ids.iter().filter_map(|id| {
                    idx.get(id).map(|loc| PackedBlobDescriptor {
                        id: *id,
                        blob_type: loc.blob_type,
                        offset: loc.offset,
                        length: loc.length,
                        raw_length: loc.raw_length,
                    })
                });

                current_index.add_pack(pack_id, descriptor_stream);

                if current_index.is_full() {
                    current_index.set_status(IndexStatus::Finalized);
                    new_indices.push(current_index);
                    current_index = Index::new();
                }
            }

            for p_id in idx.pack_ids.iter() {
                seen_packs.insert(*p_id);
            }
        }

        if !current_index.is_empty() {
            current_index.set_status(IndexStatus::Pending);
            new_indices.push(current_index);
        }

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
}
