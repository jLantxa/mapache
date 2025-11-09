use std::{
    collections::{HashMap, HashSet},
    time::Instant,
};

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

use crate::{
    backend::StorageHint,
    mapache::{self, BlobType, ContentIdType, ID},
    repository::repo::Repository,
    utils::indexset::IndexSet,
};

use super::packer::PackedBlobDescriptor;

/// Represents the location and size of a blob within a pack file.
/// This struct is optimized for internal use inside the index.
#[derive(Debug, Clone)]
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

/// Represents the location and size of a blob within a pack file.
/// This struct contains the full pack ID. This is suited for iterating.
#[derive(Debug, Clone)]
pub struct BlobLocator {
    pub pack_id: ID,
    pub offset: u32,
    pub length: u32,
    pub raw_length: u32,
    pub blob_type: BlobType,
}

/// Manages the mapping of blob IDs to their locations within pack files.
/// An `Index` can be in a 'pending' state, indicating it's still being built.
#[derive(Debug, Clone)]
pub struct Index {
    /// blob ID -> BlobLocationInternal map. This is the core lookup table.
    data_ids: HashMap<ID, BlobLocationInternal>,
    tree_ids: HashMap<ID, BlobLocationInternal>,

    /// The Pack IDs referenced in this index. Using an `IndexSet` allows us
    /// to store a small `usize` index in `BlobLocationInternal` instead of the full `ID`,
    /// significantly reducing memory usage.
    pack_ids: IndexSet<ID>,

    /// If an index is pending, it is still receiving entries from packs and is not yet finalized.
    is_pending: bool,

    create_time: Instant,

    // The ID of this index, if it is finalized and serialized
    id: Option<ID>,
}

impl Default for Index {
    fn default() -> Self {
        Self::new()
    }
}

impl Index {
    pub fn new() -> Self {
        Self {
            data_ids: HashMap::new(),
            tree_ids: HashMap::new(),
            pack_ids: IndexSet::new(),
            is_pending: true,
            create_time: Instant::now(),
            id: None,
        }
    }

    /// Marks the index as finalized. A finalized index no longer accepts new entries
    /// and is typically ready for persistence or read-only operations.
    #[inline]
    pub fn finalize(&mut self) {
        self.is_pending = false;
    }

    /// Marks the index as pending.
    #[inline]
    pub fn set_pending(&mut self) {
        self.is_pending = true;
        self.id = None;
    }

    /// Returns the id of this index
    #[inline]
    pub fn id(&self) -> Option<ID> {
        self.id
    }

    /// Sets the index ID
    #[inline]
    pub fn set_id(&mut self, id: ID) {
        self.id = Some(id);
    }

    /// Returns `true` if the index is currently pending (still receiving entries).
    #[inline]
    pub fn is_pending(&self) -> bool {
        self.is_pending
    }

    /// Returns true if the index contains enough blobs to be considered full
    #[inline]
    pub fn is_full(&self) -> bool {
        self.num_blobs() >= mapache::defaults::BLOBS_PER_INDEX_FILE
            || self.create_time.elapsed() >= mapache::defaults::INDEX_FLUSH_TIMEOUT
    }

    /// Creates an `Index` from a serialized `IndexFile`.
    /// The created index is *not* pending, as it represents a complete, loaded file.
    pub fn from_index_file(index_file: IndexFile) -> Self {
        let mut index = Self::new();
        // An index loaded from a file is considered complete and not pending.
        index.is_pending = false;

        for pack in index_file.packs {
            let pack_index = index.pack_ids.insert(pack.id);
            for blob in pack.blobs {
                let map = match blob.blob_type {
                    BlobType::Data => &mut index.data_ids,
                    BlobType::Tree => &mut index.tree_ids,
                    BlobType::Padding => continue,
                };

                map.insert(
                    blob.id,
                    BlobLocationInternal {
                        pack_array_index: pack_index as u32,
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

    /// Retrieves the pack ID, offset, and length for a given blob ID, if it exists.
    /// Returns `None` if the blob ID is not found.
    pub fn get(&self, id: &ID) -> Option<BlobLocator> {
        self.data_ids
            .get(id)
            .map(|location| {
                let pack_id = self
                    .pack_ids
                    .get_value(location.pack_array_index as usize)
                    .expect("pack_index should always be valid for an existing blob");

                BlobLocator {
                    pack_id: *pack_id,
                    blob_type: BlobType::Data,
                    offset: location.offset,
                    length: location.length,
                    raw_length: location.raw_length,
                }
            })
            .or_else(|| {
                self.tree_ids.get(id).map(|location| {
                    let pack_id = self
                        .pack_ids
                        .get_value(location.pack_array_index as usize)
                        .expect("pack_index should always be valid for an existing blob");

                    BlobLocator {
                        pack_id: *pack_id,
                        blob_type: BlobType::Tree,
                        offset: location.offset,
                        length: location.length,
                        raw_length: location.raw_length,
                    }
                })
            })
    }

    /// Adds all blob descriptors from a specific pack to the index.
    /// This method is optimized for adding multiple blobs from the same pack,
    /// as it only needs to look up the pack ID once.
    pub fn add_pack(&mut self, pack_id: &ID, packed_blob_descriptors: &[PackedBlobDescriptor]) {
        let pack_index = self.pack_ids.insert(*pack_id);
        for blob in packed_blob_descriptors {
            let map = match blob.blob_type {
                BlobType::Data => &mut self.data_ids,
                BlobType::Tree => &mut self.tree_ids,
                BlobType::Padding => continue,
            };

            map.insert(
                blob.id,
                BlobLocationInternal {
                    pack_array_index: pack_index as u32,
                    offset: blob.offset,
                    length: blob.length,
                    raw_length: blob.raw_length,
                },
            );
        }
    }

    /// Saves the index to the repository.
    /// Returns the total uncompressed and compressed sizes of the saved index files.
    pub fn finalize_and_save(&mut self, repo: &Repository) -> Result<(u64, u64)> {
        self.finalize();

        // Don't do anything if the index is empty.
        if self.data_ids.is_empty() && self.tree_ids.is_empty() {
            return Ok((0, 0));
        }

        let mut packs_with_blobs: HashMap<usize, (ID, Vec<IndexFileBlob>)> = HashMap::new();

        for (idx, pack_id) in self.pack_ids.iter().enumerate() {
            packs_with_blobs.insert(idx, (*pack_id, Vec::new()));
        }

        // Populate Blobs in a single pass over data_ids and tree_ids
        let mut process_blobs = |blob_map: &HashMap<ID, BlobLocationInternal>,
                                 blob_type: BlobType| {
            for (blob_id, location) in blob_map {
                let (_pack_id, blobs) = packs_with_blobs
                    .get_mut(&(location.pack_array_index as usize))
                    .expect("Pack index must exist in packs_with_blobs map");

                blobs.push(IndexFileBlob {
                    id: *blob_id,
                    blob_type,
                    offset: location.offset,
                    length: location.length,
                    raw_length: location.raw_length,
                });
            }
        };

        process_blobs(&self.data_ids, BlobType::Data);
        process_blobs(&self.tree_ids, BlobType::Tree);

        let mut index_file = IndexFile {
            packs: Vec::with_capacity(self.pack_ids.len()),
        };

        for (pack_index, pack_id) in self.pack_ids.iter().enumerate() {
            if let Some((_original_pack_id, blobs)) = packs_with_blobs.remove(&pack_index) {
                // Check if any blobs were actually added to this pack's vector
                if !blobs.is_empty() {
                    index_file.packs.push(IndexFilePack {
                        id: *pack_id,
                        blobs,
                    });
                }
            }
        }

        // Save to Repository
        let (id, raw_size, encoded_size) = repo.save_file(
            &mapache::SaveID::CalculateID,
            serde_json::to_string(&index_file)?.as_bytes(),
            StorageHint {
                is_metadata: true,
                file_type: ContentIdType::Index,
            },
            None,
        )?;
        self.id = Some(id);

        Ok((raw_size, encoded_size))
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
        self.num_blobs() == 0 && self.num_packs() == 0
    }

    pub fn iter_ids(&self) -> impl Iterator<Item = (&ID, BlobLocator)> {
        let pack_ids = &self.pack_ids;

        let data_ids = self.data_ids.iter().map(move |(id, loc)| {
            (
                id,
                BlobLocator {
                    pack_id: *pack_ids.get_value(loc.pack_array_index as usize).unwrap(),
                    offset: loc.offset,
                    length: loc.length,
                    raw_length: loc.raw_length,
                    blob_type: BlobType::Data,
                },
            )
        });

        let tree_ids = self.tree_ids.iter().map(move |(id, loc)| {
            (
                id,
                BlobLocator {
                    pack_id: *pack_ids.get_value(loc.pack_array_index as usize).unwrap(),
                    offset: loc.offset,
                    length: loc.length,
                    raw_length: loc.raw_length,
                    blob_type: BlobType::Tree,
                },
            )
        });

        data_ids.chain(tree_ids)
    }

    fn remove_pack(&mut self, target_pack_id: &ID) {
        if let Some(pack_index) = self.pack_ids.get_index(target_pack_id).cloned() {
            let pack_index_u32 = pack_index as u32;
            let old_pack_ids_len = self.pack_ids.len();

            let moved_pack_is_needed = pack_index < old_pack_ids_len - 1;
            let old_pack_index_u32 = (old_pack_ids_len - 1) as u32;

            self.pack_ids.remove(target_pack_id);

            let process_blobs = |blob_map: &mut HashMap<ID, BlobLocationInternal>| {
                blob_map.retain(|_, loc| {
                    let current_pack_index_u32 = loc.pack_array_index;

                    if current_pack_index_u32 == pack_index_u32 {
                        return false;
                    }

                    if moved_pack_is_needed && current_pack_index_u32 == old_pack_index_u32 {
                        loc.pack_array_index = pack_index_u32;
                    }

                    true
                });
            };

            process_blobs(&mut self.data_ids);
            process_blobs(&mut self.tree_ids);
        }
    }
}

/// Manages a collection of `Index` instances, providing a unified view
/// over all known blobs in the repository.
#[derive(Debug, Clone)]
pub struct MasterIndex {
    /// A list of individual indices, some of which might be pending.
    indices: Vec<Index>,

    /// Stores the IDs of blobs that are waiting to be serialized into a pack file.
    pending_blobs: HashSet<ID>,
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
            indices: Vec::with_capacity(1),
            pending_blobs: HashSet::new(),
        }
    }

    /// Returns `true` if the object ID is known either in a finalized index
    /// or is currently a pending blob.
    pub fn contains(&self, id: &ID) -> bool {
        if self.pending_blobs.contains(id) {
            return true;
        }

        self.indices.iter().rev().any(|idx| idx.contains(id))
    }

    /// Retrieves an entry for a given blob ID by searching through finalized indices.
    /// Pending blobs (those not yet packed) cannot be retrieved via this method.
    pub fn get(&self, id: &ID) -> Option<BlobLocator> {
        self.indices
            .iter()
            .rev()
            .find_map(|idx| if !idx.is_pending { idx.get(id) } else { None })
    }

    /// Adds a fully constructed `Index` to the master index.
    /// This is typically used for adding loaded, finalized indices.
    pub fn add_index(&mut self, index: Index) {
        self.indices.push(index);
    }

    /// Adds a blob ID to the set of blobs that are waiting to be packed.
    /// Returns `true` if the ID did not exist in the set and was inserted; `false` otherwise.
    pub fn add_pending_blob(&mut self, id: ID) -> bool {
        self.pending_blobs.insert(id)
    }

    /// Processes a newly created pack of blobs. It removes these blobs from the
    /// `pending_blobs` set and adds them to all currently pending `Index` instances.
    ///
    /// It's assumed that there is at least one pending index that should receive these blobs,
    /// or that a new one will be created as part of the overall backup process if needed.
    pub fn add_pack(
        &mut self,
        repo: &Repository,
        pack_id: &ID,
        packed_blob_descriptors: Vec<PackedBlobDescriptor>,
    ) -> Result<(u64, u64)> {
        for blob in &packed_blob_descriptors {
            self.pending_blobs.remove(&blob.id);
        }

        let pending_index_exists = self.indices.iter().any(|idx| idx.is_pending());

        if !pending_index_exists {
            let new_idx = Index::new();
            self.indices.push(new_idx);
        }

        let pending_index = self
            .indices
            .iter_mut()
            .find(|idx| idx.is_pending())
            .expect("A pending index must exist at this point.");

        // Add the pack's blobs to the pending index.
        pending_index.add_pack(pack_id, &packed_blob_descriptors);

        // Check if the pending index is now full and save it if it is.
        if pending_index.is_full() {
            pending_index.finalize_and_save(repo)
        } else {
            Ok((0, 0))
        }
    }

    /// Saves all pending indices managed by the `MasterIndex` to the repository.
    /// Finalized indices are not saved again.
    ///
    /// Returns the total raw and encoded sizes of the saved index files.
    pub fn save(&mut self, repo: &Repository) -> Result<(u64, u64)> {
        let mut uncompressed_size: u64 = 0;
        let mut compressed_size: u64 = 0;

        for idx in &mut self.indices {
            if idx.is_pending() {
                let (uncompressed, compressed) = idx.finalize_and_save(repo)?;
                uncompressed_size += uncompressed;
                compressed_size += compressed;
            }
        }

        Ok((uncompressed_size, compressed_size))
    }

    pub fn iter_ids(&self) -> impl Iterator<Item = (&ID, BlobLocator)> {
        let mut chained_iterator: Box<dyn Iterator<Item = (&ID, BlobLocator)>> =
            Box::new(std::iter::empty());

        for index in &self.indices {
            chained_iterator = Box::new(chained_iterator.chain(index.iter_ids()));
        }
        chained_iterator
    }

    /// Returns the IDs of all finalized (serialized) indices
    pub fn ids(&self) -> HashSet<ID> {
        self.indices
            .iter()
            .filter_map(|idx| if !idx.is_pending() { idx.id() } else { None })
            .collect()
    }

    pub fn cleanup(&mut self, obsolete_packs: Option<&HashSet<ID>>) {
        if let Some(packs_to_remove) = obsolete_packs {
            for idx in &mut self.indices {
                // IMPORTANT: Mark index as pending so it can be overwritten/merged.
                idx.set_pending();
                for pack_id in packs_to_remove {
                    idx.remove_pack(pack_id);
                }
            }
        }

        self.merge_index();
    }

    /// Merges all current indices into a new collection of full indices.
    fn merge_index(&mut self) {
        let mut new_indices = Vec::new();
        let mut processed_pack_ids = HashSet::new();

        // Temporarily take ownership of the indices vector
        let old_indices: Vec<Index> = std::mem::take(&mut self.indices);

        let mut current_index = Index::new();

        for idx in old_indices {
            let mut packs_to_merge: HashMap<&ID, Vec<PackedBlobDescriptor>> = HashMap::new();

            // Helper closure to avoid code duplication
            let mut collect_blobs = |blob_map: &HashMap<ID, BlobLocationInternal>,
                                     blob_type: BlobType| {
                for (blob_id, loc) in blob_map.iter() {
                    // Resolve the Pack ID reference using the index
                    let pack_id_ref = idx
                        .pack_ids
                        .get_value(loc.pack_array_index as usize)
                        .expect("pack_index should always be valid for an existing blob");

                    let descriptor = PackedBlobDescriptor {
                        id: *blob_id,
                        blob_type,
                        offset: loc.offset,
                        length: loc.length,
                        raw_length: loc.raw_length,
                    };

                    packs_to_merge
                        .entry(pack_id_ref)
                        .or_default()
                        .push(descriptor);
                }
            };

            collect_blobs(&idx.data_ids, BlobType::Data);
            collect_blobs(&idx.tree_ids, BlobType::Tree);
            for (pack_id_ref, packed_blob_descriptors) in packs_to_merge {
                if processed_pack_ids.contains(pack_id_ref) {
                    continue;
                }

                if current_index.is_full() {
                    current_index.set_pending();
                    new_indices.push(current_index);
                    current_index = Index::new();
                }

                let pack_id = *pack_id_ref;
                processed_pack_ids.insert(pack_id);

                current_index.add_pack(pack_id_ref, &packed_blob_descriptors);
            }
        }

        if !current_index.is_empty() {
            current_index.set_pending();
            new_indices.push(current_index);
        }

        // Assign the new, merged indices back
        self.indices = new_indices;
    }

    pub fn search_prefix(&self, prefix: &str) -> Result<Option<&ID>> {
        let ids = self.iter_ids();
        let matched_ids: Vec<_> = ids
            .filter(|(id, _)| id.to_hex().starts_with(prefix))
            .collect();

        if matched_ids.len() > 1 {
            bail!("Prefix '{prefix}' is ambiguous");
        }

        match matched_ids.first() {
            None => Ok(None),
            Some((blob_id, _)) => Ok(Some(blob_id)),
        }
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
#[derive(Debug, Default, Serialize, Deserialize)]
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
        index.add_pack(&pack_id_a, &[data_blob.clone(), padding_blob.clone()]);
        index.add_pack(&pack_id_b, &[tree_blob.clone()]);

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
        let ids: HashSet<&ID> = index.iter_ids().map(|(id, _)| id).collect();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&data_blob.id));
        assert!(ids.contains(&tree_blob.id));
    }
}
