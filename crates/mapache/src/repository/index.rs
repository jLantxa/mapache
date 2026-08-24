use std::{
    str::FromStr,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use async_trait::async_trait;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::{
    backend::StorageHint,
    common::error::{MapacheError, Result},
    common::{self, BlobType, ContentIdType, ID},
    repository::{
        packer::PackedBlobDescriptor,
        repo::{Repository, SizePair},
    },
    utils::{
        binary::{get_array, get_u8, get_u32, put_bytes, put_u32},
        collections::{BloomFilter, IdIndexSet, IdMap, IdSet, Lru, ShardedIdSet},
    },
};

/// Index loading mode: eager (load all) or lazy (hot + cold).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum IndexMode {
    /// Load all indices into RAM. Fastest lookups.
    #[default]
    Eager,
    /// Only load N most recent indices (hot), rest use lazy loading (cold).
    /// Saves memory; cold lookups re-load the full index from disk.
    /// The value is the maximum total blob count in the LRU cache.
    Lazy(u64),
}

impl IndexMode {
    pub fn is_eager(&self) -> bool {
        matches!(self, Self::Eager)
    }
}

impl Serialize for IndexMode {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for IndexMode {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Self::from_str(&s).map_err(serde::de::Error::custom)
    }
}

impl std::fmt::Display for IndexMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Eager => write!(f, "eager"),
            Self::Lazy(_) => write!(f, "lazy"),
        }
    }
}

impl FromStr for IndexMode {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "eager" => Ok(Self::Eager),
            "lazy" => Ok(Self::Lazy(common::defaults::DEFAULT_LRU_MAX_BLOBS)),
            _ => Err(format!(
                "Invalid index mode: '{s}'. Valid values: eager, lazy"
            )),
        }
    }
}

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
    /// Whether the blob's encoded payload is zstd-compressed.
    pub compressed: bool,
}

/// Internal representation of blob ID to location mappings.
/// Uses a `HashMap` for mutable indices (under construction) and
/// a sorted `Vec` with a per-map Bloom filter for immutable indices
/// (loaded from disk). This reduces memory vs a HashMap and
/// avoids O(log n) binary search for entries not present in a given index.
#[derive(Debug, Clone)]
enum BlobMap {
    Mutable(IdMap<ID, BlobLocationInternal>),
    Immutable(Vec<(ID, BlobLocationInternal)>, BloomFilter),
}

impl BlobMap {
    fn new_mutable() -> Self {
        BlobMap::Mutable(IdMap::default())
    }

    fn contains(&self, id: &ID) -> bool {
        match self {
            BlobMap::Mutable(map) => map.contains_key(id),
            BlobMap::Immutable(vec, bf) => {
                bf.contains(id) && vec.binary_search_by_key(&id, |(k, _)| k).is_ok()
            }
        }
    }

    fn get(&self, id: &ID) -> Option<&BlobLocationInternal> {
        match self {
            BlobMap::Mutable(map) => map.get(id),
            BlobMap::Immutable(vec, bf) => {
                if !bf.contains(id) {
                    return None;
                }
                let Ok(idx) = vec.binary_search_by_key(&id, |(k, _)| k) else {
                    return None;
                };
                Some(&vec[idx].1)
            }
        }
    }

    fn insert(&mut self, id: ID, loc: BlobLocationInternal) {
        match self {
            BlobMap::Mutable(map) => {
                map.insert(id, loc);
            }
            BlobMap::Immutable(_, _) => {
                // This is a programming error — insert is only called during
                // snapshotting on pending indices, which are always Mutable.
                debug_assert!(
                    false,
                    "insert into immutable BlobMap for id={}",
                    id.to_short_hex(8)
                );
                tracing::error!(target: "index", "Attempted insert into immutable BlobMap for id={}", id.to_short_hex(8));
            }
        }
    }

    fn len(&self) -> usize {
        match self {
            BlobMap::Mutable(map) => map.len(),
            BlobMap::Immutable(vec, _) => vec.len(),
        }
    }

    fn freeze(&mut self) {
        let BlobMap::Mutable(map) = std::mem::replace(
            self,
            BlobMap::Immutable(Vec::new(), BloomFilter::new(1, 0.01)),
        ) else {
            return;
        };
        let mut vec: Vec<(ID, BlobLocationInternal)> = map.into_iter().collect();
        vec.sort_unstable_by_key(|(id, _)| *id);
        let mut bf = BloomFilter::new(vec.len(), 0.01);
        for (id, _) in &vec {
            bf.insert(id);
        }
        *self = BlobMap::Immutable(vec, bf);
    }

    fn iter(&self) -> Box<dyn Iterator<Item = (&ID, &BlobLocationInternal)> + '_> {
        match self {
            BlobMap::Mutable(map) => Box::new(map.iter()),
            BlobMap::Immutable(vec, _) => Box::new(vec.iter().map(|(id, loc)| (id, loc))),
        }
    }
}

/// Full descriptor of a blob's location, including the resolved Pack ID.
#[derive(Debug, Clone, Copy)]
pub struct BlobLocator {
    pub pack_id: ID,
    pub offset: u32,
    pub length: u32,
    pub raw_length: u32,
    pub blob_type: BlobType,
    pub compressed: bool,
}

/// Loader for cold indices. Implemented by Repository to provide async disk I/O.
#[async_trait]
pub(crate) trait ColdIndexLoader: Send + Sync {
    async fn load_index(&self, file_id: &ID) -> Result<Index>;
}

/// Lightweight metadata for a cold (lazy-loaded) index file.
/// Contains only the BloomFilter + pack IDs + zero blob info needed
/// to determine if a blob lookup requires loading the full index.
#[derive(Debug, Clone)]
pub struct IndexMetadata {
    /// The file ID of this index file (for loading from disk).
    pub file_id: ID,
    /// BloomFilter for fast negative lookups.
    pub bloom_filter: BloomFilter,
    /// Pack IDs referenced by this index file.
    pub pack_ids: Vec<ID>,
    /// Zero blobs: ID -> raw_length (usually empty, small even when populated).
    pub zero_blobs: Vec<(ID, u32)>,
    /// Number of blobs in this index (for statistics).
    pub blob_count: usize,
}

impl IndexMetadata {
    /// Create IndexMetadata from an existing `Index`.
    pub fn from_index(index: &Index, file_id: ID) -> Self {
        let total_blobs = index.num_blobs();
        let mut bloom_filter = BloomFilter::new(total_blobs, 0.01);
        for (id, _) in index.iter_ids() {
            bloom_filter.insert(id);
        }

        let pack_ids: Vec<ID> = index.pack_ids.iter().copied().collect();
        let blob_count = index.num_blobs();

        let zero_blobs: Vec<(ID, u32)> = index
            .zero_ids
            .iter()
            .map(|(id, loc)| (*id, loc.raw_length))
            .collect();

        Self {
            file_id,
            bloom_filter,
            pack_ids,
            zero_blobs,
            blob_count,
        }
    }

    /// Create IndexMetadata directly from an `IndexFile` (without building a full Index).
    pub fn from_index_file(index_file: IndexFile, bloom_filter: BloomFilter, file_id: ID) -> Self {
        let blob_count: usize = index_file.packs.iter().map(|p| p.blobs.len()).sum();
        let pack_ids: Vec<ID> = index_file.packs.iter().map(|p| p.id).collect();

        Self {
            file_id,
            bloom_filter,
            pack_ids,
            zero_blobs: Vec::new(),
            blob_count,
        }
    }

    /// Create a BloomFilter from an `IndexFile` for cold index metadata.
    pub fn bloom_filter_from_index_file(index_file: &IndexFile) -> BloomFilter {
        let total_blobs: usize = index_file.packs.iter().map(|p| p.blobs.len()).sum();
        let mut bf = BloomFilter::new(total_blobs, 0.01);
        for pack in &index_file.packs {
            for blob in &pack.blobs {
                bf.insert(&blob.id);
            }
        }
        bf
    }

    /// Check if a blob might be in this index (no false negatives).
    pub fn might_contain(&self, id: &ID) -> bool {
        self.bloom_filter.contains(id)
    }
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

    /// The file ID of this index on disk (None for pending indices).
    file_id: Option<ID>,

    /// blob ID -> BlobLocationInternal map. This is the core lookup table.
    /// Uses `BlobMap` which is a HashMap while mutable and a sorted Vec
    /// once persisted, reducing memory vs a HashMap for on-disk indices.
    data_ids: BlobMap,
    tree_ids: BlobMap,

    /// Zero blobs: ID -> BlobLocationInternal. Listed in data pack footers
    /// with length=0, raw_length=N. During restore, N bytes of zeros are produced.
    zero_ids: BlobMap,

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
            file_id: None,
            data_ids: BlobMap::new_mutable(),
            tree_ids: BlobMap::new_mutable(),
            zero_ids: BlobMap::new_mutable(),
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
        self.num_blobs() >= common::defaults::runtime().blobs_per_index_file
    }

    /// Creates an `Index` from a serialized `IndexFile`.
    /// Builds sorted `Vec` entries directly (immutable representation) to save memory.
    pub fn from_index_file(index_file: IndexFile, id: ID) -> Self {
        let mut index = Self::new();
        tracing::debug!(target: "index", "Loading index {} into instance #{}", id.to_short_hex(8), index.instance_id);
        index.file_id = Some(id);
        index.set_status(IndexStatus::Persisted(id));

        let mut data_entries = Vec::new();
        let mut tree_entries = Vec::new();
        let mut zero_entries = Vec::new();

        for pack in index_file.packs {
            let pack_index = index.pack_ids.insert(pack.id) as u32;

            for blob in pack.blobs {
                if matches!(blob.blob_type, BlobType::Padding) {
                    continue;
                }

                let loc = BlobLocationInternal {
                    pack_array_index: pack_index,
                    offset: blob.offset,
                    length: blob.length,
                    raw_length: blob.raw_length,
                    compressed: blob.compressed,
                };

                match blob.blob_type {
                    BlobType::Data => data_entries.push((blob.id, loc)),
                    BlobType::Tree => tree_entries.push((blob.id, loc)),
                    BlobType::Zero => zero_entries.push((blob.id, loc)),
                    _ => {}
                }
            }
        }

        data_entries.sort_unstable_by_key(|(id, _)| *id);
        tree_entries.sort_unstable_by_key(|(id, _)| *id);
        zero_entries.sort_unstable_by_key(|(id, _)| *id);

        let mut data_bf = BloomFilter::new(data_entries.len(), 0.01);
        for (id, _) in &data_entries {
            data_bf.insert(id);
        }
        let mut tree_bf = BloomFilter::new(tree_entries.len(), 0.01);
        for (id, _) in &tree_entries {
            tree_bf.insert(id);
        }
        let mut zero_bf = BloomFilter::new(zero_entries.len(), 0.01);
        for (id, _) in &zero_entries {
            zero_bf.insert(id);
        }

        index.data_ids = BlobMap::Immutable(data_entries, data_bf);
        index.tree_ids = BlobMap::Immutable(tree_entries, tree_bf);
        index.zero_ids = BlobMap::Immutable(zero_entries, zero_bf);

        index
    }

    /// Checks if the index contains the given object ID.
    #[inline]
    pub fn contains(&self, id: &ID) -> bool {
        self.data_ids.contains(id) || self.tree_ids.contains(id) || self.zero_ids.contains(id)
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
            compressed: loc.compressed,
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
            .or_else(|| {
                // Zero blobs don't live in packs — synthesize a locator directly.
                // resolve_location would fail because pack_array_index is synthetic.
                self.zero_ids.get(id).map(|l| BlobLocator {
                    pack_id: ID::default(),
                    blob_type: BlobType::Zero,
                    offset: 0,
                    length: 0,
                    raw_length: l.raw_length,
                    compressed: false,
                })
            })
    }

    /// Look up a data blob location directly.
    fn get_data_location(&self, id: &ID) -> Option<&BlobLocationInternal> {
        self.data_ids.get(id)
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
                BlobType::Zero => &mut self.zero_ids,
                _ => continue,
            };

            map.insert(
                blob.id,
                BlobLocationInternal {
                    pack_array_index: pack_index,
                    offset: blob.offset,
                    length: blob.length,
                    raw_length: blob.raw_length,
                    compressed: blob.compressed,
                },
            );
        }
    }

    /// Saves the index to the repository.
    /// Returns the total uncompressed and compressed sizes of the saved index files.
    // TODO(v1-removal): Remove `repo_version` parameter.
    pub async fn persist(
        &mut self,
        repo: &Repository,
        repo_version: Option<u32>,
    ) -> Result<SizePair> {
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
        let mut add_to_entries = |map: &BlobMap, b_type: BlobType| {
            for (id, loc) in map.iter() {
                pack_entries[loc.pack_array_index as usize]
                    .blobs
                    .push(IndexFileBlob {
                        id: *id,
                        blob_type: b_type,
                        offset: loc.offset,
                        length: loc.length,
                        raw_length: loc.raw_length,
                        compressed: loc.compressed,
                    });
            }
        };

        add_to_entries(&self.data_ids, BlobType::Data);
        add_to_entries(&self.tree_ids, BlobType::Tree);
        add_to_entries(&self.zero_ids, BlobType::Zero);

        // Sort blobs within each pack for deterministic serialization
        for pack in &mut pack_entries {
            pack.blobs.sort_unstable_by_key(|b| b.id);
        }

        // Filter out empty packs (though there shouldn't be any in a healthy index)
        pack_entries.retain(|p| !p.blobs.is_empty());

        // Sort packs themselves
        pack_entries.sort_unstable_by_key(|p| p.id);

        let effective_version = repo_version.unwrap_or(repo.repo_version());
        let serialized = IndexFile {
            packs: pack_entries,
        }
        .serialize(effective_version)?;

        let (id, size) = repo
            .save_file(
                &common::SaveID::CalculateID,
                &serialized,
                StorageHint {
                    is_metadata: true,
                    file_type: ContentIdType::Index,
                },
                None,
            )
            .await?;

        self.set_status(IndexStatus::Persisted(id));

        // Free memory: convert Mutable (HashMap) to Immutable (sorted Vec)
        self.data_ids.freeze();
        self.tree_ids.freeze();
        self.zero_ids.freeze();

        Ok(size)
    }

    #[inline]
    pub fn num_blobs(&self) -> usize {
        self.data_ids.len() + self.tree_ids.len() + self.zero_ids.len()
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
        let mut result: Vec<(&ID, BlobLocator)> = Vec::with_capacity(self.num_blobs());
        for (id, loc) in self.data_ids.iter() {
            if let Some(locator) = self.resolve_location(loc, BlobType::Data) {
                result.push((id, locator));
            }
        }
        for (id, loc) in self.tree_ids.iter() {
            if let Some(locator) = self.resolve_location(loc, BlobType::Tree) {
                result.push((id, locator));
            }
        }
        for (id, loc) in self.zero_ids.iter() {
            if let Some(locator) = self.resolve_location(loc, BlobType::Zero) {
                result.push((id, locator));
            }
        }
        result.into_iter()
    }

    /// Returns a map of Pack ID -> List of descriptors for all packs in this index.
    fn get_pack_descriptors(
        &self,
        obsolete_packs: Option<&IdSet<ID>>,
    ) -> IdMap<ID, Vec<PackedBlobDescriptor>> {
        let mut pack_descriptors = IdMap::default();

        let mut process_map = |map: &BlobMap, b_type: BlobType| {
            for (id, loc) in map.iter() {
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
                        compressed: loc.compressed,
                    });
            }
        };

        process_map(&self.data_ids, BlobType::Data);
        process_map(&self.tree_ids, BlobType::Tree);
        // Zero blobs don't live in packs (stored in footer with length=0).
        // Skip them to avoid phantom descriptors from backward-compat synthetic entries.

        pack_descriptors
    }
}

/// Tracks blob IDs that are waiting to be serialized into a pack file.
/// Wraps a `ShardedIdSet` for low-contention parallel snapshotting.
#[derive(Debug, Clone)]
struct PendingBlobs(Arc<ShardedIdSet>);

impl PendingBlobs {
    fn new() -> Self {
        Self(Arc::new(ShardedIdSet::new()))
    }

    fn contains(&self, id: &ID) -> bool {
        self.0.contains(id)
    }

    fn insert(&self, id: ID) -> bool {
        self.0.insert(id)
    }

    fn remove(&self, id: &ID) {
        self.0.remove(id);
    }

    fn clear(&self) {
        self.0.clear();
    }
}

/// Manages a collection of `Index` instances, providing a unified view
/// over all known blobs in the repository.
#[derive(Clone)]
pub struct MasterIndex {
    /// Internal state protected by a read-write lock.
    inner: Arc<RwLock<MasterIndexInner>>,
    /// Blob IDs waiting to be serialized into a pack file.
    pending_blobs: PendingBlobs,
    auto_save: bool,
    /// Index loading mode: eager (keep all in RAM) or lazy (move persisted to cold).
    index_mode: IndexMode,
    /// Async loader for cold indices (provided by Repository).
    loader: OnceLock<Arc<dyn ColdIndexLoader>>,
}

#[derive(Debug)]
struct MasterIndexInner {
    /// A list of individual indices, some of which might be pending.
    indices: Vec<Index>,
    /// Bloom Filter for fast deduplication checks.
    bloom_filter: Option<BloomFilter>,
    /// Cold (lazy-loaded) index metadata: BloomFilter + pack IDs for indices not loaded into RAM.
    cold_metadata: Vec<IndexMetadata>,
    /// LRU cache for cold indices that have been loaded on demand.
    /// Key: file_id of the index, Value: the loaded `Index`.
    /// Only accessed while holding a write lock on `inner`.
    lru: Lru<ID, Index>,
}

impl Default for MasterIndex {
    fn default() -> Self {
        Self::new(common::defaults::DEFAULT_INDEX_MODE)
    }
}

impl MasterIndex {
    /// Creates a new, empty `MasterIndex`.
    pub fn new(index_mode: IndexMode) -> Self {
        let lru = match &index_mode {
            IndexMode::Lazy(lru_max_blobs) => Lru::with_max_weight(*lru_max_blobs),
            IndexMode::Eager => Lru::new(),
        };
        Self {
            inner: Arc::new(RwLock::new(MasterIndexInner {
                indices: Vec::with_capacity(1),
                bloom_filter: None,
                cold_metadata: Vec::new(),
                lru,
            })),
            pending_blobs: PendingBlobs::new(),
            auto_save: true,
            index_mode,
            loader: OnceLock::new(),
        }
    }

    /// Set the async loader for cold indices. Must be called before any lookups.
    pub(crate) fn set_loader(&self, loader: Arc<dyn ColdIndexLoader>) {
        let _ = self.loader.set(loader);
    }

    pub fn clear(&self) {
        let mut lock = self.inner.write();
        let max_weight = lock.lru.max_weight;
        lock.indices.clear();
        lock.bloom_filter = None;
        lock.cold_metadata.clear();
        lock.lru = Lru::with_max_weight(max_weight);
        self.pending_blobs.clear();
    }

    /// Returns the total number of blobs in all finalized indices (hot only).
    pub fn num_blobs(&self) -> usize {
        let lock = self.inner.read();
        lock.indices.iter().map(|idx| idx.num_blobs()).sum()
    }

    /// Returns the total number of blobs in all indices (hot + cold).
    pub fn num_blobs_total(&self) -> usize {
        let lock = self.inner.read();
        let hot: usize = lock.indices.iter().map(|idx| idx.num_blobs()).sum();
        let cold: usize = lock.cold_metadata.iter().map(|meta| meta.blob_count).sum();
        hot + cold
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

    /// Look up a blob by ID, searching data_ids first, then tree_ids, then zero_ids.
    /// The fallback chain exists because callers (restorer, verify) look up blob IDs
    /// from file node descriptors without knowing the blob type upfront. A file's
    /// content may span Data, Tree, or Zero blobs depending on dedup and zero-fill.
    pub fn get_data(&self, id: &ID) -> Option<BlobLocator> {
        let lock = self.inner.read();

        lock.indices
            .iter()
            .rev()
            .find_map(|idx| {
                idx.get_data_location(id)
                    .and_then(|l| idx.resolve_location(l, BlobType::Data))
            })
            .or_else(|| {
                lock.indices.iter().rev().find_map(|idx| {
                    idx.tree_ids
                        .get(id)
                        .and_then(|l| idx.resolve_location(l, BlobType::Tree))
                })
            })
            .or_else(|| {
                lock.indices.iter().rev().find_map(|idx| {
                    idx.zero_ids
                        .get(id)
                        .and_then(|l| idx.resolve_location(l, BlobType::Zero))
                })
            })
    }

    /// Retrieves an entry for a given blob ID by searching through finalized indices.
    /// Pending blobs (those not yet packed) cannot be retrieved via this method.
    /// Searches hot indices first; in lazy mode, loads cold indices on demand.
    pub async fn get(&self, id: &ID) -> Option<BlobLocator> {
        // Single pass: check hot indices and cold metadata under one lock.
        let cold_idx = {
            let lock = self.inner.read();

            // Fast path: search hot indices
            if let Some(locator) = lock.indices.iter().rev().find_map(|idx| idx.get(id)) {
                return Some(locator);
            }

            if self.index_mode == IndexMode::Eager {
                return None;
            }

            // Check cold metadata bloom filters
            lock.cold_metadata
                .iter()
                .enumerate()
                .rev()
                .find(|(_, meta)| meta.might_contain(id))
                .map(|(i, meta)| {
                    // Zero blobs are resolved directly from cold metadata
                    if let Ok(j) = meta.zero_blobs.binary_search_by_key(id, |(zid, _)| *zid) {
                        let &(_, raw_length) = &meta.zero_blobs[j];
                        return Err(BlobLocator {
                            pack_id: ID::default(),
                            blob_type: BlobType::Zero,
                            offset: 0,
                            length: 0,
                            raw_length,
                            compressed: false,
                        });
                    }
                    Ok(i)
                })
        };

        match cold_idx {
            Some(Err(zero_locator)) => Some(zero_locator),
            Some(Ok(idx)) => {
                if self.load_and_promote(idx).await.is_some() {
                    self.get_hot(id)
                } else {
                    None
                }
            }
            None => None,
        }
    }

    /// Synchronous hot-only lookup. Used internally by get() and GC.
    fn get_hot(&self, id: &ID) -> Option<BlobLocator> {
        let lock = self.inner.read();
        lock.indices.iter().rev().find_map(|idx| idx.get(id))
    }

    /// Load a cold index from disk and promote it to hot.
    async fn load_and_promote(&self, cold_idx: usize) -> Option<()> {
        let meta = {
            let mut lock = self.inner.write();
            if cold_idx < lock.cold_metadata.len() {
                let meta = lock.cold_metadata.swap_remove(cold_idx);
                tracing::debug!(target: "index", "Loading cold index {} (file {}) into hot",
                    cold_idx, meta.file_id.to_short_hex(8));
                lock.lru.remove(&meta.file_id);
                Some(meta)
            } else {
                None
            }
        }?;

        let loader = self.loader.get()?;
        match loader.load_index(&meta.file_id).await {
            Ok(index) => {
                let mut lock = self.inner.write();
                lock.indices.push(index);
                if matches!(self.index_mode, IndexMode::Lazy(_)) {
                    self.enforce_hot_limit(&mut lock);
                }
                Some(())
            }
            Err(e) => {
                tracing::warn!(target: "index",
                    "Failed to load cold index {}: {}",
                    meta.file_id.to_short_hex(8), e);
                None
            }
        }
    }

    /// Check if a blob might be in any index (hot or cold).
    /// Returns `true` if the blob might exist (no false negatives).
    pub fn might_contain(&self, id: &ID) -> bool {
        let lock = self.inner.read();
        if lock.indices.iter().rev().any(|idx| idx.contains(id)) {
            return true;
        }
        lock.cold_metadata
            .iter()
            .rev()
            .any(|meta| meta.might_contain(id))
    }

    /// Adds a cold index metadata entry for lazy loading.
    pub fn add_cold_metadata(&self, meta: IndexMetadata) {
        let mut lock = self.inner.write();
        lock.cold_metadata.push(meta);
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

        // In lazy mode, enforce hot limit by evicting oldest non-pending to cold
        if matches!(self.index_mode, IndexMode::Lazy { .. }) {
            self.enforce_hot_limit(&mut lock);
        }
    }

    /// Iterates over cold indices one at a time, loading each from disk via the
    /// provided async loader, invoking the callback, and dropping it before loading
    /// the next. Memory bounded: only one cold index in RAM at a time.
    /// Caller must hold no locks on `inner` when calling this.
    pub async fn for_each_cold_index<F, L>(&self, load_index: L, mut f: F)
    where
        F: FnMut(&Index),
        L: FnMut(ID) -> futures::future::BoxFuture<'static, Result<Index>>,
    {
        let mut load = load_index;

        loop {
            // Get next cold index file_id under read lock, then drop lock
            let next_file_id = {
                let lock = self.inner.read();
                lock.cold_metadata.first().map(|m| m.file_id)
            };

            let file_id = match next_file_id {
                Some(id) => id,
                None => break,
            };

            // Remove from cold_metadata under write lock
            {
                let mut lock = self.inner.write();
                lock.cold_metadata.retain(|m| m.file_id != file_id);
                lock.lru.remove(&file_id);
            }

            // Load from disk (no lock held)
            match load(file_id).await {
                Ok(index) => {
                    f(&index);
                    // Evict back to cold instead of keeping hot
                    let cold_meta = IndexMetadata::from_index(&index, file_id);
                    let mut lock = self.inner.write();
                    lock.cold_metadata.push(cold_meta);
                }
                Err(e) => {
                    tracing::warn!(target: "index",
                        "Failed to load cold index {}: {}",
                        file_id.to_short_hex(8), e);
                }
            }
        }
    }

    /// Enforce the maximum number of hot indices by evicting the oldest
    /// non-pending finalized index to cold metadata.
    fn enforce_hot_limit(&self, lock: &mut MasterIndexInner) {
        let hot_limit = common::defaults::INDEX_HOT_COUNT;

        // Count non-pending indices (pending always stays hot)
        let non_pending_count = lock.indices.iter().filter(|i| !i.is_pending()).count();

        if non_pending_count <= hot_limit {
            return;
        }

        // Find the oldest non-pending index to evict
        if let Some(pos) = lock.indices.iter().position(|i| !i.is_pending()) {
            let file_id = lock.indices[pos].file_id.unwrap_or_default();
            tracing::debug!(target: "index", "Evicting hot index {} (file {}) to cold (hot limit: {})",
                lock.indices[pos].instance_id, file_id.to_short_hex(8), hot_limit);
            let cold_meta = IndexMetadata::from_index(&lock.indices[pos], file_id);
            // Cache the evicted index in the LRU so it can be reused if still cached
            let evicted = lock.indices.swap_remove(pos);
            let weight = evicted.num_blobs() as u64;
            lock.lru.insert(file_id, Arc::new(evicted), weight);
            lock.cold_metadata.push(cold_meta);
        }
    }

    /// Initializes a Bloom Filter for all blobs currently in the master index (hot only).
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

        {
            let num_blobs = descriptors.len();
            let mut lock = self.inner.write();

            // Remove non-Padding blobs from pending set and add to bloom filter.
            // Padding blobs are synthetic and should not be tracked in pending.
            for blob in &descriptors {
                if !matches!(blob.blob_type, BlobType::Padding) {
                    self.pending_blobs.remove(&blob.id);
                    if let Some(bf) = &mut lock.bloom_filter {
                        bf.insert(&blob.id);
                    }
                }
            }

            if !lock.indices.iter().any(|idx| idx.is_pending()) {
                lock.indices.push(Index::new());
            }

            let pending_pos = lock
                .indices
                .iter()
                .position(|idx| idx.is_pending())
                .ok_or_else(|| {
                    MapacheError::Repo(format!("no pending index available to add pack {pack_id}"))
                })?;

            tracing::debug!(target: "index", "Adding pack {} ({} blobs) to pending index #{}", pack_id.to_short_hex(8), num_blobs, lock.indices[pending_pos].instance_id);
            lock.indices[pending_pos].add_pack(pack_id, descriptors);

            let is_full = lock.indices[pending_pos].is_full();
            let is_timed_out = lock.indices[pending_pos].create_time.elapsed()
                >= common::defaults::runtime().index_flush_timeout;

            if self.auto_save && (is_full || is_timed_out) {
                let reason = if is_full { "full" } else { "timeout" };
                tracing::info!(target: "index", "Persisting index #{} (reason: {})", lock.indices[pending_pos].instance_id, reason);
                lock.indices[pending_pos].finalize();
                index_to_persist = Some(lock.indices.swap_remove(pending_pos));
            } else if is_full {
                tracing::debug!(target: "index", "Index #{} is full, finalizing", lock.indices[pending_pos].instance_id);
                lock.indices[pending_pos].finalize();
            }
        }

        if let Some(mut idx) = index_to_persist {
            let size = idx.persist(repo, None).await?;
            // Put the persisted index back with updated status.
            let mut lock = self.inner.write();
            lock.indices.push(idx);
            Ok(size)
        } else {
            Ok(SizePair::zero())
        }
    }

    pub async fn persist(&self, repo: &Repository) -> Result<SizePair> {
        self.persist_with_version(repo, None).await
    }

    // TODO(v1-removal): Remove `repo_version` parameter.
    pub async fn persist_with_version(
        &self,
        repo: &Repository,
        repo_version: Option<u32>,
    ) -> Result<SizePair> {
        let mut total_size = SizePair::zero();

        // Collect indices that need persisting, taking them out to avoid holding
        // the lock during IO. They'll be pushed back after persistence.
        let mut indices_to_persist = Vec::new();
        {
            let mut lock = self.inner.write();
            let mut i = 0;
            while i < lock.indices.len() {
                if !matches!(lock.indices[i].status, IndexStatus::Persisted(_))
                    && !lock.indices[i].is_empty()
                {
                    tracing::debug!(target: "index", "Marking index #{} for persistence", lock.indices[i].instance_id);
                    lock.indices[i].finalize();
                    indices_to_persist.push(lock.indices.swap_remove(i));
                } else {
                    i += 1;
                }
            }
        }

        let num_to_persist = indices_to_persist.len();
        if num_to_persist > 0 {
            tracing::info!(target: "index", "Persisting {} indices", num_to_persist);
        }

        for mut idx in indices_to_persist {
            let size = idx.persist(repo, repo_version).await?;
            total_size += size;

            // Put the persisted index back.
            let mut lock = self.inner.write();
            lock.indices.push(idx);
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

        // Flatten all descriptors into a single Vec, preserving iteration order
        // (oldest index first) so that dedup keeps the newest occurrence.
        let mut all: Vec<(ID, PackedBlobDescriptor)> = old_indices
            .iter()
            .flat_map(|idx| idx.get_pack_descriptors(obsolete_packs))
            .flat_map(|(pack_id, descs)| descs.into_iter().map(move |d| (pack_id, d)))
            .collect();

        // Sort by (pack_id, blob_id) for deterministic dedup and pack grouping.
        all.sort_unstable_by_key(|(pack_id, desc)| (*pack_id, desc.id));

        // Dedup: keep last occurrence per (pack_id, blob_id) — the newest index wins.
        // swap a↔b so that dedup_by (which keeps a) retains the newer entry.
        all.dedup_by(|a, b| {
            if a.0 == b.0 && a.1.id == b.1.id {
                std::mem::swap(a, b);
                true
            } else {
                false
            }
        });

        let mut new_indices = Vec::new();
        let mut current_index = Index::new();
        let mut current_pack_id = None;
        let mut current_blobs: Vec<PackedBlobDescriptor> = Vec::new();

        for (pack_id, desc) in all {
            if current_pack_id != Some(pack_id) {
                if let Some(pid) = current_pack_id.take() {
                    current_index.add_pack(&pid, current_blobs);
                    current_blobs = Vec::new();
                    if current_index.is_full() {
                        current_index.set_status(IndexStatus::Finalized);
                        new_indices.push(std::mem::take(&mut current_index));
                    }
                }
                current_pack_id = Some(pack_id);
            }
            current_blobs.push(desc);
        }
        if let Some(pid) = current_pack_id {
            current_index.add_pack(&pid, current_blobs);
        }

        if !current_index.is_empty() {
            current_index.set_status(IndexStatus::Pending);
            new_indices.push(current_index);
        }

        tracing::info!(target: "index", "Indices merged: {} -> {}", num_old_indices, new_indices.len());
        lock.indices = new_indices;

        // Also clean up cold_metadata that references deleted packs.
        if let Some(obsolete) = obsolete_packs {
            let before = lock.cold_metadata.len();
            lock.cold_metadata
                .retain(|meta| meta.pack_ids.iter().any(|pid| !obsolete.contains(pid)));
            let removed = before - lock.cold_metadata.len();
            if removed > 0 {
                tracing::info!(target: "index", "Removed {removed} cold index entries referencing deleted packs");
            }
        }
    }

    pub async fn search_prefix(&self, prefix: &str) -> Result<Option<ID>> {
        let mut matched = Vec::new();

        self.for_each_id(|id, _| {
            if id.to_hex().starts_with(prefix) {
                matched.push(*id);
            }
        });

        if matched.len() > 1 {
            return Err(MapacheError::Format(format!(
                "prefix '{}' is ambiguous",
                prefix
            )));
        }

        Ok(matched.first().cloned())
    }

    pub fn set_autosave(&mut self, auto_save: bool) {
        self.auto_save = auto_save;
    }
}

/// Represents the on-disk format for an index file.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct IndexFile {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub packs: Vec<IndexFilePack>,
}

impl IndexFile {
    /// Serialize the `IndexFile` based on the repository version.
    // TODO(v1-removal): Remove the v1 JSON branch.
    pub fn serialize(&self, repo_version: u32) -> Result<Vec<u8>> {
        if repo_version >= 2 {
            Ok(serialize_index_binary(self))
        } else {
            super::legacy::serialize_index_json(self)
        }
    }

    /// Deserialize an `IndexFile` based on the repository version.
    // TODO(v1-removal): Remove the v1 JSON branch.
    pub fn deserialize(data: &[u8], repo_version: u32) -> Result<Self> {
        if repo_version >= 2 {
            deserialize_index_binary(data)
                .map_err(|e| MapacheError::Format(format!("failed to deserialize index: {e}")))
        } else {
            super::legacy::deserialize_index_json(data)
        }
    }
}

/// Represents a pack's entry within an `IndexFile`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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
    /// Whether the blob's encoded payload is zstd-compressed. Encoded in the
    /// high bit of the type byte on disk (same as pack footers and bundle
    /// index entries).
    ///
    /// Defaults to `true` because v1 repos (whose index files may lack this
    /// field) always store zstd-compressed blobs.
    // TODO(v1-removal): Remove the default and always read from the marker.
    #[serde(default = "default_true")]
    pub compressed: bool,
}

fn default_true() -> bool {
    true
}

/// Serialize an `IndexFile` to the binary format.
pub fn serialize_index_binary(index_file: &IndexFile) -> Vec<u8> {
    let total_blobs: usize = index_file.packs.iter().map(|p| p.blobs.len()).sum();
    let size = 4 + index_file.packs.len() * 36 + total_blobs * 45;
    let mut buf = Vec::with_capacity(size);

    // Header
    put_u32(&mut buf, index_file.packs.len() as u32);

    for pack in &index_file.packs {
        put_bytes(&mut buf, pack.id.as_slice());
        put_u32(&mut buf, pack.blobs.len() as u32);

        for blob in &pack.blobs {
            put_bytes(&mut buf, blob.id.as_slice());
            buf.push(blob.blob_type.to_byte(blob.compressed));
            put_u32(&mut buf, blob.offset);
            put_u32(&mut buf, blob.length);
            put_u32(&mut buf, blob.raw_length);
        }
    }

    buf
}

/// Deserialize an `IndexFile` from the binary format.
pub fn deserialize_index_binary(data: &[u8]) -> Result<IndexFile> {
    let mut cur = data;

    let num_packs = get_u32(&mut cur)? as usize;
    if num_packs > 1_000_000 {
        return Err(MapacheError::Integrity(format!(
            "index claims {num_packs} packs, which exceeds sanity limit"
        )));
    }
    let mut packs = Vec::with_capacity(num_packs);

    for _ in 0..num_packs {
        let pack_id = ID::from_bytes(get_array::<32>(&mut cur)?);
        let blob_count = get_u32(&mut cur)? as usize;
        if blob_count > 100_000_000 {
            return Err(MapacheError::Integrity(format!(
                "pack {pack_id} claims {blob_count} blobs, which exceeds sanity limit"
            )));
        }
        let mut blobs = Vec::with_capacity(blob_count);

        for _ in 0..blob_count {
            let id = ID::from_bytes(get_array::<32>(&mut cur)?);
            let (blob_type, compressed) = BlobType::from_byte(get_u8(&mut cur)?)?;
            let offset = get_u32(&mut cur)?;
            let length = get_u32(&mut cur)?;
            let raw_length = get_u32(&mut cur)?;

            blobs.push(IndexFileBlob {
                id,
                blob_type,
                offset,
                length,
                raw_length,
                compressed,
            });
        }

        packs.push(IndexFilePack { id: pack_id, blobs });
    }

    Ok(IndexFile { packs })
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
            compressed: true,
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

    #[tokio::test]
    async fn test_master_index_basic() {
        let mi = MasterIndex::default();
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
        let loc = mi.get(&id2).await.unwrap();
        assert_eq!(loc.pack_id, pack_id);

        mi.clear();
        assert!(!mi.contains(&id1));
        assert!(!mi.contains(&id2));
    }

    #[test]
    fn test_master_index_cleanup_and_merge() {
        let mi = MasterIndex::default();

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
                    compressed: true,
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
        let mi = MasterIndex::default();
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
        let mi = MasterIndex::default();

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

    #[test]
    fn test_master_index_merge_multiple_packs() {
        let mi = MasterIndex::default();

        let pack_a = mock_id("pack_a");
        let pack_b = mock_id("pack_b");
        let pack_c = mock_id("pack_c");

        // Index 0: pack_a {b1, b2}, pack_b {b3}
        let mut idx0 = Index::new();
        idx0.add_pack(
            &pack_a,
            vec![
                mock_blob_desc("b1", BlobType::Data, 0, 100),
                mock_blob_desc("b2", BlobType::Data, 100, 50),
            ],
        );
        idx0.add_pack(&pack_b, vec![mock_blob_desc("b3", BlobType::Tree, 0, 200)]);
        mi.add_index(idx0);

        // Index 1: pack_a {b1 (overwritten), b4}, pack_c {b5}
        let mut idx1 = Index::new();
        idx1.add_pack(
            &pack_a,
            vec![
                mock_blob_desc("b1", BlobType::Data, 0, 90), // same ID, different length → overwrites
                mock_blob_desc("b4", BlobType::Data, 200, 80),
            ],
        );
        idx1.add_pack(&pack_c, vec![mock_blob_desc("b5", BlobType::Data, 0, 60)]);
        mi.add_index(idx1);

        assert_eq!(mi.inner.read().indices.len(), 2);

        mi.cleanup(None);

        let inner = mi.inner.read();
        assert_eq!(inner.indices.len(), 1);
        let merged = &inner.indices[0];

        // 3 packs: a, b, c
        assert_eq!(merged.num_packs(), 3);
        // 5 unique blobs: b1, b2, b3, b4, b5
        assert_eq!(merged.num_blobs(), 5);

        // b1 should have the overwritten length (90, from index 1)
        let loc = merged.get(&mock_id("b1")).unwrap();
        assert_eq!(loc.length, 90);

        // All blobs present
        for name in &["b1", "b2", "b3", "b4", "b5"] {
            assert!(merged.contains(&mock_id(name)));
        }
    }

    #[test]
    fn test_index_duplicate_blobs_in_same_pack() {
        let mut index = Index::new();
        let pack_id = mock_id("pack1");
        let b1 = mock_blob_desc("dup", BlobType::Data, 0, 50);
        let b2 = mock_blob_desc("dup", BlobType::Data, 50, 50);
        // Both have the same ID but different offsets — second one overwrites in the map
        index.add_pack(&pack_id, vec![b1.clone(), b2]);

        // Should have 1 unique blob (deduplicated by ID)
        assert_eq!(index.num_blobs(), 1);
        // Should still have 1 pack
        assert_eq!(index.num_packs(), 1);
        // The second offset should be returned (last-write-wins in the map)
        let loc = index.get(&b1.id).unwrap();
        assert_eq!(loc.offset, 50);
    }

    #[test]
    fn test_index_finalized_rejects_add() {
        let mut index = Index::new();
        let pack_id = mock_id("pack1");
        let b1 = mock_blob_desc("b1", BlobType::Data, 0, 100);

        index.add_pack(&pack_id, vec![b1.clone()]);
        index.finalize();

        // After finalize, adding more packs should not increase blob count
        let pack_id2 = mock_id("pack2");
        let b2 = mock_blob_desc("b2", BlobType::Data, 0, 100);
        index.add_pack(&pack_id2, vec![b2.clone()]);

        // The index is finalized but still has the blobs (finalize doesn't clear, it just marks state)
        assert!(index.is_finalized());
    }

    #[test]
    fn test_master_index_pending_blob_priority() {
        let mi = MasterIndex::default();
        let id = mock_id("blob");

        // Add pending blob first
        assert!(mi.add_pending_blob(id));

        // Now add an index with a different pack for the same blob ID
        let pack = mock_id("pack");
        let blob_desc = PackedBlobDescriptor {
            id,
            blob_type: BlobType::Data,
            offset: 999,
            length: 100,
            raw_length: 200,
            compressed: true,
        };
        let mut idx = Index::new();
        idx.add_pack(&pack, vec![blob_desc]);
        mi.add_index(idx);

        // The pending blob should still be found
        assert!(mi.contains(&id));
    }

    #[test]
    fn test_index_many_blobs_across_packs() {
        let mut index = Index::new();
        let mut all_ids = Vec::new();

        for pack_idx in 0..10 {
            let pack_id = mock_id(&format!("pack_{pack_idx}"));
            let mut blobs = Vec::new();
            for blob_idx in 0..50 {
                let id = mock_id(&format!("blob_{pack_idx}_{blob_idx}"));
                blobs.push(PackedBlobDescriptor {
                    id,
                    blob_type: BlobType::Data,
                    offset: blob_idx * 100,
                    length: 100,
                    raw_length: 200,
                    compressed: true,
                });
                all_ids.push(id);
            }
            index.add_pack(&pack_id, blobs);
        }

        assert_eq!(index.num_blobs(), 500);
        assert_eq!(index.num_packs(), 10);

        // Every blob should be retrievable
        for id in &all_ids {
            assert!(index.contains(id));
            assert!(index.get(id).is_some());
        }
    }

    #[test]
    fn test_master_index_clear_removes_everything() {
        let mi = MasterIndex::default();

        // Add pending blobs
        let id1 = mock_id("pending1");
        let id2 = mock_id("pending2");
        mi.add_pending_blob(id1);
        mi.add_pending_blob(id2);

        // Add an index
        let mut idx = Index::new();
        let pack = mock_id("pack");
        let b = mock_blob_desc("indexed", BlobType::Data, 0, 100);
        idx.add_pack(&pack, vec![b.clone()]);
        mi.add_index(idx);

        assert!(mi.contains(&id1));
        assert!(mi.contains(&b.id));

        mi.clear();

        assert!(!mi.contains(&id1));
        assert!(!mi.contains(&id2));
        assert!(!mi.contains(&b.id));
    }

    #[test]
    fn test_index_file_serialization_many_packs() {
        let mut packs = Vec::new();
        for i in 0..20 {
            let pack_id = mock_id(&format!("pack_{i}"));
            let blobs: Vec<IndexFileBlob> = (0..10)
                .map(|j| IndexFileBlob {
                    id: mock_id(&format!("blob_{i}_{j}")),
                    blob_type: BlobType::Data,
                    offset: j * 100,
                    length: 100,
                    raw_length: 200,
                    compressed: true,
                })
                .collect();
            packs.push(IndexFilePack { id: pack_id, blobs });
        }
        let index_file = IndexFile { packs };
        let json = serde_json::to_string(&index_file).unwrap();
        let deserialized: IndexFile = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.packs.len(), 20);
        for pack in &deserialized.packs {
            assert_eq!(pack.blobs.len(), 10);
        }
    }

    // ---- Binary index format tests ----

    #[test]
    fn test_binary_index_roundtrip_empty() {
        let index_file = IndexFile { packs: vec![] };
        let serialized = serialize_index_binary(&index_file);
        let deserialized = deserialize_index_binary(&serialized).unwrap();
        assert!(deserialized.packs.is_empty());
    }

    #[test]
    fn test_binary_index_roundtrip_single_pack() {
        let blobs = vec![
            IndexFileBlob {
                id: mock_id("blob_a"),
                blob_type: BlobType::Data,
                offset: 0,
                length: 1024,
                raw_length: 2048,
                compressed: true,
            },
            IndexFileBlob {
                id: mock_id("blob_b"),
                blob_type: BlobType::Tree,
                offset: 1024,
                length: 256,
                raw_length: 512,
                compressed: true,
            },
        ];
        let index_file = IndexFile {
            packs: vec![IndexFilePack {
                id: mock_id("pack_1"),
                blobs,
            }],
        };

        let serialized = serialize_index_binary(&index_file);
        let deserialized = deserialize_index_binary(&serialized).unwrap();

        assert_eq!(deserialized.packs.len(), 1);
        assert_eq!(deserialized.packs[0].id, mock_id("pack_1"));
        assert_eq!(deserialized.packs[0].blobs.len(), 2);

        let b0 = &deserialized.packs[0].blobs[0];
        assert_eq!(b0.id, mock_id("blob_a"));
        assert_eq!(b0.blob_type, BlobType::Data);
        assert_eq!(b0.offset, 0);
        assert_eq!(b0.length, 1024);
        assert_eq!(b0.raw_length, 2048);

        let b1 = &deserialized.packs[0].blobs[1];
        assert_eq!(b1.id, mock_id("blob_b"));
        assert_eq!(b1.blob_type, BlobType::Tree);
        assert_eq!(b1.offset, 1024);
        assert_eq!(b1.length, 256);
        assert_eq!(b1.raw_length, 512);
    }

    #[test]
    fn test_binary_index_roundtrip_many_packs() {
        let mut packs = Vec::new();
        for i in 0..50 {
            let blobs: Vec<IndexFileBlob> = (0..100)
                .map(|j| IndexFileBlob {
                    id: mock_id(&format!("blob_{i}_{j}")),
                    blob_type: if j % 3 == 0 {
                        BlobType::Tree
                    } else {
                        BlobType::Data
                    },
                    offset: j * 4096,
                    length: 4096,
                    raw_length: 8192,
                    compressed: true,
                })
                .collect();
            packs.push(IndexFilePack {
                id: mock_id(&format!("pack_{i}")),
                blobs,
            });
        }
        let index_file = IndexFile { packs };

        let serialized = serialize_index_binary(&index_file);
        let deserialized = deserialize_index_binary(&serialized).unwrap();

        assert_eq!(deserialized.packs.len(), 50);
        for (i, pack) in deserialized.packs.iter().enumerate() {
            assert_eq!(pack.blobs.len(), 100);
            for (j, blob) in pack.blobs.iter().enumerate() {
                assert_eq!(blob.id, mock_id(&format!("blob_{i}_{j}")));
                assert_eq!(blob.offset, j as u32 * 4096);
                assert_eq!(blob.length, 4096);
                assert_eq!(blob.raw_length, 8192);
            }
        }
    }

    #[test]
    fn test_binary_index_size_comparison() {
        let mut packs = Vec::new();
        for i in 0..10 {
            let blobs: Vec<IndexFileBlob> = (0..1000)
                .map(|j| IndexFileBlob {
                    id: mock_id(&format!("blob_{i}_{j}")),
                    blob_type: BlobType::Data,
                    offset: j * 1024,
                    length: 1024,
                    raw_length: 2048,
                    compressed: true,
                })
                .collect();
            packs.push(IndexFilePack {
                id: mock_id(&format!("pack_{i}")),
                blobs,
            });
        }
        let index_file = IndexFile { packs };

        let json = serde_json::to_vec(&index_file).unwrap();
        let binary = serialize_index_binary(&index_file);

        // Binary should be significantly smaller than JSON
        assert!(
            binary.len() < json.len() / 3,
            "binary ({}) should be less than 1/3 of JSON ({})",
            binary.len(),
            json.len()
        );
    }

    #[test]
    fn test_binary_index_truncated_header() {
        // Empty data — not enough bytes for num_packs u32
        assert!(deserialize_index_binary(&[]).is_err());
    }

    #[test]
    fn test_binary_index_truncated() {
        let index_file = IndexFile {
            packs: vec![IndexFilePack {
                id: mock_id("pack_1"),
                blobs: vec![IndexFileBlob {
                    id: mock_id("blob_1"),
                    blob_type: BlobType::Data,
                    offset: 0,
                    length: 100,
                    raw_length: 200,
                    compressed: true,
                }],
            }],
        };
        let serialized = serialize_index_binary(&index_file);
        // Truncate the data
        assert!(deserialize_index_binary(&serialized[..serialized.len() - 1]).is_err());
    }

    #[test]
    fn test_binary_index_deterministic() {
        let index_file = IndexFile {
            packs: vec![IndexFilePack {
                id: mock_id("pack_1"),
                blobs: vec![IndexFileBlob {
                    id: mock_id("blob_1"),
                    blob_type: BlobType::Data,
                    offset: 0,
                    length: 100,
                    raw_length: 200,
                    compressed: true,
                }],
            }],
        };
        let s1 = serialize_index_binary(&index_file);
        let s2 = serialize_index_binary(&index_file);
        assert_eq!(s1, s2);
    }

    #[test]
    fn test_zero_blob_index_roundtrip() {
        let mut index = Index::new();
        let pack_id = mock_id("pack_z");
        let id1 = mock_id("zero_1");
        let id2 = mock_id("zero_2");
        index.add_pack(
            &pack_id,
            vec![
                PackedBlobDescriptor {
                    id: id1,
                    blob_type: BlobType::Zero,
                    offset: 0,
                    length: 0,
                    raw_length: 4096,
                    compressed: false,
                },
                PackedBlobDescriptor {
                    id: id2,
                    blob_type: BlobType::Zero,
                    offset: 0,
                    length: 0,
                    raw_length: 8192,
                    compressed: false,
                },
            ],
        );

        assert!(index.contains(&id1));
        assert!(index.contains(&id2));

        let loc1 = index.get(&id1).expect("zero blob 1 should be found");
        assert_eq!(loc1.blob_type, BlobType::Zero);
        assert_eq!(loc1.raw_length, 4096);
        assert_eq!(loc1.length, 0);

        let loc2 = index.get(&id2).expect("zero blob 2 should be found");
        assert_eq!(loc2.blob_type, BlobType::Zero);
        assert_eq!(loc2.raw_length, 8192);
    }

    #[test]
    fn test_zero_blob_persist_roundtrip() {
        let index_file = IndexFile {
            packs: vec![IndexFilePack {
                id: mock_id("pack_z"),
                blobs: vec![
                    IndexFileBlob {
                        id: mock_id("zero_1"),
                        blob_type: BlobType::Zero,
                        offset: 0,
                        length: 0,
                        raw_length: 100,
                        compressed: false,
                    },
                    IndexFileBlob {
                        id: mock_id("zero_2"),
                        blob_type: BlobType::Zero,
                        offset: 0,
                        length: 0,
                        raw_length: 200,
                        compressed: false,
                    },
                ],
            }],
        };

        let binary = serialize_index_binary(&index_file);
        let restored = deserialize_index_binary(&binary).unwrap();
        assert_eq!(restored.packs.len(), 1);
        assert_eq!(restored.packs[0].blobs.len(), 2);

        let idx = Index::from_index_file(restored, mock_id("test"));
        let loc = idx.get(&mock_id("zero_1")).unwrap();
        assert_eq!(loc.blob_type, BlobType::Zero);
        assert_eq!(loc.raw_length, 100);
        assert_eq!(loc.length, 0);
    }

    #[test]
    fn test_zero_blob_in_iter_ids() {
        let mut index = Index::new();
        index.add_pack(
            &mock_id("pack1"),
            vec![
                mock_blob_desc("data_1", BlobType::Data, 0, 100),
                PackedBlobDescriptor {
                    id: mock_id("zero_a"),
                    blob_type: BlobType::Zero,
                    offset: 0,
                    length: 0,
                    raw_length: 500,
                    compressed: false,
                },
            ],
        );
        index.finalize();

        let ids: Vec<ID> = index.iter_ids().map(|(id, _)| *id).collect();
        assert!(ids.contains(&mock_id("zero_a")));
        assert!(ids.contains(&mock_id("data_1")));
    }

    #[test]
    fn test_zero_blob_metadata_from_index() {
        let mut index = Index::new();
        let pack_id = mock_id("pack_meta");
        let id1 = mock_id("zero_m1");
        let id2 = mock_id("zero_m2");
        index.add_pack(
            &pack_id,
            vec![
                PackedBlobDescriptor {
                    id: id1,
                    blob_type: BlobType::Zero,
                    offset: 0,
                    length: 0,
                    raw_length: 1024,
                    compressed: false,
                },
                PackedBlobDescriptor {
                    id: id2,
                    blob_type: BlobType::Zero,
                    offset: 0,
                    length: 0,
                    raw_length: 2048,
                    compressed: false,
                },
                mock_blob_desc("data_x", BlobType::Data, 0, 512),
            ],
        );
        index.finalize();

        let meta = IndexMetadata::from_index(&index, mock_id("file_meta"));
        assert_eq!(meta.zero_blobs.len(), 2);

        let (z1_id, z1_len) = meta.zero_blobs.iter().find(|(id, _)| *id == id1).unwrap();
        assert_eq!(*z1_id, id1);
        assert_eq!(*z1_len, 1024);

        let (z2_id, z2_len) = meta.zero_blobs.iter().find(|(id, _)| *id == id2).unwrap();
        assert_eq!(*z2_id, id2);
        assert_eq!(*z2_len, 2048);
    }

    #[tokio::test]
    async fn test_zero_blob_cold_lookup() {
        let mi = MasterIndex::new(IndexMode::Lazy(common::defaults::DEFAULT_LRU_MAX_BLOBS));

        let mut index = Index::new();
        let pack_id = mock_id("pack_cold");
        let zero_id = mock_id("cold_zero");
        index.add_pack(
            &pack_id,
            vec![PackedBlobDescriptor {
                id: zero_id,
                blob_type: BlobType::Zero,
                offset: 0,
                length: 0,
                raw_length: 16384,
                compressed: false,
            }],
        );
        index.finalize();

        let file_id = mock_id("cold_file");
        let meta = IndexMetadata::from_index(&index, file_id);
        mi.add_cold_metadata(meta);

        // Zero blobs are resolved from cold metadata without loading the index,
        // so no loader is needed for this test.
        let locator = mi.get(&zero_id).await.expect("zero blob should be found");
        assert_eq!(locator.blob_type, BlobType::Zero);
        assert_eq!(locator.raw_length, 16384);
        assert_eq!(locator.length, 0);
        assert_eq!(locator.pack_id, ID::default());
    }
}
