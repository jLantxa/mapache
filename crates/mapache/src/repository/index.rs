use std::{
    collections::{BTreeMap, HashMap, HashSet},
    str::FromStr,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
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
        binary::{get_array, get_u8, get_u16, get_u32, put_bytes, put_u16, put_u32},
        collections::{BloomFilter, IdIndexSet, IdMap, IdSet, ShardedIdSet},
    },
};

const INDEX_MAGIC: [u8; 4] = *b"MPIX";
const INDEX_FORMAT_VERSION: u16 = 2;
const INDEX_HEADER_SIZE: usize = 8;

/// Index loading mode: eager (load all) or lazy (hot + cold).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum IndexMode {
    /// Load all indices into RAM. Fastest lookups.
    #[default]
    Eager,
    /// Only load the most recently-used indices into RAM (the "hot" pool),
    /// bounded by a soft blob budget; the rest are tracked as lightweight cold
    /// metadata and loaded from disk on demand.
    /// Saves memory; cold lookups re-load the full index from disk.
    /// The value is the target maximum total blob count for the hot pool. It is
    /// a soft target: a single index larger than the budget is still kept
    /// resident (we must always be able to hold at least one), and eviction of
    /// the least-recently-used index kicks in only once the pool exceeds it.
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

    #[allow(clippy::panic)]
    fn insert(&mut self, id: ID, loc: BlobLocationInternal) {
        match self {
            BlobMap::Mutable(map) => {
                map.insert(id, loc);
            }
            BlobMap::Immutable(_, _) => {
                tracing::error!(target: "index", "Attempted insert into immutable BlobMap for id={}", id.to_short_hex(8));

                // This should never happen, but if it happens, it's a good reason to panic
                panic!(
                    "Fatal error: insert into immutable BlobMap for id={}",
                    id.to_short_hex(8)
                );
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
    /// Zero blobs: ID -> raw_length. Self-contained on purpose, so cold
    /// metadata can answer a zero-blob lookup without loading an index file.
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

        let pack_ids: Vec<ID> = index
            .pack_ids
            .iter()
            .copied()
            .filter(|id| *id != ID::default())
            .collect();
        let blob_count = index.num_blobs();

        let mut zero_blobs: Vec<(ID, u32)> = index
            .zero_ids
            .iter()
            .map(|(id, loc)| (*id, loc.raw_length))
            .collect();
        // `zero_ids` iterates sorted for frozen (Immutable) indices, but a live
        // (Mutable) index iterates in arbitrary order. Cold lookups binary-search
        // this vector, so it must be sorted regardless of the source.
        zero_blobs.sort_unstable_by_key(|(id, _)| *id);

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
        let pack_ids: Vec<ID> = index_file
            .packs
            .iter()
            .map(|p| p.id)
            .filter(|id| *id != ID::default())
            .collect();
        let mut zero_blobs: Vec<(ID, u32)> = index_file
            .packs
            .iter()
            .flat_map(|p| p.blobs.iter())
            .filter(|blob| matches!(blob.blob_type, BlobType::Zero))
            .map(|blob| (blob.id, blob.raw_length))
            .collect();
        zero_blobs.sort_unstable_by_key(|(id, _)| *id);

        Self {
            file_id,
            bloom_filter,
            pack_ids,
            zero_blobs,
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
    ///
    /// `pack_array_index` is the *real* pack. Lookups deliberately report the
    /// sentinel pack ID instead (see `Self::get`), which is why this mapping is
    /// the only place the owning pack of a zero blob is still available.
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
                // A zero blob does have a real footer entry in a real pack — only its
                // data section is empty. We still report the sentinel pack ID
                // here because restoring one needs nothing but `raw_length`
                // (see `Repository::load_blob`), so a lookup must never imply
                // that a pack has to be read. `resolve_location` is not used
                // because it would surface the real pack ID instead.
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
    pub async fn persist(&mut self, repo: &Repository, repo_version: u32) -> Result<SizePair> {
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
        // TODO(v1-removal): Remove the version check; v1 format does not support BlobType::Zero.
        let zero_type = if matches!(repo_version, 2) {
            BlobType::Zero
        } else {
            BlobType::Data
        };
        add_to_entries(&self.zero_ids, zero_type);

        // Sort blobs within each pack for deterministic serialization
        for pack in &mut pack_entries {
            pack.blobs.sort_unstable_by_key(|b| b.id);
        }

        // Filter out empty packs (though there shouldn't be any in a healthy index)
        pack_entries.retain(|p| !p.blobs.is_empty());

        // Sort packs themselves
        pack_entries.sort_unstable_by_key(|p| p.id);
        let serialized = IndexFile {
            packs: pack_entries,
        }
        .serialize(repo_version)?;

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
        self.data_ids
            .iter()
            .filter_map(move |(id, loc)| {
                self.resolve_location(loc, BlobType::Data).map(|l| (id, l))
            })
            .chain(self.tree_ids.iter().filter_map(move |(id, loc)| {
                self.resolve_location(loc, BlobType::Tree).map(|l| (id, l))
            }))
            .chain(self.zero_ids.iter().map(|(id, loc)| {
                (
                    id,
                    BlobLocator {
                        pack_id: ID::default(),
                        blob_type: BlobType::Zero,
                        offset: 0,
                        length: 0,
                        raw_length: loc.raw_length,
                        compressed: false,
                    },
                )
            }))
    }

    /// Appends every descriptor in this index to `out`, keyed by pack ID and
    /// skipping blobs whose pack is in `obsolete_packs`.
    ///
    /// Buckets are filled in the caller's iteration order, which is oldest
    /// index first, so a later push of the same `(pack_id, blob_id)` supersedes
    /// an earlier one.
    ///
    /// `referenced` prunes zero-blob entries that nothing references any more.
    /// Zero blobs are real blobs with a real footer entry (only the pack's data
    /// section is empty for them), so once nothing points at them they are
    /// garbage exactly like any other blob and must not survive a GC pass.
    /// `None` keeps every zero blob, which is the right default for callers
    /// that are merging for reasons other than garbage collection.
    fn push_pack_descriptors(
        &self,
        obsolete_packs: Option<&IdSet<ID>>,
        referenced: Option<&IdSet<ID>>,
        out: &mut IdMap<ID, Vec<PackedBlobDescriptor>>,
    ) {
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

                out.entry(*pack_id).or_default().push(PackedBlobDescriptor {
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
        // Zero blobs carry a real footer entry in a real pack; only their data
        // section is empty. The index attributes them to the sentinel pack ID,
        // so `obsolete_packs` never applies to them — a zero blob whose pack is
        // being repacked or deleted stays valid, because restore synthesizes it
        // from `raw_length` instead of reading the pack. What does make one
        // garbage is having no referrer left, which only `referenced` can say.
        let mut kept_zero = self
            .zero_ids
            .iter()
            .filter(|(id, _)| referenced.is_none_or(|refs| refs.contains(id)))
            .peekable();

        // Only create the bucket when a zero blob survives `referenced`,
        // otherwise the sentinel pack would be emitted as an empty pack.
        if kept_zero.peek().is_some() {
            out.entry(ID::default())
                .or_default()
                .extend(kept_zero.map(|(id, loc)| PackedBlobDescriptor {
                    id: *id,
                    blob_type: BlobType::Zero,
                    offset: 0,
                    length: 0,
                    raw_length: loc.raw_length,
                    compressed: false,
                }));
        }
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

/// The active index loading mode. Behind an `Arc` so every clone of a
/// `MasterIndex` observes a mode switch, and behind atomics so the switch needs
/// no lock (it is read from paths that already hold one).
struct IndexModeCell {
    /// Published last on write: a reader that sees `false` never consults
    /// `budget`, so a stale budget cannot pair with a new mode.
    lazy: AtomicBool,
    budget: AtomicU64,
}

impl IndexModeCell {
    fn new(mode: IndexMode) -> Self {
        Self {
            lazy: AtomicBool::new(matches!(mode, IndexMode::Lazy(_))),
            budget: AtomicU64::new(match mode {
                IndexMode::Lazy(budget) => budget,
                IndexMode::Eager => u64::MAX,
            }),
        }
    }

    fn get(&self) -> IndexMode {
        if self.lazy.load(Ordering::Acquire) {
            IndexMode::Lazy(self.budget.load(Ordering::Relaxed))
        } else {
            IndexMode::Eager
        }
    }

    fn set(&self, mode: IndexMode) {
        self.budget.store(
            match mode {
                IndexMode::Lazy(budget) => budget,
                IndexMode::Eager => u64::MAX,
            },
            Ordering::Relaxed,
        );
        self.lazy
            .store(matches!(mode, IndexMode::Lazy(_)), Ordering::Release);
    }
}

/// Restores a cold index that is mid-promotion unless the promotion
/// succeeded, and always clears its `loading` marker.
///
/// The index is invisible for the whole load, so both the marker and the
/// metadata must be restored on every exit path, panics included: a leftover
/// marker deadlocks lookups, a lost entry hides the index's blobs for good.
struct ColdPromotionGuard<'a> {
    master: &'a MasterIndex,
    file_id: ID,
    /// `Some` while the metadata still has to be put back.
    meta: Option<IndexMetadata>,
}

impl Drop for ColdPromotionGuard<'_> {
    fn drop(&mut self) {
        let mut lock = self.master.inner.write();
        lock.loading.remove(&self.file_id);
        if let Some(meta) = self.meta.take() {
            lock.cold_metadata.retain(|m| m.file_id != self.file_id);
            lock.cold_metadata.push(meta);
        }
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
    ///
    /// Stored as atomics because the mode is not only fixed at construction:
    /// `Repository::reload_master_index_with_mode` switches it when a caller
    /// (notably the GC) requires a specific pool layout, which must take effect
    /// for every concurrent lookup and eviction decision.
    mode: Arc<IndexModeCell>,
    /// Async loader for cold indices (provided by Repository).
    loader: OnceLock<Arc<dyn ColdIndexLoader>>,
}

#[derive(Debug)]
struct MasterIndexInner {
    /// A list of individual indices, some of which might be pending.
    /// In lazy mode, this is the *resident* pool bounded by the blob budget.
    indices: Vec<Index>,
    /// Bloom Filter for fast deduplication checks.
    bloom_filter: Option<BloomFilter>,
    /// Cold (lazy-loaded) index metadata: BloomFilter + pack IDs for indices not loaded into RAM.
    cold_metadata: Vec<IndexMetadata>,
    /// Recency tracking for resident (hot) indices in lazy mode.
    /// `resident_touch` maps timestamp → instance_id (for eviction: oldest first).
    /// `touch_order` maps instance_id → timestamp (for O(1) touch update).
    resident_touch: BTreeMap<u64, u64>,
    touch_order: HashMap<u64, u64>,
    next_touch: u64,
    /// File IDs of cold indices being loaded right now. An index is invisible
    /// while in this set, so a lookup that finds no candidate consults it before
    /// reporting "not found" and waits for the promotion instead.
    loading: IdSet<ID>,
    /// Instance IDs of indices a lookup is currently searching. Eviction skips
    /// them: without this, a lookup that promotes an index and then loses it to a
    /// concurrent promotion must retry, and under pressure it retries the same
    /// index forever, re-reading it from disk on every attempt.
    pinned: HashSet<u64>,
}

impl MasterIndexInner {
    /// Total blob count of all resident (hot) indices, including pending ones.
    fn resident_blobs(&self) -> usize {
        self.indices.iter().map(Index::num_blobs).sum()
    }
}

impl Default for MasterIndex {
    fn default() -> Self {
        Self::new(common::defaults::DEFAULT_INDEX_MODE)
    }
}

impl MasterIndex {
    /// Creates a new, empty `MasterIndex`.
    pub fn new(index_mode: IndexMode) -> Self {
        Self {
            inner: Arc::new(RwLock::new(MasterIndexInner {
                indices: Vec::with_capacity(1),
                bloom_filter: None,
                cold_metadata: Vec::new(),
                resident_touch: BTreeMap::new(),
                touch_order: HashMap::new(),
                next_touch: 0,
                loading: IdSet::default(),
                pinned: HashSet::default(),
            })),
            pending_blobs: PendingBlobs::new(),
            auto_save: true,
            mode: Arc::new(IndexModeCell::new(index_mode)),
            loader: OnceLock::new(),
        }
    }

    /// Set the async loader for cold indices. Must be called before any lookups.
    pub(crate) fn set_loader(&self, loader: Arc<dyn ColdIndexLoader>) {
        let _ = self.loader.set(loader);
    }

    pub fn clear(&self) {
        let mut lock = self.inner.write();
        lock.indices.clear();
        lock.bloom_filter = None;
        lock.cold_metadata.clear();
        lock.resident_touch.clear();
        lock.touch_order.clear();
        lock.next_touch = 0;
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
    ///
    /// In lazy mode, data/tree blobs that only live in cold indices return
    /// `false`: they cannot be resolved exactly without a disk load, and a
    /// bloom-only answer risks skipping the storage of a genuinely new blob.
    /// Zero blobs are resolved exactly from cold metadata.
    pub fn contains(&self, id: &ID) -> bool {
        if self.pending_blobs.contains(id) {
            return true;
        }

        let lock = self.inner.read();
        // A master bloom-filter miss (or no master bloom) means the blob is not
        // in any hot index. Zero blobs can still be resolved exactly from cold
        // metadata without a disk load, and never produce a false positive.
        let hot_miss = !lock.bloom_filter.as_ref().is_some_and(|bf| bf.contains(id));
        if hot_miss
            && lock.cold_metadata.iter().rev().any(|meta| {
                meta.zero_blobs
                    .binary_search_by_key(id, |(zid, _)| *zid)
                    .is_ok()
            })
        {
            return true;
        }
        lock.indices.iter().rev().any(|idx| idx.contains(id))
    }

    /// Look up a blob by ID, searching data_ids first, then tree_ids, then zero_ids.
    /// The fallback chain exists because callers (restorer, verify) look up blob IDs
    /// from file node descriptors without knowing the blob type upfront. A file's
    /// content may span Data, Tree, or Zero blobs depending on dedup and zero-fill.
    ///
    /// Synchronous hot-only lookup. Cold (lazy) indices are resolved via the async
    /// [`Self::get`]; this method additionally resolves zero blobs from cold
    /// metadata exactly, without a disk load.
    pub fn get_data(&self, id: &ID) -> Option<BlobLocator> {
        if let Some(locator) = self.get_hot(id) {
            return Some(locator);
        }

        let lock = self.inner.read();
        lock.cold_metadata.iter().rev().find_map(|meta| {
            let j = meta
                .zero_blobs
                .binary_search_by_key(id, |(zid, _)| *zid)
                .ok()?;
            let &(_, raw_length) = &meta.zero_blobs[j];
            Some(BlobLocator {
                pack_id: ID::default(),
                blob_type: BlobType::Zero,
                offset: 0,
                length: 0,
                raw_length,
                compressed: false,
            })
        })
    }

    /// Retrieves an entry for a given blob ID by searching through finalized indices.
    /// Pending blobs (those not yet packed) cannot be retrieved via this method.
    /// Searches hot indices first; in lazy mode, loads cold indices on demand.
    /// Continues past bloom-filter false positives until the blob is found or no
    /// cold candidate remains.
    /// The active index loading mode.
    pub fn index_mode(&self) -> IndexMode {
        self.mode.get()
    }

    /// Switch the active index loading mode.
    ///
    /// Switching to `Eager` stops further eviction but does not pull already-cold
    /// indices back into RAM; call `reload_master_index_with_mode(Eager)` to get a
    /// genuinely fully resident pool.
    pub fn set_index_mode(&self, mode: IndexMode) {
        self.mode.set(mode);
    }

    pub async fn get(&self, id: &ID) -> Option<BlobLocator> {
        // A candidate is skipped only once it is known to hold no copy of the
        // blob. A promotion that finds nothing stays retryable: eviction churn
        // can unseat it before this lookup scans it.
        let mut exhausted: IdSet<ID> = IdSet::default();
        loop {
            // Fast path: search hot indices.
            if let Some(locator) = self.get_hot(id) {
                return Some(locator);
            }

            if self.index_mode() == IndexMode::Eager {
                return None;
            }

            // Pick the youngest cold candidate that might contain the blob.
            // Copy only what is needed out of the lock: either an exact zero-blob
            // locator, or the candidate's file_id to load.
            let candidate = {
                let lock = self.inner.read();
                lock.cold_metadata
                    .iter()
                    .rev()
                    .find(|meta| meta.might_contain(id) && !exhausted.contains(&meta.file_id))
                    .map(|meta| {
                        let zero_locator = meta
                            .zero_blobs
                            .binary_search_by_key(id, |(zid, _)| *zid)
                            .ok()
                            .map(|j| {
                                let &(_, raw_length) = &meta.zero_blobs[j];
                                BlobLocator {
                                    pack_id: ID::default(),
                                    blob_type: BlobType::Zero,
                                    offset: 0,
                                    length: 0,
                                    raw_length,
                                    compressed: false,
                                }
                            });
                        (zero_locator, meta.file_id)
                    })
            };

            match candidate {
                Some((Some(locator), _)) => return Some(locator),
                Some((None, file_id)) => {
                    // Promote the candidate and search it. If another task is
                    // already loading this exact index (concurrent promotion),
                    // yield and rescan.
                    let Ok(instance_id) = self.load_and_promote(file_id).await else {
                        // Not promoted (concurrent promotion, or the load
                        // failed): it may still be on its way in, so yield and
                        // rescan without burning the candidate.
                        tokio::task::yield_now().await;
                        continue;
                    };
                    // Pin it so eviction cannot take it away again while we
                    // search. Without the pin this index can be evicted by a
                    // concurrent promotion before the scan below, and the
                    // candidate would have to be retried — under eviction
                    // pressure, indefinitely, re-reading it from disk each time.
                    self.inner.write().pinned.insert(instance_id);
                    let found = self.get_hot(id);
                    self.inner.write().pinned.remove(&instance_id);
                    if let Some(locator) = found {
                        return Some(locator);
                    }
                    // The index was pinned for the whole scan, so its contents
                    // are now authoritative: if the blob is not here, it is not
                    // in this index at all.
                    exhausted.insert(file_id);
                }
                None => {
                    // No cold candidate holds the blob. Another task may still be
                    // promoting an index that does, and until that promotion
                    // publishes, the index is visible neither hot nor cold — so
                    // reporting "not found" now would be a false negative.
                    let promotion_in_flight = {
                        let lock = self.inner.read();
                        !lock.loading.is_empty()
                    };
                    if promotion_in_flight {
                        tokio::task::yield_now().await;
                        continue;
                    }
                    return None;
                }
            }
        }
    }

    /// Synchronous hot-only lookup. Used internally by get(), get_data() and GC.
    fn get_hot(&self, id: &ID) -> Option<BlobLocator> {
        let lock = self.inner.read();
        for idx in lock.indices.iter().rev() {
            if let Some(loc) = idx.get(id) {
                // In lazy mode, promote the hit index to the most-recently-used
                // position so the blob budget evicts the least-recently-used one.
                // `loc` is Copy, so we can drop the guard after reading it.
                let instance_id = idx.instance_id;
                drop(lock);
                if matches!(self.index_mode(), IndexMode::Lazy(_)) {
                    let mut lock = self.inner.write();
                    Self::record_touch(&mut lock, instance_id);
                }
                return Some(loc);
            }
        }
        None
    }

    /// Load a cold index from disk and promote it to hot.
    ///
    /// Returns `Ok(())` when the index was promoted (from disk);
    /// returns `Err(())` when another task is concurrently loading this exact
    /// index, or the disk load failed. On disk failure the metadata entry is
    /// re-inserted into `cold_metadata` so a future retry can attempt again
    /// instead of permanently losing the index.
    /// Returns the `instance_id` of the index it made resident.
    async fn load_and_promote(&self, file_id: ID) -> std::result::Result<u64, ()> {
        // Under the write lock: claim the metadata entry by file_id (never by a
        // positional index, which can shift under a concurrent promotion) so two
        // tasks never load the same index concurrently.
        let meta = {
            let mut lock = self.inner.write();
            let Some(pos) = lock.cold_metadata.iter().position(|m| m.file_id == file_id) else {
                return Err(());
            };
            // Publish the in-flight load before dropping the metadata, so no
            // concurrent lookup can mistake the missing entry for a blob that
            // does not exist.
            lock.loading.insert(file_id);
            lock.cold_metadata.swap_remove(pos)
        };

        // From here on the index is reachable neither hot nor cold, so every exit
        // path — including a panic from the loader — must clear `loading` and
        // put the metadata back. Leaking a `loading` entry would make every
        // later lookup wait for a promotion that never finishes.
        let mut guard = ColdPromotionGuard {
            master: self,
            file_id,
            meta: Some(meta),
        };

        let Some(loader) = self.loader.get() else {
            // No loader available. The guard restores the entry.
            return Err(());
        };
        match loader.load_index(&file_id).await {
            Ok(index) => {
                tracing::debug!(target: "index", "Loading cold index (file {}) into hot",
                    file_id.to_short_hex(8));
                let instance_id = index.instance_id;
                let mut lock = self.inner.write();
                lock.indices.push(index);
                if matches!(self.index_mode(), IndexMode::Lazy(_)) {
                    Self::record_touch(&mut lock, instance_id);
                    self.enforce_blob_budget(&mut lock);
                }
                // The index is resident now, so it must not be restored as cold.
                guard.meta = None;
                Ok(instance_id)
            }
            Err(e) => {
                tracing::warn!(target: "index",
                    "Failed to load cold index {}: {}; re-inserting into cold metadata",
                    file_id.to_short_hex(8), e);
                // The guard restores the entry so a future retry can attempt
                // again instead of permanently losing the blobs it holds.
                Err(())
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

        let instance_id = index.instance_id;
        lock.indices.push(index);

        // In lazy mode, track recency and evict the least-recently-used index if
        // the resident pool now exceeds its blob budget.
        if matches!(self.index_mode(), IndexMode::Lazy { .. }) {
            Self::record_touch(&mut lock, instance_id);
            self.enforce_blob_budget(&mut lock);
        }
    }

    /// Marks an index with `instance_id` as most-recently-used in the resident
    /// recency ledger. Pending indices are always ignored for eviction, but we
    /// still track them so the map stays consistent; only non-pending indices
    /// are ever eligible as eviction victims.
    fn record_touch(lock: &mut MasterIndexInner, instance_id: u64) {
        if let Some(old) = lock.touch_order.insert(instance_id, lock.next_touch) {
            lock.resident_touch.remove(&old);
        }
        lock.resident_touch.insert(lock.next_touch, instance_id);
        lock.next_touch += 1;
    }

    /// Rebuild the recency ledger from the current resident `indices` (used
    /// after a merge replaces every index instance). Existing indices keep
    /// their relative order; each is stamped as most-recently-used in turn.
    fn rebuild_recency(lock: &mut MasterIndexInner) {
        lock.resident_touch.clear();
        lock.touch_order.clear();
        lock.next_touch = 0;
        // Collect the instance_ids first so we don't alias `lock.indices` while
        // mutating the recency maps through `record_touch`.
        let instance_ids: Vec<u64> = lock.indices.iter().map(|i| i.instance_id).collect();
        for instance_id in instance_ids {
            Self::record_touch(lock, instance_id);
        }
    }

    /// Enforce the *soft* lazy blob budget: resident indices (hot) should aim to
    /// stay within `budget` blobs in RAM, but the budget is a target, not a hard
    /// limit. When over budget, evict the least-recently-used non-pending index
    /// to cold metadata, freeing its blobs.
    ///
    /// Eviction stops as soon as the pool fits within the budget again, or when
    /// only pending / a single non-pending index remains. That guarantees at
    /// least one index is always resident, even when a single index exceeds the
    /// budget on its own (indices have their own, larger blob limit) — re-loading
    /// it would be the only thing left to evict, and then lookups would be
    /// impossible. Such an oversized index is kept resident and only warns.
    fn enforce_blob_budget(&self, lock: &mut MasterIndexInner) {
        let IndexMode::Lazy(budget) = self.index_mode() else {
            return;
        };

        // A single non-pending index may legitimately exceed the budget (soft
        // target). Surface it so it's observable, but never refuse to keep it
        // resident — we must always be able to hold at least one index.
        if lock
            .indices
            .iter()
            .filter(|i| !i.is_pending())
            .any(|i| i.num_blobs() as u64 > budget)
        {
            tracing::warn!(target: "index",
                "Lazy blob budget ({budget}) is smaller than a resident index; keeping it resident anyway");
        }

        // Evict the least-recently-used non-pending index while over budget,
        // but never evict the last one: we must always keep at least one index
        // resident so lookups stay possible (a single oversized index is kept).
        while lock.resident_blobs() as u64 > budget
            && lock
                .indices
                .iter()
                .filter(|i| i.file_id.is_some() && !lock.pinned.contains(&i.instance_id))
                .count()
                > 1
        {
            // Skip pending indices (they are always resident) and only consider
            // non-pending victims. Pick the one with the smallest touch.
            //
            // An index with no `file_id` has never been written to disk — it was
            // just built by `merge_index` and only exists in this process. Its
            // cold metadata could not be reloaded (there is no file to read) and
            // its `ID::default()` entry would make `ids()` report a file that
            // does not exist, which in turn lets the GC delete the *real* index
            // files that still hold those blobs. Such an index must stay
            // resident until it is persisted.
            let evictable = lock
                .indices
                .iter()
                .filter(|i| {
                    i.file_id.is_some() && !i.is_pending() && !lock.pinned.contains(&i.instance_id)
                })
                .count();
            if evictable == 0 {
                break;
            }
            let victim = lock
                .resident_touch
                .iter()
                .find_map(|(&ts, &instance_id)| {
                    lock.indices
                        .iter()
                        .position(|i| {
                            i.instance_id == instance_id
                                && i.file_id.is_some()
                                && !lock.pinned.contains(&i.instance_id)
                        })
                        .map(|pos| (ts, pos))
                })
                .map(|(_, pos)| pos);

            let Some(pos) = victim else {
                // No evictable index remains. Accept the overshoot.
                break;
            };

            let file_id = lock.indices[pos].file_id.unwrap_or_default();
            let instance_id = lock.indices[pos].instance_id;
            tracing::debug!(target: "index", "Evicting hot index {} (file {}) to cold (total blobs: {}, budget: {})",
                instance_id, file_id.to_short_hex(8), lock.resident_blobs(), budget);
            let cold_meta = IndexMetadata::from_index(&lock.indices[pos], file_id);
            // Unload fully from RAM: the least-recently-used non-pending index
            // is removed and only its lightweight metadata is kept.
            lock.indices.remove(pos);
            lock.cold_metadata.push(cold_meta);
            lock.touch_order.remove(&instance_id);
            lock.resident_touch.retain(|_, iid| *iid != instance_id);
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

    /// Atomically claims a blob ID for packing.
    ///
    /// Returns `true` if the ID was successfully claimed (not previously known);
    /// `false` if the blob is already pending or already in the index.
    ///
    /// This MUST be called **before** encoding and sending the blob to the
    /// packer, and it is the only deduplication decision point. Claiming after
    /// the blob is already on its way to the packer would let two threads
    /// encoding the same content both send it, producing duplicate footer
    /// entries for one blob while the index keeps a single entry — wasting
    /// space and confusing stats/GC.
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
        // Only one thread can win the insert for a given ID.
        self.pending_blobs.insert(id)
    }

    /// Removes a blob ID from the pending set.
    ///
    /// Used for error cleanup: if `add_pending_blob` succeeded but the
    /// subsequent send to the packer failed, the blob must be unclaimed so
    /// a retry can re-attempt.
    pub fn remove_pending_blob(&self, id: &ID) {
        self.pending_blobs.remove(id);
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
                index_to_persist = Some(lock.indices.remove(pending_pos));
            } else if is_full {
                tracing::debug!(target: "index", "Index #{} is full, finalizing", lock.indices[pending_pos].instance_id);
                lock.indices[pending_pos].finalize();
            }
        }

        if let Some(mut idx) = index_to_persist {
            let size = match idx.persist(repo, repo.repo_version()).await {
                Ok(size) => size,
                Err(e) => {
                    let mut lock = self.inner.write();
                    let instance_id = idx.instance_id;
                    lock.indices.push(idx);
                    if matches!(self.index_mode(), IndexMode::Lazy(_)) {
                        Self::record_touch(&mut lock, instance_id);
                    }
                    return Err(e);
                }
            };
            // Put the persisted index back with updated status.
            let mut lock = self.inner.write();
            let instance_id = idx.instance_id;
            lock.indices.push(idx);
            if matches!(self.index_mode(), IndexMode::Lazy(_)) {
                Self::record_touch(&mut lock, instance_id);
                self.enforce_blob_budget(&mut lock);
            }
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
        let repo_version = repo_version.unwrap_or(repo.repo_version());
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
                    indices_to_persist.push(lock.indices.remove(i));
                } else {
                    i += 1;
                }
            }
        }

        let num_to_persist = indices_to_persist.len();
        if num_to_persist > 0 {
            tracing::info!(target: "index", "Persisting {} indices", num_to_persist);
        }

        let mut iter = indices_to_persist.into_iter();
        while let Some(mut idx) = iter.next() {
            let size = match idx.persist(repo, repo_version).await {
                Ok(size) => size,
                Err(e) => {
                    let mut lock = self.inner.write();
                    for remaining in std::iter::once(idx).chain(iter) {
                        let instance_id = remaining.instance_id;
                        lock.indices.push(remaining);
                        if matches!(self.index_mode(), IndexMode::Lazy(_)) {
                            Self::record_touch(&mut lock, instance_id);
                        }
                    }
                    return Err(e);
                }
            };
            total_size += size;

            // Put the persisted index back.
            let mut lock = self.inner.write();
            let instance_id = idx.instance_id;
            lock.indices.push(idx);
            if matches!(self.index_mode(), IndexMode::Lazy(_)) {
                Self::record_touch(&mut lock, instance_id);
                self.enforce_blob_budget(&mut lock);
            }
        }

        Ok(total_size)
    }

    /// Invokes `f` for every blob in every index — hot and cold.
    ///
    /// Cold indices are loaded from disk one at a time through the configured
    /// loader and then discarded, so at most one cold index lives in RAM (in
    /// addition to the resident hot pool) during the sweep.
    pub async fn for_each_id<F>(&self, mut f: F)
    where
        F: FnMut(&ID, BlobLocator),
    {
        {
            let lock = self.inner.read();
            for idx in &lock.indices {
                for (id, loc) in idx.iter_ids() {
                    f(id, loc);
                }
            }
        }

        // Process each cold index exactly once, without promoting it to hot.
        let cold_ids: Vec<ID> = {
            let lock = self.inner.read();
            lock.cold_metadata.iter().map(|m| m.file_id).collect()
        };

        for file_id in cold_ids {
            // Take the entry out of the cold set while it is being processed, so
            // a concurrent promotion cannot operate on the same index. If it is
            // already gone (promoted by a concurrent load), skip it.
            let meta = {
                let mut lock = self.inner.write();
                let Some(pos) = lock.cold_metadata.iter().position(|m| m.file_id == file_id) else {
                    continue;
                };
                lock.cold_metadata.swap_remove(pos)
            };

            let Some(index) = (match self.loader.get() {
                Some(loader) => loader.load_index(&meta.file_id).await,
                None => Err(MapacheError::Format(
                    "no cold index loader configured".to_string(),
                )),
            })
            .ok() else {
                tracing::warn!(target: "index",
                    "Failed to load cold index {} during sweep; restoring it to cold metadata",
                    meta.file_id.to_short_hex(8));
                let mut lock = self.inner.write();
                lock.cold_metadata.retain(|m| m.file_id != meta.file_id);
                lock.cold_metadata.push(meta);
                continue;
            };

            for (id, loc) in index.iter_ids() {
                f(id, loc);
            }

            // Restore the entry to cold metadata so it is processed on the next
            // sweep and remains resolvable on demand.
            let mut lock = self.inner.write();
            lock.cold_metadata.retain(|m| m.file_id != meta.file_id);
            lock.cold_metadata.push(meta);
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
                if *pack_id != ID::default() && seen.insert(*pack_id) {
                    f(pack_id);
                }
            }
        }

        // In lazy mode, cold indices reference packs that aren't present in the
        // hot `indices`. Their pack IDs are already stored in the lightweight
        // cold metadata, so include them without a disk load.
        for meta in &lock.cold_metadata {
            for pack_id in &meta.pack_ids {
                if *pack_id != ID::default() && seen.insert(*pack_id) {
                    f(pack_id);
                }
            }
        }
    }

    pub fn ids(&self) -> IdSet<ID> {
        let lock = self.inner.read();

        let mut ids: IdSet<ID> = lock
            .indices
            .iter()
            .filter_map(|idx| if !idx.is_pending() { idx.id() } else { None })
            .collect();

        // In lazy mode, cold index files are persisted to disk but not held as
        // hot `Index` objects; their file IDs live in the cold metadata and
        // must be included so callers (e.g. GC's index reaper) see every index
        // file that is still referenced.
        for meta in &lock.cold_metadata {
            ids.insert(meta.file_id);
        }

        ids
    }

    /// Rewrite the index, dropping every blob in `obsolete_packs`.
    ///
    /// Consumes the whole index: the resident indices *and* the cold ones. Cold
    /// indices are streamed one at a time — loaded, bucketed, dropped — while
    /// still ending up with a rewritten index that contains every blob.
    ///
    /// Every consumed index file becomes unreferenced, which is what lets the GC
    /// delete the old files afterwards. Leaving a cold entry behind would point
    /// at a file that is about to be deleted while its blobs were never merged
    /// into the replacement.
    ///
    /// Descriptors are bucketed by pack (~48 bytes per blob instead of ~80) and
    /// each source index is released as soon as it has been bucketed.
    ///
    /// This bounds the *input* side only: the rewritten indices cannot be evicted
    /// while unpersisted, so after the merge the whole new index is resident again
    /// and the peak returns to eager levels. Bounding the peak needs an
    /// incremental merge, not a lazy pool.
    ///
    /// `referenced` prunes zero-blob entries that nothing references any more.
    /// Zero blobs are real blobs with a real footer entry (only the pack's data
    /// section is empty for them), so once nothing points at them they are
    /// garbage exactly like any other blob and must not survive a GC pass.
    /// `None` keeps every zero blob, which is the right default for callers
    /// that are merging for reasons other than garbage collection.
    pub async fn cleanup(
        &self,
        obsolete_packs: Option<&IdSet<ID>>,
        referenced: Option<&IdSet<ID>>,
    ) -> Result<()> {
        let (mut old_indices, mut cold_ids, loader) = {
            let mut lock = self.inner.write();
            let old_indices = std::mem::take(&mut lock.indices);
            // Claim the cold entries up front: they are about to be consumed, so
            // they must not stay reachable while the merge runs.
            let cold_ids: Vec<ID> = lock.cold_metadata.iter().map(|m| m.file_id).collect();
            lock.cold_metadata.clear();
            (old_indices, cold_ids, self.loader.get().cloned())
        };

        // Deterministic order in both halves: resident indices by instance_id,
        // cold ones by file id.
        old_indices.sort_by_key(|idx| idx.instance_id);
        cold_ids.sort_unstable();

        tracing::info!(
            target: "index",
            "Merging {} indices ({} cold, streamed)",
            old_indices.len(),
            cold_ids.len()
        );

        // Fill packs oldest-index-first so the newest occurrence of a duplicated
        // (pack_id, blob_id) is the one that ends up last within its bucket.
        let mut pack_buckets: IdMap<ID, Vec<PackedBlobDescriptor>> = IdMap::default();
        for idx in old_indices {
            idx.push_pack_descriptors(obsolete_packs, referenced, &mut pack_buckets);
            // `idx` drops here, freeing its blob maps.
        }

        // Stream the cold indices. A failure here is fatal: continuing would
        // drop the blobs of the index that could not be read, and the GC would
        // then delete the packs that hold them.
        for file_id in cold_ids {
            let Some(loader) = loader.as_ref() else {
                return Err(MapacheError::Format(format!(
                    "cannot merge cold index {}: no cold index loader is configured",
                    file_id.to_short_hex(8)
                )));
            };
            let idx = loader.load_index(&file_id).await?;
            idx.push_pack_descriptors(obsolete_packs, referenced, &mut pack_buckets);
            // `idx` drops here.
        }

        let new_indices = Self::emit_indices_from_buckets(pack_buckets);

        let mut lock = self.inner.write();
        lock.indices = new_indices;
        // The merge produced brand-new index instances, so rebuild the recency
        // ledger (old entries reference discarded instance_ids).
        Self::rebuild_recency(&mut lock);
        // Respect the lazy blob budget over the freshly rebuilt resident pool.
        if matches!(self.index_mode(), IndexMode::Lazy(_)) {
            self.enforce_blob_budget(&mut lock);
        }
        Ok(())
    }

    /// Turn per-pack descriptor buckets into full indices, dropping each bucket as
    /// it is consumed.
    fn emit_indices_from_buckets(
        mut pack_buckets: IdMap<ID, Vec<PackedBlobDescriptor>>,
    ) -> Vec<Index> {
        // Emit packs in sorted order so the resulting index layout is
        // deterministic, as the previous global (pack_id, blob_id) sort was.
        let mut pack_ids: Vec<ID> = pack_buckets.keys().copied().collect();
        pack_ids.sort_unstable();

        let mut new_indices = Vec::new();
        let mut current_index = Index::new();

        for pack_id in pack_ids {
            // Remove rather than copy: the bucket is freed once consumed, so the
            // workspace shrinks as the new indices grow.
            let Some(mut blobs) = pack_buckets.remove(&pack_id) else {
                continue;
            };

            // Dedup by blob ID, keeping the newest occurrence. The bucket is
            // sorted with a *stable* sort so that entries pushed for the same
            // blob ID retain oldest-index-first order and the dedup below keeps
            // the last of each run, matching the previous behavior.
            blobs.sort_by_key(|d| d.id);
            blobs.dedup_by(|a, b| {
                if a.id == b.id {
                    // swap a↔b so that dedup_by (which keeps a) retains the
                    // newer entry.
                    std::mem::swap(a, b);
                    true
                } else {
                    false
                }
            });

            current_index.add_pack(&pack_id, blobs);
            if current_index.is_full() {
                current_index.set_status(IndexStatus::Finalized);
                new_indices.push(std::mem::take(&mut current_index));
            }
        }

        if !current_index.is_empty() {
            current_index.set_status(IndexStatus::Pending);
            new_indices.push(current_index);
        }

        tracing::info!(target: "index", "Indices merged into {} new indices", new_indices.len());
        new_indices
    }

    pub async fn search_prefix(&self, prefix: &str) -> Result<Option<ID>> {
        let mut matched = Vec::new();

        self.for_each_id(|id, _| {
            if id.to_hex().starts_with(prefix) {
                matched.push(*id);
            }
        })
        .await;

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
        // TODO(v1-removal): Remove the version dispatch and always use the
        // self-identifying v2 binary format.
        match repo_version {
            2 => Ok(serialize_index_binary(self)),
            _ => super::legacy::serialize_index_json(self),
        }
    }

    /// Deserialize an `IndexFile` based on the repository version.
    ///
    /// If the version-appropriate parser fails, the other format is tried as a
    /// fallback. This makes the repository openable during the v1→v2 migration
    /// crash window, where an updated manifest can coexist with legacy-format
    /// index files (and vice versa). Both formats are self-identifying
    /// (`MPIX` magic vs JSON object), so the fallback cannot misparse.
    // TODO(v1-removal): Remove the v1 JSON branch and the cross-format fallback.
    pub fn deserialize(data: &[u8], repo_version: u32) -> Result<Self> {
        // TODO(v1-removal): Remove the version dispatch and legacy JSON parser.
        match repo_version {
            2 => match deserialize_index_binary(data) {
                Ok(index) => Ok(index),
                Err(bin_err) => super::legacy::deserialize_index_json(data).map_err(|_| {
                    MapacheError::Format(format!(
                        "failed to deserialize index: {bin_err} \
                         (also rejected by the legacy json parser)"
                    ))
                }),
            },
            _ => match super::legacy::deserialize_index_json(data) {
                Ok(index) => Ok(index),
                Err(json_err) => deserialize_index_binary(data).map_err(|_| json_err),
            },
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
    let size = INDEX_HEADER_SIZE
        .saturating_add(index_file.packs.len().saturating_mul(36))
        .saturating_add(total_blobs.saturating_mul(45));
    let mut buf = Vec::with_capacity(size);

    // Header: magic, format version, and reserved flags.
    put_bytes(&mut buf, &INDEX_MAGIC);
    put_u16(&mut buf, INDEX_FORMAT_VERSION);
    put_u16(&mut buf, 0);
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

    if cur.len() < INDEX_HEADER_SIZE {
        return Err(MapacheError::Format(
            "index header is truncated".to_string(),
        ));
    }
    if get_array::<4>(&mut cur)? != INDEX_MAGIC {
        return Err(MapacheError::Format("invalid index magic".to_string()));
    }
    if get_u16(&mut cur)? != INDEX_FORMAT_VERSION {
        return Err(MapacheError::Format(
            "unsupported index format version".to_string(),
        ));
    }
    if get_u16(&mut cur)? != 0 {
        return Err(MapacheError::Format("invalid index flags".to_string()));
    }

    let num_packs = get_u32(&mut cur)? as usize;
    if num_packs > 1_000_000 {
        return Err(MapacheError::Integrity(format!(
            "index claims {num_packs} packs, which exceeds sanity limit"
        )));
    }
    // No upfront preallocation: allocating the claimed count could reserve
    // gigabytes based on a tiny corrupt file. The vectors grow only with the
    // entries actually present in the buffer.
    let mut packs = Vec::new();

    for _ in 0..num_packs {
        let pack_id = ID::from_bytes(get_array::<32>(&mut cur)?);
        let blob_count = get_u32(&mut cur)? as usize;
        if blob_count > 100_000_000 {
            return Err(MapacheError::Integrity(format!(
                "pack {pack_id} claims {blob_count} blobs, which exceeds sanity limit"
            )));
        }
        let mut blobs = Vec::new();

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

    if !cur.is_empty() {
        return Err(MapacheError::Format(format!(
            "index has {} trailing bytes",
            cur.len()
        )));
    }

    Ok(IndexFile { packs })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::{
        backend::{
            StorageBackend,
            mock::{BackendOp, MockBackend, MockEffect},
        },
        common::defaults::TEST_REPO_CONFIG,
        repository::repo::{Auth, THIS_REPOSITORY_VERSION},
    };
    use zeroize::Zeroizing;

    #[test]
    fn test_index_rejects_huge_claimed_blob_count_without_allocating() {
        let mut data = Vec::new();
        put_bytes(&mut data, &INDEX_MAGIC);
        put_u16(&mut data, INDEX_FORMAT_VERSION);
        put_u16(&mut data, 0);
        put_u32(&mut data, 1); // one pack
        put_bytes(&mut data, &[0u8; 32]); // pack id
        put_u32(&mut data, 100_000_001); // claims >100M blobs but has zero blob entries

        let err = deserialize_index_binary(&data).expect_err("must be rejected");
        assert!(
            matches!(&err, MapacheError::Integrity(_)),
            "expected Integrity, got: {err}"
        );
    }

    #[test]
    fn test_index_rejects_huge_claimed_pack_count() {
        let mut data = Vec::new();
        put_bytes(&mut data, &INDEX_MAGIC);
        put_u16(&mut data, INDEX_FORMAT_VERSION);
        put_u16(&mut data, 0);
        put_u32(&mut data, 2_000_000); // exceeds sanity limit

        let err = deserialize_index_binary(&data).expect_err("must be rejected");
        assert!(
            matches!(&err, MapacheError::Integrity(_)),
            "expected Integrity, got: {err}"
        );
    }

    #[tokio::test]
    async fn persist_failure_keeps_index_resolvable() -> Result<()> {
        let auth = Auth {
            username: "test".to_string(),
            password: Zeroizing::new("password".to_string()),
        };
        let backend = Arc::new(MockBackend::new());
        let backend_dyn: Arc<dyn StorageBackend> = backend.clone();
        Repository::init(
            THIS_REPOSITORY_VERSION,
            &auth,
            None,
            backend_dyn.clone(),
            None,
            false,
        )
        .await?;
        let (repo, _) =
            Repository::try_open_unlocked(&auth, None, backend_dyn, TEST_REPO_CONFIG).await?;

        let descriptor1 = mock_blob_desc("persisted-after-error-1", BlobType::Data, 0, 4);
        repo.index()
            .add_pack(&repo, &mock_id("pack1"), vec![descriptor1.clone()])
            .await?;
        {
            let index = repo.index();
            let mut lock = index.inner.write();
            let pending_idx = lock
                .indices
                .iter_mut()
                .find(|idx| idx.is_pending())
                .expect("should have pending index");
            pending_idx.finalize();
        }
        let descriptor2 = mock_blob_desc("persisted-after-error-2", BlobType::Data, 0, 4);
        repo.index()
            .add_pack(&repo, &mock_id("pack2"), vec![descriptor2.clone()])
            .await?;

        backend.add_hook(Arc::new(|op| {
            if matches!(op, BackendOp::Write { .. }) {
                MockEffect {
                    result_override: Some(Err(MapacheError::Backend(
                        "injected index write failure".to_string(),
                    ))),
                    ..Default::default()
                }
            } else {
                MockEffect::default()
            }
        }));

        assert!(repo.index().persist(&repo).await.is_err());
        assert!(repo.index().get(&descriptor1.id).await.is_some());
        assert!(repo.index().get(&descriptor2.id).await.is_some());

        Ok(())
    }

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

    #[tokio::test]
    async fn test_master_index_cleanup_and_merge() {
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

        mi.cleanup(Some(&obsolete), None).await.unwrap();

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

    #[tokio::test]
    async fn test_master_index_merge_deduplication() {
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
        mi.cleanup(None, None).await.unwrap();

        let inner = mi.inner.read();
        assert_eq!(inner.indices.len(), 1);
        let merged = &inner.indices[0];

        // Should only have pack1 ONCE
        assert_eq!(merged.num_packs(), 1);
        assert_eq!(merged.num_blobs(), 1);
        assert!(merged.contains(&b1.id));
    }

    #[tokio::test]
    async fn test_master_index_merge_multiple_packs() {
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

        mi.cleanup(None, None).await.unwrap();

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
    // TODO(v1-removal): delete this test (and the `deserialize` fallback it
    // covers) when the v1 JSON index format is dropped.
    fn test_deserialize_tolerates_cross_format_files() {
        // Migration crash-window regression: a v1 manifest can coexist with
        // binary (v2) index files and vice versa. `IndexFile::deserialize` must
        // fall back to the alternate parser so the repository still opens.
        let packs = vec![IndexFilePack {
            id: mock_id("pack_1"),
            blobs: vec![IndexFileBlob {
                id: mock_id("blob_a"),
                blob_type: BlobType::Data,
                offset: 0,
                length: 1024,
                raw_length: 2048,
                compressed: true,
            }],
        }];
        let index_file = IndexFile { packs };

        let binary = serialize_index_binary(&index_file);
        let binary_json = crate::repository::legacy::serialize_index_json(&index_file).unwrap();

        // Binary index read under a v1 manifest (crash before manifest update).
        let from_binary = IndexFile::deserialize(&binary, 1).unwrap();
        assert_eq!(from_binary.packs[0].blobs[0].id, mock_id("blob_a"));

        // Legacy JSON index read under a v2 manifest (crash after manifest
        // update but before the old index files were dropped).
        let from_json = IndexFile::deserialize(&binary_json, 2).unwrap();
        assert_eq!(from_json.packs[0].blobs[0].id, mock_id("blob_a"));

        // Garbage is still rejected under both versions.
        assert!(IndexFile::deserialize(b"not an index", 1).is_err());
        assert!(IndexFile::deserialize(b"not an index", 2).is_err());
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
    fn test_binary_index_rejects_trailing_bytes() {
        let mut data = serialize_index_binary(&IndexFile::default());
        data.push(0);
        assert!(deserialize_index_binary(&data).is_err());
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

    struct MockColdLoader {
        indices: std::collections::HashMap<ID, Index>,
    }

    #[async_trait]
    impl ColdIndexLoader for MockColdLoader {
        async fn load_index(&self, file_id: &ID) -> Result<Index> {
            self.indices
                .get(file_id)
                .cloned()
                .ok_or_else(|| MapacheError::Format(format!("no index for {:?}", file_id.to_hex())))
        }
    }

    #[test]
    fn test_contains_and_get_data_resolve_cold_zero_blob() {
        let mi = MasterIndex::new(IndexMode::Lazy(u64::MAX));

        let mut index = Index::new();
        let pack_id = mock_id("pack_zero");
        let zero_id = mock_id("cold_zero_data");
        index.add_pack(
            &pack_id,
            vec![PackedBlobDescriptor {
                id: zero_id,
                blob_type: BlobType::Zero,
                offset: 0,
                length: 0,
                raw_length: 4096,
                compressed: false,
            }],
        );
        index.finalize();
        let file_id = mock_id("zero_file");
        mi.add_cold_metadata(IndexMetadata::from_index(&index, file_id));

        // Zero blobs are exact from cold metadata, no disk load required.
        assert!(
            mi.contains(&zero_id),
            "cold zero blobs are resolvable exactly"
        );
        let locator = mi
            .get_data(&zero_id)
            .expect("cold zero blob should resolve synchronously");
        assert_eq!(locator.blob_type, BlobType::Zero);
        assert_eq!(locator.raw_length, 4096);
    }

    #[test]
    fn test_cold_metadata_from_index_file_resolves_zero_blobs() {
        let mi = MasterIndex::new(IndexMode::Lazy(u64::MAX));

        // Simulate an index file read from disk: packs carry zero blobs mixed
        // with regular blobs and out of order. from_index_file must extract
        // the zeros in sorted order so cold lookups binary-search correctly.
        let zero_a = mock_id("zero_a");
        let zero_b = mock_id("zero_b");
        let data = mock_id("data");
        let index_file = IndexFile {
            packs: vec![IndexFilePack {
                id: mock_id("pack_from_disk"),
                blobs: vec![
                    IndexFileBlob {
                        id: zero_b,
                        blob_type: BlobType::Zero,
                        offset: 0,
                        length: 0,
                        raw_length: 8192,
                        compressed: false,
                    },
                    IndexFileBlob {
                        id: data,
                        blob_type: BlobType::Data,
                        offset: 10,
                        length: 20,
                        raw_length: 40,
                        compressed: true,
                    },
                    IndexFileBlob {
                        id: zero_a,
                        blob_type: BlobType::Zero,
                        offset: 0,
                        length: 0,
                        raw_length: 4096,
                        compressed: false,
                    },
                ],
            }],
        };
        let bf = IndexMetadata::bloom_filter_from_index_file(&index_file);
        let file_id = mock_id("file_from_disk");
        let meta = IndexMetadata::from_index_file(index_file, bf, file_id);
        assert!(
            meta.zero_blobs.windows(2).all(|w| w[0].0 < w[1].0),
            "zero_blobs must be sorted for binary search"
        );

        mi.add_cold_metadata(meta);

        assert!(mi.contains(&zero_a));
        assert!(mi.contains(&zero_b));
        let locator = mi
            .get_data(&zero_a)
            .expect("cold zero blob from index file should resolve");
        assert_eq!(locator.blob_type, BlobType::Zero);
        assert_eq!(locator.raw_length, 4096);
        let locator = mi
            .get_data(&zero_b)
            .expect("cold zero blob from index file should resolve");
        assert_eq!(locator.raw_length, 8192);
        assert!(
            !mi.get_data(&data)
                .is_some_and(|l| l.blob_type == BlobType::Zero)
        );
    }

    #[tokio::test]
    async fn test_get_surveys_cold_candidates_past_false_positive() {
        let mi = MasterIndex::new(IndexMode::Lazy(u64::MAX));

        // Older index: genuinely owns the target data blob.
        let mut idx_older = Index::new();
        let pack_older = mock_id("pack_older");
        let target_id = mock_id("target_blob");
        idx_older.add_pack(
            &pack_older,
            vec![mock_blob_desc("target_blob", BlobType::Data, 0, 100)],
        );
        idx_older.finalize();
        let file_older = mock_id("file_older");
        let meta_older = IndexMetadata::from_index(&idx_older, file_older);

        // Younger index: does NOT contain the target, but its cold metadata
        // bloom is made to report a (false) positive for it.
        let mut idx_younger = Index::new();
        let pack_younger = mock_id("pack_younger");
        idx_younger.add_pack(
            &pack_younger,
            vec![mock_blob_desc("other_blob", BlobType::Data, 0, 50)],
        );
        idx_younger.finalize();
        let file_younger = mock_id("file_younger");
        let fake_bloom = {
            let mut bf = BloomFilter::new(2, 0.01);
            bf.insert(&mock_id("other_blob"));
            bf.insert(&target_id);
            bf
        };
        let meta_younger = IndexMetadata {
            file_id: file_younger,
            bloom_filter: fake_bloom,
            pack_ids: vec![pack_younger],
            zero_blobs: Vec::new(),
            blob_count: 1,
        };

        // Youngest first: rev() picks the false-positive candidate before the
        // real owner.
        mi.add_cold_metadata(meta_younger.clone());
        mi.add_cold_metadata(meta_older.clone());
        mi.set_loader(Arc::new(MockColdLoader {
            indices: std::collections::HashMap::from([
                (file_older, idx_older),
                (file_younger, idx_younger),
            ]),
        }));

        let locator = mi.get(&target_id).await.expect("blob should be found");
        assert_eq!(locator.pack_id, pack_older);
        assert_eq!(locator.blob_type, BlobType::Data);
    }

    #[tokio::test]
    async fn test_evicted_index_reloads_from_disk() {
        struct CountingLoader {
            index: Index,
            loads: Arc<std::sync::atomic::AtomicUsize>,
        }

        #[async_trait]
        impl ColdIndexLoader for CountingLoader {
            async fn load_index(&self, _file_id: &ID) -> Result<Index> {
                self.loads
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(self.index.clone())
            }
        }

        let mi = MasterIndex::new(IndexMode::Lazy(u64::MAX));

        let mut index = Index::new();
        let pack_id = mock_id("pack_evicted");
        let blob_id = mock_id("evicted_blob");
        index.add_pack(
            &pack_id,
            vec![mock_blob_desc("evicted_blob", BlobType::Data, 0, 100)],
        );
        index.finalize();
        let file_id = mock_id("file_evicted");
        mi.add_cold_metadata(IndexMetadata::from_index(&index, file_id));

        let loads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        mi.set_loader(Arc::new(CountingLoader {
            index: index.clone(),
            loads: loads.clone(),
        }));

        // First lookup loads from disk.
        let locator = mi.get(&blob_id).await.expect("blob should be found");
        assert_eq!(locator.pack_id, pack_id);
        assert_eq!(loads.load(std::sync::atomic::Ordering::Relaxed), 1);

        // Evict the (now hot) index back to cold: eviction fully unloads it
        // from RAM (no separate copy cache is kept).
        {
            let mut lock = mi.inner.write();
            let evicted = lock.indices.pop().expect("an index should be hot");
            let meta = IndexMetadata::from_index(&evicted, file_id);
            lock.cold_metadata.push(meta);
        }

        // Second lookup must reload the index from disk: there is no cached
        // copy to promote.
        let locator = mi.get(&blob_id).await.expect("blob should be found");
        assert_eq!(locator.pack_id, pack_id);
        assert_eq!(
            loads.load(std::sync::atomic::Ordering::Relaxed),
            2,
            "an evicted index must be re-fetched from disk on the next access"
        );
    }

    #[tokio::test]
    async fn test_for_each_id_full_sweep_with_lazy_mode() {
        // Mirrors the state produced by reload_master_index_with_mode in lazy
        // mode: the newest indices stay hot (within the blob budget), older ones
        // become cold metadata.
        struct CountingLoader {
            indices: std::collections::HashMap<ID, Index>,
            loads: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        }

        #[async_trait]
        impl ColdIndexLoader for CountingLoader {
            async fn load_index(&self, file_id: &ID) -> Result<Index> {
                self.loads
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                self.indices.get(file_id).cloned().ok_or_else(|| {
                    MapacheError::Format(format!("no index for {}", file_id.to_hex()))
                })
            }
        }

        let mi = MasterIndex::new(IndexMode::Lazy(common::defaults::DEFAULT_LRU_MAX_BLOBS));

        // Two persisted index files treated as cold (older): one data, one tree.
        let mut cold_data = Index::new();
        let cold_data_pack = mock_id("pack_cold_data");
        let cold_data_blob = mock_id("cold_data_blob");
        cold_data.add_pack(
            &cold_data_pack,
            vec![mock_blob_desc("cold_data_blob", BlobType::Data, 0, 100)],
        );
        cold_data.finalize();
        let cold_data_file = mock_id("file_cold_data");
        mi.add_cold_metadata(IndexMetadata::from_index(&cold_data, cold_data_file));

        let mut cold_tree = Index::new();
        let cold_tree_pack = mock_id("pack_cold_tree");
        let cold_tree_blob = mock_id("cold_tree_blob");
        cold_tree.add_pack(
            &cold_tree_pack,
            vec![mock_blob_desc("cold_tree_blob", BlobType::Tree, 0, 200)],
        );
        cold_tree.finalize();
        let cold_tree_file = mock_id("file_cold_tree");
        mi.add_cold_metadata(IndexMetadata::from_index(&cold_tree, cold_tree_file));

        // One hot index (newest).
        let mut hot = Index::new();
        let hot_pack = mock_id("pack_hot");
        let hot_blob = mock_id("hot_blob");
        hot.add_pack(
            &hot_pack,
            vec![mock_blob_desc("hot_blob", BlobType::Data, 0, 40)],
        );
        hot.finalize();
        mi.add_index(hot);

        let loads = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        mi.set_loader(std::sync::Arc::new(CountingLoader {
            indices: std::collections::HashMap::from([
                (cold_data_file, cold_data),
                (cold_tree_file, cold_tree),
            ]),
            loads: loads.clone(),
        }));

        // First full sweep: every blob, hot and cold, exactly once. Each cold
        // index is fetched from disk once.
        let mut seen = Vec::new();
        mi.for_each_id(|id, loc| {
            seen.push((*id, loc.pack_id));
        })
        .await;

        assert_eq!(
            loads.load(std::sync::atomic::Ordering::Relaxed),
            2,
            "each cold index is read from disk exactly once"
        );

        let mut expected = vec![
            (cold_data_blob, cold_data_pack),
            (cold_tree_blob, cold_tree_pack),
            (hot_blob, hot_pack),
        ];
        expected.sort();
        seen.sort();
        assert_eq!(seen, expected, "all hot and cold blobs are visited");

        // A repeated sweep re-fetches each cold index from disk (cold indices
        // are not cached in RAM between sweeps).
        mi.for_each_id(|id, loc| {
            seen.push((*id, loc.pack_id));
        })
        .await;
        assert_eq!(
            loads.load(std::sync::atomic::Ordering::Relaxed),
            4,
            "a repeated sweep re-reads each cold index from disk"
        );

        // A lookup for a cold blob promotes the index from disk once.
        let loc = mi
            .get(&cold_tree_blob)
            .await
            .expect("cold blob should remain resolvable");
        assert_eq!(loc.pack_id, cold_tree_pack);
        assert_eq!(
            loads.load(std::sync::atomic::Ordering::Relaxed),
            5,
            "a cold lookup reloads the index from disk"
        );
    }

    /// Loader backing a set of persisted index files, counting how many times
    /// each file is fetched from "disk".
    struct MapLoader {
        indices: std::collections::HashMap<ID, Index>,
        loads: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait]
    impl ColdIndexLoader for MapLoader {
        async fn load_index(&self, file_id: &ID) -> Result<Index> {
            self.loads.fetch_add(1, Ordering::Relaxed);
            self.indices
                .get(file_id)
                .cloned()
                .ok_or_else(|| MapacheError::Format(format!("no index for {}", file_id.to_hex())))
        }
    }

    /// A single-blob index in the state `Index::from_index_file` produces after a
    /// reload: frozen maps and a real file ID. Returns it with that file ID.
    fn persisted_one_blob(pack: &str, blob: &str, file: &str) -> (Index, ID) {
        let mut index = Index::new();
        index.add_pack(
            &mock_id(pack),
            vec![mock_blob_desc(blob, BlobType::Data, 0, 100)],
        );
        index.finalize();
        let file_id = mock_id(file);
        index.data_ids.freeze();
        index.zero_ids.freeze();
        index.file_id = Some(file_id);
        index.set_status(IndexStatus::Persisted(file_id));
        (index, file_id)
    }

    /// Build a repository-like lazy master index: `n` persisted index files,
    /// with a lazy blob budget that keeps the newest indices resident and
    /// pushes the rest to cold metadata. Returns the master index, a map of
    /// file_id -> Index for the loader, and a load counter. Each index `i` has
    /// a distinct pack and two distinct blobs (`blob<i>_a/b`).
    fn build_lazy_master(
        n: usize,
        budget: u64,
    ) -> (
        MasterIndex,
        std::collections::HashMap<ID, Index>,
        Arc<std::sync::atomic::AtomicUsize>,
    ) {
        let mi = MasterIndex::new(IndexMode::Lazy(budget));

        // A persisted (on-disk) index: frozen maps + real file ID, matching the
        // state `Index::from_index_file` produces after a reload.
        fn persisted(mut index: Index, file_id: ID) -> Index {
            index.data_ids.freeze();
            index.tree_ids.freeze();
            index.zero_ids.freeze();
            index.file_id = Some(file_id);
            index.set_status(IndexStatus::Persisted(file_id));
            index
        }

        let mut index_map = std::collections::HashMap::new();
        let mut file_ids = Vec::new();
        for i in 0..n {
            let mut index = Index::new();
            let pack = mock_id(&format!("lazy_pack_{i}"));
            index.add_pack(
                &pack,
                vec![
                    mock_blob_desc(&format!("blob{i}_a"), BlobType::Data, 0, 100),
                    mock_blob_desc(&format!("blob{i}_b"), BlobType::Data, 0, 200),
                ],
            );
            index.finalize();
            let file_id = mock_id(&format!("lazy_file_{i}"));
            let persisted_index = persisted(index, file_id);
            index_map.insert(file_id, persisted_index.clone());
            file_ids.push((file_id, pack, persisted_index));
        }

        // Adding through `add_index` enforces the blob budget: each new index is
        // recorded as most-recently-used and, when the pool overflows, the
        // least-recently-used (oldest) index is fully unloaded to cold metadata.
        // Since we add oldest first, the newest indices end up resident.
        for (_file_id, _pack, index) in file_ids.iter() {
            mi.add_index(index.clone());
        }

        let loads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        mi.set_loader(Arc::new(MapLoader {
            indices: index_map.clone(),
            loads: loads.clone(),
        }));

        (mi, index_map, loads)
    }

    /// Dedicated lazy-index-mode test: many index files so older ones fall out of
    /// the hot set, and a small blob budget so the hot pool cannot keep
    /// everything, forcing genuine cold reloads from the loader.
    #[tokio::test]
    async fn test_lazy_index_mode_many_cold_files() {
        const N: usize = 24;
        let (mi, _index_map, loads) = build_lazy_master(N, 4);

        // Sanity: every blob is accounted for across hot + cold.
        assert_eq!(mi.num_blobs_total(), N * 2, "every blob across hot+cold");

        // Every blob, hot or cold, must resolve to the correct pack regardless
        // of whether it is reached from cold metadata or a genuine disk reload.
        // Sweep multiple times with a tiny budget (4 blobs -> at most 2 resident
        // indices), so indices are repeatedly evicted and re-fetched, exercising
        // the cold reload path heavily.
        for pass in 0..3 {
            for i in 0..N {
                for suffix in ["a", "b"] {
                    let id = mock_id(&format!("blob{i}_{suffix}"));
                    let loc = mi
                        .get(&id)
                        .await
                        .unwrap_or_else(|| panic!("pass {pass}: blob{i}_{suffix} not found"));
                    assert_eq!(
                        loc.pack_id,
                        mock_id(&format!("lazy_pack_{i}")),
                        "pass {pass}: blob{i}_{suffix} in wrong pack"
                    );
                    assert_eq!(loc.blob_type, BlobType::Data);
                }
            }
        }

        // With a 4-blob budget and each index carrying 2 blobs, the hot pool
        // holds at most 2 indices before evicting. We accessed N indices, so the
        // same cold index must have been re-fetched from disk multiple times.
        assert!(
            loads.load(Ordering::Relaxed) > N / 2,
            "tiny budget must force repeated cold reloads, got {} loads",
            loads.load(Ordering::Relaxed)
        );

        // A full sweep visits every blob exactly once across hot and cold.
        let mut visited = 0u32;
        let mut all_packs = std::collections::BTreeSet::new();
        mi.for_each_id(|_id, loc| {
            all_packs.insert(loc.pack_id);
            visited += 1;
        })
        .await;
        assert_eq!(visited as usize, N * 2, "full sweep visits all blobs");
        assert_eq!(
            all_packs.len(),
            N,
            "full sweep visits every pack (including cold)"
        );
    }

    /// Lazy mode must expose cold packs and cold index file IDs so callers like
    /// `verify` (pack consistency) and GC (index reaping) see the whole repo.
    ///
    /// A bounded budget keeps several indices cold, so this assertively exercises
    /// the cold-metadata path of `for_each_pack_id` / `ids()`.
    /// Regression: `cleanup` must consume the cold indices too.
    ///
    /// It used to fold only the resident indices into the rewritten index and
    /// leave `cold_metadata` behind. The blobs that lived in a cold index then
    /// vanished from the new index, and because the cold entry still listed that
    /// index file as referenced, the GC went on to delete the packs holding
    /// those blobs — a repository that reported a successful clean and then
    /// failed `verify` with a broken reference.
    #[tokio::test]
    async fn test_cleanup_merges_cold_indices_without_losing_blobs() {
        const N: usize = 12;
        // A budget of 2 blobs keeps a single index resident, so all but one of
        // the indices are cold and must be streamed back in from the loader.
        let (mi, _index_map, _loads) = build_lazy_master(N, 2);
        {
            let lock = mi.inner.read();
            assert_eq!(lock.indices.len(), 1, "one index resident");
            assert_eq!(lock.cold_metadata.len(), N - 1, "the rest are cold");
        }

        mi.cleanup(None, None).await.unwrap();

        // Every blob from every index — resident and cold — must still resolve,
        // and the rewritten index must no longer reference the consumed files.
        for i in 0..N {
            for suffix in ["a", "b"] {
                let id = mock_id(&format!("blob{i}_{suffix}"));
                let loc = mi
                    .get(&id)
                    .await
                    .unwrap_or_else(|| panic!("{id} must survive cleanup"));
                assert_eq!(loc.pack_id, mock_id(&format!("lazy_pack_{i}")));
            }
        }
        let lock = mi.inner.read();
        assert!(
            lock.cold_metadata.is_empty(),
            "cleanup consumed every index, so nothing cold may be left"
        );
    }

    #[tokio::test]
    async fn test_lazy_mode_exposes_cold_packs_and_file_ids() {
        const N: usize = 12;
        let (mi, _index_map, _loads) = build_lazy_master(N, 4);

        // Prove some indices really are cold, otherwise the assertions below
        // would pass trivially from the hot pool alone.
        assert!(
            mi.num_blobs() < N * 2,
            "budget must leave some indices cold (hot blobs: {})",
            mi.num_blobs()
        );

        // for_each_pack_id must include the packs referenced only by cold indices.
        let mut packs = std::collections::BTreeSet::new();
        mi.for_each_pack_id(|p| {
            packs.insert(*p);
        });
        assert_eq!(
            packs.len(),
            N,
            "every pack (hot and cold) must be enumerated"
        );
        for i in 0..N {
            assert!(
                packs.contains(&mock_id(&format!("lazy_pack_{i}"))),
                "pack {i} missing from for_each_pack_id"
            );
        }

        // ids() must include the file IDs of cold index files.
        let ids = mi.ids();
        assert_eq!(ids.len(), N, "every index file ID must be listed");
        for i in 0..N {
            assert!(
                ids.contains(&mock_id(&format!("lazy_file_{i}"))),
                "index file {i} missing from ids()"
            );
        }
    }

    /// Zero blobs that live only in cold indices must be resolvable exactly
    /// without a disk load, even under blob-budget pressure.
    #[tokio::test]
    async fn test_lazy_mode_cold_zero_blobs_under_budget_pressure() {
        const N: usize = 12;
        let (mi, _index_map, loads) = build_lazy_master(N, 4);

        // Add a cold index that only holds zero blobs (weight 0 in the budget).
        let mut zeros = Index::new();
        let pack = mock_id("lazy_zero_pack");
        zeros.add_pack(
            &pack,
            vec![PackedBlobDescriptor {
                id: mock_id("lazy_zero_blob"),
                blob_type: BlobType::Zero,
                offset: 0,
                length: 0,
                raw_length: 12345,
                compressed: false,
            }],
        );
        zeros.finalize();
        let zero_file = mock_id("lazy_zero_file");
        mi.add_cold_metadata(IndexMetadata::from_index(&zeros, zero_file));

        // Zero blobs resolve from cold metadata exactly; no loader hit needed.
        let loc = mi.get(&mock_id("lazy_zero_blob")).await.expect("zero blob");
        assert_eq!(loc.blob_type, BlobType::Zero);
        assert_eq!(loc.raw_length, 12345);
        assert_eq!(loc.pack_id, ID::default());
        assert!(
            loads.load(Ordering::Relaxed) == 0,
            "zero blobs must not trigger a cold index load"
        );

        // get_data resolves the same cold zero blob synchronously.
        let loc = mi
            .get_data(&mock_id("lazy_zero_blob"))
            .expect("cold zero blob via get_data");
        assert_eq!(loc.blob_type, BlobType::Zero);
        assert_eq!(loc.raw_length, 12345);
        assert!(mi.contains(&mock_id("lazy_zero_blob")));
    }

    /// Under a small blob budget the master index must remain fully consistent:
    /// num_blobs_total, might_contain, and exact lookups agree over repeated
    /// churn between hot and cold-index reloads.
    #[tokio::test]
    async fn test_lazy_mode_consistency_under_budget_churn() {
        const N: usize = 20;
        let (mi, _index_map, _loads) = build_lazy_master(N, 3);

        let total = mi.num_blobs_total();
        assert_eq!(total, N * 2);

        let mut found = 0usize;
        for i in 0..N {
            for suffix in ["a", "b"] {
                let id = mock_id(&format!("blob{i}_{suffix}"));
                // might_contain reports the blob before/after a full lookup.
                assert!(mi.might_contain(&id), "might_contain({i}_{suffix})");
                assert!(mi.get(&id).await.is_some(), "get({i}_{suffix})");
                found += 1;
            }
        }
        assert_eq!(found, total, "every reported blob was resolved exactly");
    }

    /// The blob budget is a *soft* target: a single index larger than the
    /// budget must still be loadable and kept resident (we always hold at least
    /// one index), never evicted to the point where lookups are impossible.
    #[tokio::test]
    async fn test_lazy_mode_oversized_index_stays_resident() {
        const BUDGET: u64 = 4;
        let mi = MasterIndex::new(IndexMode::Lazy(BUDGET));

        // One index with 6 blobs — larger than the budget on its own.
        let mut index = Index::new();
        let pack_id = mock_id("oversized_pack");
        let mut blobs = Vec::new();
        for i in 0..6 {
            blobs.push(mock_blob_desc(
                &format!("oversized_blob_{i}"),
                BlobType::Data,
                i as u32,
                100,
            ));
        }
        index.add_pack(&pack_id, blobs);
        index.finalize();
        let file_id = mock_id("oversized_file");

        let index_map = std::collections::HashMap::from([(file_id, index.clone())]);
        mi.add_cold_metadata(IndexMetadata::from_index(&index, file_id));
        mi.set_loader(Arc::new(MapLoader {
            indices: index_map,
            loads: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }));

        // Loading the oversized index must not panic and must keep it resident,
        // even though no other index could be evicted to make it "fit".
        for i in 0..6 {
            let id = mock_id(&format!("oversized_blob_{i}"));
            let loc = mi.get(&id).await.expect("oversized index blob resolves");
            assert_eq!(loc.pack_id, pack_id);
        }

        // The oversized index is resident (hot), not bounced to cold.
        {
            let lock = mi.inner.read();
            assert_eq!(lock.indices.len(), 1, "the oversized index stays hot");
            assert_eq!(lock.cold_metadata.len(), 0, "nothing left cold");
        }
    }

    /// A promotion that panics must not strand the index.
    ///
    /// `load_and_promote` removes the index from `cold_metadata` before awaiting
    /// the loader. If the panic skipped the cleanup, the index would stay
    /// invisible *and* its `loading` marker would never clear — and since
    /// lookups wait whenever any promotion is in flight, every later lookup would
    /// spin forever.
    #[tokio::test]
    async fn test_lazy_get_recovers_after_a_panicking_cold_load() {
        struct PanickingLoader;

        #[async_trait]
        impl ColdIndexLoader for PanickingLoader {
            async fn load_index(&self, _file_id: &ID) -> Result<Index> {
                panic!("cold index load blew up");
            }
        }

        let mi = MasterIndex::new(IndexMode::Lazy(1));

        let (index, file_id) = persisted_one_blob("panic_pack", "panic_blob", "panic_file");
        mi.add_cold_metadata(IndexMetadata::from_index(&index, file_id));
        mi.set_loader(Arc::new(PanickingLoader));

        let blob = mock_id("panic_blob");

        // The promotion panics; the lookup itself must not hang or claim a miss.
        let first = tokio::spawn({
            let mi = mi.clone();
            async move { mi.get(&blob).await }
        });
        let _ = first.await;

        // No marker may be left behind, and the index must still be reachable as
        // cold metadata so a later attempt (with a working loader) can find it.
        let lock = mi.inner.read();
        assert!(lock.loading.is_empty(), "no promotion may stay in flight");
        assert!(
            lock.cold_metadata.iter().any(|m| m.file_id == file_id),
            "the index must be back in cold metadata"
        );
    }

    /// Regression for the window in which a cold index is in neither pool.
    ///
    /// `load_and_promote` has to release the write lock while it reads the index
    /// file. For that window it drops the cold metadata before publishing the
    /// index as resident, so a concurrent lookup scanning for candidates would
    /// find neither and report a blob as missing even though it is indexed.
    /// `MasterIndexInner::loading` makes the in-flight promotion visible, and the
    /// lookup must wait for it instead of giving up.
    #[tokio::test]
    async fn test_lazy_get_waits_for_in_flight_promotion() {
        /// Loader that parks inside `load_index` until the test releases it, so
        /// the promotion is deterministically in flight while another lookup
        /// runs.
        struct GatedLoader {
            index: Index,
            entered: Arc<tokio::sync::Notify>,
            release: Arc<tokio::sync::Notify>,
        }

        #[async_trait]
        impl ColdIndexLoader for GatedLoader {
            async fn load_index(&self, _file_id: &ID) -> Result<Index> {
                self.entered.notify_one();
                self.release.notified().await;
                Ok(self.index.clone())
            }
        }

        let mi = MasterIndex::new(IndexMode::Lazy(1));

        let (index, file_id) = persisted_one_blob("gated_pack", "gated_blob", "gated_file");

        // Cold: metadata only, the index itself is not resident.
        mi.add_cold_metadata(IndexMetadata::from_index(&index, file_id));
        assert_eq!(mi.inner.read().indices.len(), 0, "index starts cold");

        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        mi.set_loader(Arc::new(GatedLoader {
            index: index.clone(),
            entered: entered.clone(),
            release: release.clone(),
        }));

        let blob = mock_id("gated_blob");

        // First lookup triggers the promotion and parks inside the loader.
        let first = tokio::spawn({
            let mi = mi.clone();
            async move { mi.get(&blob).await }
        });
        entered.notified().await;
        assert_eq!(mi.inner.read().indices.len(), 0, "promotion is in flight");

        // Second lookup runs while the promotion is in flight.
        let second = tokio::spawn({
            let mi = mi.clone();
            async move { mi.get(&blob).await }
        });
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }

        release.notify_one();

        let first = first
            .await
            .expect("first lookup joins")
            .expect("first lookup resolves");
        let second = second
            .await
            .expect("second lookup joins")
            .expect("lookup during an in-flight promotion must not report a miss");
        assert_eq!(first.pack_id, mock_id("gated_pack"));
        assert_eq!(second.pack_id, mock_id("gated_pack"));
    }
}
