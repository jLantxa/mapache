use std::time::Duration;

use chunker::Normalization;

use crate::{commands::Compression, repository::repo::RepoConfig, utils::size};

pub(crate) const APP_NAME: &str = "mapache";

// --- Snapshot ---
pub(crate) const DEFAULT_SNAPSHOT_READERS: usize = 4;
pub(crate) const DEFAULT_SNAPSHOT_PACKERS: usize = 4;

// --- Restore ---
pub(crate) const DEFAULT_RESTORE_PACK_PREFETCH: usize = 4;
pub(crate) const DEFAULT_RESTORE_BLOB_CONCURRENCY: usize = 8;
pub(crate) const DEFAULT_RESTORE_MAX_OPEN_FILES: usize = 128;
pub(crate) const DEFAULT_RESTORE_PACK_PREFETCH_MEMORY_BYTES: usize = (128 * size::MiB) as usize;
pub(crate) const DEFAULT_RESTORE_PACK_PREFETCH_MEMORY_UNIT: usize = (256 * size::KiB) as usize;
pub(crate) const DEFAULT_RESTORE_PACK_SEGMENT_MAX_SIZE: u64 = 32 * size::MiB;
pub(crate) const DEFAULT_RESTORE_PACK_READ_MERGE_THRESHOLD: u64 = 2 * size::MiB;

// --- Index ---
pub(crate) const INDEX_FLUSH_TIMEOUT: Duration = Duration::from_secs(10 * 60);
pub(crate) const BLOBS_PER_INDEX_FILE: usize = 65535;

// --- Packing ---
pub(crate) const MIN_CONFIGURABLE_PACK_SIZE_MIB: f32 = 1.0_f32;
pub(crate) const MAX_CONFIGURABLE_PACK_SIZE_MIB: f32 = (u32::MAX as u64 / size::MiB) as f32;
pub const DEFAULT_DEFAULT_PACK_SIZE_MIB: f32 = 16.0;
pub const DEFAULT_PACK_SIZE: u64 = (DEFAULT_DEFAULT_PACK_SIZE_MIB * size::MiB as f32) as u64;
pub(crate) const FOOTER_BLOB_MULTIPLE: usize = 64;

// --- Chunking ---
// The chunker parameters must remain stable across versions, otherwise
// same contents will no longer produce same chunks and IDs.
/// Minimum chunk size.
pub(crate) const MIN_CHUNK_SIZE: u64 = 512 * size::KiB;
/// Average chunk size.
pub(crate) const NORMAL_CHUNK_SIZE: u64 = size::MiB;
/// Maximum chunk size.
pub(crate) const MAX_CHUNK_SIZE: u64 = 8 * size::MiB;
/// Chunk normalization level.
pub(crate) const CHUNKER_NORMALIZATION: Normalization = Normalization::L2;

// --- Encoding ---
pub(crate) const DEFAULT_COMPRESSION: Compression = Compression::Fast;

// --- Display ---
/// Display length for the repository ID in bytes
pub(crate) const SHORT_REPO_ID_LEN: usize = 5;

/// Display length for a Snapshot ID in bytes
pub(crate) const SHORT_SNAPSHOT_ID_LEN: usize = 4;

pub(crate) const DEFAULT_VERBOSITY: u32 = 1;

// --- Garbage collection ---
/// Percentage of garbage to tolerate per pack
pub(crate) const DEFAULT_GC_TOLERANCE: f32 = 0.0; // [0 - 1]

/// Repack files smaller than this factor of the max pack size
pub(crate) const DEFAULT_MIN_PACK_SIZE_FACTOR: f32 = 0.05;

// --- UI ---
pub(crate) const DEFAULT_PROGRESS_REFRESH_RATE_HZ: f32 = 10.0;
pub(crate) const MAX_PATH_DISPLAY_LEN: usize = 100;

/// Minimum file size to show active progress (spinners/active files) in the UI always.
/// Files smaller than this will be sampled.
/// Set to `None` to disable sampling and track all files (adds significant overhead).
pub(crate) const UI_SNAPSHOT_PROGRESS_ITEM_MIN_SIZE: Option<u64> = Some(128 * size::KiB);

// --- FUSE ---
#[cfg(all(feature = "fuse", unix))]
pub(crate) const DEFAULT_FUSE_STASH_CACHE_SIZE_MIB: f32 = 64.0;

// --- S3 ---
pub(crate) const S3_MULTIPART_THRESHOLD: u64 = 128 * size::MiB;
pub(crate) const S3_MULTIPART_PART_SIZE: u64 = 128 * size::MiB;

// --- Others ---
/// A default RepoConfig for use in tests.
pub const TEST_REPO_CONFIG: RepoConfig = RepoConfig {
    pack_size: DEFAULT_PACK_SIZE,
    use_cache: false,
    compression: Compression::Fastest,
};
