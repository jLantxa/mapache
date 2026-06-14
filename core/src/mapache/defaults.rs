use std::{sync::OnceLock, time::Duration};

use chunker::Normalization;

use crate::{commands::Compression, mapache::config, repository::repo::RepoConfig, utils::size};

pub(crate) const APP_NAME: &str = "mapache";

// --- Snapshot ---
pub(crate) const DEFAULT_SNAPSHOT_READERS: usize = 4;
pub(crate) const DEFAULT_SNAPSHOT_PACKERS: usize = 4;

// --- Restore ---
pub(crate) const DEFAULT_RESTORE_BLOB_CONCURRENCY: usize = 8;
pub(crate) const DEFAULT_RESTORE_DECODED_BUDGET: u64 = 8 * size::MiB;
pub(crate) const DEFAULT_RESTORE_MAX_OPEN_FILES: usize = 128;
pub(crate) const DEFAULT_RESTORE_PACK_PREFETCH: usize = 4;
pub(crate) const DEFAULT_RESTORE_PACK_READ_MERGE_THRESHOLD: u64 = 2 * size::MiB;
pub(crate) const DEFAULT_RESTORE_PACK_SEGMENT_MAX_SIZE: u64 = 32 * size::MiB;

// --- Index ---
pub(crate) const INDEX_FLUSH_TIMEOUT: Duration = Duration::from_secs(10 * 60);
pub(crate) const BLOBS_PER_INDEX_FILE: usize = 65535;

// --- Packing ---
pub(crate) const MIN_CONFIGURABLE_PACK_SIZE_MIB: f32 = 1.0_f32;
pub(crate) const MAX_CONFIGURABLE_PACK_SIZE_MIB: f32 = (u32::MAX as u64 / size::MiB) as f32;
pub const DEFAULT_PACK_SIZE_MIB: f32 = 16.0;
pub const DEFAULT_PACK_SIZE: u64 = (DEFAULT_PACK_SIZE_MIB * size::MiB as f32) as u64;
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

/// Maximum decoded bytes in memory during GC repack.
pub(crate) const DEFAULT_GC_DECODED_BUDGET: u64 = 256 * size::MiB;

/// Maximum concurrent repack chunks during GC.
pub(crate) const DEFAULT_GC_REPACK_CONCURRENCY: usize = 2;

// --- UI ---
pub(crate) const DEFAULT_PROGRESS_REFRESH_RATE_HZ: f32 = 10.0;
pub(crate) const MAX_PATH_DISPLAY_LEN: usize = 100;

/// Minimum file size to show active progress (spinners/active files) in the UI always.
/// Files smaller than this will be sampled.
/// Set to `None` to disable sampling and track all files (adds significant overhead).
pub(crate) const UI_SNAPSHOT_PROGRESS_ITEM_MIN_SIZE: Option<u64> = Some(128 * size::KiB);

/// Sliding window for the rate estimator used by progress bars.
/// Shorter values make ETA/throughput more responsive to recent bursts;
/// longer values make them more stable.
pub(crate) const UI_RATE_ESTIMATOR_WINDOW: Duration = Duration::from_secs(10);

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

// --- Runtime defaults (configurable via TOML) ---

/// Runtime-configurable defaults. These override the compile-time constants
/// when set in the `[runtime]` section of the config file.
#[derive(Debug, Clone)]
pub struct RuntimeDefaults {
    /// Maximum concurrent file writes when flushing decoded blobs to disk.
    pub restore_blob_concurrency: usize,
    /// Per-pack decoded (decompressed + decrypted) data budget in bytes.
    /// When this budget is exceeded, accumulated blobs are flushed to disk,
    /// bounding peak decoded memory across concurrent pack streams.
    pub restore_decoded_budget: u64,
    /// Maximum number of simultaneously open file handles during restore.
    pub restore_max_open_files: usize,
    /// Number of packs to download and process concurrently during restore.
    pub restore_pack_prefetch: usize,
    /// Maximum gap (bytes) between consecutive blob reads to merge them
    /// into one pack segment download.
    pub restore_pack_read_merge_threshold: u64,
    /// Maximum byte size of a single pack segment. Larger segments reduce
    /// the number of sequential reads per pack but increase per-segment
    /// encoded memory.
    pub restore_pack_segment_max_size: u64,
    // GC
    pub min_pack_size_factor: f32,
    pub gc_decoded_budget: u64,
    pub gc_repack_concurrency: usize,
    // Index
    pub blobs_per_index_file: usize,
    pub index_flush_timeout: Duration,
    // S3
    pub s3_multipart_threshold: u64,
    pub s3_multipart_part_size: u64,
    // UI
    pub max_path_display_len: usize,
    pub ui_snapshot_progress_item_min_size: Option<u64>,
}

impl RuntimeDefaults {
    pub fn new(config: Option<&config::RuntimeConfig>) -> Self {
        let c = config;
        Self {
            restore_blob_concurrency: c
                .and_then(|c| c.restore_blob_concurrency)
                .unwrap_or(DEFAULT_RESTORE_BLOB_CONCURRENCY),
            restore_decoded_budget: c
                .and_then(|c| c.restore_decoded_budget)
                .unwrap_or(DEFAULT_RESTORE_DECODED_BUDGET),
            restore_max_open_files: c
                .and_then(|c| c.restore_max_open_files)
                .unwrap_or(DEFAULT_RESTORE_MAX_OPEN_FILES),
            restore_pack_prefetch: c
                .and_then(|c| c.restore_pack_prefetch)
                .unwrap_or(DEFAULT_RESTORE_PACK_PREFETCH),
            restore_pack_read_merge_threshold: c
                .and_then(|c| c.restore_pack_read_merge_threshold)
                .unwrap_or(DEFAULT_RESTORE_PACK_READ_MERGE_THRESHOLD),
            restore_pack_segment_max_size: c
                .and_then(|c| c.restore_pack_segment_max_size)
                .unwrap_or(DEFAULT_RESTORE_PACK_SEGMENT_MAX_SIZE),
            min_pack_size_factor: c
                .and_then(|c| c.min_pack_size_factor)
                .unwrap_or(DEFAULT_MIN_PACK_SIZE_FACTOR),
            gc_decoded_budget: c
                .and_then(|c| c.gc_decoded_budget)
                .unwrap_or(DEFAULT_GC_DECODED_BUDGET),
            gc_repack_concurrency: c
                .and_then(|c| c.gc_repack_concurrency)
                .unwrap_or(DEFAULT_GC_REPACK_CONCURRENCY),
            blobs_per_index_file: c
                .and_then(|c| c.blobs_per_index_file)
                .unwrap_or(BLOBS_PER_INDEX_FILE),
            index_flush_timeout: c
                .and_then(|c| c.index_flush_timeout_secs)
                .map(Duration::from_secs)
                .unwrap_or(INDEX_FLUSH_TIMEOUT),
            s3_multipart_threshold: c
                .and_then(|c| c.s3_multipart_threshold)
                .unwrap_or(S3_MULTIPART_THRESHOLD),
            s3_multipart_part_size: c
                .and_then(|c| c.s3_multipart_part_size)
                .unwrap_or(S3_MULTIPART_PART_SIZE),
            max_path_display_len: c
                .and_then(|c| c.max_path_display_len)
                .unwrap_or(MAX_PATH_DISPLAY_LEN),
            ui_snapshot_progress_item_min_size: c
                .and_then(|c| c.ui_snapshot_progress_item_min_size)
                .or(UI_SNAPSHOT_PROGRESS_ITEM_MIN_SIZE),
        }
    }
}

static RUNTIME_DEFAULTS: OnceLock<RuntimeDefaults> = OnceLock::new();

/// Initialize the runtime defaults. Must be called once before any use.
pub fn init_runtime_defaults(config: Option<&config::RuntimeConfig>) {
    let _ = RUNTIME_DEFAULTS.set(RuntimeDefaults::new(config));
}

/// Get a reference to the runtime defaults. Auto-initializes with compile-time
/// defaults if not yet set (useful for tests).
pub fn runtime() -> &'static RuntimeDefaults {
    RUNTIME_DEFAULTS.get_or_init(|| RuntimeDefaults::new(None))
}
