use std::time::Duration;

use crate::{repository::repo::RepoConfig, utils::size};

pub(crate) const APP_NAME: &str = "mapache";

// -- Concurrency --
pub(crate) const DEFAULT_READ_CONCURRENCY: usize = 4;
pub(crate) const DEFAULT_WRITE_CONCURRENCY: usize = 4;

// -- Index --
pub(crate) const INDEX_FLUSH_TIMEOUT: Duration = Duration::from_secs(10 * 60);
pub(crate) const BLOBS_PER_INDEX_FILE: usize = 65535;

// -- Packing --
pub(crate) const MIN_CONFIGURABLE_PACK_SIZE_MIB: f32 = 1.0_f32;
pub(crate) const MAX_CONFIGURABLE_PACK_SIZE_MIB: f32 = (u32::MAX as u64 / size::MiB) as f32;
pub const DEFAULT_DEFAULT_PACK_SIZE_MIB: f32 = 16.0;
pub const DEFAULT_PACK_SIZE: u64 = (DEFAULT_DEFAULT_PACK_SIZE_MIB * size::MiB as f32) as u64;
pub(crate) const HEADER_BLOB_MULTIPLE: usize = 64;

// -- Chunking --
/// Minimum chunk size
pub(crate) const MIN_CHUNK_SIZE: u64 = 512 * size::KiB;
/// Average chunk size
pub(crate) const AVG_CHUNK_SIZE: u64 = size::MiB;
/// Maximum chunk size
pub(crate) const MAX_CHUNK_SIZE: u64 = 8 * size::MiB;

// -- Display --
/// Display length for the repository ID in bytes
pub(crate) const SHORT_REPO_ID_LEN: usize = 5;

/// Display length for a Snapshot ID in bytes
pub(crate) const SHORT_SNAPSHOT_ID_LEN: usize = 4;

pub(crate) const DEFAULT_VERBOSITY: u32 = 1;

// -- Garbage collection --
/// Percentage of garbage to tolerate per pack
pub(crate) const DEFAULT_GC_TOLERANCE: f32 = 0.0; // [0 - 1]

/// Repack files smaller than this factor of the max pack size
pub(crate) const DEFAULT_MIN_PACK_SIZE_FACTOR: f32 = 0.05;

// -- UI --
pub(crate) const DEFAULT_PROGRESS_REFRESH_RATE_HZ: f32 = 10.0;
pub(crate) const MAX_PATH_DISPLAY_LEN: usize = 100;

// -- Others --
/// A default RepoConfig for use in tests.
pub const TEST_REPO_CONFIG: RepoConfig = RepoConfig {
    pack_size: DEFAULT_PACK_SIZE,
    use_cache: false,
};
