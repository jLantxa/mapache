use chrono::{DateTime, Local};
use serde::{Deserialize, Serialize};

use crate::common::ID;

/// ECC configuration for the repository.
///
/// When present, all repo files (packs, index, snapshots, manifest, keys)
/// are protected by Reed-Solomon erasure codes stored as `.ecc` sidecars.
///
/// K and P are stored explicitly for forward compatibility: if the formula
/// changes in the future, old repos still decode correctly.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EccConfig {
    /// Data shards per stripe (K).
    pub data_shards: u32,
    /// Parity shards per stripe (P).
    pub parity_shards: u32,
}

/// Default number of data shards for ECC.
const DEFAULT_DATA_SHARDS: u32 = 100;

impl EccConfig {
    /// Create ECC config from an overhead percentage.
    ///
    /// Fixed K=100. P = overhead (1–100).
    /// Returns `None` if `overhead_percent` is 0 (ECC disabled).
    pub fn from_overhead(overhead_percent: u32) -> Option<Self> {
        if overhead_percent == 0 {
            return None;
        }
        let p = overhead_percent.min(100);
        Some(Self {
            data_shards: DEFAULT_DATA_SHARDS,
            parity_shards: p,
        })
    }
}

/// Repository manifest. This struct contains metadata about the repository itself.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    version: u32,
    id: ID,
    created_time: DateTime<Local>,
    #[serde(default)]
    ecc: Option<EccConfig>,
}

impl Manifest {
    pub fn new(version: u32) -> Self {
        Self {
            version,
            id: ID::new_random(),
            created_time: Local::now(),
            ecc: None,
        }
    }

    pub fn new_with_ecc(version: u32, ecc: EccConfig) -> Self {
        Self {
            version,
            id: ID::new_random(),
            created_time: Local::now(),
            ecc: Some(ecc),
        }
    }

    pub fn version(&self) -> u32 {
        self.version
    }

    pub fn set_version(&mut self, version: u32) {
        self.version = version;
    }

    pub fn id(&self) -> &ID {
        &self.id
    }

    pub fn created_time(&self) -> DateTime<Local> {
        self.created_time
    }

    pub fn ecc(&self) -> Option<&EccConfig> {
        self.ecc.as_ref()
    }

    pub fn set_ecc(&mut self, ecc: Option<EccConfig>) {
        self.ecc = ecc;
    }
}
