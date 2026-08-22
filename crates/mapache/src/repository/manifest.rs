use crate::common::error::{MapacheError, Result};
use chrono::{DateTime, Local, TimeZone, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    common::ID,
    utils::binary::{get_array, get_i64, get_u8, get_u32, put_bytes, put_i64, put_u32},
};

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

impl EccConfig {
    /// Create ECC config from an overhead percentage.
    ///
    /// Fixed K=100. P = overhead (1–100).
    /// Returns `None` if `overhead_percent` is 0 (ECC disabled).
    pub fn from_overhead(overhead_percent: u32) -> Option<Self> {
        if overhead_percent == 0 {
            return None;
        }
        let k = 100u32;
        let p = overhead_percent.min(100);
        Some(Self {
            data_shards: k,
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

    pub fn to_binary(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        put_u32(&mut buf, self.version);
        put_bytes(&mut buf, self.id.as_slice());
        put_i64(&mut buf, self.created_time.timestamp());
        put_u32(&mut buf, self.created_time.timestamp_subsec_nanos());
        // ECC config: present if enabled. Format:
        //   u8 flag (1 = has ecc, 0 = no ecc)
        //   if 1: u32 data_shards + u32 parity_shards
        match &self.ecc {
            Some(ecc) => {
                buf.push(1);
                put_u32(&mut buf, ecc.data_shards);
                put_u32(&mut buf, ecc.parity_shards);
            }
            None => {
                buf.push(0);
            }
        }
        buf
    }

    pub fn from_binary(bytes: &[u8]) -> Result<Self> {
        let mut cur = bytes;
        let version = get_u32(&mut cur)?;
        let id = ID::from_bytes(get_array(&mut cur)?);
        let timestamp_secs = get_i64(&mut cur)?;
        let timestamp_nsecs = get_u32(&mut cur)?;
        let utc = Utc
            .timestamp_opt(timestamp_secs, timestamp_nsecs)
            .single()
            .ok_or_else(|| MapacheError::Format("invalid manifest timestamp".to_string()))?;
        let created_time = utc.with_timezone(&Local);

        // ECC config: optional, forward-compatible.
        let ecc = if !cur.is_empty() {
            let flag = get_u8(&mut cur)?;
            if flag == 1 && cur.len() >= 8 {
                let data_shards = get_u32(&mut cur)?;
                let parity_shards = get_u32(&mut cur)?;
                Some(EccConfig {
                    data_shards,
                    parity_shards,
                })
            } else {
                None
            }
        } else {
            None
        };

        Ok(Self {
            version,
            id,
            created_time,
            ecc,
        })
    }
}
