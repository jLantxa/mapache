use crate::common::error::{MapacheError, Result};
use chrono::{DateTime, Local, TimeZone, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    common::ID,
    utils::binary::{get_array, get_i64, get_u32, put_bytes, put_i64, put_u32},
};

/// Repository manifest. This struct contains metadata about the repository itself.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    version: u32,
    id: ID,
    created_time: DateTime<Local>,
}

impl Manifest {
    pub fn new(version: u32) -> Self {
        Self {
            version,
            id: ID::new_random(),
            created_time: Local::now(),
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

    pub fn to_binary(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        put_u32(&mut buf, self.version);
        put_bytes(&mut buf, self.id.as_slice());
        put_i64(&mut buf, self.created_time.timestamp());
        put_u32(&mut buf, self.created_time.timestamp_subsec_nanos());
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
        Ok(Self {
            version,
            id,
            created_time,
        })
    }
}
