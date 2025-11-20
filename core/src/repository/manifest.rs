use chrono::{DateTime, Local};
use serde::{Deserialize, Serialize};

use crate::mapache::ID;

/// Repository manifest. This struct contains metadata about the repository itself.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    version: u32,
    id: ID,
    created_time: DateTime<Local>,
}

impl Manifest {
    /// Creates a new manifest with a given version, a new random ID, and the current UTC time.
    pub fn new(version: u32) -> Self {
        Self {
            version,
            id: ID::new_random(),
            created_time: Local::now(),
        }
    }

    /// Returns the version of the manifest.
    pub fn version(&self) -> u32 {
        self.version
    }

    /// Returns the unique ID of the repository.
    pub fn id(&self) -> &ID {
        &self.id
    }

    /// Returns the creation timestamp of the repository.
    pub fn created_time(&self) -> DateTime<Local> {
        self.created_time
    }
}
