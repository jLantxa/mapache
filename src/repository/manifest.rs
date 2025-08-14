// mapache is an incremental backup tool
// Copyright (C) 2025  Javier Lancha Vázquez <javier.lancha@gmail.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::global::ID;

/// Repository manifest. This struct contains metadata about the repository itself.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    version: u32,
    id: ID,
    created_time: DateTime<Utc>,
}

impl Manifest {
    /// Creates a new manifest with a given version, a new random ID, and the current UTC time.
    pub fn new(version: u32) -> Self {
        Self {
            version,
            id: ID::new_random(),
            created_time: Utc::now(),
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
    pub fn created_time(&self) -> DateTime<Utc> {
        self.created_time
    }
}
