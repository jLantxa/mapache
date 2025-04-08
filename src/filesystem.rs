// [backup] is an incremental backup tool
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

use std::time::SystemTime;

use blake3::Hasher;
use serde::{Deserialize, Serialize};

use crate::utils::hashing::{Hash, Hashable};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DirectoryMetadata {
    pub modified: SystemTime,
    pub created: Option<SystemTime>,
    pub permissions: Option<u32>, // For Unix-like modes
    pub owner_uid: Option<u32>,
    pub owner_gid: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FileMetadata {
    pub size: u64,
    pub modified: SystemTime,
    pub created: Option<SystemTime>,
    pub permissions: Option<u32>, // For Unix-like modes
}

impl Hashable for DirectoryMetadata {
    fn hash(&self) -> Hash {
        let hasher = Hasher::new();

        let hash = hasher.finalize();
        format!("{}", hash)
    }
}

impl Hashable for FileMetadata {
    fn hash(&self) -> Hash {
        let hasher = Hasher::new();

        let hash = hasher.finalize();
        format!("{}", hash)
    }
}
