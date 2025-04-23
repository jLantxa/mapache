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

use chrono::Duration;
use serde::{Deserialize, Serialize};

/// Repository config
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Config {
    pub compression_level: CompressionLevel,
    pub retention_policy: SnapshotRetentionPolicy,
}

/// Compression level used by the tool.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub enum CompressionLevel {
    #[default]
    Auto,

    None,
    Low,
    Normal,
    High,
}

impl CompressionLevel {
    /// Converts a `CompressionLevel` to an i32 level or None if no compression is applicable.
    pub fn to_i32(&self) -> Option<i32> {
        match &self {
            CompressionLevel::None => None,

            CompressionLevel::Low => Some(1),
            CompressionLevel::Normal => Some(10),
            CompressionLevel::High => Some(22),

            CompressionLevel::Auto => Self::Normal.to_i32(),
        }
    }
}

/// Retention policy for the snapshots stored in the repository.
///
/// The retention policy describes which snapshots get cleaned from the repository, and when.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub enum SnapshotRetentionPolicy {
    #[default]
    /// Keep all snapshots
    KeepAll,

    /// Keep the last N snapshots
    KeepLastN(usize),

    /// Keep snapshots for the specified duration
    KeepForTime(Duration),
}
