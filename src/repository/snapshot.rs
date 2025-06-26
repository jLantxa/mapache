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

use std::{path::PathBuf, sync::Arc};

use anyhow::Result;
use chrono::{DateTime, Local};
use serde::{Deserialize, Serialize};

use crate::repository::RepositoryBackend;

use super::ID;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    /// The snapshot timestamp is the Local time at which the snapshot was created
    pub timestamp: DateTime<Local>,

    /// Hash ID for the tree object root.
    pub tree: ID,

    /// Snapshot root path
    pub root: PathBuf,

    /// Absolute paths to the targets
    pub paths: Vec<PathBuf>,

    /// Description of the snapshot.
    pub description: Option<String>,

    /// Summary of the Snapshot.
    pub summary: SnapshotSummary,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SnapshotSummary {
    pub processed_items_count: u64, // Number of files processed
    pub processed_bytes: u64,       // Bytes processed (only data)

    pub raw_bytes: u64,           // Bytes 'written' before encoding
    pub encoded_bytes: u64,       // Bytes written after encoding
    pub meta_raw_bytes: u64,      // Metadata bytes 'written' before encoding
    pub meta_encoded_bytes: u64,  // Metadata bytes written after encoding
    pub total_raw_bytes: u64,     // Total raw bytes
    pub total_encoded_bytes: u64, // Total bytes after encoding

    pub new_files: u32,
    pub changed_files: u32,
    pub unchanged_files: u32,
    pub deleted_files: u32,

    pub new_dirs: u32,
    pub changed_dirs: u32,
    pub unchanged_dirs: u32,
    pub deleted_dirs: u32,
}

/// A snapshot streamer.
///
/// This streamer loads Snapshots on demand.
pub struct SnapshotStreamer {
    snapshot_ids: Vec<ID>,
    repo: Arc<dyn RepositoryBackend>,
}

impl SnapshotStreamer {
    /// Creates a new SnapshotStreamer. It needs a repo to load snapshots.
    pub fn new(repo: Arc<dyn RepositoryBackend>) -> Result<Self> {
        Ok(Self {
            snapshot_ids: repo.list_snapshot_ids()?,
            repo,
        })
    }

    /// The streamer has no more Snapshot IDs to load. It is therefore empty.
    pub fn is_empty(&self) -> bool {
        self.snapshot_ids.is_empty()
    }

    /// Returns the number of Snapshot IDs remaining.
    pub fn len(&self) -> usize {
        self.snapshot_ids.len()
    }

    /// Consumes the iterator and returns the Snapshot with the latest ID.
    pub fn latest(&mut self) -> Option<(ID, Snapshot)> {
        let (mut latest_id, mut latest_sn) = self.next()?;

        for (mut id, mut snapshot) in self.by_ref() {
            if snapshot.timestamp > latest_sn.timestamp {
                std::mem::swap(&mut id, &mut latest_id);
                std::mem::swap(&mut snapshot, &mut latest_sn);
            }
        }

        self.snapshot_ids.clear();
        Some((latest_id, latest_sn))
    }
}

impl Iterator for SnapshotStreamer {
    type Item = (ID, Snapshot);

    fn next(&mut self) -> Option<Self::Item> {
        let id = self.snapshot_ids.pop()?;
        self.repo
            .load_snapshot(&id)
            .map_or(None, |snapshot| Some((id, snapshot)))
    }
}
