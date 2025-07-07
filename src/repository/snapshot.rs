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

use std::{collections::BTreeSet, path::PathBuf, sync::Arc};

use anyhow::Result;
use chrono::{DateTime, Local};
use serde::{Deserialize, Serialize};

use crate::{
    commands::EMPTY_TAG_MARK,
    repository::{RepositoryBackend, streamers::NodeDiff},
};

use super::ID;

pub type SnapshotTuple = (ID, Snapshot);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    /// The snapshot timestamp is the Local time at which the snapshot was created
    pub timestamp: DateTime<Local>,

    /// The ID of the parent snapshot, if any
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<ID>,

    /// Hash ID for the tree object root.
    pub tree: ID,

    /// Snapshot root path
    pub root: PathBuf,

    /// Absolute paths to the targets
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub paths: Vec<PathBuf>,

    /// Tags
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub tags: BTreeSet<String>,

    /// Description of the snapshot.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Summary of the Snapshot.
    pub summary: SnapshotSummary,
}

impl Snapshot {
    #[inline]
    pub fn size(&self) -> u64 {
        self.summary.processed_bytes
    }

    pub fn has_tags(&self, tags: &BTreeSet<String>) -> bool {
        if tags.contains(EMPTY_TAG_MARK) && self.tags.is_empty() {
            return true;
        }

        for tag in &self.tags {
            if tags.contains(tag) {
                return true;
            }
        }
        false
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct DiffCounts {
    pub new_files: usize,
    pub deleted_files: usize,
    pub changed_files: usize,
    pub new_dirs: usize,
    pub deleted_dirs: usize,
    pub changed_dirs: usize,
    pub unchanged_files: usize,
    pub unchanged_dirs: usize,
}

impl DiffCounts {
    pub fn increment(&mut self, is_dir: bool, diff_type: &NodeDiff) {
        match diff_type {
            NodeDiff::New => {
                if is_dir {
                    self.new_dirs += 1;
                } else {
                    self.new_files += 1;
                }
            }
            NodeDiff::Deleted => {
                if is_dir {
                    self.deleted_dirs += 1;
                } else {
                    self.deleted_files += 1;
                }
            }
            NodeDiff::Changed => {
                if is_dir {
                    self.changed_dirs += 1;
                } else {
                    self.changed_files += 1;
                }
            }
            NodeDiff::Unchanged => {
                if is_dir {
                    self.unchanged_dirs += 1;
                } else {
                    self.unchanged_files += 1;
                }
            }
        }
    }
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

    #[serde(flatten)]
    pub diff_counts: DiffCounts,
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
