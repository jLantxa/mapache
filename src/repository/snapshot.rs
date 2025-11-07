use std::{collections::BTreeSet, path::PathBuf, sync::Arc};

use anyhow::Result;
use chrono::{DateTime, Local};
use serde::{Deserialize, Serialize};

use crate::repository::repo::REPO_DROPPED_EXTENSION;
use crate::{commands::EMPTY_TAG_MARK, fs::tree::NodeDiff, repository::repo::Repository};

use crate::mapache::ID;

pub type SnapshotTuple = (ID, Snapshot);

/// Represents a complete snapshot of backed-up data at a specific point in time.
///
/// A Snapshot links metadata (like timestamp, paths, and user info) to the
/// immutable root tree object that represents the actual file hierarchy.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(default)]
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

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,

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
#[serde(default)]
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
#[serde(default)]
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

    #[serde(skip_serializing_if = "Option::is_none")]
    pub amends: Option<ID>, // The ID of the snapshot amended by this one
}

/// A snapshot streamer.
///
/// This streamer loads Snapshots on demand.
pub struct SnapshotStreamer {
    snapshot_ids: Vec<ID>,
    repo: Arc<Repository>,
    num_snapshots: usize,
    ext: Option<String>,
}

impl SnapshotStreamer {
    /// Creates a new SnapshotStreamer for active snapshots.
    pub fn new(repo: Arc<Repository>) -> Result<Self> {
        let snapshot_ids = repo.list_snapshot_ids()?;
        let num_snapshots = snapshot_ids.len();

        Ok(Self {
            snapshot_ids,
            repo,
            num_snapshots,
            ext: None,
        })
    }

    /// Creates a new SnapshotStreamer for dropped snapshots.
    pub fn dropped(repo: Arc<Repository>) -> Result<Self> {
        let snapshot_ids = repo.list_dropped_snapshot_ids()?;
        let num_snapshots = snapshot_ids.len();

        Ok(Self {
            snapshot_ids,
            repo,
            num_snapshots,
            ext: Some(REPO_DROPPED_EXTENSION.to_owned()),
        })
    }

    /// The streamer has no more Snapshot IDs to load. It is therefore empty.
    pub fn is_empty(&self) -> bool {
        self.snapshot_ids.is_empty()
    }

    /// Returns the total number of Snapshots without consuming the iterator
    pub fn len(&self) -> usize {
        self.num_snapshots
    }

    /// Returns the number of Snapshot IDs remaining.
    pub fn remaining(&self) -> usize {
        self.snapshot_ids.len()
    }

    /// Consumes the iterator and returns the Snapshot with the latest ID.
    pub fn latest(&mut self) -> Option<(ID, Snapshot)> {
        self.snapshot_ids.sort_by_key(|id| {
            // Load each snapshot just to get its timestamp
            self.repo
                .load_snapshot(id, self.ext.as_deref())
                .ok()
                .map(|s| s.timestamp)
        });

        // Now the last ID in the sorted vector is the latest one.
        // Pop it and load the snapshot one last time.
        let latest_id = self.snapshot_ids.pop()?;
        let latest_snapshot = self
            .repo
            .load_snapshot(&latest_id, self.ext.as_deref())
            .ok()?;

        Some((latest_id, latest_snapshot))
    }
}

impl Iterator for SnapshotStreamer {
    type Item = (ID, Snapshot);

    fn next(&mut self) -> Option<Self::Item> {
        let id = self.snapshot_ids.pop()?;
        self.repo
            .load_snapshot(&id, self.ext.as_deref())
            .map_or(None, |snapshot| Some((id, snapshot)))
    }
}
