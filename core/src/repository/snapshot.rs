use std::{
    collections::BTreeSet,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use anyhow::Result;
use chrono::{DateTime, Local};
use serde::{Deserialize, Serialize};

use crate::{
    commands::EMPTY_TAG_MARK,
    fs::tree::NodeDiff,
    mapache::ID,
    repository::repo::{REPO_DROPPED_EXTENSION, Repository},
};

#[derive(Debug, Clone)]
pub struct SnapshotPair {
    pub id: ID,
    pub snapshot: Snapshot,
}

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

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,

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
    pub new_files: u64,
    pub deleted_files: u64,
    pub changed_files: u64,
    pub new_dirs: u64,
    pub deleted_dirs: u64,
    pub changed_dirs: u64,
    pub unchanged_files: u64,
    pub unchanged_dirs: u64,
}

impl DiffCounts {
    pub fn increment(&mut self, is_dir: bool, diff_type: &NodeDiff) {
        match diff_type {
            NodeDiff::New => {
                if is_dir {
                    self.new_dirs += 1
                } else {
                    self.new_files += 1
                }
            }
            NodeDiff::Deleted => {
                if is_dir {
                    self.deleted_dirs += 1
                } else {
                    self.deleted_files += 1
                }
            }
            NodeDiff::Changed => {
                if is_dir {
                    self.changed_dirs += 1
                } else {
                    self.changed_files += 1
                }
            }
            NodeDiff::Unchanged => {
                if is_dir {
                    self.unchanged_dirs += 1
                } else {
                    self.unchanged_files += 1
                }
            }
        }
    }
}

/// Contadores concurrentes, lock-free.
#[derive(Debug, Default)]
pub struct DiffCountsAtomic {
    pub new_files: AtomicU64,
    pub deleted_files: AtomicU64,
    pub changed_files: AtomicU64,
    pub new_dirs: AtomicU64,
    pub deleted_dirs: AtomicU64,
    pub changed_dirs: AtomicU64,
    pub unchanged_files: AtomicU64,
    pub unchanged_dirs: AtomicU64,
}

impl DiffCountsAtomic {
    #[inline]
    pub fn increment(&self, is_dir: bool, diff_type: &NodeDiff) {
        let o = Ordering::Relaxed;

        match diff_type {
            NodeDiff::New => {
                if is_dir {
                    self.new_dirs.fetch_add(1, o);
                } else {
                    self.new_files.fetch_add(1, o);
                }
            }
            NodeDiff::Deleted => {
                if is_dir {
                    self.deleted_dirs.fetch_add(1, o);
                } else {
                    self.deleted_files.fetch_add(1, o);
                }
            }
            NodeDiff::Changed => {
                if is_dir {
                    self.changed_dirs.fetch_add(1, o);
                } else {
                    self.changed_files.fetch_add(1, o);
                }
            }
            NodeDiff::Unchanged => {
                if is_dir {
                    self.unchanged_dirs.fetch_add(1, o);
                } else {
                    self.unchanged_files.fetch_add(1, o);
                }
            }
        }
    }

    #[inline]
    pub fn snapshot(&self) -> DiffCounts {
        let o = Ordering::Relaxed;
        DiffCounts {
            new_files: self.new_files.load(o),
            deleted_files: self.deleted_files.load(o),
            changed_files: self.changed_files.load(o),
            new_dirs: self.new_dirs.load(o),
            deleted_dirs: self.deleted_dirs.load(o),
            changed_dirs: self.changed_dirs.load(o),
            unchanged_files: self.unchanged_files.load(o),
            unchanged_dirs: self.unchanged_dirs.load(o),
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
    pub data_blobs: u64,
    pub meta_blobs: u64,
    pub total_blobs: u64,

    #[serde(flatten)]
    pub diff_counts: DiffCounts,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub amends: Option<ID>, // The ID of the snapshot amended by this one
}

/// A snapshot stream.
///
/// This stream loads Snapshots on demand.
pub struct SnapshotStream {
    snapshot_ids: Vec<ID>,
    repo: Arc<Repository>,
    num_snapshots: usize,
    ext: Option<String>,
}

impl SnapshotStream {
    /// Creates a new SnapshotStream for active snapshots.
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

    /// Creates a new SnapshotStream for dropped snapshots.
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

    /// The stream has no more Snapshot IDs to load. It is therefore empty.
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

impl Iterator for SnapshotStream {
    type Item = (ID, Snapshot);

    fn next(&mut self) -> Option<Self::Item> {
        let id = self.snapshot_ids.pop()?;
        self.repo
            .load_snapshot(&id, self.ext.as_deref())
            .map_or(None, |snapshot| Some((id, snapshot)))
    }
}
