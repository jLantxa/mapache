//! This module defines the repository snapshot format and related types.
//! A snapshot represents a complete backup of a directory tree at a point in time.

use std::{
    collections::BTreeSet,
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
};

use anyhow::Result;
use async_stream::stream;
use chrono::{DateTime, Local};
use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};

use crate::{
    commands::EMPTY_TAG_MARK,
    common::ID,
    fs::tree::NodeDiff,
    repository::repo::{REPO_DROPPED_EXTENSION, Repository},
};

/// A pair containing a snapshot ID and the snapshot itself.
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

/// Represents a snapshot entry in a list, including its ID and active status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotEntry {
    pub id: ID,
    pub snapshot: Snapshot,
    pub active: bool,
}

pub type SnapshotEntryList = Vec<SnapshotEntry>;

impl Snapshot {
    #[inline]
    pub fn size(&self) -> u64 {
        self.summary.processed_bytes
    }

    pub fn has_tags(&self, tags: &BTreeSet<String>) -> bool {
        if tags.contains(EMPTY_TAG_MARK) && self.tags.is_empty() {
            return true;
        }
        self.tags.iter().any(|tag| tags.contains(tag))
    }
}

/// Tracks the number of new, deleted, and changed files and directories in a snapshot.
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

/// Concurrent counters. Lock-free.
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
        match diff_type {
            NodeDiff::New => {
                if is_dir {
                    self.new_dirs.fetch_add(1, Ordering::Relaxed);
                } else {
                    self.new_files.fetch_add(1, Ordering::Relaxed);
                }
            }
            NodeDiff::Deleted => {
                if is_dir {
                    self.deleted_dirs.fetch_add(1, Ordering::Relaxed);
                } else {
                    self.deleted_files.fetch_add(1, Ordering::Relaxed);
                }
            }
            NodeDiff::Changed => {
                if is_dir {
                    self.changed_dirs.fetch_add(1, Ordering::Relaxed);
                } else {
                    self.changed_files.fetch_add(1, Ordering::Relaxed);
                }
            }
            NodeDiff::Unchanged => {
                if is_dir {
                    self.unchanged_dirs.fetch_add(1, Ordering::Relaxed);
                } else {
                    self.unchanged_files.fetch_add(1, Ordering::Relaxed);
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

/// Summary of the snapshot process, including counts and byte sizes.
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

    #[serde(flatten)]
    pub diff_counts: DiffCounts,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub amends: Option<ID>,
}

/// A stream of Snapshot Results.
type PinnedSnapshotStream = Pin<Box<dyn Stream<Item = Result<(ID, Snapshot)>> + Send>>;

/// A snapshot stream that loads Snapshots on demand.
/// Implements `Stream` to allow for functional adapters like `map`, `filter`, etc.
pub struct SnapshotStream {
    inner: PinnedSnapshotStream,
    num_snapshots: usize,
    remaining_count: Arc<AtomicU64>,
}

impl SnapshotStream {
    pub async fn new(repo: Arc<Repository>) -> Result<Self> {
        let snapshot_ids = repo.list_snapshot_ids().await?;
        Ok(Self::make_stream(repo, snapshot_ids, None))
    }

    pub async fn dropped(repo: Arc<Repository>) -> Result<Self> {
        let snapshot_ids = repo.list_dropped_snapshot_ids().await?;
        Ok(Self::make_stream(
            repo,
            snapshot_ids,
            Some(REPO_DROPPED_EXTENSION.to_owned()),
        ))
    }

    fn make_stream(repo: Arc<Repository>, mut ids: Vec<ID>, ext: Option<String>) -> Self {
        let num_snapshots = ids.len();
        let remaining_count = Arc::new(AtomicU64::new(num_snapshots as u64));
        let remaining_ptr = remaining_count.clone();

        // Use the stream! macro to generate the state machine automatically
        let inner = stream! {
            while let Some(id) = ids.pop() {
                tracing::debug!(target: "snapshot", "Loading snapshot {}", id.to_short_hex(8));
                let res = repo.load_snapshot(&id, ext.as_deref()).await;

                // Decrement the remaining counter regardless of success
                remaining_ptr.fetch_sub(1, Ordering::Relaxed);

                match res {
                    Ok(snapshot) => yield Ok((id, snapshot)),
                    Err(e) => yield Err(e),
                }
            }
        };

        Self {
            inner: Box::pin(inner),
            num_snapshots,
            remaining_count,
        }
    }

    /// Returns total snapshots found at creation.
    pub fn len(&self) -> usize {
        self.num_snapshots
    }

    /// Returns true if no snapshots were found at creation.
    pub fn is_empty(&self) -> bool {
        self.num_snapshots == 0
    }

    /// Returns snapshots currently left in the stream buffer.
    pub fn remaining(&self) -> usize {
        self.remaining_count.load(Ordering::Relaxed) as usize
    }

    pub async fn collect_entries(mut self, active: bool) -> Result<SnapshotEntryList> {
        let mut entries = Vec::new();
        while let Some(res) = self.next().await {
            let (id, snapshot) = res?;
            entries.push(SnapshotEntry {
                id,
                snapshot,
                active,
            });
        }
        Ok(entries)
    }

    pub async fn latest(self) -> Result<Option<(ID, Snapshot)>> {
        self.fold::<Result<Option<(ID, Snapshot)>>, _, _>(Ok(None), |latest, res| async move {
            let (id, snap) = res?;
            match latest? {
                Some((lid, lsnap)) if lsnap.timestamp > snap.timestamp => Ok(Some((lid, lsnap))),
                _ => Ok(Some((id, snap))),
            }
        })
        .await
    }
}

impl Stream for SnapshotStream {
    type Item = Result<(ID, Snapshot)>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_diff_counts_increment() {
        let mut counts = DiffCounts::default();

        counts.increment(false, &NodeDiff::New);
        assert_eq!(counts.new_files, 1);
        assert_eq!(counts.new_dirs, 0);

        counts.increment(true, &NodeDiff::New);
        assert_eq!(counts.new_dirs, 1);

        counts.increment(false, &NodeDiff::Deleted);
        assert_eq!(counts.deleted_files, 1);

        counts.increment(true, &NodeDiff::Deleted);
        assert_eq!(counts.deleted_dirs, 1);

        counts.increment(false, &NodeDiff::Changed);
        assert_eq!(counts.changed_files, 1);

        counts.increment(true, &NodeDiff::Changed);
        assert_eq!(counts.changed_dirs, 1);

        counts.increment(false, &NodeDiff::Unchanged);
        assert_eq!(counts.unchanged_files, 1);

        counts.increment(true, &NodeDiff::Unchanged);
        assert_eq!(counts.unchanged_dirs, 1);
    }

    #[test]
    fn test_diff_counts_atomic_increment() {
        let counts_atomic = DiffCountsAtomic::default();

        counts_atomic.increment(false, &NodeDiff::New);
        counts_atomic.increment(true, &NodeDiff::New);
        counts_atomic.increment(false, &NodeDiff::Deleted);
        counts_atomic.increment(true, &NodeDiff::Deleted);
        counts_atomic.increment(false, &NodeDiff::Changed);
        counts_atomic.increment(true, &NodeDiff::Changed);
        counts_atomic.increment(false, &NodeDiff::Unchanged);
        counts_atomic.increment(true, &NodeDiff::Unchanged);

        let counts = counts_atomic.snapshot();
        assert_eq!(counts.new_files, 1);
        assert_eq!(counts.new_dirs, 1);
        assert_eq!(counts.deleted_files, 1);
        assert_eq!(counts.deleted_dirs, 1);
        assert_eq!(counts.changed_files, 1);
        assert_eq!(counts.changed_dirs, 1);
        assert_eq!(counts.unchanged_files, 1);
        assert_eq!(counts.unchanged_dirs, 1);
    }

    #[test]
    fn test_snapshot_has_tags() {
        let mut snapshot = Snapshot::default();
        snapshot.tags.insert("tag1".to_string());
        snapshot.tags.insert("tag2".to_string());

        let mut search_tags = BTreeSet::new();
        search_tags.insert("tag1".to_string());
        assert!(snapshot.has_tags(&search_tags));

        search_tags.clear();
        search_tags.insert("tag3".to_string());
        assert!(!snapshot.has_tags(&search_tags));

        search_tags.clear();
        search_tags.insert(EMPTY_TAG_MARK.to_string());
        assert!(!snapshot.has_tags(&search_tags));

        let empty_snapshot = Snapshot::default();
        assert!(empty_snapshot.has_tags(&search_tags));
    }

    #[test]
    fn test_snapshot_size() {
        let mut snapshot = Snapshot::default();
        snapshot.summary.processed_bytes = 12345;
        assert_eq!(snapshot.size(), 12345);
    }
}
