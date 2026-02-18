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

/// A snapshot stream that loads Snapshots on demand.
/// Implements `Stream` to allow for functional adapters like `map`, `filter`, etc.
pub struct SnapshotStream {
    inner: Pin<Box<dyn Stream<Item = (ID, Snapshot)> + Send>>,
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
                let res = repo.load_snapshot(&id, ext.as_deref()).await;

                // Decrement the remaining counter regardless of success
                remaining_ptr.fetch_sub(1, Ordering::Relaxed);

                if let Ok(snapshot) = res {
                    yield (id, snapshot);
                } else {
                    // Log error or skip corrupted snapshot silently
                    continue;
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

    pub async fn collect_entries(self, active: bool) -> SnapshotEntryList {
        self.map(|(id, snapshot)| SnapshotEntry {
            id,
            snapshot,
            active,
        })
        .collect()
        .await
    }

    pub async fn latest(self) -> Option<(ID, Snapshot)> {
        self.fold::<Option<(ID, Snapshot)>, _, _>(None, |latest, (id, snap)| async move {
            match latest {
                Some((lid, lsnap)) if lsnap.timestamp > snap.timestamp => Some((lid, lsnap)),
                _ => Some((id, snap)),
            }
        })
        .await
    }
}

impl Stream for SnapshotStream {
    type Item = (ID, Snapshot);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}
