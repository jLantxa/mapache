use std::sync::atomic::{AtomicU64, Ordering};

use crate::{
    fs::tree::NodeDiff, repository::snapshot::DiffCountsAtomic,
    ui::snapshot::SnapshotProcessSummary,
};
/// Centralized snapshot progress counters owned by the archiver.
#[derive(Debug, Default)]
pub struct SnapshotProgress {
    pub processed_items: AtomicU64,
    pub processed_bytes: AtomicU64,
    pub diff_counts: DiffCountsAtomic,
}

impl SnapshotProgress {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn processed_node(&self) {
        self.processed_items.fetch_add(1, Ordering::Relaxed);
    }

    pub fn processed_bytes(&self, bytes: u64) {
        self.processed_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn increment_diff(&self, is_dir: bool, diff: &NodeDiff) {
        self.diff_counts.increment(is_dir, diff);
    }

    pub fn summary(&self) -> SnapshotProcessSummary {
        SnapshotProcessSummary {
            processed_items_count: self.processed_items.load(Ordering::Relaxed),
            processed_bytes: self.processed_bytes.load(Ordering::Relaxed),
            diff_counts: self.diff_counts.snapshot(),
        }
    }
}
