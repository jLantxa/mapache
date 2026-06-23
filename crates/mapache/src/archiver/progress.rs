//! The progress module provides thread-safe counters for tracking the progress
//! of a snapshot operation.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::{
    fs::tree::NodeDiff,
    repository::snapshot::{DiffCounts, DiffCountsAtomic},
};

#[derive(Debug, Clone, serde::Serialize)]
pub struct SnapshotProcessSummary {
    pub processed_items_count: u64,
    pub processed_bytes: u64,
    pub diff_counts: DiffCounts,
}
/// Centralized snapshot progress counters owned by the archiver.
#[derive(Debug, Default)]
pub(crate) struct SnapshotProgress {
    pub processed_items: AtomicU64,
    pub processed_bytes: AtomicU64,
    pub diff_counts: DiffCountsAtomic,
}

impl SnapshotProgress {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn processed_node(&self) {
        self.processed_items.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn processed_bytes(&self, bytes: u64) {
        self.processed_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub(crate) fn increment_diff(&self, is_dir: bool, diff: &NodeDiff) {
        self.diff_counts.increment(is_dir, diff);
    }

    pub(crate) fn summary(&self) -> SnapshotProcessSummary {
        SnapshotProcessSummary {
            processed_items_count: self.processed_items.load(Ordering::Relaxed),
            processed_bytes: self.processed_bytes.load(Ordering::Relaxed),
            diff_counts: self.diff_counts.snapshot(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::fs::tree::NodeDiff;

    #[test]
    fn test_empty_progress() {
        let progress = SnapshotProgress::new();
        let summary = progress.summary();
        assert_eq!(summary.processed_items_count, 0);
        assert_eq!(summary.processed_bytes, 0);
        assert_eq!(summary.diff_counts.new_files, 0);
        assert_eq!(summary.diff_counts.deleted_files, 0);
        assert_eq!(summary.diff_counts.changed_files, 0);
        assert_eq!(summary.diff_counts.unchanged_files, 0);
        assert_eq!(summary.diff_counts.new_dirs, 0);
        assert_eq!(summary.diff_counts.deleted_dirs, 0);
        assert_eq!(summary.diff_counts.changed_dirs, 0);
        assert_eq!(summary.diff_counts.unchanged_dirs, 0);
    }

    #[test]
    fn test_single_node_progress() {
        let progress = SnapshotProgress::new();
        progress.processed_node();
        progress.processed_bytes(42);
        progress.increment_diff(false, &NodeDiff::New);
        let summary = progress.summary();
        assert_eq!(summary.processed_items_count, 1);
        assert_eq!(summary.processed_bytes, 42);
        assert_eq!(summary.diff_counts.new_files, 1);
    }

    #[test]
    fn test_concurrent_progress() {
        let progress = Arc::new(SnapshotProgress::new());
        let mut handles = Vec::new();
        for _ in 0..10 {
            let p = progress.clone();
            handles.push(std::thread::spawn(move || {
                for _ in 0..100 {
                    p.processed_node();
                    p.processed_bytes(10);
                    p.increment_diff(false, &NodeDiff::New);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let summary = progress.summary();
        assert_eq!(summary.processed_items_count, 1000);
        assert_eq!(summary.processed_bytes, 10000);
        assert_eq!(summary.diff_counts.new_files, 1000);
    }

    #[test]
    fn test_all_diff_types() {
        let progress = SnapshotProgress::new();
        progress.increment_diff(false, &NodeDiff::New);
        progress.increment_diff(false, &NodeDiff::Deleted);
        progress.increment_diff(false, &NodeDiff::Changed);
        progress.increment_diff(false, &NodeDiff::Unchanged);
        progress.increment_diff(true, &NodeDiff::New);
        progress.increment_diff(true, &NodeDiff::Deleted);
        progress.increment_diff(true, &NodeDiff::Changed);
        progress.increment_diff(true, &NodeDiff::Unchanged);

        let s = progress.summary();
        assert_eq!(s.diff_counts.new_files, 1);
        assert_eq!(s.diff_counts.deleted_files, 1);
        assert_eq!(s.diff_counts.changed_files, 1);
        assert_eq!(s.diff_counts.unchanged_files, 1);
        assert_eq!(s.diff_counts.new_dirs, 1);
        assert_eq!(s.diff_counts.deleted_dirs, 1);
        assert_eq!(s.diff_counts.changed_dirs, 1);
        assert_eq!(s.diff_counts.unchanged_dirs, 1);
    }
}
