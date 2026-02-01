use std::path::PathBuf;

use crate::fs::tree::NodeDiff;
use crate::repository::snapshot::DiffCounts;

pub(crate) mod cli;
pub(crate) mod json;

/// Summary of snapshot processing statistics
#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct SnapshotProcessSummary {
    pub processed_items_count: u64,
    pub processed_bytes: u64,
    pub diff_counts: DiffCounts,
}

/// Trait for snapshot progress reporting. Implementations can provide different UI backends
/// like terminal UI, JSON output, or GUI updates.
pub trait SnapshotProgressReporter: Send + Sync {
    /// Called when starting to process a node
    fn processing_node(&self, path: PathBuf, diff: NodeDiff);

    /// Called when a node has been processed
    fn processed_node(&self, path: PathBuf, diff: NodeDiff);

    /// Called when bytes have been processed
    fn processed_bytes(&self, bytes: u64);

    /// Called when expected items count is known
    fn add_expected_items(&self, val: u64);

    /// Called when expected bytes count is known
    fn add_expected_bytes(&self, val: u64);

    /// Called when scan is finished and we know total expected bytes
    fn scan_finished(&self);

    /// Called when a new file is found
    fn new_file(&self);

    /// Called when a changed file is found
    fn changed_file(&self);

    /// Called when an unchanged file is found
    fn unchanged_file(&self);

    /// Called when a deleted file is found
    fn deleted_file(&self);

    /// Called when a new directory is found
    fn new_dir(&self);

    /// Called when a changed directory is found
    fn changed_dir(&self);

    /// Called when a deleted directory is found
    fn deleted_dir(&self);

    /// Called when an unchanged directory is found
    fn unchanged_dir(&self);

    /// Called when an error occurs
    fn error(&self, msg: &str);

    /// Finalize the reporter (cleanup resources)
    fn finalize(&self);

    /// Get the current summary of processed items
    fn summary(&self) -> SnapshotProcessSummary;
}
