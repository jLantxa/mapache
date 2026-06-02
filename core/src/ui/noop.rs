use std::path::Path;

use crate::fs::tree::NodeDiff;
use crate::ui::{GcProgressReporter, GcTask, RestoreProgressReporter, SnapshotProgressReporter};

/// A no-op implementation of `SnapshotProgressReporter`.
pub struct NoopSnapshotReporter;

impl SnapshotProgressReporter for NoopSnapshotReporter {
    fn processing_node(&self, _: &Path, _: NodeDiff, _: Option<u64>) {}
    fn processed_node(&self, _: &Path, _: NodeDiff, _: Option<u64>) {}
    fn processed_bytes(&self, _: u64) {}
    fn add_expected_items(&self, _: u64) {}
    fn add_expected_bytes(&self, _: u64) {}
    fn scan_finished(&self) {}
    fn error(&self, _: &str) {}
    fn warning(&self, _: &str) {}
    fn log(&self, _: String) {}
    fn verbose_1(&self, _: String) {}
    fn verbose_2(&self, _: String) {}
    fn finalize(&self) {}
}

/// A no-op implementation of `RestoreProgressReporter`.
pub struct NoopRestoreReporter;

impl RestoreProgressReporter for NoopRestoreReporter {
    fn set_message(&self, _: String) {}
    fn resize_workload(&self, _: u64, _: u64) {}
    fn processed_item(&self, _: &Path) {}
    fn processed_bytes(&self, _: u64) {}
    fn error(&self, _: &str) {}
    fn warning(&self, _: &str) {}
    fn error_count(&self) -> u64 {
        0
    }
    fn warning_count(&self) -> u64 {
        0
    }
    fn log(&self, _: String) {}
    fn verbose_1(&self, _: String) {}
    fn verbose_2(&self, _: String) {}
    fn finalize(&self) {}
}

/// A no-op implementation of `GcProgressReporter`.
pub struct NoopGcReporter;

impl GcProgressReporter for NoopGcReporter {
    fn log(&self, _: String) {}
    fn warning(&self, _: String) {}
    fn start_task(&self, _: GcTask, _: Option<u64>) {}
    fn update_task(&self, _: GcTask, _: u64) {}
    fn finish_task(&self, _: GcTask) {}
}
