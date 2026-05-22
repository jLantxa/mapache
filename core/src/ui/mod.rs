use std::path::Path;

use indicatif::ProgressDrawTarget;

use crate::{fs::tree::NodeDiff, repository::snapshot::DiffCounts};

pub mod cli;
pub(crate) mod debug;
pub mod json;
#[cfg(feature = "tui")]
pub mod tui;

pub(crate) const SPINNER_TICK_CHARS: &str = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏";

/// Returns the default draw target for progress bars, with a preconfigured refresh rate
/// and verbosity.
pub(crate) fn default_bar_draw_target() -> ProgressDrawTarget {
    let verbosity = crate::mapache::global::GlobalOpts::verbosity();
    let refresh_interval = crate::mapache::global::GlobalOpts::progress_refresh_interval();

    if verbosity > 0 {
        ProgressDrawTarget::stderr_with_hz((1.0 / refresh_interval.as_secs_f64()) as u8)
    } else {
        ProgressDrawTarget::hidden()
    }
}

pub trait RestoreProgressReporter: Send + Sync {
    fn set_message(&self, msg: String);
    fn resize_workload(&self, num_expected_items: u64, num_expected_bytes: u64);
    fn processed_item(&self, path: &Path);
    fn processed_bytes(&self, bytes: u64);
    fn error(&self, msg: &str);
    fn warning(&self, msg: &str);
    fn error_count(&self) -> u64;
    fn warning_count(&self) -> u64;
    fn log(&self, msg: String);
    fn finalize(&self);
}

/// Summary of snapshot processing statistics
#[derive(Debug, Clone, serde::Serialize)]
pub struct SnapshotProcessSummary {
    pub processed_items_count: u64,
    pub processed_bytes: u64,
    pub diff_counts: DiffCounts,
}

/// Trait for snapshot progress reporting. Implementations can provide different UI backends
/// like terminal UI, JSON output, or GUI updates.
pub trait SnapshotProgressReporter: Send + Sync {
    /// Called when starting to process a node
    fn processing_node(&self, path: &Path, diff: NodeDiff, size_hint: Option<u64>);

    /// Called when a node has been processed
    fn processed_node(&self, path: &Path, diff: NodeDiff, size_hint: Option<u64>);

    /// Called when bytes have been processed
    fn processed_bytes(&self, bytes: u64);

    /// Called when expected items count is known
    fn add_expected_items(&self, val: u64);

    /// Called when expected bytes count is known
    fn add_expected_bytes(&self, val: u64);

    /// Called when scan is finished and we know total expected bytes
    fn scan_finished(&self);

    /// Called when an error occurs
    fn error(&self, msg: &str);

    /// Called when a warning occurs
    fn warning(&self, msg: &str);

    /// Log a message
    fn log(&self, msg: String);

    /// Finalize the reporter (cleanup resources)
    fn finalize(&self);
}
