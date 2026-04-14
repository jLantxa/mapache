pub(crate) mod cli;
pub(crate) mod json;

pub use cli::CliRestoreProgressReporter;
pub use json::JsonRestoreProgressReporter;

use std::path::Path;

pub trait RestoreProgressReporter: Send + Sync {
    fn set_message(&self, msg: String);
    fn resize_workload(&self, num_expected_items: u64, num_expected_bytes: u64);
    fn processed_item(&self, path: &Path);
    fn processed_bytes(&self, bytes: u64);
    fn error(&self, msg: &str);
    fn warning(&self, msg: &str);
    fn error_count(&self) -> u64;
    fn warning_count(&self) -> u64;
    fn finalize(&self);
}
