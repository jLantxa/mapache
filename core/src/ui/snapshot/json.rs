use parking_lot::RwLock;
use serde::Serialize;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, atomic::AtomicU64};
use std::time::{Duration, Instant};

use crate::ui::json_reporter::JsonReporter;
use crate::{
    fs::tree::NodeDiff, mapache::global::GlobalOpts, repository::snapshot::DiffCountsAtomic,
};

use super::{SnapshotProcessSummary, SnapshotProgressReporter};

#[derive(Serialize)]
struct StatusUpdateMsg {
    processed_items: u64,
    processed_bytes: u64,
    total_items: Option<u64>,
    total_bytes: Option<u64>,
    active_files: Vec<String>,
    elapsed_seconds: f64,
}

#[derive(Serialize)]
struct ErrorMsg<'a> {
    message: &'a str,
}

pub(crate) struct JsonSnapshotProgressReporter {
    json_reporter: JsonReporter,
    diff_counts: DiffCountsAtomic,
    processed_items: AtomicU64,
    processed_bytes: AtomicU64,
    expected_items: Arc<RwLock<Option<AtomicU64>>>,
    expected_bytes: Arc<RwLock<Option<AtomicU64>>>,
    active_files: Mutex<HashSet<PathBuf>>,
    refresh_interval: Duration,
    last_update: Mutex<Instant>,
    start_time: Instant,
}

impl JsonSnapshotProgressReporter {
    pub(crate) fn new(expected_items: Option<u64>, expected_bytes: Option<u64>) -> Self {
        let json_reporter = JsonReporter::new(true); // auto-flush for progress
        let refresh_interval = GlobalOpts::progress_refresh_interval();

        Self {
            json_reporter,
            diff_counts: DiffCountsAtomic::default(),
            processed_items: AtomicU64::new(0),
            processed_bytes: AtomicU64::new(0),
            expected_items: Arc::new(RwLock::new(expected_items.map(AtomicU64::new))),
            expected_bytes: Arc::new(RwLock::new(expected_bytes.map(AtomicU64::new))),
            active_files: Mutex::new(HashSet::new()),
            refresh_interval,
            last_update: Mutex::new(Instant::now()),
            start_time: Instant::now(),
        }
    }

    fn should_emit_update(&self) -> bool {
        let mut last_update = self.last_update.lock().unwrap();
        if last_update.elapsed() >= self.refresh_interval {
            *last_update = Instant::now();
            true
        } else {
            false
        }
    }

    fn emit_status_update(&self) {
        let processed_items = self
            .processed_items
            .load(std::sync::atomic::Ordering::Relaxed);
        let processed_bytes = self
            .processed_bytes
            .load(std::sync::atomic::Ordering::Relaxed);
        let total_items = self
            .expected_items
            .read()
            .as_ref()
            .map(|a| a.load(std::sync::atomic::Ordering::Relaxed));
        let total_bytes = self
            .expected_bytes
            .read()
            .as_ref()
            .map(|a| a.load(std::sync::atomic::Ordering::Relaxed));

        let active_files = self
            .active_files
            .lock()
            .unwrap()
            .iter()
            .map(|p| p.display().to_string())
            .collect::<Vec<_>>();

        let elapsed_seconds = self.start_time.elapsed().as_secs_f64();

        self.json_reporter.emit(
            "status_update",
            &StatusUpdateMsg {
                processed_items,
                processed_bytes,
                total_items,
                total_bytes,
                active_files,
                elapsed_seconds,
            },
        );
    }
}

impl SnapshotProgressReporter for JsonSnapshotProgressReporter {
    fn processing_node(&self, path: PathBuf, _diff: NodeDiff) {
        {
            let mut active = self.active_files.lock().unwrap();
            active.insert(path);
        }
        if self.should_emit_update() {
            self.emit_status_update();
        }
    }

    fn processed_node(&self, path: PathBuf, _diff: NodeDiff) {
        self.processed_items
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        {
            let mut active = self.active_files.lock().unwrap();
            active.remove(&path);
        }
        if self.should_emit_update() {
            self.emit_status_update();
        }
    }

    fn processed_bytes(&self, bytes: u64) {
        self.processed_bytes
            .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
        if self.should_emit_update() {
            self.emit_status_update();
        }
    }

    fn add_expected_items(&self, val: u64) {
        let mut lock = self.expected_items.write();
        match lock.as_ref() {
            Some(a) => {
                a.fetch_add(val, std::sync::atomic::Ordering::Relaxed);
            }
            None => {
                let _ = lock.insert(AtomicU64::new(val));
            }
        }
    }

    fn add_expected_bytes(&self, val: u64) {
        let mut lock = self.expected_bytes.write();
        match lock.as_ref() {
            Some(a) => {
                a.fetch_add(val, std::sync::atomic::Ordering::Relaxed);
            }
            None => {
                let _ = lock.insert(AtomicU64::new(val));
            }
        }
    }

    fn scan_finished(&self) {
        // Could emit a message here if needed
    }

    fn new_file(&self) {
        self.diff_counts
            .new_files
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn changed_file(&self) {
        self.diff_counts
            .changed_files
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn unchanged_file(&self) {
        self.diff_counts
            .unchanged_files
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn deleted_file(&self) {
        self.diff_counts
            .deleted_files
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn new_dir(&self) {
        self.diff_counts
            .new_dirs
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn changed_dir(&self) {
        self.diff_counts
            .changed_dirs
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn deleted_dir(&self) {
        self.diff_counts
            .deleted_dirs
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn unchanged_dir(&self) {
        self.diff_counts
            .unchanged_dirs
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn error(&self, msg: &str) {
        self.json_reporter.emit("error", &ErrorMsg { message: msg });
    }

    fn finalize(&self) {
        // Emit final status update
        self.emit_status_update();
        self.json_reporter.flush();
    }

    fn summary(&self) -> SnapshotProcessSummary {
        SnapshotProcessSummary {
            processed_items_count: self
                .processed_items
                .load(std::sync::atomic::Ordering::Relaxed),
            processed_bytes: self
                .processed_bytes
                .load(std::sync::atomic::Ordering::Relaxed),
            diff_counts: self.diff_counts.snapshot(),
        }
    }
}
