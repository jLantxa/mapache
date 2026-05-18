use std::{
    collections::HashSet,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use parking_lot::RwLock;
use serde::Serialize;

use crate::{
    mapache::defaults,
    {
        fs::tree::NodeDiff,
        mapache::global::GlobalOpts,
        ui::{json_reporter::JsonReporter, snapshot::SnapshotProgressReporter},
    },
};

#[derive(Serialize)]
struct StatusUpdateMsg {
    processed_items: u64,
    processed_bytes: u64,
    scan_finished: bool,
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
    processed_items: AtomicU64,
    processed_bytes: AtomicU64,
    scan_finished: AtomicBool,
    expected_items: Arc<RwLock<Option<AtomicU64>>>,
    expected_bytes: Arc<RwLock<Option<AtomicU64>>>,
    active_files: Mutex<HashSet<PathBuf>>,
    sampled_paths: Mutex<Vec<PathBuf>>,
    refresh_interval: Duration,
    last_update: Mutex<Instant>,
    start_time: Instant,

    // Sampling budget (Global across threads)
    sampling_limit: usize,
    sampling_interval_ns: u64,
    sampling_last_reset_ns: AtomicU64,
    sampling_count: AtomicU64,
}

impl JsonSnapshotProgressReporter {
    pub(crate) fn new(expected_items: Option<u64>, expected_bytes: Option<u64>) -> Self {
        let json_reporter = JsonReporter::new(true); // auto-flush for progress
        let refresh_interval = GlobalOpts::progress_refresh_interval();

        Self {
            json_reporter,
            processed_items: AtomicU64::new(0),
            processed_bytes: AtomicU64::new(0),
            expected_items: Arc::new(RwLock::new(expected_items.map(AtomicU64::new))),
            expected_bytes: Arc::new(RwLock::new(expected_bytes.map(AtomicU64::new))),
            active_files: Mutex::new(HashSet::new()),
            sampled_paths: Mutex::new(Vec::new()),
            refresh_interval,
            last_update: Mutex::new(Instant::now()),
            start_time: Instant::now(),
            scan_finished: AtomicBool::new(expected_bytes.is_some() && expected_items.is_some()),
            sampling_limit: 8, // Default budget for JSON status updates
            sampling_interval_ns: refresh_interval.as_nanos() as u64,
            sampling_last_reset_ns: AtomicU64::new(0),
            sampling_count: AtomicU64::new(0),
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
        let processed_items = self.processed_items.load(Ordering::Relaxed);
        let processed_bytes = self.processed_bytes.load(Ordering::Relaxed);
        let total_items = self
            .expected_items
            .read()
            .as_ref()
            .map(|a| a.load(Ordering::Relaxed));
        let total_bytes = self
            .expected_bytes
            .read()
            .as_ref()
            .map(|a| a.load(Ordering::Relaxed));

        let mut active_files_vec = {
            let active = self.active_files.lock().unwrap();
            active
                .iter()
                .map(|p| p.display().to_string())
                .collect::<Vec<_>>()
        };

        // Add sampled paths to the list
        {
            let mut sampled = self.sampled_paths.lock().unwrap();
            for p in sampled.drain(..) {
                active_files_vec.push(p.display().to_string());
            }
        }

        let elapsed_seconds = self.start_time.elapsed().as_secs_f64();

        let scan_finished = self.scan_finished.load(Ordering::Relaxed);

        self.json_reporter.emit(
            "status_update",
            &StatusUpdateMsg {
                processed_items,
                processed_bytes,
                total_items,
                total_bytes,
                active_files: active_files_vec,
                elapsed_seconds,
                scan_finished,
            },
        );
    }
}

impl SnapshotProgressReporter for JsonSnapshotProgressReporter {
    fn processing_node(&self, path: &std::path::Path, _diff: NodeDiff, size_hint: Option<u64>) {
        let is_slow = defaults::runtime()
            .ui_snapshot_progress_item_min_size
            .is_none_or(|t| size_hint.is_none_or(|s| s >= t));

        if is_slow {
            let mut active = self.active_files.lock().unwrap();
            active.insert(path.to_path_buf());
        } else {
            // Budgeted sampling for small files (Global N per T)
            let elapsed_ns = self.start_time.elapsed().as_nanos() as u64;

            let last_reset = self.sampling_last_reset_ns.load(Ordering::Relaxed);

            if elapsed_ns.saturating_sub(last_reset) >= self.sampling_interval_ns {
                // New time slot: reset budget and clear stale samples
                if self
                    .sampling_last_reset_ns
                    .compare_exchange(
                        last_reset,
                        elapsed_ns,
                        std::sync::atomic::Ordering::SeqCst,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    self.sampling_count.store(1, Ordering::Relaxed);
                    let mut guard = self.sampled_paths.lock().unwrap();
                    guard.clear();
                    guard.push(path.to_path_buf());
                }
            } else {
                // Current time slot: check remaining budget
                let count = self.sampling_count.fetch_add(1, Ordering::Relaxed);
                if (count as usize) < self.sampling_limit {
                    let mut guard = self.sampled_paths.lock().unwrap();
                    guard.push(path.to_path_buf());
                }
            }
        }
        if self.should_emit_update() {
            self.emit_status_update();
        }
    }

    fn processed_node(&self, path: &std::path::Path, _diff: NodeDiff, size_hint: Option<u64>) {
        self.processed_items.fetch_add(1, Ordering::Relaxed);

        let is_slow = defaults::runtime()
            .ui_snapshot_progress_item_min_size
            .is_none_or(|t| size_hint.is_none_or(|s| s >= t));

        if is_slow {
            let mut active = self.active_files.lock().unwrap();
            active.remove(path);
        }
        if self.should_emit_update() {
            self.emit_status_update();
        }
    }

    fn processed_bytes(&self, bytes: u64) {
        self.processed_bytes.fetch_add(bytes, Ordering::Relaxed);
        if self.should_emit_update() {
            self.emit_status_update();
        }
    }

    fn add_expected_items(&self, val: u64) {
        let mut lock = self.expected_items.write();
        match lock.as_ref() {
            Some(a) => {
                a.fetch_add(val, Ordering::Relaxed);
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
                a.fetch_add(val, Ordering::Relaxed);
            }
            None => {
                let _ = lock.insert(AtomicU64::new(val));
            }
        }
    }

    fn scan_finished(&self) {
        self.scan_finished.store(true, Ordering::Relaxed);
    }

    fn error(&self, msg: &str) {
        self.json_reporter.emit("error", &ErrorMsg { message: msg });
    }

    fn warning(&self, msg: &str) {
        self.json_reporter
            .emit("warning", &ErrorMsg { message: msg });
    }

    fn finalize(&self) {
        // Emit final status update
        self.emit_status_update();
        self.json_reporter.flush();
    }
}
