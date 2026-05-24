use std::{
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use serde::Serialize;

use crate::{
    mapache::global::GlobalOpts,
    ui::{RestoreProgressReporter, json::JsonReporter},
};

#[derive(Serialize)]
struct RestoreStatusUpdateMsg {
    stage: String,
    processed_items: u64,
    processed_bytes: u64,
    total_items: u64,
    total_bytes: u64,
    errors: u64,
    warnings: u64,
    elapsed_seconds: f64,
}

#[derive(Serialize)]
struct ErrorMsg<'a> {
    message: &'a str,
}

#[derive(Serialize)]
struct WarningMsg<'a> {
    message: &'a str,
}

pub struct JsonRestoreProgressReporter {
    json_reporter: JsonReporter,
    processed_items_count: AtomicU64,
    processed_bytes_count: AtomicU64,
    error_counter: AtomicU64,
    warning_counter: AtomicU64,
    current_stage: Arc<Mutex<String>>,
    last_update: Arc<Mutex<Instant>>,
    start_time: Instant,
    num_expected_items: AtomicU64,
    num_expected_bytes: AtomicU64,
}

impl JsonRestoreProgressReporter {
    pub(crate) fn new(num_expected_items: Option<u64>, num_expected_bytes: Option<u64>) -> Self {
        Self {
            json_reporter: JsonReporter::new(true),
            processed_items_count: AtomicU64::new(0),
            processed_bytes_count: AtomicU64::new(0),
            error_counter: AtomicU64::new(0),
            warning_counter: AtomicU64::new(0),
            current_stage: Arc::new(Mutex::new(String::new())),
            last_update: Arc::new(Mutex::new(Instant::now())),
            start_time: Instant::now(),
            num_expected_items: AtomicU64::new(num_expected_items.unwrap_or(0)),
            num_expected_bytes: AtomicU64::new(num_expected_bytes.unwrap_or(0)),
        }
    }

    fn should_emit_update(&self) -> bool {
        let mut last_update = self.last_update.lock().unwrap();
        if last_update.elapsed() >= GlobalOpts::progress_refresh_interval() {
            *last_update = Instant::now();
            return true;
        }
        false
    }

    fn emit_status_update(&self) {
        let stage = self.current_stage.lock().unwrap().clone();
        let processed_items = self.processed_items_count.load(Ordering::Relaxed);
        let processed_bytes = self.processed_bytes_count.load(Ordering::Relaxed);
        let errors = self.error_counter.load(Ordering::Relaxed);
        let warnings = self.warning_counter.load(Ordering::Relaxed);
        let total_items = self.num_expected_items.load(Ordering::Relaxed);
        let total_bytes = self.num_expected_bytes.load(Ordering::Relaxed);

        self.json_reporter.emit(
            "restore_status",
            &RestoreStatusUpdateMsg {
                stage,
                processed_items,
                processed_bytes,
                total_items,
                total_bytes,
                errors,
                warnings,
                elapsed_seconds: self.start_time.elapsed().as_secs_f64(),
            },
        );
    }
}

impl RestoreProgressReporter for JsonRestoreProgressReporter {
    fn set_message(&self, msg: String) {
        let mut stage = self.current_stage.lock().unwrap();
        *stage = msg;

        if self.should_emit_update() {
            self.emit_status_update();
        }
    }

    fn resize_workload(&self, num_expected_items: u64, num_expected_bytes: u64) {
        self.num_expected_items
            .store(num_expected_items, Ordering::Relaxed);
        self.num_expected_bytes
            .store(num_expected_bytes, Ordering::Relaxed);
        self.emit_status_update();
    }

    fn processed_item(&self, _path: &Path) {
        self.processed_items_count.fetch_add(1, Ordering::Relaxed);
        if self.should_emit_update() {
            self.emit_status_update();
        }
    }

    fn processed_bytes(&self, bytes: u64) {
        self.processed_bytes_count
            .fetch_add(bytes, Ordering::Relaxed);
        if self.should_emit_update() {
            self.emit_status_update();
        }
    }

    fn error(&self, msg: &str) {
        self.error_counter.fetch_add(1, Ordering::Relaxed);
        self.json_reporter.emit("error", &ErrorMsg { message: msg });
    }

    fn warning(&self, msg: &str) {
        self.warning_counter.fetch_add(1, Ordering::Relaxed);
        self.json_reporter
            .emit("warning", &WarningMsg { message: msg });
    }

    fn error_count(&self) -> u64 {
        self.error_counter.load(Ordering::Relaxed)
    }

    fn warning_count(&self) -> u64 {
        self.warning_counter.load(Ordering::Relaxed)
    }

    fn log(&self, msg: String) {
        #[derive(Serialize)]
        struct LogMsg {
            message: String,
        }
        self.json_reporter.emit("log", &LogMsg { message: msg });
    }

    fn verbose_1(&self, msg: String) {
        #[derive(Serialize)]
        struct VerboseMsg {
            message: String,
            level: u32,
        }
        if GlobalOpts::verbosity() >= 2 {
            self.json_reporter.emit(
                "verbose",
                &VerboseMsg {
                    message: msg,
                    level: 1,
                },
            );
        }
    }

    fn verbose_2(&self, msg: String) {
        #[derive(Serialize)]
        struct VerboseMsg {
            message: String,
            level: u32,
        }
        if GlobalOpts::verbosity() >= 3 {
            self.json_reporter.emit(
                "verbose",
                &VerboseMsg {
                    message: msg,
                    level: 2,
                },
            );
        }
    }

    fn finalize(&self) {
        self.emit_status_update();
        self.json_reporter.flush();
    }
}
