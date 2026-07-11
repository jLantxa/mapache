use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use parking_lot::Mutex;
use serde::Serialize;

use crate::{
    common::global::GlobalOpts,
    ui::events::{Event, EventSender, RestoreEvent},
    ui::json::JsonReporter,
};

#[derive(Serialize)]
struct RestoreStatusUpdateMsg {
    stage: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    visited_nodes: Option<u64>,
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

struct JsonRestoreState {
    json_reporter: JsonReporter,
    visited_nodes: AtomicU64,
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

impl JsonRestoreState {
    fn should_emit_update(&self) -> bool {
        let mut last_update = self.last_update.lock();
        if last_update.elapsed() >= GlobalOpts::progress_refresh_interval() {
            *last_update = Instant::now();
            return true;
        }
        false
    }

    fn emit_status_update(&self) {
        let stage = self.current_stage.lock().clone();
        let visited = self.visited_nodes.load(Ordering::Relaxed);
        let visited_nodes = if visited > 0 { Some(visited) } else { None };
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
                visited_nodes,
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

pub(crate) fn make_event_sender(
    num_expected_items: Option<u64>,
    num_expected_bytes: Option<u64>,
) -> (
    EventSender,
    Arc<AtomicU64>,
    Arc<AtomicU64>,
    Arc<AtomicU64>,
    Arc<AtomicU64>,
) {
    let error_counter = Arc::new(AtomicU64::new(0));
    let warning_counter = Arc::new(AtomicU64::new(0));
    let processed_items_count = Arc::new(AtomicU64::new(0));
    let processed_bytes_count = Arc::new(AtomicU64::new(0));

    let state = Arc::new(JsonRestoreState {
        json_reporter: JsonReporter::new(true),
        visited_nodes: AtomicU64::new(0),
        processed_items_count: AtomicU64::new(0),
        processed_bytes_count: AtomicU64::new(0),
        error_counter: AtomicU64::new(0),
        warning_counter: AtomicU64::new(0),
        current_stage: Arc::new(Mutex::new(String::new())),
        last_update: Arc::new(Mutex::new(Instant::now())),
        start_time: Instant::now(),
        num_expected_items: AtomicU64::new(num_expected_items.unwrap_or(0)),
        num_expected_bytes: AtomicU64::new(num_expected_bytes.unwrap_or(0)),
    });

    let sender: EventSender = Arc::new(move |event: Event| {
        let Event::Restore(ev) = event else { return };
        match ev {
            RestoreEvent::NodeVisited(count) => {
                state.visited_nodes.store(count, Ordering::Relaxed);
                if state.should_emit_update() {
                    state.emit_status_update();
                }
            }
            RestoreEvent::PlanBuilt {
                total_items: t,
                total_bytes: b,
            } => {
                state.visited_nodes.store(0, Ordering::Relaxed);
                state.num_expected_items.store(t, Ordering::Relaxed);
                state.num_expected_bytes.store(b, Ordering::Relaxed);
                state.emit_status_update();
            }
            RestoreEvent::ItemProcessed(_) => {
                state.processed_items_count.fetch_add(1, Ordering::Relaxed);
                if state.should_emit_update() {
                    state.emit_status_update();
                }
            }
            RestoreEvent::BytesProcessed(bytes) => {
                state
                    .processed_bytes_count
                    .fetch_add(bytes, Ordering::Relaxed);
                if state.should_emit_update() {
                    state.emit_status_update();
                }
            }
            RestoreEvent::BlobsSkipped { count, bytes } => {
                #[derive(Serialize)]
                struct BlobsSkippedMsg {
                    count: u64,
                    bytes: u64,
                }
                state
                    .json_reporter
                    .emit("blobs_skipped", &BlobsSkippedMsg { count, bytes });
            }
            RestoreEvent::Warning(ref msg) => {
                state.warning_counter.fetch_add(1, Ordering::Relaxed);
                state
                    .json_reporter
                    .emit("warning", &WarningMsg { message: msg });
            }
            RestoreEvent::Error(ref msg) => {
                state.error_counter.fetch_add(1, Ordering::Relaxed);
                state
                    .json_reporter
                    .emit("error", &ErrorMsg { message: msg });
            }
            RestoreEvent::Log(ref msg) => {
                #[derive(Serialize)]
                struct LogMsg {
                    message: String,
                }
                state.json_reporter.emit(
                    "log",
                    &LogMsg {
                        message: msg.clone(),
                    },
                );
            }
            RestoreEvent::Finished => {
                state.emit_status_update();
                state.json_reporter.flush();
            }
        }
    });

    (
        sender,
        error_counter,
        warning_counter,
        processed_items_count,
        processed_bytes_count,
    )
}
