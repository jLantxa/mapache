use std::{
    collections::HashSet,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use parking_lot::{Mutex, RwLock};
use serde::Serialize;

use crate::{
    mapache::{defaults, global::GlobalOpts},
    ui::{
        events::{BackupEvent, Event, EventSender},
        json::JsonReporter,
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

pub(crate) fn make_event_sender(
    expected_items_val: Option<u64>,
    expected_bytes_val: Option<u64>,
) -> EventSender {
    let json_reporter = JsonReporter::new(true);
    let refresh_interval = GlobalOpts::progress_refresh_interval();

    let state = Arc::new(JsonSnapshotState {
        json_reporter,
        processed_items: AtomicU64::new(0),
        processed_bytes: AtomicU64::new(0),
        scan_finished: AtomicBool::new(
            expected_bytes_val.is_some() && expected_items_val.is_some(),
        ),
        expected_items: Arc::new(RwLock::new(expected_items_val.map(AtomicU64::new))),
        expected_bytes: Arc::new(RwLock::new(expected_bytes_val.map(AtomicU64::new))),
        active_files: Mutex::new(HashSet::new()),
        sampled_paths: Mutex::new(Vec::new()),
        refresh_interval,
        last_update: Mutex::new(Instant::now()),
        start_time: Instant::now(),
        sampling_limit: 8,
        sampling_interval_ns: refresh_interval.as_nanos() as u64,
        sampling_last_reset_ns: AtomicU64::new(0),
        sampling_count: AtomicU64::new(0),
    });

    Arc::new(move |event: Event| {
        let Event::Backup(ev) = event else { return };
        match ev {
            BackupEvent::ScanProgress { items, bytes } => {
                let mut lock = state.expected_items.write();
                match lock.as_ref() {
                    Some(a) => {
                        a.fetch_add(items, Ordering::Relaxed);
                    }
                    None => {
                        let _ = lock.insert(AtomicU64::new(items));
                    }
                }
                drop(lock);
                let mut lock = state.expected_bytes.write();
                match lock.as_ref() {
                    Some(a) => {
                        a.fetch_add(bytes, Ordering::Relaxed);
                    }
                    None => {
                        let _ = lock.insert(AtomicU64::new(bytes));
                    }
                }
                if should_emit_update(&state) {
                    emit_status_update(&state);
                }
            }
            BackupEvent::ScanFinished { .. } => {
                state.scan_finished.store(true, Ordering::Relaxed);
                if should_emit_update(&state) {
                    emit_status_update(&state);
                }
            }
            BackupEvent::NodeProcessing {
                ref path,
                size_hint,
                ..
            } => {
                let is_slow = defaults::runtime()
                    .ui_snapshot_progress_item_min_size
                    .is_none_or(|t| size_hint.is_none_or(|s| s >= t));

                if is_slow {
                    let mut active = state.active_files.lock();
                    active.insert(path.to_path_buf());
                } else {
                    let elapsed_ns = state.start_time.elapsed().as_nanos() as u64;
                    let last_reset = state.sampling_last_reset_ns.load(Ordering::Relaxed);

                    if elapsed_ns.saturating_sub(last_reset) >= state.sampling_interval_ns {
                        if state
                            .sampling_last_reset_ns
                            .compare_exchange(
                                last_reset,
                                elapsed_ns,
                                std::sync::atomic::Ordering::SeqCst,
                                Ordering::Relaxed,
                            )
                            .is_ok()
                        {
                            state.sampling_count.store(1, Ordering::Relaxed);
                            let mut guard = state.sampled_paths.lock();
                            guard.clear();
                            guard.push(path.to_path_buf());
                        }
                    } else {
                        let count = state.sampling_count.fetch_add(1, Ordering::Relaxed);
                        if (count as usize) < state.sampling_limit {
                            let mut guard = state.sampled_paths.lock();
                            guard.push(path.to_path_buf());
                        }
                    }
                }
                if should_emit_update(&state) {
                    emit_status_update(&state);
                }
            }
            BackupEvent::NodeProcessed {
                ref path,
                size_hint,
                ..
            } => {
                state.processed_items.fetch_add(1, Ordering::Relaxed);

                let is_slow = defaults::runtime()
                    .ui_snapshot_progress_item_min_size
                    .is_none_or(|t| size_hint.is_none_or(|s| s >= t));

                if is_slow {
                    let mut active = state.active_files.lock();
                    active.remove(path);
                }
                if should_emit_update(&state) {
                    emit_status_update(&state);
                }
            }
            BackupEvent::BytesProcessed(bytes) => {
                state.processed_bytes.fetch_add(bytes, Ordering::Relaxed);
                if should_emit_update(&state) {
                    emit_status_update(&state);
                }
            }
            BackupEvent::Error(ref msg) => {
                state
                    .json_reporter
                    .emit("error", &ErrorMsg { message: msg });
            }
            BackupEvent::Warning(ref msg) => {
                state
                    .json_reporter
                    .emit("warning", &ErrorMsg { message: msg });
            }
            BackupEvent::Log(ref msg) => {
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
            BackupEvent::Finished(_) => {
                emit_status_update(&state);
                state.json_reporter.flush();
            }
            BackupEvent::ScanStarted => {}
        }
    })
}

struct JsonSnapshotState {
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
    sampling_limit: usize,
    sampling_interval_ns: u64,
    sampling_last_reset_ns: AtomicU64,
    sampling_count: AtomicU64,
}

fn should_emit_update(state: &JsonSnapshotState) -> bool {
    let mut last_update = state.last_update.lock();
    if last_update.elapsed() >= state.refresh_interval {
        *last_update = Instant::now();
        true
    } else {
        false
    }
}

fn emit_status_update(state: &JsonSnapshotState) {
    let processed_items = state.processed_items.load(Ordering::Relaxed);
    let processed_bytes = state.processed_bytes.load(Ordering::Relaxed);
    let total_items = state
        .expected_items
        .read()
        .as_ref()
        .map(|a| a.load(Ordering::Relaxed));
    let total_bytes = state
        .expected_bytes
        .read()
        .as_ref()
        .map(|a| a.load(Ordering::Relaxed));

    let mut active_files_vec = {
        let active = state.active_files.lock();
        active
            .iter()
            .map(|p| p.display().to_string())
            .collect::<Vec<_>>()
    };

    {
        let mut sampled = state.sampled_paths.lock();
        for p in sampled.drain(..) {
            active_files_vec.push(p.display().to_string());
        }
    }

    let elapsed_seconds = state.start_time.elapsed().as_secs_f64();
    let scan_finished = state.scan_finished.load(Ordering::Relaxed);

    state.json_reporter.emit(
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
