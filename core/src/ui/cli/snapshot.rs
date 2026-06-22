use std::{
    collections::VecDeque,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use crossbeam_channel::{Receiver, Sender};
use indicatif::{MultiProgress, ProgressBar, ProgressState, ProgressStyle};
use parking_lot::Mutex;

use crate::{
    fs::{abbreviate_path, tree::NodeDiff},
    mapache::{defaults, global::GlobalOpts},
    ui::{
        SPINNER_TICK_CHARS,
        cli::color::Colorize,
        default_bar_draw_target,
        events::{BackupEvent, Event, EventSender},
    },
    utils::{self, rate_estimator::RateEstimator},
};

enum UiEvent {
    Start(PathBuf),
    Done(PathBuf),
    Shutdown,
}

fn ui_loop(
    rx: Receiver<UiEvent>,
    update_interval: Duration,
    spinners: Vec<ProgressBar>,
    sampled_paths: Arc<Mutex<VecDeque<PathBuf>>>,
) {
    let slots_limit = spinners.len();
    let mut active: Vec<PathBuf> = Vec::with_capacity(slots_limit);
    let mut display_queue: VecDeque<PathBuf> = VecDeque::with_capacity(slots_limit);

    let mut last_redraw = Instant::now();

    loop {
        let ev_res = rx.recv_timeout(update_interval);
        match ev_res {
            Ok(UiEvent::Start(path)) => {
                if !active.contains(&path) {
                    active.push(path);
                }
            }
            Ok(UiEvent::Done(path)) => {
                if let Some(pos) = active.iter().position(|x| x == &path) {
                    active.remove(pos);
                }
            }
            Ok(UiEvent::Shutdown) => break,
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
        }

        let now = Instant::now();
        if rx.is_empty() || now.duration_since(last_redraw) >= update_interval {
            {
                let mut guard = sampled_paths.lock();
                while let Some(path) = guard.pop_front() {
                    display_queue.push_back(path);
                    if display_queue.len() > slots_limit {
                        display_queue.pop_front();
                    }
                }
            }

            for (i, spinner) in spinners.iter().enumerate().take(slots_limit) {
                let path = if i < active.len() {
                    active[i].clone()
                } else {
                    display_queue.pop_front().unwrap_or_default()
                };

                spinner.set_message(abbreviate_path(
                    &path,
                    defaults::runtime().max_path_display_len,
                ));
            }
            last_redraw = now;
        }
    }
}

struct CliSnapshotState {
    mp: MultiProgress,
    progress_bar: ProgressBar,
    companion_bar: ProgressBar,
    file_spinners: Vec<ProgressBar>,

    error_counter: Arc<AtomicU64>,
    warning_counter: Arc<AtomicU64>,
    determined_style: ProgressStyle,

    verbosity: u32,

    ui_tx: Sender<UiEvent>,

    sampled_paths: Arc<Mutex<VecDeque<PathBuf>>>,
    sampling_limit: usize,
    sampling_interval_ns: u64,
    sampling_last_reset_ns: AtomicU64,
    sampling_count: AtomicU64,
    start_time: Instant,

    rate_estimator: Arc<Mutex<RateEstimator>>,

    ui_thread: Mutex<Option<JoinHandle<()>>>,
}

impl CliSnapshotState {
    fn finalize(&self) {
        let _ = self.ui_tx.try_send(UiEvent::Shutdown);

        if let Some(h) = self.ui_thread.lock().take() {
            let _ = h.join();
        }

        for sp in self.file_spinners.iter() {
            sp.finish_and_clear();
        }
        self.companion_bar.finish_and_clear();
        self.progress_bar.finish_and_clear();
        let _ = self.mp.clear();
    }
}

pub fn make_event_sender(
    expected_items_val: Option<u64>,
    expected_size_val: Option<u64>,
    num_display_items: usize,
) -> EventSender {
    let verbosity = GlobalOpts::verbosity();
    let refresh_interval = GlobalOpts::progress_refresh_interval();
    let mp = MultiProgress::with_draw_target(default_bar_draw_target());

    let error_counter = Arc::new(AtomicU64::new(0));
    let warning_counter = Arc::new(AtomicU64::new(0));

    let progress_bar = match expected_size_val {
        Some(size) => mp.add(ProgressBar::new(size)),
        None => mp.add(ProgressBar::no_length()),
    };
    progress_bar.enable_steady_tick(refresh_interval);

    let companion_bar = mp.add(ProgressBar::no_length());
    companion_bar.enable_steady_tick(refresh_interval);
    if let Some(items) = expected_items_val {
        companion_bar.set_length(items);
    }

    let rate_estimator = Arc::new(Mutex::new(RateEstimator::new(
        defaults::UI_RATE_ESTIMATOR_WINDOW,
    )));

    let base_style = ProgressStyle::default_bar()
        .progress_chars("=> ")
        .with_key(
            "custom_elapsed",
            |state: &ProgressState, w: &mut dyn std::fmt::Write| {
                let _ = w.write_str(&utils::pretty_print_duration(state.elapsed()));
            },
        )
        .with_key("data_rate", {
            let re = rate_estimator.clone();
            move |_state: &ProgressState, w: &mut dyn std::fmt::Write| {
                let rate = re.lock().rate().floor() as u64;
                let _ = w.write_str(&utils::format_size_binary(rate, 1));
            }
        })
        .with_key(
            "processed_bytes_fmt",
            |state: &ProgressState, w: &mut dyn std::fmt::Write| {
                let bytes = state.pos();
                let processed_bytes_str = utils::format_size_binary(bytes, 3);

                match state.len() {
                    None => {
                        let _ = w.write_str(&processed_bytes_str);
                    }
                    Some(total) => {
                        if bytes >= total {
                            let _ = w.write_str(&processed_bytes_str);
                        } else {
                            let _ = write!(
                                w,
                                "{} / {}",
                                processed_bytes_str,
                                utils::format_size_binary(total, 3)
                            );
                        }
                    }
                }
            },
        )
        .with_key("custom_eta", {
            let re = rate_estimator.clone();
            move |state: &ProgressState, w: &mut dyn std::fmt::Write| {
                let pos = state.pos() as f64;
                let total = state.len().map(|l| l as f64);
                match re.lock().eta(pos, total.unwrap_or(pos)) {
                    Some(d) => {
                        let _ = w.write_str(&utils::pretty_print_duration(d));
                    }
                    None => {
                        let _ = w.write_str("--");
                    }
                }
            }
        });

    let determined_style = base_style
        .clone()
        .template("[{percent} %] [{bar:20.cyan/white}] [{custom_elapsed}] [{processed_bytes_fmt}] [{data_rate}/s] [ETA: {custom_eta}]")
        .expect("Invalid progress bar template for snapshot progress (determined)");

    let undetermined_style = base_style
        .clone()
        .template("[{custom_elapsed}] [{processed_bytes_fmt}] [{data_rate}/s]")
        .expect("Invalid progress bar template for snapshot progress (undetermined)");

    progress_bar.set_style(undetermined_style.clone());

    let error_counter_for_style = Arc::clone(&error_counter);
    let warning_counter_for_style = Arc::clone(&warning_counter);
    companion_bar.set_style(
        ProgressStyle::default_bar()
            .template("[{items_info}] [{errors} errors, {warnings} warnings]")
            .expect("Invalid progress bar template for snapshot companion bar")
            .with_key(
                "items_info",
                |state: &ProgressState, w: &mut dyn std::fmt::Write| {
                    let pos: u64 = state.pos();
                    match state.len() {
                        None => {
                            let _ = write!(w, "{} items", pos);
                        }
                        Some(len) => {
                            if pos >= len {
                                let _ = write!(w, "{} items", pos);
                            } else {
                                let _ = write!(w, "{} / {} items", pos, len);
                            }
                        }
                    }
                },
            )
            .with_key(
                "errors",
                move |_state: &ProgressState, w: &mut dyn std::fmt::Write| {
                    let count = error_counter_for_style.load(Ordering::Relaxed);
                    let _ = write!(w, "{}", count);
                },
            )
            .with_key(
                "warnings",
                move |_state: &ProgressState, w: &mut dyn std::fmt::Write| {
                    let count = warning_counter_for_style.load(Ordering::Relaxed);
                    let _ = write!(w, "{}", count);
                },
            ),
    );

    let mut file_spinners = Vec::with_capacity(num_display_items);
    for _ in 0..num_display_items {
        let s = mp.add(ProgressBar::new_spinner());
        s.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.cyan} {msg}")
                .expect("invalid progress bar template for snapshot spinner")
                .tick_chars(SPINNER_TICK_CHARS),
        );
        s.enable_steady_tick(refresh_interval);
        file_spinners.push(s);
    }

    let (ui_tx, ui_rx) = crossbeam_channel::unbounded::<UiEvent>();
    let spinners_for_thread = file_spinners.clone();
    let sampled_paths = Arc::new(Mutex::new(VecDeque::with_capacity(num_display_items)));
    let sampled_paths_for_thread = Arc::clone(&sampled_paths);
    let ui_thread = Mutex::new(Some(thread::spawn(move || {
        ui_loop(
            ui_rx,
            refresh_interval,
            spinners_for_thread,
            sampled_paths_for_thread,
        );
    })));

    let start_time = Instant::now();

    let state = Arc::new(CliSnapshotState {
        mp,
        progress_bar,
        companion_bar,
        file_spinners,
        error_counter,
        warning_counter,
        determined_style,
        verbosity,
        ui_tx,
        sampled_paths,
        sampling_limit: num_display_items,
        sampling_interval_ns: refresh_interval.as_nanos() as u64,
        sampling_last_reset_ns: AtomicU64::new(0),
        sampling_count: AtomicU64::new(0),
        start_time,
        rate_estimator,
        ui_thread,
    });

    Arc::new(move |event: Event| {
        let Event::Backup(ev) = event else { return };
        match ev {
            BackupEvent::ScanProgress { items, bytes } => {
                state
                    .companion_bar
                    .set_length(state.companion_bar.length().unwrap_or(0) + items);
                state
                    .progress_bar
                    .set_length(state.progress_bar.length().unwrap_or(0) + bytes);
            }
            BackupEvent::ScanFinished { .. } => {
                let total_bytes = state.progress_bar.length().unwrap_or(0);
                state.progress_bar.set_style(state.determined_style.clone());
                state.progress_bar.set_length(total_bytes);
                state.rate_estimator.lock().reset();
            }
            BackupEvent::NodeProcessing {
                ref path,
                diff,
                size_hint,
            } => {
                let should_print_node = match state.verbosity {
                    v if v >= 3 => true,
                    2 => diff != NodeDiff::Unchanged,
                    _ => false,
                };

                if should_print_node {
                    let diff_mark = match diff {
                        NodeDiff::New => "+".bold().green(),
                        NodeDiff::Deleted => "-".bold().red(),
                        NodeDiff::Changed => "M".bold().yellow(),
                        NodeDiff::Unchanged => "U".bold(),
                    };
                    state
                        .progress_bar
                        .println(format!("{}  {}", diff_mark, path.display()));
                }

                let is_slow = defaults::runtime()
                    .ui_snapshot_progress_item_min_size
                    .is_none_or(|t| size_hint.is_none_or(|s| s >= t));

                if !state.file_spinners.is_empty()
                    && diff != NodeDiff::Deleted
                    && diff != NodeDiff::Unchanged
                {
                    if is_slow {
                        let _ = state.ui_tx.try_send(UiEvent::Start(path.to_path_buf()));
                    } else {
                        let elapsed_ns = state.start_time.elapsed().as_nanos() as u64;
                        let last_reset = state.sampling_last_reset_ns.load(Ordering::Relaxed);

                        if elapsed_ns.saturating_sub(last_reset) >= state.sampling_interval_ns {
                            if state
                                .sampling_last_reset_ns
                                .compare_exchange(
                                    last_reset,
                                    elapsed_ns,
                                    Ordering::SeqCst,
                                    Ordering::Relaxed,
                                )
                                .is_ok()
                            {
                                state.sampling_count.store(1, Ordering::Relaxed);
                                let mut guard = state.sampled_paths.lock();
                                guard.clear();
                                guard.push_back(path.to_path_buf());
                            }
                        } else {
                            let count = state.sampling_count.fetch_add(1, Ordering::Relaxed);
                            if (count as usize) < state.sampling_limit {
                                state.sampled_paths.lock().push_back(path.to_path_buf());
                            }
                        }
                    }
                }
            }
            BackupEvent::NodeProcessed {
                ref path,
                ref diff,
                size_hint,
            } => {
                if *diff != NodeDiff::Deleted {
                    state.companion_bar.inc(1);
                }

                let is_slow = defaults::runtime()
                    .ui_snapshot_progress_item_min_size
                    .is_none_or(|t| size_hint.is_none_or(|s| s >= t));

                if !state.file_spinners.is_empty() && *diff != NodeDiff::Deleted && is_slow {
                    let _ = state.ui_tx.try_send(UiEvent::Done(path.to_path_buf()));
                }
            }
            BackupEvent::BytesProcessed(bytes) => {
                state.progress_bar.inc(bytes);
                let pos = state.progress_bar.position() as f64;
                state.rate_estimator.lock().observe(pos);
            }
            BackupEvent::Error(ref msg) => {
                state.error_counter.fetch_add(1, Ordering::Relaxed);
                let _ = state.mp.println(format!("{} {msg}", "Error:".bold().red()));
            }
            BackupEvent::Warning(ref msg) => {
                state.warning_counter.fetch_add(1, Ordering::Relaxed);
                let _ = state
                    .mp
                    .println(format!("{} {msg}", "Warning:".bold().yellow()));
            }
            BackupEvent::Log(ref msg) => {
                let _ = state.mp.println(msg);
            }
            BackupEvent::Finished(_) => {
                state.finalize();
            }
            BackupEvent::ScanStarted => {}
        }
    })
}
