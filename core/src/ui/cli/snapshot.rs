use std::{
    collections::VecDeque,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use colored::Colorize;
use crossbeam_channel::{Receiver, Sender};
use indicatif::{MultiProgress, ProgressBar, ProgressState, ProgressStyle};
use parking_lot::Mutex;

use crate::{
    fs::{abbreviate_path, tree::NodeDiff},
    mapache::{defaults, global::GlobalOpts},
    ui::{SPINNER_TICK_CHARS, SnapshotProgressReporter, default_bar_draw_target},
    utils,
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

    // Throttle UI redraws
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
            // Drain sampled paths from shared state into our display queue
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

pub struct CliSnapshotProgressReporter {
    mp: MultiProgress,
    progress_bar: ProgressBar,
    companion_bar: ProgressBar,
    file_spinners: Vec<ProgressBar>,

    expected_items: AtomicU64,
    expected_bytes: AtomicU64,

    error_counter: Arc<AtomicU64>,
    determined_style: ProgressStyle,

    verbosity: u32,

    ui_tx: Sender<UiEvent>,
    ui_stop: AtomicBool,

    // Sampling budget (Global across threads)
    sampled_paths: Arc<Mutex<VecDeque<PathBuf>>>,
    sampling_limit: usize,
    sampling_interval_ns: u64,
    sampling_last_reset_ns: AtomicU64,
    sampling_count: AtomicU64,
    start_time: Instant,

    _ui_thread: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for CliSnapshotProgressReporter {
    fn drop(&mut self) {
        // We call finalize to ensure the thread is joined and
        // the terminal is restored even if finalize wasn't called manually.
        self.finalize();
    }
}

impl CliSnapshotProgressReporter {
    pub fn new(
        expected_items_val: Option<u64>,
        expected_size_val: Option<u64>,
        num_display_items: usize,
    ) -> Self {
        let verbosity = GlobalOpts::verbosity();
        let refresh_interval = GlobalOpts::progress_refresh_interval();
        let mp = MultiProgress::with_draw_target(default_bar_draw_target());

        let error_counter = Arc::new(AtomicU64::new(0));
        let expected_items = AtomicU64::new(expected_items_val.unwrap_or(0));
        let expected_bytes = AtomicU64::new(expected_size_val.unwrap_or(0));

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

        // ---------------- Styles ----------------
        let base_style = ProgressStyle::default_bar()
            .progress_chars("=> ")
            .with_key(
                "custom_elapsed",
                |state: &ProgressState, w: &mut dyn std::fmt::Write| {
                    let _ = w.write_str(&utils::pretty_print_duration(state.elapsed()));
                },
            )
            .with_key(
                "data_rate",
                |state: &ProgressState, w: &mut dyn std::fmt::Write| {
                    let rate = state.per_sec().floor() as u64;
                    let _ = w.write_str(&utils::format_size_binary(rate, 1));
                },
            )
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
                                // If the scanner fall behind the processed bytes, don't show it.
                                let _ = w.write_str(&processed_bytes_str);
                            } else {
                                let _ = write!(
                                    w,
                                    "{} / {}",
                                    &processed_bytes_str,
                                    utils::format_size_binary(total, 3)
                                );
                            }
                        }
                    }
                },
            )
            .with_key(
                "custom_eta",
                |state: &ProgressState, w: &mut dyn std::fmt::Write| {
                    let _ = w.write_str(&utils::pretty_print_duration(state.eta()));
                },
            );

        let determined_style = base_style.clone()
        .template("[{percent} %] [{bar:20.cyan/white}] [{custom_elapsed}] [{processed_bytes_fmt}] [{data_rate}/s] [ETA: {custom_eta}]")
        .expect("template");

        let undetermined_style = base_style
            .clone()
            .template("[{custom_elapsed}] [{processed_bytes_fmt}] [{data_rate}/s]")
            .expect("template");

        progress_bar.set_style(undetermined_style.clone());

        // Companion Bar: Dynamic Items and Errors
        let error_counter_for_style = Arc::clone(&error_counter);
        companion_bar.set_style(
            ProgressStyle::default_bar()
                .template("[{items_info}] [{errors} errors]")
                .expect("template")
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
                                    // If the scanner fall behind the processed items, don't show it.
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
                ),
        );

        // ---------------- Spinners ----------------
        let mut file_spinners = Vec::with_capacity(num_display_items);
        for _ in 0..num_display_items {
            let s = mp.add(ProgressBar::new_spinner());
            s.set_style(
                ProgressStyle::default_spinner()
                    .template("{spinner:.cyan} {msg}")
                    .unwrap()
                    .tick_chars(SPINNER_TICK_CHARS),
            );
            s.enable_steady_tick(refresh_interval);
            file_spinners.push(s);
        }

        // ---------------- UI Thread ----------------
        let (ui_tx, ui_rx) = crossbeam_channel::unbounded::<UiEvent>();
        let spinners_for_thread = file_spinners.clone();
        let sampled_paths = Arc::new(Mutex::new(VecDeque::with_capacity(num_display_items)));
        let sampled_paths_for_thread = Arc::clone(&sampled_paths);
        let ui_thread = Some(thread::spawn(move || {
            ui_loop(
                ui_rx,
                refresh_interval,
                spinners_for_thread,
                sampled_paths_for_thread,
            );
        }));

        let start_time = Instant::now();

        Self {
            mp,
            progress_bar,
            companion_bar,
            file_spinners,
            error_counter,
            expected_items,
            expected_bytes,
            determined_style,
            verbosity,
            ui_tx,
            ui_stop: AtomicBool::new(false),
            sampled_paths,
            sampling_limit: num_display_items,
            sampling_interval_ns: refresh_interval.as_nanos() as u64,
            sampling_last_reset_ns: AtomicU64::new(0),
            sampling_count: AtomicU64::new(0),
            start_time,
            _ui_thread: Mutex::new(ui_thread),
        }
    }
}

impl SnapshotProgressReporter for CliSnapshotProgressReporter {
    fn add_expected_items(&self, val: u64) {
        let new_total = self.expected_items.fetch_add(val, Ordering::Relaxed) + val;
        self.companion_bar.set_length(new_total);
    }

    fn add_expected_bytes(&self, val: u64) {
        let new_total = self.expected_bytes.fetch_add(val, Ordering::Relaxed) + val;
        self.progress_bar.set_length(new_total);
    }

    fn scan_finished(&self) {
        let total_bytes = self.expected_bytes.load(Ordering::Relaxed);

        // Reset the position if needed, or just switch styles.
        // Once this is called, the UI will jump from "Scan" mode to "Progress Bar" mode.
        self.progress_bar.set_style(self.determined_style.clone());
        self.progress_bar.set_length(total_bytes);
    }

    fn processing_node(&self, path: &std::path::Path, diff: NodeDiff, size_hint: Option<u64>) {
        if self.ui_stop.load(Ordering::Relaxed) {
            return;
        }

        let should_print_node = match self.verbosity {
            v if v >= 3 => true,              // Print everything at v3+
            2 => diff != NodeDiff::Unchanged, // Print everything EXCEPT unchanged at v2
            _ => false,                       // v1 or lower prints nothing here
        };

        if should_print_node {
            let diff_mark = match diff {
                NodeDiff::New => "+".bold().green(),
                NodeDiff::Deleted => "-".bold().red(),
                NodeDiff::Changed => "M".bold().yellow(),
                NodeDiff::Unchanged => "U".bold(),
            };
            self.progress_bar
                .println(format!("{}  {}", diff_mark, path.display()));
        }

        // Optimization: Only show "slow" items in the spinner.
        // Files under the threshold are sampled.
        let is_slow = defaults::runtime()
            .ui_snapshot_progress_item_min_size
            .is_none_or(|t| size_hint.is_none_or(|s| s >= t));

        if !self.file_spinners.is_empty()
            && diff != NodeDiff::Deleted
            && diff != NodeDiff::Unchanged
        {
            if is_slow {
                let _ = self.ui_tx.try_send(UiEvent::Start(path.to_path_buf()));
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
                            Ordering::SeqCst,
                            Ordering::Relaxed,
                        )
                        .is_ok()
                    {
                        self.sampling_count.store(1, Ordering::Relaxed);
                        let mut guard = self.sampled_paths.lock();
                        guard.clear();
                        guard.push_back(path.to_path_buf());
                    }
                } else {
                    // Current time slot: check remaining budget
                    let count = self.sampling_count.fetch_add(1, Ordering::Relaxed);
                    if (count as usize) < self.sampling_limit {
                        self.sampled_paths.lock().push_back(path.to_path_buf());
                    }
                }
            }
        }
    }

    fn processed_node(&self, path: &std::path::Path, diff: NodeDiff, size_hint: Option<u64>) {
        if diff != NodeDiff::Deleted {
            self.companion_bar.inc(1);
        }

        let is_slow = defaults::runtime()
            .ui_snapshot_progress_item_min_size
            .is_none_or(|t| size_hint.is_none_or(|s| s >= t));

        if !self.ui_stop.load(Ordering::Relaxed)
            && !self.file_spinners.is_empty()
            && diff != NodeDiff::Deleted
            && is_slow
        {
            let _ = self.ui_tx.try_send(UiEvent::Done(path.to_path_buf()));
        }
    }

    #[inline]
    fn processed_bytes(&self, bytes: u64) {
        self.progress_bar.inc(bytes);
    }

    fn error(&self, msg: &str) {
        self.error_counter.fetch_add(1, Ordering::Relaxed);
        let _ = self.mp.println(format!("{} {msg}", "Error:".bold().red()));
    }

    fn warning(&self, msg: &str) {
        self.error_counter.fetch_add(1, Ordering::Relaxed);
        let _ = self
            .mp
            .println(format!("{} {msg}", "Warning:".bold().yellow()));
    }

    fn log(&self, msg: String) {
        let _ = self.mp.println(msg);
    }

    fn finalize(&self) {
        self.ui_stop.store(true, Ordering::Relaxed);
        let _ = self.ui_tx.try_send(UiEvent::Shutdown);

        if let Some(h) = self._ui_thread.lock().take() {
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
