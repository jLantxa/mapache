use std::{
    path::Path,
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
    fs::abbreviate_path,
    mapache::{defaults::MAX_PATH_DISPLAY_LEN, global::GlobalOpts},
    ui::{SPINNER_TICK_CHARS, default_bar_draw_target},
    utils,
};

pub enum UiEvent {
    Start(String),
    Done(String),
    Shutdown,
}

pub fn ui_loop(rx: Receiver<UiEvent>, update_interval: Duration, spinners: Vec<ProgressBar>) {
    let slots_limit = spinners.len();
    let mut active: Vec<String> = Vec::with_capacity(128);
    let mut last_update = Instant::now();

    while let Ok(ev) = rx.recv() {
        match ev {
            UiEvent::Start(path) => {
                if !active.contains(&path) {
                    active.push(path);
                }
            }
            UiEvent::Done(path) => {
                if let Some(pos) = active.iter().position(|x| x == &path) {
                    active.remove(pos);
                }
            }
            UiEvent::Shutdown => break,
        }

        // Apply the same high-performance throttling
        if rx.is_empty() || last_update.elapsed() >= update_interval {
            for (i, spinner) in spinners.iter().enumerate().take(slots_limit) {
                let msg = active.get(i).cloned().unwrap_or_default();
                spinner.set_message(msg);
            }
            last_update = Instant::now();
        }
    }
}

pub struct RestoreProgressReporter {
    // Hot-path counters
    processed_items_count: Arc<AtomicU64>,
    pub(crate) error_counter: Arc<AtomicU64>,
    pub(crate) warning_counter: Arc<AtomicU64>,

    mp: MultiProgress,
    progress_bar: ProgressBar,
    companion_bar: ProgressBar,
    file_spinners: Vec<ProgressBar>,

    // UI thread control
    ui_tx: Sender<UiEvent>,
    ui_stop: AtomicBool,
    _ui_thread: Mutex<Option<JoinHandle<()>>>,
}

impl RestoreProgressReporter {
    pub fn new(num_expected_items: u64, num_expected_bytes: u64, num_display_items: usize) -> Self {
        let refresh_interval = GlobalOpts::progress_refresh_interval();
        let mp = MultiProgress::with_draw_target(default_bar_draw_target());

        let processed_items_count = Arc::new(AtomicU64::new(0));
        let error_counter = Arc::new(AtomicU64::new(0));
        let warning_counter = Arc::new(AtomicU64::new(0));

        let progress_bar = mp.add(ProgressBar::new(num_expected_bytes));
        let companion_bar = mp.add(ProgressBar::no_length());

        // ---------------- Styles ----------------
        // Note: Template closures pull from Atomics to avoid lock-stepping workers with UI
        progress_bar.set_style(
            ProgressStyle::default_bar()
                .template("[{percent} %] [{bar:20.cyan/white}] [{custom_elapsed}] [{processed_bytes_fmt}] [{data_rate}/s] [ETA: {custom_eta}]")
                .expect("progress bar template")
                .progress_chars("=> ")
                .with_key("custom_elapsed", |_state: &ProgressState, w: &mut dyn std::fmt::Write| {
                    let _ = w.write_str(&utils::pretty_print_duration(_state.elapsed()));
                })
                .with_key("processed_bytes_fmt", move |_state: &ProgressState, w: &mut dyn std::fmt::Write| {
                    // FIX: Access the internal indicatif position and length
                    let current = _state.pos();
                    let total = _state.len().unwrap_or(0);

                    let s = format!(
                        "{} / {}",
                        utils::format_size_binary(current, 3),
                        utils::format_size_binary(total, 3)
                    );
                    let _ = w.write_str(&s);
                })
                .with_key("custom_eta", |_state: &ProgressState, w: &mut dyn std::fmt::Write| {
                    let _ = w.write_str(&utils::pretty_print_duration(_state.eta()));
                })
                .with_key(
                  "data_rate",
                    move |state: &ProgressState, w: &mut dyn std::fmt::Write| {
                        let rate = state.per_sec().floor() as u64;
                        let _ = w.write_str(&utils::format_size_binary(rate, 1));
                    },
                )
        );
        progress_bar.enable_steady_tick(refresh_interval);

        let items_clone = processed_items_count.clone();
        let err_clone = error_counter.clone();
        let warn_clone = warning_counter.clone();
        companion_bar.set_style(
            ProgressStyle::default_bar()
                .template("[{processed_items_fmt}] [{errors} errors, {warnings} warnings]")
                .expect("companion bar template")
                .with_key(
                    "processed_items_fmt",
                    move |_state: &ProgressState, w: &mut dyn std::fmt::Write| {
                        let items = items_clone.load(Ordering::Relaxed);
                        let _ = write!(w, "{items} / {num_expected_items} items");
                    },
                )
                .with_key(
                    "errors",
                    move |_state: &ProgressState, w: &mut dyn std::fmt::Write| {
                        let _ = write!(w, "{}", err_clone.load(Ordering::Relaxed));
                    },
                )
                .with_key(
                    "warnings",
                    move |_state: &ProgressState, w: &mut dyn std::fmt::Write| {
                        let _ = write!(w, "{}", warn_clone.load(Ordering::Relaxed));
                    },
                ),
        );
        companion_bar.enable_steady_tick(refresh_interval);

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

        let (ui_tx, ui_rx) = crossbeam_channel::unbounded::<UiEvent>();
        let ui_stop = AtomicBool::new(false);
        let spinners_clone = file_spinners.clone();

        let ui_thread = thread::spawn(move || {
            ui_loop(ui_rx, refresh_interval, spinners_clone);
        });

        Self {
            processed_items_count,
            error_counter,
            warning_counter,
            mp,
            progress_bar,
            companion_bar,
            file_spinners,
            ui_tx,
            ui_stop,
            _ui_thread: Mutex::new(Some(ui_thread)),
        }
    }

    pub fn finalize(&self) {
        self.ui_stop.store(true, Ordering::SeqCst);
        let _ = self.ui_tx.send(UiEvent::Shutdown);

        if let Some(h) = self._ui_thread.lock().take() {
            let _ = h.join();
        }

        for sp in &self.file_spinners {
            sp.finish_and_clear();
        }
        self.companion_bar.finish_and_clear();
        self.progress_bar.finish_and_clear();
        let _ = self.mp.clear();
    }

    pub fn processing_node(&self, path: &Path) {
        if !self.ui_stop.load(Ordering::Relaxed) {
            let abbr = abbreviate_path(path, MAX_PATH_DISPLAY_LEN);
            let _ = self.ui_tx.try_send(UiEvent::Start(abbr));
        }
    }

    pub fn processed_item(&self, path: &Path) {
        let total = self.processed_items_count.fetch_add(1, Ordering::Relaxed) + 1;
        // Periodic sync to keep internal ETA logic fresh without hammering Mutex
        if total.is_multiple_of(10) {
            self.companion_bar.set_position(total);
        }

        if !self.ui_stop.load(Ordering::Relaxed) {
            let abbr = abbreviate_path(path, MAX_PATH_DISPLAY_LEN);
            let _ = self.ui_tx.try_send(UiEvent::Done(abbr));
        }
    }

    #[inline]
    pub fn processed_bytes(&self, bytes: u64) {
        self.progress_bar.inc(bytes);
    }

    pub fn error(&self, msg: &str) {
        self.error_counter.fetch_add(1, Ordering::Relaxed);
        let _ = self.mp.println(format!("{} {msg}", "Error:".bold().red()));
    }

    pub fn warning(&self, msg: &str) {
        self.warning_counter.fetch_add(1, Ordering::Relaxed);
        let _ = self
            .mp
            .println(format!("{} {msg}", "Warning:".bold().yellow()));
    }
}

impl Drop for RestoreProgressReporter {
    fn drop(&mut self) {
        self.finalize();
    }
}
