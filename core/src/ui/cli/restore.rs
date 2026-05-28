use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use parking_lot::Mutex;

use colored::Colorize;
use indicatif::{MultiProgress, ProgressBar, ProgressState, ProgressStyle};

use crate::{
    mapache::global::GlobalOpts,
    ui::{RestoreProgressReporter, SPINNER_TICK_CHARS, default_bar_draw_target},
    utils,
};

pub struct CliRestoreProgressReporter {
    processed_items_count: Arc<AtomicU64>,
    processed_bytes_count: Arc<AtomicU64>,
    pub(crate) error_counter: Arc<AtomicU64>,
    pub(crate) warning_counter: Arc<AtomicU64>,
    mp: MultiProgress,
    progress_bar: ProgressBar,
    companion_bar: ProgressBar,
    current_stage: Arc<Mutex<String>>,
    num_expected_items: Arc<AtomicU64>,
    num_expected_bytes: Arc<AtomicU64>,
}

impl CliRestoreProgressReporter {
    pub(crate) fn new(num_expected_items: Option<u64>, num_expected_bytes: Option<u64>) -> Self {
        let refresh_interval = GlobalOpts::progress_refresh_interval();
        let mp = MultiProgress::with_draw_target(default_bar_draw_target());

        let processed_items_count = Arc::new(AtomicU64::new(0));
        let processed_bytes_count = Arc::new(AtomicU64::new(0));
        let error_counter = Arc::new(AtomicU64::new(0));
        let warning_counter = Arc::new(AtomicU64::new(0));
        let num_expected_items_atom = Arc::new(AtomicU64::new(num_expected_items.unwrap_or(0)));
        let num_expected_bytes_atom = Arc::new(AtomicU64::new(num_expected_bytes.unwrap_or(0)));

        let progress_bar = if num_expected_items.is_none() || num_expected_bytes.is_none() {
            mp.add(ProgressBar::no_length())
        } else {
            mp.add(ProgressBar::new(num_expected_bytes.unwrap_or(0)))
        };

        let companion_bar = mp.add(ProgressBar::no_length());

        progress_bar.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.cyan} {msg}\n[{percent} %] [{bar:20.cyan/white}] [{custom_elapsed}] [{processed_bytes_fmt}] [{data_rate}/s] [ETA: {custom_eta}]")
                .expect("Invalid progress bar template for restore progress")
                .progress_chars("=> ")
                .tick_chars(SPINNER_TICK_CHARS)
                .with_key("custom_elapsed", |_state: &ProgressState, w: &mut dyn std::fmt::Write| {
                    let _ = w.write_str(&utils::pretty_print_duration(_state.elapsed()));
                })
                .with_key("processed_bytes_fmt", move |state: &ProgressState, w: &mut dyn std::fmt::Write| {
                    let current = state.pos();
                    let total = state.len().unwrap_or(0);
                    let s = format!(
                        "{} / {}",
                        utils::format_size_binary(current, 3),
                        utils::format_size_binary(total, 3)
                    );
                    let _ = w.write_str(&s);
                })
                .with_key("custom_eta", |state: &ProgressState, w: &mut dyn std::fmt::Write| {
                    let _ = w.write_str(&utils::pretty_print_duration(state.eta()));
                })
                .with_key(
                    "data_rate",
                    move |state: &ProgressState, w: &mut dyn std::fmt::Write| {
                        let rate = state.per_sec().floor() as u64;
                        let _ = w.write_str(&utils::format_size_binary(rate, 1));
                    },
                ),
        );
        progress_bar.enable_steady_tick(refresh_interval);

        let items_clone = processed_items_count.clone();
        let num_items_clone = num_expected_items_atom.clone();
        let err_clone = error_counter.clone();
        let warn_clone = warning_counter.clone();
        companion_bar.set_style(
            ProgressStyle::default_bar()
                .template("[{processed_items_fmt}] [{errors} errors, {warnings} warnings]")
                .expect("Invalid progress bar template for restore companion bar")
                .with_key(
                    "processed_items_fmt",
                    move |_state: &ProgressState, w: &mut dyn std::fmt::Write| {
                        let items = items_clone.load(Ordering::Relaxed);
                        let total = num_items_clone.load(Ordering::Relaxed);

                        if items <= total {
                            let _ = write!(w, "{items} / {total} items");
                        } else {
                            let _ = write!(w, "{items} items");
                        }
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

        Self {
            processed_items_count,
            processed_bytes_count,
            error_counter,
            warning_counter,
            mp,
            progress_bar,
            companion_bar,
            current_stage: Arc::new(Mutex::new(String::new())),
            num_expected_items: num_expected_items_atom,
            num_expected_bytes: num_expected_bytes_atom,
        }
    }
}

impl RestoreProgressReporter for CliRestoreProgressReporter {
    fn set_message(&self, msg: String) {
        self.progress_bar.set_message(msg.clone());
        let mut stage = self.current_stage.lock();
        *stage = msg;
    }

    fn set_visited_nodes(&self, count: u64) {
        self.processed_items_count.store(count, Ordering::Relaxed);
    }

    fn resize_workload(&self, num_expected_items: u64, num_expected_bytes: u64) {
        self.num_expected_items
            .store(num_expected_items, Ordering::Relaxed);
        self.num_expected_bytes
            .store(num_expected_bytes, Ordering::Relaxed);
        self.progress_bar.set_length(num_expected_bytes);
        self.progress_bar.set_message(String::new());
        self.processed_items_count.store(0, Ordering::Relaxed);
    }

    fn processed_item(&self, _path: &Path) {
        let total = self.processed_items_count.fetch_add(1, Ordering::Relaxed) + 1;
        let expected = self.num_expected_items.load(Ordering::Relaxed);
        if total.is_multiple_of(10) || total == expected {
            self.companion_bar.set_position(total);
        }
    }

    fn processed_bytes(&self, bytes: u64) {
        self.processed_bytes_count
            .fetch_add(bytes, Ordering::Relaxed);
        self.progress_bar.inc(bytes);
    }

    fn error(&self, msg: &str) {
        self.error_counter.fetch_add(1, Ordering::Relaxed);
        let _ = self.mp.println(format!("{} {msg}", "Error:".bold().red()));
    }

    fn warning(&self, msg: &str) {
        self.warning_counter.fetch_add(1, Ordering::Relaxed);
        let _ = self
            .mp
            .println(format!("{} {msg}", "Warning:".bold().yellow()));
    }

    fn error_count(&self) -> u64 {
        self.error_counter.load(Ordering::Relaxed)
    }

    fn warning_count(&self) -> u64 {
        self.warning_counter.load(Ordering::Relaxed)
    }

    fn log(&self, msg: String) {
        let _ = self.mp.println(msg);
    }

    fn verbose_1(&self, msg: String) {
        if GlobalOpts::verbosity() >= 2 {
            let _ = self.mp.println(msg);
        }
    }

    fn verbose_2(&self, msg: String) {
        if GlobalOpts::verbosity() >= 3 {
            let _ = self.mp.println(msg);
        }
    }

    fn finalize(&self) {
        self.companion_bar.finish_and_clear();
        self.progress_bar.finish_and_clear();
        let _ = self.mp.clear();
    }

    fn total_items(&self) -> u64 {
        self.num_expected_items.load(Ordering::Relaxed)
    }

    fn total_bytes(&self) -> u64 {
        self.num_expected_bytes.load(Ordering::Relaxed)
    }
}

impl Drop for CliRestoreProgressReporter {
    fn drop(&mut self) {
        self.finalize();
    }
}
