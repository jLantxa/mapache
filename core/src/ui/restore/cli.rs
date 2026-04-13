use std::{
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use colored::Colorize;
use indicatif::{MultiProgress, ProgressBar, ProgressState, ProgressStyle};

use crate::{
    mapache::global::GlobalOpts,
    ui::{SPINNER_TICK_CHARS, default_bar_draw_target},
    utils,
};

use super::RestoreProgressReporter;

pub struct CliRestoreProgressReporter {
    processed_items_count: Arc<AtomicU64>,
    processed_bytes_count: Arc<AtomicU64>,
    pub(crate) error_counter: Arc<AtomicU64>,
    pub(crate) warning_counter: Arc<AtomicU64>,
    mp: MultiProgress,
    progress_bar: ProgressBar,
    companion_bar: ProgressBar,
    current_stage: Arc<Mutex<String>>,
    num_expected_items: u64,
}

impl CliRestoreProgressReporter {
    pub(crate) fn new(
        num_expected_items: u64,
        num_expected_bytes: u64,
        _num_display_items: usize,
    ) -> Self {
        let refresh_interval = GlobalOpts::progress_refresh_interval();
        let mp = MultiProgress::with_draw_target(default_bar_draw_target());

        let processed_items_count = Arc::new(AtomicU64::new(0));
        let processed_bytes_count = Arc::new(AtomicU64::new(0));
        let error_counter = Arc::new(AtomicU64::new(0));
        let warning_counter = Arc::new(AtomicU64::new(0));

        let progress_bar = mp.add(ProgressBar::new(num_expected_bytes));
        let companion_bar = mp.add(ProgressBar::no_length());

        progress_bar.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.cyan} {msg}\n[{percent} %] [{bar:20.cyan/white}] [{custom_elapsed}] [{processed_bytes_fmt}] [{data_rate}/s] [ETA: {custom_eta}]")
                .expect("progress bar template")
                .progress_chars("=> ")
                .tick_chars(SPINNER_TICK_CHARS)
                .with_key("custom_elapsed", |_state: &ProgressState, w: &mut dyn std::fmt::Write| {
                    let _ = w.write_str(&utils::pretty_print_duration(_state.elapsed()));
                })
                .with_key("processed_bytes_fmt", move |_state: &ProgressState, w: &mut dyn std::fmt::Write| {
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
                ),
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

        Self {
            processed_items_count,
            processed_bytes_count,
            error_counter,
            warning_counter,
            mp,
            progress_bar,
            companion_bar,
            current_stage: Arc::new(Mutex::new(String::new())),
            num_expected_items,
        }
    }
}

impl RestoreProgressReporter for CliRestoreProgressReporter {
    fn set_message(&self, msg: String) {
        self.progress_bar.set_message(msg.clone());
        let mut stage = self.current_stage.lock().unwrap();
        *stage = msg;
    }

    fn processing_node(&self, _path: &Path) {
        // No-op for restore; path-based updates are too verbose for pack-centric restores.
    }

    fn processed_item(&self, _path: &Path) {
        let total = self.processed_items_count.fetch_add(1, Ordering::Relaxed) + 1;
        if total.is_multiple_of(10) || total == self.num_expected_items {
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

    fn finalize(&self) {
        self.companion_bar.finish_and_clear();
        self.progress_bar.finish_and_clear();
        let _ = self.mp.clear();
    }
}

impl Drop for CliRestoreProgressReporter {
    fn drop(&mut self) {
        self.finalize();
    }
}
