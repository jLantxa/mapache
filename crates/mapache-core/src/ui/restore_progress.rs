use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use colored::Colorize;
use indicatif::{MultiProgress, ProgressBar, ProgressState, ProgressStyle};
use parking_lot::RwLock;

use crate::{
    mapache::{defaults::MAX_PATH_DISPLAY_LEN, global::GlobalOpts},
    ui::{EMPTY_PATHBUF, SPINNER_TICK_CHARS, default_bar_draw_target},
    utils,
};

pub struct RestoreProgressReporter {
    processed_items_count: Arc<AtomicU64>, // Number of files processed
    processing_items: Arc<RwLock<VecDeque<PathBuf>>>, // List of items being processed (for displaying)

    pub(crate) error_counter: Arc<AtomicU64>,
    pub(crate) warning_counter: Arc<AtomicU64>,

    #[allow(dead_code)]
    mp: MultiProgress,
    progress_bar: ProgressBar,
    companion_bar: ProgressBar,
    file_spinners: Vec<ProgressBar>,
}

impl RestoreProgressReporter {
    pub fn new(num_expected_items: u64, num_expected_bytes: u64, num_display_items: usize) -> Self {
        let processed_items_count_arc = Arc::new(AtomicU64::new(0));
        let error_counter_arc = Arc::new(AtomicU64::new(0));
        let warning_counter_arc = Arc::new(AtomicU64::new(0));

        let mp = MultiProgress::with_draw_target(default_bar_draw_target());
        let progress_bar = mp.add(ProgressBar::new(num_expected_bytes));
        let companion_bar = mp.add(ProgressBar::no_length());

        let processed_items_count_arc_clone = processed_items_count_arc.clone();
        let error_counter_arc_clone = error_counter_arc.clone();
        let warning_counter_arc_clone = warning_counter_arc.clone();
        progress_bar.set_style(
            ProgressStyle::default_bar()
                .template(
                    "[{percent} %] [{bar:20.cyan/white}] [{custom_elapsed}]  [{processed_bytes_formated}]  [ETA: {custom_eta}]"
                )
                .unwrap()
                .progress_chars("=> ")
                .with_key("custom_elapsed", move |state:&ProgressState, w: &mut dyn std::fmt::Write| {
                    let elapsed = state.elapsed();
                    let custom_elapsed= utils::pretty_print_duration(elapsed);
                    let _ = w.write_str(&custom_elapsed);
                })
                .with_key("processed_bytes_formated", move |state:&ProgressState, w: &mut dyn std::fmt::Write|{
                    let s = format!("{} / {}", utils::format_size_binary(state.pos(), 3), utils::format_size_binary(state.len().unwrap(), 3));
                    let _ = w.write_str(&s);
                })
                .with_key("custom_eta", move |state:&ProgressState, w: &mut dyn std::fmt::Write| {
                    let eta = state.eta();
                    let custom_eta= utils::pretty_print_duration(eta);
                    let _ = w.write_str(&custom_eta);
                })
        );

        companion_bar.set_style(
            ProgressStyle::default_bar()
                .template("[{processed_items_fmt}]  [{errors} errors, {warnings} warnings]")
                .expect("The snapshot progress bar should have been created")
                .progress_chars("=> ")
                .with_key(
                    "processed_items_fmt",
                    move |_state: &ProgressState, w: &mut dyn std::fmt::Write| {
                        let item_count = processed_items_count_arc_clone.load(Ordering::SeqCst);
                        let s = format!("{item_count} / {num_expected_items} items");
                        let _ = w.write_str(&s);
                    },
                )
                .with_key(
                    "errors",
                    move |_state: &ProgressState, w: &mut dyn std::fmt::Write| {
                        let errors = error_counter_arc_clone.load(Ordering::SeqCst);
                        let _ = w.write_str(&errors.to_string());
                    },
                )
                .with_key(
                    "warnings",
                    move |_state: &ProgressState, w: &mut dyn std::fmt::Write| {
                        let warnings = warning_counter_arc_clone.load(Ordering::SeqCst);
                        let _ = w.write_str(&warnings.to_string());
                    },
                ),
        );

        let refresh_interval = GlobalOpts::progress_refresh_interval();

        progress_bar.enable_steady_tick(refresh_interval);

        let mut file_spinners = Vec::with_capacity(num_display_items);
        for _ in 0..num_display_items {
            let file_spinner = mp.add(ProgressBar::new_spinner());
            file_spinner.set_style(
                ProgressStyle::default_spinner()
                    .template("{spinner:.cyan} {msg}")
                    .unwrap()
                    .tick_chars(SPINNER_TICK_CHARS),
            );
            file_spinner.enable_steady_tick(refresh_interval);
            file_spinners.push(file_spinner);
        }

        Self {
            processed_items_count: processed_items_count_arc,
            processing_items: Arc::new(RwLock::new(VecDeque::new())),
            error_counter: error_counter_arc,
            warning_counter: warning_counter_arc,
            mp,
            progress_bar,
            companion_bar,
            file_spinners,
        }
    }

    fn update_processing_items(&self) {
        for (i, spinner) in self.file_spinners.iter().enumerate() {
            let processing_items_guard = self.processing_items.read();
            let path = processing_items_guard.get(i).unwrap_or(&EMPTY_PATHBUF);
            let abbr_path = utils::abbreviate_path(path, MAX_PATH_DISPLAY_LEN);
            spinner.set_message(abbr_path);
        }
    }

    pub fn finalize(&self) {
        self.progress_bar.finish_and_clear();
        self.companion_bar.finish_and_clear();
        for spinner in self.file_spinners.iter() {
            spinner.finish_and_clear();
        }
        let _ = self.mp.clear();
    }

    pub fn processing_node(&self, path: PathBuf) {
        self.processing_items.write().push_back(path);
        self.update_processing_items();
        self.progress_bar.tick();
    }

    pub fn processed_item(&self, path: &Path) {
        self.progress_bar.tick();
        self.companion_bar.inc(1);

        let idx = self.processing_items.read().iter().position(|p| *p == path);
        if let Some(i) = idx {
            self.processing_items.write().remove(i);
            self.processed_items_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn processed_bytes(&self, bytes: u64) {
        self.progress_bar.inc(bytes);
        self.companion_bar.tick();
    }

    pub fn error(&self, msg: &str) {
        self.error_counter.fetch_add(1, Ordering::SeqCst);
        let _ = self.mp.println(format!("{} {msg}", "Error:".bold().red()));
    }

    pub fn warning(&self, msg: &str) {
        self.warning_counter.fetch_add(1, Ordering::SeqCst);
        let _ = self
            .mp
            .println(format!("{} {msg}", "Warning:".bold().yellow()));
    }
}
