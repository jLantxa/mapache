use std::{
    collections::VecDeque,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use colored::Colorize;
use indicatif::{MultiProgress, ProgressBar, ProgressState, ProgressStyle};
use parking_lot::RwLock;

use crate::{
    fs::tree::NodeDiff,
    mapache::{defaults::MAX_PATH_DISPLAY_LEN, global::GlobalOpts},
    repository::{
        repo::SizePair,
        snapshot::{DiffCountsAtomic, SnapshotSummary},
    },
    ui::{SPINNER_TICK_CHARS, default_bar_draw_target},
    utils,
};

pub struct SnapshotProgressReporter {
    // Processed items
    processed_items_count: Arc<AtomicU64>, // Number of files processed (written or not)
    processed_bytes: Arc<AtomicU64>,       // Bytes processed (only data)
    raw_bytes: Arc<AtomicU64>,             // Bytes 'written' before encoding
    encoded_bytes: Arc<AtomicU64>,         // Bytes written after encoding
    expected_items: Arc<RwLock<Option<AtomicU64>>>, // Num expected items
    expected_bytes: Arc<RwLock<Option<AtomicU64>>>, // Num expected bytes

    determined_style: ProgressStyle,

    // Metadata
    meta_raw_bytes: Arc<AtomicU64>, // Metadata bytes 'written' before encoding
    meta_encoded_bytes: Arc<AtomicU64>, // Metadata bytes written after encoding

    diff_counts: Arc<DiffCountsAtomic>,

    processing_items: Arc<RwLock<VecDeque<String>>>, // List of items being processed (for displaying)

    error_counter: Arc<AtomicU64>,

    #[allow(dead_code)]
    mp: MultiProgress,
    progress_bar: ProgressBar,
    companion_bar: ProgressBar,
    file_spinners: Vec<ProgressBar>,

    verbosity: u32,
}

impl SnapshotProgressReporter {
    pub fn new(
        expected_items: Option<u64>,
        expected_size: Option<u64>,
        num_display_items: usize,
    ) -> Self {
        let mp = MultiProgress::with_draw_target(default_bar_draw_target());

        let progress_bar = match expected_size {
            Some(size) => mp.add(ProgressBar::new(size)),
            None => mp.add(ProgressBar::no_length()),
        };

        let companion_bar = mp.add(ProgressBar::no_length());

        let processed_items_count_arc = Arc::new(AtomicU64::new(0));
        let processed_bytes_arc = Arc::new(AtomicU64::new(0));
        let raw_bytes_arc = Arc::new(AtomicU64::new(0));
        let encoded_bytes_arc = Arc::new(AtomicU64::new(0));

        let expected_items_arc = match expected_items {
            Some(val) => Arc::new(RwLock::new(Some(AtomicU64::new(val)))),
            None => Arc::new(RwLock::new(None)),
        };

        let expected_bytes_arc = match expected_size {
            Some(val) => Arc::new(RwLock::new(Some(AtomicU64::new(val)))),
            None => Arc::new(RwLock::new(None)),
        };

        let meta_raw_bytes_arc = Arc::new(AtomicU64::new(0));
        let meta_encoded_bytes_arc = Arc::new(AtomicU64::new(0));

        let processing_items_arc = Arc::new(RwLock::new(VecDeque::new()));
        let error_counter_arc = Arc::new(AtomicU64::new(0));

        let processed_bytes_arc_clone = processed_bytes_arc.clone();
        let expected_bytes_arc_clone = expected_bytes_arc.clone();
        let undetermined_style = ProgressStyle::default_bar()
            .template("[{custom_elapsed}]  [{processed_bytes_fmt}]")
            .expect("The snapshot progress bar should have been created")
            .progress_chars("=> ")
            .with_key(
                "custom_elapsed",
                move |state: &ProgressState, w: &mut dyn std::fmt::Write| {
                    let elapsed = state.elapsed();
                    let custom_elapsed = utils::pretty_print_duration(elapsed);
                    let _ = w.write_str(&custom_elapsed);
                },
            )
            .with_key(
                "processed_bytes_fmt",
                move |_state: &ProgressState, w: &mut dyn std::fmt::Write| {
                    let bytes = processed_bytes_arc_clone.load(Ordering::Relaxed);
                    let expected_bytes_lock = expected_bytes_arc_clone.write();
                    let s = match expected_bytes_lock.as_ref() {
                        Some(atomic_val) => {
                            let expected_bytes = atomic_val.load(Ordering::Relaxed);
                            format!(
                                "{} / {}",
                                utils::format_size_binary(bytes, 3),
                                utils::format_size_binary(expected_bytes, 3)
                            )
                        }
                        None => utils::format_size_binary(bytes, 3).to_string(),
                    };
                    let _ = w.write_str(&s);
                },
            )
            .with_key(
                "custom_eta",
                move |state: &ProgressState, w: &mut dyn std::fmt::Write| {
                    let eta = state.eta();
                    let custom_eta = utils::pretty_print_duration(eta);
                    let _ = w.write_str(&custom_eta);
                },
            );

        let processed_bytes_arc_clone = processed_bytes_arc.clone();
        let expected_bytes_arc_clone = expected_bytes_arc.clone();
        let determined_style = ProgressStyle::default_bar()
            .template("[{percent} %] [{bar:20.cyan/white}] [{custom_elapsed}]  [{processed_bytes_fmt}]  [ETA: {custom_eta}]")
            .expect("The snapshot progress bar should have been created")
            .progress_chars("=> ")
            .with_key(
                "custom_elapsed",
                move |state: &ProgressState, w: &mut dyn std::fmt::Write| {
                    let elapsed = state.elapsed();
                    let custom_elapsed = utils::pretty_print_duration(elapsed);
                    let _ = w.write_str(&custom_elapsed);
                },
            )
            .with_key(
                "processed_bytes_fmt",
                move |_state: &ProgressState, w: &mut dyn std::fmt::Write| {
                    let bytes = processed_bytes_arc_clone.load(Ordering::Relaxed);
                    let expected_bytes_lock = expected_bytes_arc_clone.read();
                    let s = match expected_bytes_lock.as_ref() {
                        Some(atomic_val) => {
                            let expected_bytes = atomic_val.load(Ordering::Relaxed);
                            format!(
                                "{} / {}",
                                utils::format_size_binary(bytes, 3),
                                utils::format_size_binary(expected_bytes, 3)
                            )
                        },
                        None => utils::format_size_binary(bytes, 3).to_string(),
                    };
                    let _ = w.write_str(&s);
                },
            )
            .with_key(
                "custom_eta",
                move |state: &ProgressState, w: &mut dyn std::fmt::Write| {
                    let eta = state.eta();
                    let custom_eta = utils::pretty_print_duration(eta);
                    let _ = w.write_str(&custom_eta);
                },
            );

        match expected_size {
            Some(_) => progress_bar.set_style(determined_style.clone()),
            None => progress_bar.set_style(undetermined_style.clone()),
        };

        let error_counter_arc_clone = error_counter_arc.clone();
        let expected_items_arc_clone = expected_items_arc.clone();
        let processed_items_count_arc_clone = processed_items_count_arc.clone();
        companion_bar.set_style(
            ProgressStyle::default_bar()
                .template("[{processed_items_fmt}]  [{errors} errors]")
                .expect("The snapshot progress bar should have been created")
                .progress_chars("=> ")
                .with_key(
                    "processed_items_fmt",
                    move |_state: &ProgressState, w: &mut dyn std::fmt::Write| {
                        let item_count = processed_items_count_arc_clone.load(Ordering::Relaxed);
                        let expected_items_lock = expected_items_arc_clone.write();
                        let s = match expected_items_lock.as_ref() {
                            Some(atomic_val) => {
                                let expected_count = atomic_val.load(Ordering::Relaxed);
                                format!("{item_count} / {expected_count} items")
                            }
                            None => format!("{item_count} items"),
                        };
                        let _ = w.write_str(&s);
                    },
                )
                .with_key(
                    "errors",
                    move |_state: &ProgressState, w: &mut dyn std::fmt::Write| {
                        let errors = error_counter_arc_clone.load(Ordering::Relaxed);
                        let _ = w.write_str(&errors.to_string());
                    },
                ),
        );

        let refresh_interval = GlobalOpts::progress_refresh_interval();

        progress_bar.enable_steady_tick(refresh_interval);
        companion_bar.enable_steady_tick(refresh_interval);

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
            processed_bytes: processed_bytes_arc,
            raw_bytes: raw_bytes_arc,
            encoded_bytes: encoded_bytes_arc,
            meta_raw_bytes: meta_raw_bytes_arc,
            meta_encoded_bytes: meta_encoded_bytes_arc,
            expected_items: expected_items_arc,
            expected_bytes: expected_bytes_arc,
            diff_counts: Arc::new(DiffCountsAtomic::default()),
            processing_items: processing_items_arc,
            determined_style,
            mp,
            companion_bar,
            progress_bar,
            file_spinners,
            verbosity: GlobalOpts::verbosity(),
            error_counter: error_counter_arc,
        }
    }

    pub fn add_expected_items(&self, val: u64) {
        let mut expected_items_lock = self.expected_items.write();
        match expected_items_lock.as_ref() {
            Some(expected_items_atomic) => {
                expected_items_atomic.fetch_add(val, Ordering::Relaxed);
            }
            None => {
                let _ = expected_items_lock.insert(AtomicU64::new(val));
            }
        }
    }

    pub fn add_expected_bytes(&self, val: u64) {
        let mut expected_bytes_lock = self.expected_bytes.write();
        match expected_bytes_lock.as_ref() {
            Some(expected_bytes_atomic) => {
                expected_bytes_atomic.fetch_add(val, Ordering::Relaxed);
            }
            None => {
                let _ = expected_bytes_lock.insert(AtomicU64::new(val));
            }
        }
    }

    pub fn scan_finished(&self) {
        let expected_bytes_lock = self.expected_bytes.read();
        let bytes_opt = expected_bytes_lock
            .as_ref()
            .map(|bytes_atomic| bytes_atomic.load(Ordering::Relaxed));
        drop(expected_bytes_lock); // Drop the lock before setting the progress bar length!

        if let Some(bytes) = bytes_opt {
            self.progress_bar.set_length(bytes);
            self.progress_bar.set_style(self.determined_style.clone());
        }
    }

    fn update_processing_items(&self) {
        for (i, spinner) in self.file_spinners.iter().enumerate() {
            let guard = self.processing_items.read();
            let msg = guard.get(i).map(|s| s.as_str()).unwrap_or("");
            spinner.set_message(msg.to_string());
        }
    }

    pub fn finalize(&self) {
        for spinner in self.file_spinners.iter() {
            spinner.finish_and_clear();
        }
        self.companion_bar.finish_and_clear();
        self.progress_bar.finish_and_clear();
        let _ = self.mp.clear();
    }

    pub fn processing_node(&self, path: &Path, diff: NodeDiff) {
        if diff != NodeDiff::Deleted {
            let abbr = utils::abbreviate_path(path, MAX_PATH_DISPLAY_LEN);
            let cap = self.file_spinners.len();

            {
                let mut q = self.processing_items.write();
                if cap != 0 && q.len() == cap {
                    q.pop_front();
                }
                q.push_back(abbr);
            }

            self.update_processing_items();
        }

        if self.verbosity >= 3 {
            let diff_mark = match diff {
                NodeDiff::New => "+".bold().green(),
                NodeDiff::Deleted => "-".bold().red(),
                NodeDiff::Changed => "M".bold().yellow(),
                NodeDiff::Unchanged => "U".bold(),
            };
            self.progress_bar
                .println(format!("{}  {}", diff_mark, path.display()));
        }

        self.progress_bar.tick();
        self.companion_bar.tick();
    }

    pub fn processed_node(&self, _path: &Path) {
        self.processed_items_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn processed_bytes(&self, bytes: u64) {
        self.processed_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.progress_bar.inc(bytes);
        self.companion_bar.tick();
    }

    #[inline]
    pub fn written_data_bytes(&self, size: SizePair) {
        self.raw_bytes.fetch_add(size.raw, Ordering::Relaxed);
        self.encoded_bytes
            .fetch_add(size.encoded, Ordering::Relaxed);
    }

    #[inline]
    pub fn written_meta_bytes(&self, size: SizePair) {
        self.meta_raw_bytes.fetch_add(size.raw, Ordering::Relaxed);
        self.meta_encoded_bytes
            .fetch_add(size.encoded, Ordering::Relaxed);
    }

    #[inline]
    pub fn new_file(&self) {
        self.diff_counts.new_files.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn changed_file(&self) {
        self.diff_counts
            .changed_files
            .fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn unchanged_file(&self) {
        self.diff_counts
            .unchanged_files
            .fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn deleted_file(&self) {
        self.diff_counts
            .deleted_files
            .fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn new_dir(&self) {
        self.diff_counts.new_dirs.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn changed_dir(&self) {
        self.diff_counts
            .changed_dirs
            .fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn deleted_dir(&self) {
        self.diff_counts
            .deleted_dirs
            .fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn unchanged_dir(&self) {
        self.diff_counts
            .unchanged_dirs
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn error(&self, msg: &str) {
        self.error_counter.fetch_add(1, Ordering::Relaxed);
        let _ = self.mp.println(format!("{} {msg}", "Error:".bold().red()));
    }

    pub fn get_summary(&self) -> SnapshotSummary {
        let total_raw_bytes =
            self.raw_bytes.load(Ordering::Relaxed) + self.meta_raw_bytes.load(Ordering::Relaxed);
        let total_encoded_bytes = self.encoded_bytes.load(Ordering::Relaxed)
            + self.meta_encoded_bytes.load(Ordering::Relaxed);

        SnapshotSummary {
            processed_items_count: self.processed_items_count.load(Ordering::Relaxed),
            processed_bytes: self.processed_bytes.load(Ordering::Relaxed),
            raw_bytes: self.raw_bytes.load(Ordering::Relaxed),
            encoded_bytes: self.encoded_bytes.load(Ordering::Relaxed),
            meta_raw_bytes: self.meta_raw_bytes.load(Ordering::Relaxed),
            meta_encoded_bytes: self.meta_encoded_bytes.load(Ordering::Relaxed),
            total_raw_bytes,
            total_encoded_bytes,
            diff_counts: self.diff_counts.snapshot(),
            amends: None,
        }
    }
}
