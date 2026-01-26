use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::{self, JoinHandle},
};

use colored::Colorize;
use crossbeam_channel::{Receiver, Sender};
use indicatif::{MultiProgress, ProgressBar, ProgressState, ProgressStyle};
use parking_lot::{Mutex, RwLock};

use crate::{
    fs::tree::NodeDiff,
    mapache::global::GlobalOpts,
    repository::snapshot::{DiffCounts, DiffCountsAtomic},
    ui::{SPINNER_TICK_CHARS, default_bar_draw_target},
    utils::{self},
};

enum UiEvent {
    Start(String),
    Done(String),
    Shutdown,
}

fn ui_loop(rx: Receiver<UiEvent>, spinners: Vec<ProgressBar>) {
    let slots_limit = spinners.len();
    let mut active: Vec<String> = Vec::with_capacity(slots_limit);

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

        for (i, spinner) in spinners.iter().enumerate().take(slots_limit) {
            if let Some(path) = active.get(i) {
                spinner.set_message(path.clone());
            } else {
                spinner.set_message("");
            }
        }
    }
}

pub(crate) struct SnapshotProcessSummary {
    pub processed_items_count: u64,
    pub processed_bytes: u64,
    pub diff_counts: DiffCounts,
}

pub struct SnapshotProgressReporter {
    // Hot-path counters
    processed_items_count: Arc<AtomicU64>,
    processed_bytes: Arc<AtomicU64>,

    expected_items: Arc<RwLock<Option<AtomicU64>>>,
    expected_bytes: Arc<RwLock<Option<AtomicU64>>>,

    diff_counts: Arc<DiffCountsAtomic>,
    error_counter: Arc<AtomicU64>,

    determined_style: ProgressStyle,

    mp: MultiProgress,
    progress_bar: ProgressBar,
    companion_bar: ProgressBar,
    file_spinners: Vec<ProgressBar>,

    verbosity: u32,

    // UI thread
    ui_tx: Sender<UiEvent>,
    ui_stop: AtomicBool,
    _ui_thread: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for SnapshotProgressReporter {
    fn drop(&mut self) {
        // We call finalize to ensure the thread is joined and
        // the terminal is restored even if finalize wasn't called manually.
        self.finalize();
    }
}

impl SnapshotProgressReporter {
    pub fn new(
        expected_items: Option<u64>,
        expected_size: Option<u64>,
        num_display_items: usize,
    ) -> Self {
        let verbosity = GlobalOpts::verbosity();

        let refresh_interval = GlobalOpts::progress_refresh_interval();
        let mp = MultiProgress::with_draw_target(default_bar_draw_target());

        let progress_bar = match expected_size {
            Some(size) => mp.add(ProgressBar::new(size)),
            None => mp.add(ProgressBar::no_length()),
        };
        progress_bar.enable_steady_tick(refresh_interval);
        let companion_bar = mp.add(ProgressBar::no_length());

        // ---------------- Hot-path counters ----------------
        let processed_items_count = Arc::new(AtomicU64::new(0));
        let processed_bytes = Arc::new(AtomicU64::new(0));

        let expected_items = Arc::new(RwLock::new(expected_items.map(AtomicU64::new)));
        let expected_bytes = Arc::new(RwLock::new(expected_size.map(AtomicU64::new)));

        let diff_counts = Arc::new(DiffCountsAtomic::default());
        let error_counter = Arc::new(AtomicU64::new(0));

        // ---------------- Styles ----------------
        // IMPORTANT: closures must not take write locks.
        let processed_bytes_arc_clone = processed_bytes.clone();
        let expected_bytes_arc_clone = expected_bytes.clone();
        let undetermined_style = ProgressStyle::default_bar()
            .template("[{custom_elapsed}] [{processed_bytes_fmt}]")
            .expect("progress bar template")
            .progress_chars("=> ")
            .with_key(
                "custom_elapsed",
                move |state: &ProgressState, w: &mut dyn std::fmt::Write| {
                    let s = utils::pretty_print_duration(state.elapsed());
                    let _ = w.write_str(&s);
                },
            )
            .with_key(
                "processed_bytes_fmt",
                move |_state: &ProgressState, w: &mut dyn std::fmt::Write| {
                    let bytes = processed_bytes_arc_clone.load(Ordering::Relaxed);
                    let lock = expected_bytes_arc_clone.read();
                    let s = match lock.as_ref() {
                        Some(a) => {
                            let expected = a.load(Ordering::Relaxed);
                            format!(
                                "{} / {}",
                                utils::format_size_binary(bytes, 3),
                                utils::format_size_binary(expected, 3),
                            )
                        }
                        None => utils::format_size_binary(bytes, 3).to_string(),
                    };
                    let _ = w.write_str(&s);
                },
            );

        let processed_bytes_arc_clone = processed_bytes.clone();
        let expected_bytes_arc_clone = expected_bytes.clone();
        let determined_style = ProgressStyle::default_bar()
        .template("[{percent} %] [{bar:20.cyan/white}] [{custom_elapsed}] [{processed_bytes_fmt}] [ETA: {custom_eta}]")
        .expect("progress bar template")
        .progress_chars("=> ")
        .with_key(
            "custom_elapsed",
            move |state: &ProgressState, w: &mut dyn std::fmt::Write| {
                let s = utils::pretty_print_duration(state.elapsed());
                let _ = w.write_str(&s);
            },
        )
        .with_key(
            "processed_bytes_fmt",
            move |_state: &ProgressState, w: &mut dyn std::fmt::Write| {
                let bytes = processed_bytes_arc_clone.load(Ordering::Relaxed);
                let lock = expected_bytes_arc_clone.read();
                let s = match lock.as_ref() {
                    Some(a) => {
                        let expected = a.load(Ordering::Relaxed);
                        format!(
                            "{} / {}",
                            utils::format_size_binary(bytes, 3),
                            utils::format_size_binary(expected, 3),
                        )
                    }
                    None => utils::format_size_binary(bytes, 3).to_string(),
                };
                let _ = w.write_str(&s);
            },
        )
        .with_key("custom_eta", move |state: &ProgressState, w: &mut dyn std::fmt::Write| {
            let s = utils::pretty_print_duration(state.eta());
            let _ = w.write_str(&s);
        });

        match expected_size {
            Some(_) => progress_bar.set_style(determined_style.clone()),
            None => progress_bar.set_style(undetermined_style),
        };

        let error_counter_clone = error_counter.clone();
        let expected_items_clone = expected_items.clone();
        let processed_items_count_clone = processed_items_count.clone();
        companion_bar.set_style(
            ProgressStyle::default_bar()
                .template("[{processed_items_fmt}] [{errors} errors]")
                .expect("companion bar template")
                .progress_chars("=> ")
                .with_key(
                    "processed_items_fmt",
                    move |_state: &ProgressState, w: &mut dyn std::fmt::Write| {
                        let item_count = processed_items_count_clone.load(Ordering::Relaxed);
                        let lock = expected_items_clone.read();
                        let s = match lock.as_ref() {
                            Some(a) => {
                                let expected = a.load(Ordering::Relaxed);
                                format!("{item_count} / {expected} items")
                            }
                            None => format!("{item_count} items"),
                        };
                        let _ = w.write_str(&s);
                    },
                )
                .with_key(
                    "errors",
                    move |_state: &ProgressState, w: &mut dyn std::fmt::Write| {
                        let errors = error_counter_clone.load(Ordering::Relaxed);
                        let _ = w.write_str(&errors.to_string());
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

        // ---------------- UI channel + thread ----------------
        let (ui_tx, ui_rx) = crossbeam_channel::unbounded::<UiEvent>();
        let ui_stop = AtomicBool::new(false);

        // Clone handles (cheap: indicatif internals are Arc-based)
        let spinners_for_thread = file_spinners.to_vec();

        let ui_thread = Some(thread::spawn(move || {
            ui_loop(ui_rx, spinners_for_thread);
        }));

        Self {
            processed_items_count,
            processed_bytes,

            expected_items,
            expected_bytes,

            diff_counts,
            error_counter,

            determined_style,

            mp,
            progress_bar,
            companion_bar,
            file_spinners,

            verbosity,

            ui_tx,
            ui_stop,
            _ui_thread: Mutex::new(ui_thread),
        }
    }

    pub fn add_expected_items(&self, val: u64) {
        let mut lock = self.expected_items.write();
        match lock.as_ref() {
            Some(a) => {
                a.fetch_add(val, Ordering::Relaxed);
            }
            None => {
                let _ = lock.insert(AtomicU64::new(val));
            }
        }
    }

    pub fn add_expected_bytes(&self, val: u64) {
        let mut lock = self.expected_bytes.write();
        match lock.as_ref() {
            Some(a) => {
                a.fetch_add(val, Ordering::Relaxed);
            }
            None => {
                let _ = lock.insert(AtomicU64::new(val));
            }
        }
    }

    /// Call when scan completed and you now know total expected bytes.
    pub fn scan_finished(&self) {
        let lock = self.expected_bytes.read();
        let bytes_opt = lock.as_ref().map(|a| a.load(Ordering::Relaxed));
        drop(lock);

        if let Some(bytes) = bytes_opt {
            self.progress_bar.set_length(bytes);
            self.progress_bar.set_style(self.determined_style.clone());
        }
    }

    pub fn finalize(&self) {
        self.ui_stop.store(true, Ordering::Relaxed);

        let _ = self.ui_tx.send(UiEvent::Shutdown);

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

    fn abbr(path: &Path) -> String {
        crate::utils::abbreviate_path(path, crate::mapache::defaults::MAX_PATH_DISPLAY_LEN)
    }

    pub fn processing_node(&self, path: &Path, diff: NodeDiff) {
        if self.ui_stop.load(Ordering::Relaxed) {
            return;
        }

        if !self.file_spinners.is_empty() && diff != NodeDiff::Deleted {
            let _ = self.ui_tx.send(UiEvent::Start(Self::abbr(path)));
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
    }

    pub fn processed_node(&self, path: &Path) {
        self.processed_items_count.fetch_add(1, Ordering::Relaxed);
        self.companion_bar.inc(1);

        if !self.ui_stop.load(Ordering::Relaxed) && !self.file_spinners.is_empty() {
            let _ = self.ui_tx.send(UiEvent::Done(Self::abbr(path)));
        }
    }

    #[inline]
    pub fn processed_bytes(&self, bytes: u64) {
        self.processed_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.progress_bar.inc(bytes);
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

    pub(crate) fn summary(&self) -> SnapshotProcessSummary {
        SnapshotProcessSummary {
            processed_items_count: self.processed_items_count.load(Ordering::Relaxed),
            processed_bytes: self.processed_bytes.load(Ordering::Relaxed),
            diff_counts: self.diff_counts.snapshot(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// Helper to create a test environment for the ui_loop.
    /// We use hidden bars to avoid messing with the cargo test output.
    fn setup_ui_test(num_slots: usize) -> (Sender<UiEvent>, Vec<ProgressBar>, JoinHandle<()>) {
        let (tx, rx) = crossbeam_channel::unbounded();
        let spinners: Vec<ProgressBar> = (0..num_slots).map(|_| ProgressBar::hidden()).collect();

        let s_clone = spinners.clone();

        let handle = std::thread::spawn(move || {
            ui_loop(rx, s_clone);
        });

        (tx, spinners, handle)
    }

    #[test]
    fn test_ui_logic_persistence_and_sliding() {
        let (tx, spinners, _handle) = setup_ui_test(2);

        // Start 3 items. A and B should occupy the 2 slots.
        tx.send(UiEvent::Start("A".into())).unwrap();
        tx.send(UiEvent::Start("B".into())).unwrap();
        tx.send(UiEvent::Start("C".into())).unwrap();

        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(spinners[0].message(), "A");
        assert_eq!(spinners[1].message(), "B");

        // Finish A. B should slide up to Slot 0, C should appear in Slot 1.
        tx.send(UiEvent::Done("A".into())).unwrap();
        std::thread::sleep(Duration::from_millis(50));

        assert_eq!(spinners[0].message(), "B");
        assert_eq!(spinners[1].message(), "C");
    }

    #[test]
    fn test_ui_out_of_order_done() {
        let (tx, spinners, _handle) = setup_ui_test(2);

        tx.send(UiEvent::Start("A".into())).unwrap();
        tx.send(UiEvent::Start("B".into())).unwrap();
        tx.send(UiEvent::Start("C".into())).unwrap();

        std::thread::sleep(Duration::from_millis(20));

        // Finish B (the second visible item).
        // A should remain in Slot 0, C should move into Slot 1.
        tx.send(UiEvent::Done("B".into())).unwrap();
        std::thread::sleep(Duration::from_millis(50));

        assert_eq!(spinners[0].message(), "A");
        assert_eq!(spinners[1].message(), "C");
    }

    #[test]
    fn test_reporter_atomic_counters() {
        // GlobalOpts and utils are required by the reporter logic
        let reporter = SnapshotProgressReporter::new(Some(10), Some(1000), 2);

        reporter.processed_bytes(500);
        reporter.new_file();
        reporter.new_dir();
        reporter.error("Test error");

        let summary = reporter.summary();
        assert_eq!(summary.processed_bytes, 500);
        assert_eq!(summary.diff_counts.new_files, 1);
        assert_eq!(summary.diff_counts.new_dirs, 1);
        assert_eq!(reporter.error_counter.load(Ordering::Relaxed), 1);

        reporter.finalize();
    }

    #[test]
    fn test_reporter_expected_updates() {
        let reporter = SnapshotProgressReporter::new(None, None, 2);

        // Initial state is undetermined
        assert_eq!(reporter.progress_bar.length(), None);

        reporter.add_expected_bytes(2048);
        reporter.add_expected_items(5);
        reporter.scan_finished();

        // Length should now be set
        assert_eq!(reporter.progress_bar.length(), Some(2048));

        reporter.finalize();
    }

    #[test]
    fn test_ui_duplicate_start_prevention() {
        let (tx, spinners, _handle) = setup_ui_test(2);

        tx.send(UiEvent::Start("OnlyOnce".into())).unwrap();
        tx.send(UiEvent::Start("OnlyOnce".into())).unwrap();

        std::thread::sleep(Duration::from_millis(30));
        assert_eq!(spinners[0].message(), "OnlyOnce");
        assert_eq!(spinners[1].message(), "");
    }
}
