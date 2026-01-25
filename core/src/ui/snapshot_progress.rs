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
    repository::snapshot::{DiffCountsAtomic, SnapshotSummary},
    ui::{SPINNER_TICK_CHARS, default_bar_draw_target},
    utils,
};

enum UiEvent {
    Start(String),
    Done(String),
    Shutdown,
}

fn ui_loop(
    rx: Receiver<UiEvent>,
    processed_bytes: Arc<AtomicU64>,
    pb: ProgressBar,
    spinners: Vec<ProgressBar>,
) {
    let slots_limit = spinners.len();
    let mut active: Vec<String> = Vec::with_capacity(slots_limit);

    while let Ok(ev) = rx.recv() {
        match ev {
            UiEvent::Start(path) => {
                if !active.contains(&path) {
                    active.push(path);
                }
                if active.len() > slots_limit {
                    active.remove(0);
                }
            }
            UiEvent::Done(path) => {
                if let Some(pos) = active.iter().position(|x| x == &path) {
                    active.remove(pos);
                }
            }
            UiEvent::Shutdown => break, // Clean exit
        }

        pb.set_position(processed_bytes.load(Ordering::Relaxed));

        for (i, spinner) in spinners.iter().enumerate().take(slots_limit) {
            if let Some(path) = active.get(i) {
                spinner.set_message(path.clone());
            } else {
                spinner.set_message("");
            }
        }
    }
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

        let refresh_rate = GlobalOpts::progress_refresh_interval();
        let mp = MultiProgress::with_draw_target(default_bar_draw_target());

        let progress_bar = match expected_size {
            Some(size) => mp.add(ProgressBar::new(size)),
            None => mp.add(ProgressBar::no_length()),
        };
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
            .template("[{custom_elapsed}]  [{processed_bytes_fmt}]")
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
        .template("[{percent} %] [{bar:20.cyan/white}] [{custom_elapsed}]  [{processed_bytes_fmt}]  [ETA: {custom_eta}]")
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
                .template("[{processed_items_fmt}]  [{errors} errors]")
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
            s.enable_steady_tick(refresh_rate);
            file_spinners.push(s);
        }

        // ---------------- UI channel + thread ----------------
        let (ui_tx, ui_rx) = crossbeam_channel::unbounded::<UiEvent>();
        let ui_stop = AtomicBool::new(false);

        // Clone handles (cheap: indicatif internals are Arc-based)
        let pb = progress_bar.clone();
        let spinners_for_thread: Vec<ProgressBar> = file_spinners.to_vec();

        let processed_bytes_for_thread = processed_bytes.clone();

        let ui_thread = Some(thread::spawn(move || {
            ui_loop(ui_rx, processed_bytes_for_thread, pb, spinners_for_thread);
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
            // 2. Direct send: No Mutex, no Option unwrap
            let _ = self.ui_tx.try_send(UiEvent::Start(Self::abbr(path)));
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

        if !self.ui_stop.load(Ordering::Relaxed) && !self.file_spinners.is_empty() {
            let _ = self.ui_tx.try_send(UiEvent::Done(Self::abbr(path)));
        }
    }

    #[inline]
    pub fn processed_bytes(&self, bytes: u64) {
        self.processed_bytes.fetch_add(bytes, Ordering::Relaxed);
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
        SnapshotSummary {
            processed_items_count: self.processed_items_count.load(Ordering::Relaxed),
            processed_bytes: self.processed_bytes.load(Ordering::Relaxed),
            raw_bytes: 0,
            encoded_bytes: 0,
            meta_raw_bytes: 0,
            meta_encoded_bytes: 0,
            total_raw_bytes: 0,
            total_encoded_bytes: 0,
            diff_counts: self.diff_counts.snapshot(),
            amends: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn setup_test() -> (Sender<UiEvent>, Vec<ProgressBar>, JoinHandle<()>) {
        let (tx, rx) = crossbeam_channel::unbounded();
        let pb = ProgressBar::hidden();
        let spinners = vec![ProgressBar::hidden(), ProgressBar::hidden()]; // 2 slots
        let processed_bytes = Arc::new(AtomicU64::new(0));

        let s_clone = spinners.clone();
        let handle = std::thread::spawn(move || {
            ui_loop(rx, processed_bytes, pb, s_clone);
        });

        (tx, spinners, handle)
    }

    #[test]
    fn test_ui_overflow_scrolling() {
        let (tx, spinners, _handle) = setup_test();

        // Fill slots
        tx.send(UiEvent::Start("A".into())).unwrap();
        tx.send(UiEvent::Start("B".into())).unwrap();

        // Add a third file (overflow)
        // Expected behavior: A is removed, B moves to index 0, C at index 1
        tx.send(UiEvent::Start("C".into())).unwrap();

        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(spinners[0].message(), "B");
        assert_eq!(spinners[1].message(), "C");
    }

    #[test]
    fn test_ui_duplicate_prevention() {
        let (tx, spinners, _handle) = setup_test();

        tx.send(UiEvent::Start("A".into())).unwrap();
        tx.send(UiEvent::Start("A".into())).unwrap(); // Duplicate

        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(spinners[0].message(), "A");
        assert_eq!(spinners[1].message(), ""); // Second slot should stay empty
    }

    #[test]
    fn test_ui_loop_scroll_up_logic() {
        let (tx, rx) = crossbeam_channel::unbounded();
        let pb = ProgressBar::hidden();
        let spinners = vec![ProgressBar::hidden(), ProgressBar::hidden()];
        let processed_bytes = Arc::new(AtomicU64::new(0));

        // Start the loop in a background thread
        let spinners_clone = spinners.clone();
        let pb_clone = pb.clone();
        std::thread::spawn(move || {
            ui_loop(rx, processed_bytes, pb_clone, spinners_clone);
        });

        // Start two files
        tx.send(UiEvent::Start("file_A".to_string())).unwrap();
        tx.send(UiEvent::Start("file_B".to_string())).unwrap();

        // Give the loop a moment to process
        std::thread::sleep(std::time::Duration::from_millis(10));
        assert_eq!(spinners[0].message(), "file_A");
        assert_eq!(spinners[1].message(), "file_B");

        // Finish the FIRST file (file_A)
        // file_B should move UP to slot 0
        tx.send(UiEvent::Done("file_A".to_string())).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(10));
        assert_eq!(spinners[0].message(), "file_B");
        assert_eq!(spinners[1].message(), ""); // Slot 1 cleared

        // Drop sender to close the loop
        drop(tx);
    }
}
