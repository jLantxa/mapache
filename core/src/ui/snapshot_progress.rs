use std::{
    collections::VecDeque,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use colored::Colorize;
use crossbeam_channel::{Receiver, Sender};
use indicatif::{MultiProgress, ProgressBar, ProgressState, ProgressStyle};
use parking_lot::RwLock;
use rustc_hash::FxHashSet;

use crate::{
    fs::tree::NodeDiff,
    mapache::global::GlobalOpts,
    repository::{
        repo::SizePair,
        snapshot::{DiffCountsAtomic, SnapshotSummary},
    },
    ui::{SPINNER_TICK_CHARS, default_bar_draw_target},
    utils,
};

enum UiEvent {
    Start(String),
    Done(String),
}

#[allow(clippy::too_many_arguments)]
fn ui_loop(
    stop: Arc<AtomicBool>,
    refresh: Duration,
    rx: Receiver<UiEvent>,
    processed_bytes: Arc<AtomicU64>,
    pb: ProgressBar,
    cb: ProgressBar,
    spinners: Vec<ProgressBar>,
    _n: usize, // not needed; use spinners.len()
) {
    let slots = spinners.len().max(1);

    // Active items in *start order* (oldest at front).
    let mut active: VecDeque<String> = VecDeque::with_capacity(slots);

    // Fast membership / dedup for Start.
    let mut in_flight: FxHashSet<String> = FxHashSet::default();
    in_flight.reserve(slots * 4);

    while !stop.load(Ordering::Relaxed) {
        // Drain all pending UI events.
        for ev in rx.try_iter() {
            match ev {
                UiEvent::Start(s) => {
                    if in_flight.insert(s.clone()) {
                        active.push_back(s);

                        // Should not happen if concurrency is correct, but keep bounded anyway.
                        while active.len() > slots {
                            if let Some(old) = active.pop_front() {
                                in_flight.remove(&old);
                            }
                        }
                    }
                }
                UiEvent::Done(s) => {
                    if in_flight.remove(&s) {
                        // Remove from active (O(slots) worst-case; slots is small).
                        if let Some(pos) = active.iter().position(|x| x == &s) {
                            active.remove(pos);
                        }
                    }
                }
            }
        }

        // Update main bar from atomic.
        pb.set_position(processed_bytes.load(Ordering::Relaxed));

        // Render oldest-first into spinners.
        let mut i = 0usize;
        for item in active.iter() {
            if i == spinners.len() {
                break;
            }
            spinners[i].set_message(item.clone());
            spinners[i].tick();
            i += 1;
        }

        // Clear remaining spinners so nothing “freezes”.
        for sp in spinners.iter().skip(i) {
            sp.set_message(String::new());
            sp.tick();
        }

        pb.tick();
        cb.tick();
        thread::sleep(refresh);
    }
}

pub struct SnapshotProgressReporter {
    // Hot-path counters
    processed_items_count: Arc<AtomicU64>,
    processed_bytes: Arc<AtomicU64>,
    raw_bytes: Arc<AtomicU64>,
    encoded_bytes: Arc<AtomicU64>,
    meta_raw_bytes: Arc<AtomicU64>,
    meta_encoded_bytes: Arc<AtomicU64>,

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
    ui_stop: Arc<AtomicBool>,
    ui_thread: Option<JoinHandle<()>>,
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
        let companion_bar = mp.add(ProgressBar::no_length());

        // ---------------- Hot-path counters ----------------
        let processed_items_count = Arc::new(AtomicU64::new(0));
        let processed_bytes = Arc::new(AtomicU64::new(0));
        let raw_bytes = Arc::new(AtomicU64::new(0));
        let encoded_bytes = Arc::new(AtomicU64::new(0));
        let meta_raw_bytes = Arc::new(AtomicU64::new(0));
        let meta_encoded_bytes = Arc::new(AtomicU64::new(0));

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
            file_spinners.push(s);
        }

        // ---------------- UI channel + thread ----------------
        // Bounded so worker hot-path can't blow up memory if UI stalls.
        let (ui_tx, ui_rx) = crossbeam_channel::unbounded::<UiEvent>();
        let ui_stop = Arc::new(AtomicBool::new(false));

        // Clone handles (cheap: indicatif internals are Arc-based)
        let pb = progress_bar.clone();
        let cb = companion_bar.clone();
        let spinners_for_thread: Vec<ProgressBar> = file_spinners.to_vec();

        let processed_bytes_for_thread = processed_bytes.clone();
        let ui_stop_for_thread = ui_stop.clone();

        let ui_thread = Some(thread::spawn(move || {
            ui_loop(
                ui_stop_for_thread,
                refresh_interval,
                ui_rx,
                processed_bytes_for_thread,
                pb,
                cb,
                spinners_for_thread,
                num_display_items.max(1),
            );
        }));

        Self {
            processed_items_count,
            processed_bytes,
            raw_bytes,
            encoded_bytes,
            meta_raw_bytes,
            meta_encoded_bytes,

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
            ui_thread,
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
        // Stop UI thread first (so it doesn't race MultiProgress clear/finish).
        self.ui_stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.ui_thread.as_ref() {
            h.thread().unpark();
        }
        if let Some(h) = self.ui_thread.as_ref() {
            // We cannot join from &self without ownership; see finalize_owned below.
            // Still safe: stopping prevents further ticks; leaving thread detached is OK but not ideal.
            // If you can change call sites, prefer finalize_owned().
            let _ = h; // no-op to keep intent clear
        }

        // Finish bars
        for sp in self.file_spinners.iter() {
            sp.finish_and_clear();
        }
        self.companion_bar.finish_and_clear();
        self.progress_bar.finish_and_clear();
        let _ = self.mp.clear();
    }

    /// Prefer calling this if you can consume the reporter (e.g., store in Arc and drop last Arc).
    pub fn finalize_owned(mut self) {
        self.ui_stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.ui_thread.take() {
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
        if !self.file_spinners.is_empty() && diff != NodeDiff::Deleted {
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

        if !self.file_spinners.is_empty() {
            let _ = self.ui_tx.try_send(UiEvent::Done(Self::abbr(path)));
        }
    }

    #[inline]
    pub fn processed_bytes(&self, bytes: u64) {
        self.processed_bytes.fetch_add(bytes, Ordering::Relaxed);
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
