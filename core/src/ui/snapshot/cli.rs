use std::{
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
    mapache::global::GlobalOpts,
    ui::{SPINNER_TICK_CHARS, default_bar_draw_target},
    utils,
};

use super::SnapshotProgressReporter;

enum UiEvent {
    Start(PathBuf),
    Done(PathBuf),
    Shutdown,
}

fn ui_loop(rx: Receiver<UiEvent>, update_interval: Duration, spinners: Vec<ProgressBar>) {
    let slots_limit = spinners.len();
    let mut active: Vec<PathBuf> = Vec::with_capacity(slots_limit);

    // Throttle UI updates
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

        // Only redraw if the channel is drained OR enough time has passed.
        // This ensures "snappy" updates when idle, but "batched" updates during bursts.
        if rx.is_empty() || last_update.elapsed() >= update_interval {
            for (i, spinner) in spinners.iter().enumerate().take(slots_limit) {
                let path = active.get(i).cloned().unwrap_or_default();
                spinner.set_message(abbreviate_path(
                    &path,
                    crate::mapache::defaults::MAX_PATH_DISPLAY_LEN,
                ));
            }
            last_update = Instant::now();
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
                    match state.len() {
                        // Scan phase: "1.234 MB"
                        None => {
                            let _ = w.write_str(&utils::format_size_binary(bytes, 3));
                        }
                        // Final phase: "1.234 MB / 10.552 GB"
                        Some(total) => {
                            let _ = write!(
                                w,
                                "{} / {}",
                                utils::format_size_binary(bytes, 3),
                                utils::format_size_binary(total, 3)
                            );
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

        progress_bar.set_style(if expected_size_val.is_some() {
            determined_style.clone()
        } else {
            undetermined_style
        });

        // Companion Bar: Dynamic Items and Errors
        let error_counter_for_style = Arc::clone(&error_counter);
        companion_bar.set_style(
            ProgressStyle::default_bar()
                .template("[{items_info}] [{errors} errors]")
                .expect("template")
                .with_key(
                    "items_info",
                    |state: &ProgressState, w: &mut dyn std::fmt::Write| match state.len() {
                        None => {
                            let _ = write!(w, "{} items", state.pos());
                        }
                        Some(len) => {
                            let _ = write!(w, "{} / {} items", state.pos(), len);
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
        let ui_thread = Some(thread::spawn(move || {
            ui_loop(ui_rx, refresh_interval, spinners_for_thread);
        }));

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

        if new_total > 0 {
            self.progress_bar.set_style(self.determined_style.clone());
        }
    }

    fn scan_finished(&self) {
        let bytes = self.expected_bytes.load(Ordering::Relaxed);
        self.progress_bar.set_length(bytes);
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

    fn processing_node(&self, path: PathBuf, diff: NodeDiff) {
        if self.ui_stop.load(Ordering::Relaxed) {
            return;
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

        if !self.file_spinners.is_empty() && diff != NodeDiff::Deleted {
            let _ = self.ui_tx.try_send(UiEvent::Start(path));
        }
    }

    fn processed_node(&self, path: PathBuf, diff: NodeDiff) {
        if diff != NodeDiff::Deleted {
            self.companion_bar.inc(1);
        }

        if !self.ui_stop.load(Ordering::Relaxed) && !self.file_spinners.is_empty() {
            let _ = self.ui_tx.send(UiEvent::Done(path));
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn setup_ui_test(num_slots: usize) -> (Sender<UiEvent>, Vec<ProgressBar>, JoinHandle<()>) {
        let (tx, rx) = crossbeam_channel::unbounded();
        let refresh_interval = Duration::from_millis(100);
        let spinners: Vec<ProgressBar> = (0..num_slots).map(|_| ProgressBar::hidden()).collect();

        let s_clone = spinners.clone();
        let handle = std::thread::spawn(move || {
            ui_loop(rx, refresh_interval, s_clone);
        });

        (tx, spinners, handle)
    }

    #[test]
    fn test_ui_logic_persistence_and_sliding() {
        let (tx, spinners, _handle) = setup_ui_test(2);

        tx.send(UiEvent::Start("A".into())).unwrap();
        tx.send(UiEvent::Start("B".into())).unwrap();
        tx.send(UiEvent::Start("C".into())).unwrap();

        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(spinners[0].message(), "A");
        assert_eq!(spinners[1].message(), "B");

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

        tx.send(UiEvent::Done("B".into())).unwrap();
        std::thread::sleep(Duration::from_millis(50));

        assert_eq!(spinners[0].message(), "A");
        assert_eq!(spinners[1].message(), "C");
    }

    #[test]
    fn test_reporter_progress_tracking() {
        // We test that the ProgressBar's internal state is updated correctly
        let reporter = CliSnapshotProgressReporter::new(Some(10), Some(1000), 2);

        reporter.processed_bytes(500);
        reporter.error("Test error");

        // Assert against the ProgressBar state directly
        assert_eq!(reporter.progress_bar.position(), 500);
        assert_eq!(reporter.error_counter.load(Ordering::Relaxed), 1);

        reporter.finalize();
    }

    #[test]
    fn test_reporter_expected_updates() {
        let reporter = CliSnapshotProgressReporter::new(None, None, 2);

        // Initial state is undetermined
        assert_eq!(reporter.progress_bar.length(), None);
        assert_eq!(reporter.companion_bar.length(), None);
        assert_eq!(reporter.progress_bar.length(), None);
        assert_eq!(reporter.companion_bar.length(), None);

        reporter.add_expected_bytes(2048);
        reporter.add_expected_items(5);
        reporter.scan_finished();

        // Indicatif uses Option<u64> for length
        assert_eq!(reporter.progress_bar.length(), Some(2048));
        assert_eq!(reporter.companion_bar.length(), Some(5));

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
