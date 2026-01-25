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
use parking_lot::Mutex;

use crate::{
    mapache::{defaults::MAX_PATH_DISPLAY_LEN, global::GlobalOpts},
    ui::{SPINNER_TICK_CHARS, default_bar_draw_target},
    utils,
};

/// Events sent from worker threads to the UI thread.
pub enum UiEvent {
    Start(String),
    Done(String),
    Shutdown,
}

/// Background loop that manages active file slots and terminal rendering.
pub fn ui_loop(rx: Receiver<UiEvent>, spinners: Vec<ProgressBar>) {
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
            UiEvent::Shutdown => break,
        }

        // Update all spinners based on current active state
        for (i, spinner) in spinners.iter().enumerate() {
            if let Some(msg) = active.get(i) {
                spinner.set_message(msg.clone());
            } else {
                spinner.set_message("");
            }
        }
    }
}

pub struct RestoreProgressReporter {
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
        progress_bar.set_style(
            ProgressStyle::default_bar()
                .template("[{percent} %] [{bar:20.cyan/white}] [{custom_elapsed}]  [{processed_bytes_fmt}]  [ETA: {custom_eta}]")
                .expect("progress bar template")
                .progress_chars("=> ")
                .with_key("custom_elapsed", |_state: &ProgressState, w: &mut dyn std::fmt::Write| {
                    let _ = w.write_str(&utils::pretty_print_duration(_state.elapsed()));
                })
                .with_key("processed_bytes_fmt", |_state: &ProgressState, w: &mut dyn std::fmt::Write| {
                    let s = format!(
                        "{} / {}",
                        utils::format_size_binary(_state.pos(), 3),
                        utils::format_size_binary(_state.len().unwrap_or(0), 3)
                    );
                    let _ = w.write_str(&s);
                })
                .with_key("custom_eta", |_state: &ProgressState, w: &mut dyn std::fmt::Write| {
                    let _ = w.write_str(&utils::pretty_print_duration(_state.eta()));
                })
        );

        companion_bar.set_style(
            ProgressStyle::default_bar()
                .template("[{processed_items_fmt}]  [{errors} errors, {warnings} warnings]")
                .expect("companion bar template")
                .with_key("processed_items_fmt", {
                    let count = processed_items_count.clone();
                    move |_state: &ProgressState, w: &mut dyn std::fmt::Write| {
                        let items = count.load(Ordering::Relaxed);
                        let _ = write!(w, "{items} / {num_expected_items} items");
                    }
                })
                .with_key("errors", {
                    let errors = error_counter.clone();
                    move |_state: &ProgressState, w: &mut dyn std::fmt::Write| {
                        let _ = write!(w, "{}", errors.load(Ordering::Relaxed));
                    }
                })
                .with_key("warnings", {
                    let warnings = warning_counter.clone();
                    move |_state: &ProgressState, w: &mut dyn std::fmt::Write| {
                        let _ = write!(w, "{}", warnings.load(Ordering::Relaxed));
                    }
                }),
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

        // ---------------- UI Thread Setup ----------------
        let (ui_tx, ui_rx) = crossbeam_channel::unbounded::<UiEvent>();
        let ui_stop = AtomicBool::new(false);
        let spinners_clone = file_spinners.clone();

        let ui_thread = thread::spawn(move || {
            ui_loop(ui_rx, spinners_clone);
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

    /// Safely shuts down the UI thread and cleans the terminal.
    pub fn finalize(&self) {
        self.ui_stop.store(true, Ordering::Relaxed);

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
        if self.ui_stop.load(Ordering::Relaxed) {
            return;
        }
        let abbr = utils::abbreviate_path(path, MAX_PATH_DISPLAY_LEN);
        let _ = self.ui_tx.try_send(UiEvent::Start(abbr));
    }

    pub fn processed_item(&self, path: &Path) {
        self.processed_items_count.fetch_add(1, Ordering::Relaxed);
        if !self.ui_stop.load(Ordering::Relaxed) {
            let abbr = utils::abbreviate_path(path, MAX_PATH_DISPLAY_LEN);
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn setup_test() -> (Sender<UiEvent>, Vec<ProgressBar>, JoinHandle<()>) {
        let (tx, rx) = crossbeam_channel::unbounded();
        // Use hidden bars so we don't spam the test console
        let spinners = vec![ProgressBar::hidden(), ProgressBar::hidden()];

        let s_clone = spinners.clone();
        let handle = std::thread::spawn(move || {
            ui_loop(rx, s_clone);
        });

        (tx, spinners, handle)
    }

    #[test]
    fn test_ui_overflow_scrolling() {
        let (tx, spinners, _handle) = setup_test();

        // Fill 2 slots
        tx.send(UiEvent::Start("file_A".into())).unwrap();
        tx.send(UiEvent::Start("file_B".into())).unwrap();

        // Overflow with third file
        // Expected: [file_A, file_B] -> [file_B, file_C]
        tx.send(UiEvent::Start("file_C".into())).unwrap();

        // Small sleep to let the UI thread catch up
        std::thread::sleep(Duration::from_millis(50));

        assert_eq!(spinners[0].message(), "file_B");
        assert_eq!(spinners[1].message(), "file_C");
    }

    #[test]
    fn test_ui_duplicate_prevention() {
        let (tx, spinners, _handle) = setup_test();

        tx.send(UiEvent::Start("unique_A".into())).unwrap();
        tx.send(UiEvent::Start("unique_A".into())).unwrap();

        std::thread::sleep(Duration::from_millis(50));

        assert_eq!(spinners[0].message(), "unique_A");
        assert_eq!(spinners[1].message(), ""); // Should stay empty
    }

    #[test]
    fn test_ui_shift_up_on_done() {
        let (tx, spinners, _handle) = setup_test();

        tx.send(UiEvent::Start("1".into())).unwrap();
        tx.send(UiEvent::Start("2".into())).unwrap();

        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(spinners[0].message(), "1");
        assert_eq!(spinners[1].message(), "2");

        // When "1" finishes, "2" should shift into the first slot
        tx.send(UiEvent::Done("1".into())).unwrap();

        std::thread::sleep(Duration::from_millis(50));

        assert_eq!(spinners[0].message(), "2");
        assert_eq!(spinners[1].message(), "");
    }

    #[test]
    fn test_ui_shutdown_signal() {
        let (tx, _spinners, handle) = setup_test();

        tx.send(UiEvent::Shutdown).unwrap();

        // If the shutdown signal works, the thread should join within a reasonable time
        let result = std::thread::spawn(move || handle.join()).join();
        assert!(result.is_ok(), "UI thread failed to shut down on signal");
    }
}
