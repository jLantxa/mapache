use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::JoinHandle;

use colored::Colorize;
use crossbeam_channel::Sender;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use parking_lot::Mutex;

use crate::ui::snapshot::SnapshotProgressReporter;
use crate::{
    fs::{abbreviate_path, tree::NodeDiff},
    mapache::{defaults, global::GlobalOpts},
    ui::{SPINNER_TICK_CHARS, default_bar_draw_target},
};

pub enum ArchiveMode {
    Archive,
    Extract,
}

enum UiEvent {
    Start(PathBuf),
    Done(PathBuf),
    Shutdown,
}

pub struct ArchiveCliProgressReporter {
    mp: MultiProgress,
    items_bar: ProgressBar,
    data_bar: ProgressBar,

    ui_tx: Sender<UiEvent>,
    ui_stop: AtomicBool,
    _ui_thread: Mutex<Option<JoinHandle<()>>>,

    error_counter: AtomicU64,
}

impl ArchiveCliProgressReporter {
    pub fn new(mode: ArchiveMode, total_items: u64, total_bytes: u64, num_spinners: usize) -> Self {
        let refresh_interval = GlobalOpts::progress_refresh_interval();
        let mp = MultiProgress::with_draw_target(default_bar_draw_target());

        let (items_label, bytes_label) = match mode {
            ArchiveMode::Archive => ("Items", "Data"),
            ArchiveMode::Extract => ("Items", "Data"),
        };

        // Items Bar
        let items_bar = mp.add(ProgressBar::new(total_items));
        items_bar.set_style(
            ProgressStyle::default_bar()
                .template(&format!("{{spinner:.cyan}} {items_label:<6} [{{bar:30.cyan/blue}}] {{pos}}/{{len}} ({{percent}}%)"))
                .unwrap()
                .tick_chars(SPINNER_TICK_CHARS)
                .progress_chars("█#░"),
        );
        items_bar.enable_steady_tick(refresh_interval);

        //  Data Bar
        let data_bar = mp.add(ProgressBar::new(total_bytes));
        data_bar.set_style(
            ProgressStyle::default_bar()
                .template(&format!("  {bytes_label:<6} [{{bar:30.green/blue}}] {{bytes}}/{{total_bytes}} [{{bytes_per_sec}}]"))
                .unwrap()
                .progress_chars("█#░"),
        );
        data_bar.enable_steady_tick(refresh_interval);

        let (ui_tx, ui_rx) = crossbeam_channel::unbounded();

        let mp_clone = mp.clone();
        let ui_handle = std::thread::spawn(move || {
            let mut active_spinners: HashMap<PathBuf, ProgressBar> = HashMap::new();

            let file_style = ProgressStyle::default_spinner()
                .template("    {spinner:.green} {msg}")
                .unwrap()
                .tick_chars(SPINNER_TICK_CHARS);

            loop {
                match ui_rx.recv_timeout(refresh_interval) {
                    Ok(UiEvent::Start(path)) => {
                        if !active_spinners.contains_key(&path)
                            && active_spinners.len() < num_spinners
                        {
                            let pb = mp_clone.add(ProgressBar::new_spinner());
                            pb.set_style(file_style.clone());
                            pb.set_message(abbreviate_path(&path, defaults::MAX_PATH_DISPLAY_LEN));
                            pb.enable_steady_tick(refresh_interval);
                            active_spinners.insert(path, pb);
                        }
                    }
                    Ok(UiEvent::Done(path)) => {
                        if let Some(pb) = active_spinners.remove(&path) {
                            pb.finish_and_clear();
                        }
                    }
                    Ok(UiEvent::Shutdown) => break,
                    Err(_) => {}
                }
            }

            // Clean up any remaining
            for (_, pb) in active_spinners {
                pb.finish_and_clear();
            }
        });

        Self {
            mp,
            items_bar,
            data_bar,
            ui_tx,
            ui_stop: AtomicBool::new(false),
            _ui_thread: Mutex::new(Some(ui_handle)),
            error_counter: AtomicU64::new(0),
        }
    }
}

impl SnapshotProgressReporter for ArchiveCliProgressReporter {
    fn processing_node(&self, path: &Path, diff: NodeDiff, size_hint: Option<u64>) {
        if self.ui_stop.load(Ordering::Relaxed) || diff == NodeDiff::Deleted {
            return;
        }

        let is_slow = defaults::UI_SNAPSHOT_PROGRESS_ITEM_MIN_SIZE
            .is_none_or(|t| size_hint.is_none_or(|s| s >= t));

        if is_slow {
            let _ = self.ui_tx.try_send(UiEvent::Start(path.to_path_buf()));
        }
    }

    fn processed_node(&self, path: &Path, diff: NodeDiff, _size_hint: Option<u64>) {
        if diff != NodeDiff::Deleted {
            self.items_bar.inc(1);
        }

        if !self.ui_stop.load(Ordering::Relaxed) {
            let _ = self.ui_tx.try_send(UiEvent::Done(path.to_path_buf()));
        }
    }

    fn processed_bytes(&self, bytes: u64) {
        self.data_bar.inc(bytes);
    }

    fn add_expected_items(&self, val: u64) {
        self.items_bar.inc_length(val);
    }

    fn add_expected_bytes(&self, val: u64) {
        self.data_bar.inc_length(val);
    }

    fn scan_finished(&self) {}

    fn error(&self, msg: &str) {
        self.error_counter.fetch_add(1, Ordering::Relaxed);
        let _ = self.mp.println(format!("{} {msg}", "Error:".bold().red()));
    }

    fn warning(&self, msg: &str) {
        let _ = self
            .mp
            .println(format!("{} {msg}", "Warning:".bold().yellow()));
    }

    fn finalize(&self) {
        if self.ui_stop.swap(true, Ordering::Relaxed) {
            return;
        }

        let _ = self.ui_tx.send(UiEvent::Shutdown);
        if let Some(handle) = self._ui_thread.lock().take() {
            let _ = handle.join();
        }

        self.items_bar.finish_and_clear();
        self.data_bar.finish_and_clear();
    }
}
