use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    thread::JoinHandle,
};

use crossbeam_channel::Sender;
use indicatif::{MultiProgress, ProgressBar, ProgressState, ProgressStyle};
use parking_lot::Mutex;

use crate::{
    common::{defaults, global::GlobalOpts},
    fs::{abbreviate_path, tree::NodeDiff},
    ui::{
        SPINNER_TICK_CHARS,
        cli::color::Colorize,
        default_bar_draw_target, default_progress_style,
        events::{BackupEvent, Event, EventSender},
    },
    utils,
};

pub enum BundleMode {
    Create,
    Extract,
}

enum UiEvent {
    Start(PathBuf),
    Done(PathBuf),
    Shutdown,
}

struct BundleCliState {
    mp: MultiProgress,
    items_bar: ProgressBar,
    data_bar: ProgressBar,

    ui_tx: Sender<UiEvent>,

    error_counter: AtomicU64,
    ui_thread: Mutex<Option<JoinHandle<()>>>,
}

impl BundleCliState {
    fn finalize(&self) {
        let _ = self.ui_tx.send(UiEvent::Shutdown);
        if let Some(handle) = self.ui_thread.lock().take() {
            let _ = handle.join();
        }
        self.items_bar.finish_and_clear();
        self.data_bar.finish_and_clear();
    }
}

pub fn make_event_sender(
    mode: BundleMode,
    total_items: u64,
    total_bytes: u64,
    num_spinners: usize,
) -> EventSender {
    let refresh_interval = GlobalOpts::progress_refresh_interval();
    let mp = MultiProgress::with_draw_target(default_bar_draw_target());

    let (items_label, bytes_label) = match mode {
        BundleMode::Create => ("Items", "Data"),
        BundleMode::Extract => ("Items", "Data"),
    };

    //  Data Bar
    let data_bar = mp.add(ProgressBar::new(total_bytes));
    data_bar.set_style(
            default_progress_style()
                .template(&format!("{bytes_label:<6} [{{bar:20.cyan/white}}] [{{percent}}%] {{processed_bytes_fmt}} [{{bytes_per_sec}}]"))
                .expect("invalid progress bar template for bundle bytes")
                .with_key("processed_bytes_fmt", |state: &ProgressState, w: &mut dyn std::fmt::Write| {
                    let bytes = state.pos();
                    let total = state.len().unwrap_or(0);
                    let _ = write!(
                        w,
                        "{} / {}",
                        utils::format_size_binary(bytes, 3),
                        utils::format_size_binary(total, 3)
                    );
                }),
        );
    data_bar.enable_steady_tick(refresh_interval);

    // Items Bar
    let items_bar = mp.add(ProgressBar::new(total_items));
    items_bar.set_style(
        default_progress_style()
            .template(&format!("{items_label:<6} {{pos}} / {{len}}"))
            .expect("invalid progress bar template for bundle items")
            .tick_chars(SPINNER_TICK_CHARS),
    );
    items_bar.enable_steady_tick(refresh_interval);

    let (ui_tx, ui_rx) = crossbeam_channel::unbounded();

    let mp_clone = mp.clone();
    let ui_handle = std::thread::spawn(move || {
        let mut active_spinners: HashMap<PathBuf, ProgressBar> = HashMap::new();

        let file_style = ProgressStyle::default_spinner()
            .template("{spinner:.cyan} {msg}")
            .expect("invalid progress bar template for bundle spinner")
            .tick_chars(SPINNER_TICK_CHARS);

        loop {
            match ui_rx.recv_timeout(refresh_interval) {
                Ok(UiEvent::Start(path)) => {
                    if !active_spinners.contains_key(&path) && active_spinners.len() < num_spinners
                    {
                        let pb = mp_clone.add(ProgressBar::new_spinner());
                        pb.set_style(file_style.clone());
                        pb.set_message(abbreviate_path(
                            &path,
                            defaults::runtime().max_path_display_len,
                        ));
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

    let state = Arc::new(BundleCliState {
        mp,
        items_bar,
        data_bar,
        ui_tx,
        error_counter: AtomicU64::new(0),
        ui_thread: Mutex::new(Some(ui_handle)),
    });

    Arc::new(move |event: Event| {
        let Event::Backup(ev) = event else { return };
        match ev {
            BackupEvent::NodeProcessing {
                ref path,
                diff,
                size_hint,
            } => {
                if diff == NodeDiff::Deleted {
                    return;
                }
                let is_slow = defaults::runtime()
                    .ui_snapshot_progress_item_min_size
                    .is_none_or(|t| size_hint.is_none_or(|s| s >= t));
                if is_slow {
                    let _ = state.ui_tx.try_send(UiEvent::Start(path.to_path_buf()));
                }
            }
            BackupEvent::NodeProcessed { ref path, diff, .. } => {
                if diff != NodeDiff::Deleted {
                    state.items_bar.inc(1);
                }
                let _ = state.ui_tx.try_send(UiEvent::Done(path.to_path_buf()));
            }
            BackupEvent::BytesProcessed(bytes) => {
                state.data_bar.inc(bytes);
            }
            BackupEvent::ScanProgress { items, bytes } => {
                state.items_bar.inc_length(items);
                state.data_bar.inc_length(bytes);
            }
            BackupEvent::Error(ref msg) => {
                state.error_counter.fetch_add(1, Ordering::Relaxed);
                let _ = state.mp.println(format!("{} {msg}", "Error:".bold().red()));
            }
            BackupEvent::Warning(ref msg) => {
                let _ = state
                    .mp
                    .println(format!("{} {msg}", "Warning:".bold().yellow()));
            }
            BackupEvent::Log(ref msg) => {
                let _ = state.mp.println(msg);
            }
            BackupEvent::Finished(_) => {
                state.finalize();
            }
            _ => {}
        }
    })
}
