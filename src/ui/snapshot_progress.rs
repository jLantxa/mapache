// mapache is an incremental backup tool
// Copyright (C) 2025  Javier Lancha Vázquez <javier.lancha@gmail.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use colored::Colorize;
use indicatif::{MultiProgress, ProgressBar, ProgressState, ProgressStyle};
use parking_lot::RwLock;

use crate::{
    fs::tree::NodeDiff,
    global::global_opts,
    repository::snapshot::{DiffCounts, SnapshotSummary},
    ui::{
        EMPTY_PATHBUF, MAX_PATH_DISPLAY_LEN, PROGRESS_REFRESH_RATE_HZ, SPINNER_TICK_CHARS,
        default_bar_draw_target,
    },
    utils,
};

pub struct SnapshotProgressReporter {
    // Processed items
    processed_items_count: Arc<AtomicU64>, // Number of files processed (written or not)
    processed_bytes: Arc<AtomicU64>,       // Bytes processed (only data)
    raw_bytes: Arc<AtomicU64>,             // Bytes 'written' before encoding
    encoded_bytes: Arc<AtomicU64>,         // Bytes written after encoding

    // Metadata
    meta_raw_bytes: Arc<AtomicU64>, // Metadata bytes 'written' before encoding
    meta_encoded_bytes: Arc<AtomicU64>, // Metadata bytes written after encoding

    diff_counts: RwLock<DiffCounts>,

    processing_items: Arc<RwLock<VecDeque<PathBuf>>>, // List of items being processed (for displaying)

    error_counter: Arc<AtomicU64>,

    #[allow(dead_code)]
    mp: MultiProgress,
    progress_bar: ProgressBar,
    companion_bar: ProgressBar,
    file_spinners: Vec<ProgressBar>,

    verbosity: u32,
}

impl SnapshotProgressReporter {
    pub fn new(expected_items: u64, expected_size: u64, num_processed_items: usize) -> Self {
        let mp = MultiProgress::with_draw_target(default_bar_draw_target());

        let progress_bar = mp.add(ProgressBar::new(expected_size));
        let companion_bar = mp.add(ProgressBar::no_length());

        let processed_items_count_arc = Arc::new(AtomicU64::new(0));
        let processed_bytes_arc = Arc::new(AtomicU64::new(0));
        let raw_bytes_arc = Arc::new(AtomicU64::new(0));
        let encoded_bytes_arc = Arc::new(AtomicU64::new(0));

        let meta_raw_bytes_arc = Arc::new(AtomicU64::new(0));
        let meta_encoded_bytes_arc = Arc::new(AtomicU64::new(0));

        let processing_items_arc = Arc::new(RwLock::new(VecDeque::new()));
        let error_counter_arc = Arc::new(AtomicU64::new(0));

        let processed_bytes_arc_clone = processed_bytes_arc.clone();
        progress_bar.set_style(
            ProgressStyle::default_bar()
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
                        let bytes = processed_bytes_arc_clone.load(Ordering::SeqCst);
                        let s = format!(
                            "{} / {}",
                            utils::format_size(bytes, 3),
                            utils::format_size(expected_size, 3)
                        );
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
                ),
        );

        let error_counter_arc_clone = error_counter_arc.clone();
        let processed_items_count_arc_clone = processed_items_count_arc.clone();
        companion_bar.set_style(
            ProgressStyle::default_bar()
                .template("[{processed_items_fmt}]  [{errors} errors]")
                .expect("The snapshot progress bar should have been created")
                .progress_chars("=> ")
                .with_key(
                    "processed_items_fmt",
                    move |_state: &ProgressState, w: &mut dyn std::fmt::Write| {
                        let item_count = processed_items_count_arc_clone.load(Ordering::SeqCst);
                        let s = format!("{item_count} / {expected_items} items");
                        let _ = w.write_str(&s);
                    },
                )
                .with_key(
                    "errors",
                    move |_state: &ProgressState, w: &mut dyn std::fmt::Write| {
                        let errors = error_counter_arc_clone.load(Ordering::SeqCst);
                        let _ = w.write_str(&errors.to_string());
                    },
                ),
        );

        let mut file_spinners = Vec::with_capacity(num_processed_items);
        for _ in 0..num_processed_items {
            let file_spinner = mp.add(ProgressBar::new_spinner());
            file_spinner.set_style(
                ProgressStyle::default_spinner()
                    .template("{spinner:.cyan} {msg}")
                    .unwrap()
                    .tick_chars(SPINNER_TICK_CHARS),
            );
            file_spinner.enable_steady_tick(Duration::from_millis(
                (1000.0f32 / PROGRESS_REFRESH_RATE_HZ as f32) as u64,
            ));
            file_spinners.push(file_spinner);
        }

        Self {
            processed_items_count: processed_items_count_arc,
            processed_bytes: processed_bytes_arc,
            raw_bytes: raw_bytes_arc,
            encoded_bytes: encoded_bytes_arc,
            meta_raw_bytes: meta_raw_bytes_arc,
            meta_encoded_bytes: meta_encoded_bytes_arc,
            diff_counts: RwLock::new(DiffCounts::default()),
            processing_items: processing_items_arc,
            mp,
            companion_bar,
            progress_bar,
            file_spinners,
            verbosity: global_opts().as_ref().unwrap().verbosity,
            error_counter: error_counter_arc,
        }
    }

    fn update_processing_items(&self) {
        for (i, spinner) in self.file_spinners.iter().enumerate() {
            let processing_items_guard = self.processing_items.read();
            let path = processing_items_guard.get(i).unwrap_or(&*EMPTY_PATHBUF);
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

    pub fn processing_node(&self, path: PathBuf, diff: NodeDiff) {
        if diff != NodeDiff::Deleted {
            self.processing_items.write().push_back(path.clone());
            self.update_processing_items();
            self.progress_bar.tick();
            self.companion_bar.tick();
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
        let idx = self.processing_items.read().iter().position(|p| p.eq(path));
        if let Some(i) = idx {
            self.processing_items.write().remove(i);
            self.processed_items_count.fetch_add(1, Ordering::Relaxed);
        }
        self.progress_bar.tick();
        self.companion_bar.tick();
    }

    pub fn processed_bytes(&self, bytes: u64) {
        self.processed_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.progress_bar.inc(bytes);
        self.companion_bar.tick();
    }

    #[inline]
    pub fn written_data_bytes(&self, raw: u64, encoded: u64) {
        self.raw_bytes.fetch_add(raw, Ordering::Relaxed);
        self.encoded_bytes.fetch_add(encoded, Ordering::Relaxed);
    }

    #[inline]
    pub fn written_meta_bytes(&self, raw: u64, encoded: u64) {
        self.meta_raw_bytes.fetch_add(raw, Ordering::Relaxed);
        self.meta_encoded_bytes
            .fetch_add(encoded, Ordering::Relaxed);
    }

    #[inline]
    pub fn new_file(&self) {
        self.diff_counts.write().new_files += 1;
    }

    #[inline]
    pub fn changed_file(&self) {
        self.diff_counts.write().changed_files += 1;
    }

    #[inline]
    pub fn unchanged_file(&self) {
        self.diff_counts.write().unchanged_files += 1;
    }

    #[inline]
    pub fn deleted_file(&self) {
        self.diff_counts.write().deleted_files += 1;
    }

    #[inline]
    pub fn new_dir(&self) {
        self.diff_counts.write().new_dirs += 1;
    }

    #[inline]
    pub fn changed_dir(&self) {
        self.diff_counts.write().changed_dirs += 1;
    }

    #[inline]
    pub fn deleted_dir(&self) {
        self.diff_counts.write().deleted_dirs += 1;
    }

    #[inline]
    pub fn unchanged_dir(&self) {
        self.diff_counts.write().unchanged_dirs += 1;
    }

    #[inline]
    pub fn error(&self) {
        self.error_counter.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_summary(&self) -> SnapshotSummary {
        let total_raw_bytes =
            self.raw_bytes.load(Ordering::SeqCst) + self.meta_raw_bytes.load(Ordering::SeqCst);
        let total_encoded_bytes = self.encoded_bytes.load(Ordering::SeqCst)
            + self.meta_encoded_bytes.load(Ordering::SeqCst);

        SnapshotSummary {
            processed_items_count: self.processed_items_count.load(Ordering::SeqCst),
            processed_bytes: self.processed_bytes.load(Ordering::SeqCst),
            raw_bytes: self.raw_bytes.load(Ordering::SeqCst),
            encoded_bytes: self.encoded_bytes.load(Ordering::SeqCst),
            meta_raw_bytes: self.meta_raw_bytes.load(Ordering::SeqCst),
            meta_encoded_bytes: self.meta_encoded_bytes.load(Ordering::SeqCst),
            total_raw_bytes,
            total_encoded_bytes,
            diff_counts: self.diff_counts.read().clone(),
            amends: None,
        }
    }
}
