// [backup] is an incremental backup tool
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

use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressState, ProgressStyle};
use std::{
    collections::VecDeque,
    path::PathBuf,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU32, AtomicU64},
    },
    time::Duration,
};

use crate::{repository::snapshot::SnapshotSummary, utils};

pub struct SnapshotProgressReporter {
    // Processed items
    processed_items_count: Arc<AtomicU64>, // Number of files processed (written or not)
    processed_bytes: Arc<AtomicU64>,       // Bytes processed (only data)
    raw_bytes: Arc<AtomicU64>,             // Bytes 'written' before encoding
    encoded_bytes: Arc<AtomicU64>,         // Bytes written after encoding

    // Metadata
    meta_raw_bytes: Arc<AtomicU64>, // Metadata bytes 'written' before encoding
    meta_encoded_bytes: Arc<AtomicU64>, //Metadata bytes written after encoding

    new_files: Arc<AtomicU32>,
    changed_files: Arc<AtomicU32>,
    unchanged_files: Arc<AtomicU32>,
    deleted_files: Arc<AtomicU32>,
    new_dirs: Arc<AtomicU32>,
    changed_dirs: Arc<AtomicU32>,
    unchanged_dirs: Arc<AtomicU32>,
    deleted_dirs: Arc<AtomicU32>,

    processing_items: Arc<Mutex<VecDeque<PathBuf>>>, // List of items being processed (for displaying)

    #[allow(dead_code)]
    mp: MultiProgress,
    progress_bar: ProgressBar,
    file_spinners: Vec<ProgressBar>,
}

impl SnapshotProgressReporter {
    pub fn new(expected_items: u64, expected_size: u64, num_processed_items: usize) -> Self {
        let mp = MultiProgress::with_draw_target(ProgressDrawTarget::stderr_with_hz(30));
        let progress_bar = mp.add(ProgressBar::new(expected_size));

        let processed_items_count_arc = Arc::new(AtomicU64::new(0));
        let processed_bytes_arc = Arc::new(AtomicU64::new(0));
        let raw_bytes_arc = Arc::new(AtomicU64::new(0));
        let encoded_bytes_arc = Arc::new(AtomicU64::new(0));

        let meta_raw_bytes_arc = Arc::new(AtomicU64::new(0));
        let meta_encoded_bytes_arc = Arc::new(AtomicU64::new(0));

        let new_files_arc = Arc::new(AtomicU32::new(0));
        let changed_files_arc = Arc::new(AtomicU32::new(0));
        let unchanged_files_arc = Arc::new(AtomicU32::new(0));
        let deleted_files_arc = Arc::new(AtomicU32::new(0));
        let new_dirs_arc = Arc::new(AtomicU32::new(0));
        let changed_dirs_arc = Arc::new(AtomicU32::new(0));
        let unchanged_dirs_arc = Arc::new(AtomicU32::new(0));
        let deleted_dirs_arc = Arc::new(AtomicU32::new(0));

        let processing_items_arc = Arc::new(Mutex::new(VecDeque::new()));

        let processed_items_count_arc_clone = processed_items_count_arc.clone();
        let processed_bytes_arc_clone = processed_bytes_arc.clone();
        progress_bar.set_style(
            ProgressStyle::default_bar()
                .template(
                    "[{custom_elapsed}] [{bar:25.cyan/white}] {processed_bytes_formatted}  [{processed_items_formated}]  [ETA: {custom_eta}]"
                )
                .unwrap()
                .progress_chars("=> ")
                .with_key("custom_elapsed", move |state:&ProgressState, w: &mut dyn std::fmt::Write| {
                    let elapsed = state.elapsed();
                    let custom_elapsed= utils::pretty_print_duration(elapsed);
                    let _ = w.write_str(&custom_elapsed);
                })
                .with_key("processed_bytes_formatted", move |_state:&ProgressState, w: &mut dyn std::fmt::Write| {
                    let bytes = processed_bytes_arc_clone.load(std::sync::atomic::Ordering::SeqCst);
                    let s = format!("{} / {}", utils::format_size(bytes), utils::format_size(expected_size));

                    let _ = w.write_str(&s);
                })
                .with_key("processed_items_formated", move |_state:&ProgressState, w: &mut dyn std::fmt::Write| {
                    let item_count = processed_items_count_arc_clone.load(std::sync::atomic::Ordering::SeqCst);
                    let s = format!("{} / {} items",item_count, expected_items);

                    let _ = w.write_str(&s);
                })
                 .with_key("custom_eta", move |state:&ProgressState, w: &mut dyn std::fmt::Write| {
                    let eta = state.eta();
                    let custom_eta= utils::pretty_print_duration(eta);
                    let _ = w.write_str(&custom_eta);
                })
        );

        let mut file_spinners = Vec::with_capacity(num_processed_items);
        for _ in 0..num_processed_items {
            let file_spinner = mp.add(ProgressBar::new_spinner());
            file_spinner.set_style(
                ProgressStyle::default_spinner()
                    .template("{spinner:.cyan} {msg}")
                    .unwrap()
                    .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"),
            );
            file_spinner.enable_steady_tick(Duration::from_millis(33));
            file_spinners.push(file_spinner);
        }

        Self {
            processed_items_count: processed_items_count_arc,
            processed_bytes: processed_bytes_arc,
            raw_bytes: raw_bytes_arc,
            encoded_bytes: encoded_bytes_arc,
            meta_raw_bytes: meta_raw_bytes_arc,
            meta_encoded_bytes: meta_encoded_bytes_arc,
            new_files: new_files_arc,
            changed_files: changed_files_arc,
            unchanged_files: unchanged_files_arc,
            deleted_files: deleted_files_arc,
            new_dirs: new_dirs_arc,
            changed_dirs: changed_dirs_arc,
            unchanged_dirs: unchanged_dirs_arc,
            deleted_dirs: deleted_dirs_arc,
            processing_items: processing_items_arc,
            mp,
            progress_bar,
            file_spinners,
        }
    }

    fn update_processing_items(&self, processing_items_guard: &MutexGuard<'_, VecDeque<PathBuf>>) {
        for (i, spinner) in self.file_spinners.iter().enumerate() {
            let _ = spinner.set_message(format!(
                "{}",
                processing_items_guard
                    .get(i)
                    .unwrap_or(&PathBuf::new())
                    .to_string_lossy()
            ));
        }
    }

    pub fn finalize(&self) {
        let _ = self.mp.clear();
    }

    pub fn processing_file(&self, path: PathBuf) {
        let mut processing_items_locked = self.processing_items.lock().unwrap();
        processing_items_locked.push_back(path);
        self.update_processing_items(&processing_items_locked);
    }

    pub fn processed_file(&self, path: PathBuf) {
        let mut processing_items_locked = self.processing_items.lock().unwrap();
        if let Some(idx) = processing_items_locked.iter().position(|p| *p == path) {
            processing_items_locked.remove(idx);

            self.processed_items_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    pub fn processed_bytes(&self, bytes: u64) {
        self.processed_bytes
            .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
        self.progress_bar.inc(bytes);
    }

    #[inline]
    pub fn written_data_bytes(&self, raw: u64, encoded: u64) {
        self.raw_bytes
            .fetch_add(raw, std::sync::atomic::Ordering::Relaxed);
        self.encoded_bytes
            .fetch_add(encoded, std::sync::atomic::Ordering::Relaxed);
    }

    #[inline]
    pub fn written_meta_bytes(&self, raw: u64, encoded: u64) {
        self.meta_raw_bytes
            .fetch_add(raw, std::sync::atomic::Ordering::Relaxed);
        self.meta_encoded_bytes
            .fetch_add(encoded, std::sync::atomic::Ordering::Relaxed);
    }

    #[inline]
    pub fn new_file(&self) {
        self.new_files
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    #[inline]
    pub fn changed_file(&self) {
        self.changed_files
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    #[inline]
    pub fn unchanged_file(&self) {
        self.unchanged_files
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    #[inline]
    pub fn deleted_file(&self) {
        self.deleted_files
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    #[inline]
    pub fn new_dir(&self) {
        self.new_dirs
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    #[inline]
    pub fn changed_dir(&self) {
        self.changed_dirs
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    #[inline]
    pub fn deleted_dir(&self) {
        self.deleted_dirs
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    #[inline]
    pub fn unchanged_dir(&self) {
        self.unchanged_dirs
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn get_summary(&self) -> SnapshotSummary {
        let total_raw_bytes = self.raw_bytes.load(std::sync::atomic::Ordering::SeqCst)
            + self
                .meta_raw_bytes
                .load(std::sync::atomic::Ordering::SeqCst);
        let total_encoded_bytes = self.encoded_bytes.load(std::sync::atomic::Ordering::SeqCst)
            + self
                .meta_encoded_bytes
                .load(std::sync::atomic::Ordering::SeqCst);

        SnapshotSummary {
            processed_items_count: self
                .processed_items_count
                .load(std::sync::atomic::Ordering::SeqCst),
            processed_bytes: self
                .processed_bytes
                .load(std::sync::atomic::Ordering::SeqCst),
            raw_bytes: self.raw_bytes.load(std::sync::atomic::Ordering::SeqCst),
            encoded_bytes: self.encoded_bytes.load(std::sync::atomic::Ordering::SeqCst),
            meta_raw_bytes: self
                .meta_raw_bytes
                .load(std::sync::atomic::Ordering::SeqCst),
            meta_encoded_bytes: self
                .meta_encoded_bytes
                .load(std::sync::atomic::Ordering::SeqCst),
            total_raw_bytes: total_raw_bytes,
            total_encoded_bytes: total_encoded_bytes,
            new_files: self.new_files.load(std::sync::atomic::Ordering::SeqCst),
            changed_files: self.changed_files.load(std::sync::atomic::Ordering::SeqCst),
            unchanged_files: self
                .unchanged_files
                .load(std::sync::atomic::Ordering::SeqCst),
            deleted_files: self.deleted_files.load(std::sync::atomic::Ordering::SeqCst),
            new_dirs: self.new_dirs.load(std::sync::atomic::Ordering::SeqCst),
            changed_dirs: self.changed_dirs.load(std::sync::atomic::Ordering::SeqCst),
            unchanged_dirs: self
                .unchanged_dirs
                .load(std::sync::atomic::Ordering::SeqCst),
            deleted_dirs: self.deleted_dirs.load(std::sync::atomic::Ordering::SeqCst),
        }
    }
}
