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
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use crate::utils;

pub struct SnapshotProgressReporter {
    pub expected_items: u64,
    pub expected_size: u64,
    pub processed_items_count: Arc<Mutex<u64>>,
    pub processed_bytes: Arc<Mutex<u64>>,
    pub encoded_bytes: Arc<Mutex<u64>>,
    pub raw_bytes: Arc<Mutex<u64>>,

    pub new_files: Arc<Mutex<u32>>,
    pub changed_files: Arc<Mutex<u32>>,
    pub unchanged_files: Arc<Mutex<u32>>,
    pub deleted_files: Arc<Mutex<u32>>,
    pub new_dirs: Arc<Mutex<u32>>,
    pub changed_dirs: Arc<Mutex<u32>>,
    pub unchanged_dirs: Arc<Mutex<u32>>,
    pub deleted_dirs: Arc<Mutex<u32>>,

    pub processing_items: Arc<Mutex<VecDeque<PathBuf>>>,

    #[allow(dead_code)]
    mp: MultiProgress,
    progress_bar: ProgressBar,
    file_spinners: Vec<ProgressBar>,
}

impl SnapshotProgressReporter {
    pub fn new(expected_items: u64, expected_size: u64, num_processed_items: usize) -> Self {
        let mp = MultiProgress::with_draw_target(ProgressDrawTarget::stderr_with_hz(2));
        let progress_bar = mp.add(ProgressBar::new(expected_size));

        let processed_items_count_arc = Arc::new(Mutex::new(0));
        let processed_bytes_arc = Arc::new(Mutex::new(0));
        let encoded_bytes_arc = Arc::new(Mutex::new(0));
        let raw_bytes_arc = Arc::new(Mutex::new(0));

        let new_files_arc = Arc::new(Mutex::new(0));
        let changed_files_arc = Arc::new(Mutex::new(0));
        let unchanged_files_arc = Arc::new(Mutex::new(0));
        let deleted_files_arc = Arc::new(Mutex::new(0));
        let new_dirs_arc = Arc::new(Mutex::new(0));
        let changed_dirs_arc = Arc::new(Mutex::new(0));
        let unchanged_dirs_arc = Arc::new(Mutex::new(0));
        let deleted_dirs_arc = Arc::new(Mutex::new(0));

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
                    let bytes = processed_bytes_arc_clone.lock().unwrap();
                    let s = format!("{} / {}", utils::format_size(*bytes), utils::format_size(expected_size));
                    drop(bytes);

                    let _ = w.write_str(&s);
                })
                .with_key("processed_items_formated", move |_state:&ProgressState, w: &mut dyn std::fmt::Write| {
                    let item_count = processed_items_count_arc_clone.lock().unwrap();
                    let s = format!("{} / {} items",*item_count, expected_items);
                    drop(item_count);

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
            file_spinner.enable_steady_tick(Duration::from_millis(500));
            file_spinners.push(file_spinner);
        }

        Self {
            expected_items,
            expected_size,
            processed_items_count: processed_items_count_arc,
            processed_bytes: processed_bytes_arc,
            encoded_bytes: encoded_bytes_arc,
            raw_bytes: raw_bytes_arc,
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
                "{:?}",
                processing_items_guard.get(i).unwrap_or(&PathBuf::new())
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

            *self.processed_items_count.lock().unwrap() += 1;
            self.update_processing_items(&processing_items_locked);
        }
    }

    pub fn processed_bytes(&self, bytes: u64) {
        *self.processed_bytes.lock().unwrap() += bytes;
        self.progress_bar.inc(bytes);
    }

    #[inline]
    pub fn raw_bytes(&self, bytes: u64) {
        *self.raw_bytes.lock().unwrap() += bytes;
    }

    #[inline]
    pub fn encoded_bytes(&self, bytes: u64) {
        *self.encoded_bytes.lock().unwrap() += bytes;
    }

    #[inline]
    pub fn new_file(&self) {
        *self.new_files.lock().unwrap() += 1;
    }

    #[inline]
    pub fn changed_file(&self) {
        *self.changed_files.lock().unwrap() += 1;
    }

    #[inline]
    pub fn unchanged_file(&self) {
        *self.unchanged_files.lock().unwrap() += 1;
    }

    #[inline]
    pub fn deleted_file(&self) {
        *self.deleted_files.lock().unwrap() += 1;
    }

    #[inline]
    pub fn new_dir(&self) {
        *self.new_dirs.lock().unwrap() += 1;
    }

    #[inline]
    pub fn changed_dir(&self) {
        *self.changed_dirs.lock().unwrap() += 1;
    }

    #[inline]
    pub fn deleted_dir(&self) {
        *self.deleted_dirs.lock().unwrap() += 1;
    }

    #[inline]
    pub fn unchanged_dir(&self) {
        *self.unchanged_dirs.lock().unwrap() += 1;
    }
}
