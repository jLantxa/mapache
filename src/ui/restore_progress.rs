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

use indicatif::{MultiProgress, ProgressBar, ProgressState, ProgressStyle};
use parking_lot::RwLock;

use crate::{
    ui::{PROGRESS_REFRESH_RATE_HZ, SPINNER_TICK_CHARS, default_bar_draw_target},
    utils,
};

pub struct RestoreProgressReporter {
    processed_items_count: Arc<AtomicU64>, // Number of files processed
    processing_items: Arc<RwLock<VecDeque<PathBuf>>>, // List of items being processed (for displaying)

    #[allow(dead_code)]
    mp: MultiProgress,
    progress_bar: ProgressBar,
    file_spinners: Vec<ProgressBar>,
}

impl RestoreProgressReporter {
    pub fn new(num_expected_items: u64, num_expected_bytes: u64, num_display_items: usize) -> Self {
        let processed_items_count_arc = Arc::new(AtomicU64::new(0));

        let mp = MultiProgress::with_draw_target(default_bar_draw_target());
        let progress_bar = mp.add(ProgressBar::new(num_expected_bytes));

        let processed_items_count_arc_clone = processed_items_count_arc.clone();
        progress_bar.set_style(
            ProgressStyle::default_bar()
                .template(
                    "[{custom_elapsed}] [{bar:25.cyan/white}] [{processed_bytes_formated}] [{processed_items_formated}]  [ETA: {custom_eta}]"
                )
                .unwrap()
                .progress_chars("=> ")
                .with_key("custom_elapsed", move |state:&ProgressState, w: &mut dyn std::fmt::Write| {
                    let elapsed = state.elapsed();
                    let custom_elapsed= utils::pretty_print_duration(elapsed);
                    let _ = w.write_str(&custom_elapsed);
                })
                .with_key("processed_items_formated", move |_state:&ProgressState, w: &mut dyn std::fmt::Write| {
                    let item_count = processed_items_count_arc_clone.load(Ordering::SeqCst);
                    let s = format!("{item_count} / {num_expected_items} items");
                    let _ = w.write_str(&s);
                })
                .with_key("processed_bytes_formated", move |state:&ProgressState, w: &mut dyn std::fmt::Write|{
                    let s = format!("{} / {}", utils::format_size(state.pos(), 3), utils::format_size(state.len().unwrap(), 3));
                    let _ = w.write_str(&s);
                })
                .with_key("custom_eta", move |state:&ProgressState, w: &mut dyn std::fmt::Write| {
                    let eta = state.eta();
                    let custom_eta= utils::pretty_print_duration(eta);
                    let _ = w.write_str(&custom_eta);
                })
        );

        let mut file_spinners = Vec::with_capacity(num_display_items);
        for _ in 0..num_display_items {
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
            processing_items: Arc::new(RwLock::new(VecDeque::new())),
            mp,
            progress_bar,
            file_spinners,
        }
    }

    fn update_processing_items(&self) {
        for (i, spinner) in self.file_spinners.iter().enumerate() {
            spinner.set_message(format!(
                "{}",
                self.processing_items
                    .read()
                    .get(i)
                    .unwrap_or(&PathBuf::new())
                    .display()
            ));
        }
    }

    pub fn finalize(&self) {
        let _ = self.mp.clear();
    }

    pub fn processing_file(&self, path: PathBuf) {
        self.processing_items.write().push_back(path);
        self.update_processing_items();
        self.progress_bar.inc(1);
    }

    pub fn processed_file(&self, path: &Path) {
        let idx = self.processing_items.read().iter().position(|p| *p == path);
        if let Some(i) = idx {
            self.processing_items.write().remove(i);
            self.processed_items_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn processed_bytes(&self, bytes: u64) {
        self.progress_bar.inc(bytes);
    }
}
