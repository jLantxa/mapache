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

use std::{
    collections::VecDeque,
    path::PathBuf,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressState, ProgressStyle};

use crate::{
    global::global_opts,
    ui::{PROGRESS_REFRESH_RATE_HZ, SPINNER_TICK_CHARS},
    utils,
};

pub(crate) struct RestoreProgressReporter {
    processed_items_count: Arc<AtomicU64>, // Number of files processed
    processing_items: Arc<Mutex<VecDeque<PathBuf>>>, // List of items being processed (for displaying)

    #[allow(dead_code)]
    mp: MultiProgress,
    progress_bar: ProgressBar,
    file_spinners: Vec<ProgressBar>,
}

impl RestoreProgressReporter {
    pub fn new(num_expected_items: u64, num_processed_items: usize) -> Self {
        let processed_items_count_arc = Arc::new(AtomicU64::new(0));

        let verbosity = global_opts().as_ref().unwrap().verbosity;
        let draw_target = if verbosity > 0 {
            ProgressDrawTarget::stderr_with_hz(PROGRESS_REFRESH_RATE_HZ)
        } else {
            ProgressDrawTarget::hidden()
        };

        let mp = MultiProgress::with_draw_target(draw_target);
        let progress_bar = mp.add(ProgressBar::new(num_expected_items));

        let processed_items_count_arc_clone = processed_items_count_arc.clone();
        progress_bar.set_style(
            ProgressStyle::default_bar()
                .template(
                    "[{custom_elapsed}] [{bar:25.cyan/white}] {processed_items_formated}  [ETA: {custom_eta}]"
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
                    let s = format!("{} / {} items",item_count, num_expected_items);

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
                    .tick_chars(SPINNER_TICK_CHARS),
            );
            file_spinner.enable_steady_tick(Duration::from_millis(
                (1.0f32 / PROGRESS_REFRESH_RATE_HZ as f32) as u64,
            ));
            file_spinners.push(file_spinner);
        }

        Self {
            processed_items_count: processed_items_count_arc,
            processing_items: Arc::new(Mutex::new(VecDeque::new())),
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
                    .display()
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
        self.progress_bar.inc(1);
    }

    pub fn processed_file(&self, path: PathBuf) {
        let mut processing_items_locked = self.processing_items.lock().unwrap();
        if let Some(idx) = processing_items_locked.iter().position(|p| *p == path) {
            processing_items_locked.remove(idx);

            self.processed_items_count.fetch_add(1, Ordering::AcqRel);
        }
    }
}
