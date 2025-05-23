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

use std::{collections::VecDeque, path::PathBuf, time::Duration};

use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};

use crate::utils;

pub struct CommitProgressReporter {
    pub expected_items: u64,
    pub expected_size: u64,
    pub processing_items: VecDeque<PathBuf>,

    pub processed_bytes: u64,
    pub encoded_bytes: u64,
    pub raw_bytes: u64,

    pub new_files: u32,
    pub changed_files: u32,
    pub unchanged_files: u32,
    pub deleted_files: u32,
    pub new_dirs: u32,
    pub changed_dirs: u32,
    pub unchanged_dirs: u32,
    pub deleted_dirs: u32,

    #[allow(dead_code)]
    mp: MultiProgress,
    progress_bar: ProgressBar,
    file_spinners: Vec<ProgressBar>,
}

impl CommitProgressReporter {
    pub fn new(expected_items: u64, expected_size: u64, num_processed_items: usize) -> Self {
        let mp = MultiProgress::with_draw_target(ProgressDrawTarget::stderr_with_hz(5));

        let mut file_spinners = Vec::with_capacity(num_processed_items);
        for _ in 0..num_processed_items {
            let file_spinner = mp.add(ProgressBar::new_spinner());
            file_spinner.set_style(
                ProgressStyle::default_spinner()
                    .template("{spinner:.cyan} {msg}")
                    .unwrap()
                    .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"),
            );
            file_spinner.enable_steady_tick(Duration::from_millis(200));
            file_spinners.push(file_spinner);
        }

        let progress_bar = mp.add(ProgressBar::new(expected_size));
        progress_bar.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed}] [{bar:40.cyan/white}] {msg} [ETA: {eta}]")
                .unwrap()
                .progress_chars("=> "),
        );

        Self {
            expected_items,
            expected_size,
            processing_items: VecDeque::new(),
            processed_bytes: 0,
            encoded_bytes: 0,
            raw_bytes: 0,
            new_files: 0,
            changed_files: 0,
            unchanged_files: 0,
            deleted_files: 0,
            new_dirs: 0,
            changed_dirs: 0,
            unchanged_dirs: 0,
            deleted_dirs: 0,
            mp,
            progress_bar,
            file_spinners,
        }
    }

    fn update_processing_items(&mut self) {
        for (i, spinner) in self.file_spinners.iter().enumerate() {
            let _ = spinner.set_message(format!(
                "{:?}",
                self.processing_items.get(i).unwrap_or(&PathBuf::new())
            ));
        }
    }

    pub fn finalize(&self) {
        let _ = self.mp.clear();
    }

    pub fn processing_file(&mut self, path: PathBuf) {
        self.processing_items.push_back(path);
        self.update_processing_items();
    }

    pub fn processed_file(&mut self, path: PathBuf) {
        if let Some(idx) = self.processing_items.iter().position(|p| *p == path) {
            self.processing_items.remove(idx);
            self.progress_bar.inc(1);
            self.update_processing_items();
        }
    }

    pub fn processed_bytes(&mut self, bytes: u64) {
        self.processed_bytes += bytes;
        self.progress_bar.inc(bytes);

        let progress_msg = format!(
            "{} / {}",
            utils::format_size(self.processed_bytes),
            utils::format_size(self.expected_size)
        );
        self.progress_bar.set_message(progress_msg);
    }

    #[inline]
    pub fn raw_bytes(&mut self, bytes: u64) {
        self.raw_bytes += bytes;
    }

    #[inline]
    pub fn encoded_bytes(&mut self, bytes: u64) {
        self.encoded_bytes += bytes;
    }

    #[inline]
    pub fn new_file(&mut self) {
        self.new_files += 1
    }

    #[inline]
    pub fn changed_file(&mut self) {
        self.changed_files += 1
    }

    #[inline]
    pub fn unchanged_file(&mut self) {
        self.unchanged_files += 1;
    }

    #[inline]
    pub fn deleted_file(&mut self) {
        self.deleted_files += 1;
    }

    #[inline]
    pub fn new_dir(&mut self) {
        self.new_dirs += 1;
    }

    #[inline]
    pub fn changed_dir(&mut self) {
        self.changed_dirs += 1;
    }

    #[inline]
    pub fn deleted_dir(&mut self) {
        self.deleted_dirs += 1;
    }

    #[inline]
    pub fn unchanged_dir(&mut self) {
        self.unchanged_dirs += 1;
    }
}
