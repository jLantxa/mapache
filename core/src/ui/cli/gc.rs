use colored::Colorize;
use indicatif::{ProgressBar, ProgressStyle};
use parking_lot::Mutex;

use crate::{
    mapache::global::GlobalOpts,
    ui::{GcProgressReporter, GcTask, SPINNER_TICK_CHARS, default_bar_draw_target},
};

pub struct CliGcProgressReporter {
    pb: Mutex<ProgressBar>,
}

impl Default for CliGcProgressReporter {
    fn default() -> Self {
        Self::new()
    }
}

impl CliGcProgressReporter {
    pub fn new() -> Self {
        Self {
            pb: Mutex::new(ProgressBar::hidden()),
        }
    }
}

impl GcProgressReporter for CliGcProgressReporter {
    fn log(&self, msg: String) {
        crate::ui::cli::log!("{}", msg);
    }

    fn warning(&self, msg: String) {
        crate::ui::cli::warning!("{}", msg);
    }

    fn start_task(&self, task: GcTask, total: Option<u64>) {
        let new_pb = match total {
            Some(len) => {
                let pb = ProgressBar::with_draw_target(Some(len), default_bar_draw_target());
                let template = match task {
                    GcTask::CheckingGarbageLevels => {
                        "{spinner:.cyan} Checking garbage levels ({pos} / {len} packs)"
                    }
                    GcTask::DeletingUnusedPacks => {
                        "[{percent} %] [{bar:20.cyan/white}] Deleting unused packs: {pos} / {len}"
                    }
                    GcTask::RepackingBlobs => {
                        "[{percent} %] [{bar:20.cyan/white}] Repacking blobs: {pos} / {len}"
                    }
                    GcTask::DeletingOldIndices => {
                        "[{percent} %] [{bar:20.cyan/white}] Deleting old index files: {pos}/{len}"
                    }
                    GcTask::DeletingObsoletePacks => {
                        "[{percent} %] [{bar:20.cyan/white}] Deleting obsolete pack files: {pos}/{len}"
                    }
                    _ => "[{percent} %] [{bar:20.cyan/white}] {pos} / {len}",
                };
                pb.set_style(
                    ProgressStyle::default_bar()
                        .template(template)
                        .unwrap()
                        .progress_chars("=> ")
                        .tick_chars(SPINNER_TICK_CHARS),
                );
                pb
            }
            None => {
                let pb = ProgressBar::new_spinner();
                pb.set_draw_target(default_bar_draw_target());
                let template = match task {
                    GcTask::SearchingReferencedBlobs => {
                        "{spinner:.cyan} Searching referenced blobs: {pos}"
                    }
                    GcTask::FindingObsoleteBlobs => "{spinner:.cyan} Finding obsolete blobs: {pos}",
                    _ => "{spinner:.cyan} {pos}",
                };
                pb.set_style(
                    ProgressStyle::default_spinner()
                        .template(template)
                        .unwrap()
                        .tick_chars(SPINNER_TICK_CHARS),
                );
                pb
            }
        };
        new_pb.enable_steady_tick(GlobalOpts::progress_refresh_interval());

        let mut pb = self.pb.lock();
        pb.finish_and_clear();
        *pb = new_pb;
    }

    fn update_task(&self, _task: GcTask, pos: u64) {
        let pb = self.pb.lock();
        pb.set_position(pos);
    }

    fn finish_task(&self, _task: GcTask) {
        let pb = self.pb.lock();
        pb.finish_and_clear();
    }
}
