use std::sync::Arc;

use indicatif::{ProgressBar, ProgressStyle};
use parking_lot::Mutex;

use crate::{
    common::global::GlobalOpts,
    ui::{
        SPINNER_TICK_CHARS, default_bar_draw_target,
        events::{Event, EventSender, GcEvent, GcTaskKind},
    },
};

struct CliGcState {
    pb: Mutex<ProgressBar>,
}

impl Drop for CliGcState {
    fn drop(&mut self) {
        let pb = self.pb.lock();
        pb.finish_and_clear();
    }
}

pub fn make_event_sender() -> EventSender {
    let state = Arc::new(CliGcState {
        pb: Mutex::new(ProgressBar::hidden()),
    });

    Arc::new(move |event: Event| {
        let Event::Gc(ev) = event else { return };
        match ev {
            GcEvent::TaskProgress { kind, pos, total } => {
                let mut pb_lock = state.pb.lock();
                let current = &*pb_lock;
                if current.length() != total && (total.is_some() || current.length() == Some(0)) {
                    drop(pb_lock);
                    start_new_bar(&state, kind, total);
                    pb_lock = state.pb.lock();
                }
                pb_lock.set_position(pos);
            }
            GcEvent::TaskFinished { .. } => {
                let mut pb_lock = state.pb.lock();
                pb_lock.finish_and_clear();
                *pb_lock = ProgressBar::hidden();
            }
            GcEvent::Warning(ref msg) => {
                crate::ui::cli::warning!("{}", msg);
            }
            GcEvent::Error(ref msg) => {
                crate::ui::cli::error!("{}", msg);
            }
            GcEvent::Log(ref msg) => {
                crate::ui::cli::log!("{}", msg);
            }
            GcEvent::Finished { .. } => {
                let pb = state.pb.lock();
                pb.finish_and_clear();
            }
        }
    })
}

fn start_new_bar(state: &CliGcState, kind: GcTaskKind, total: Option<u64>) {
    let new_pb = match total {
        Some(len) => {
            let pb = ProgressBar::with_draw_target(Some(len), default_bar_draw_target());
            let template = match kind {
                GcTaskKind::CheckingGarbageLevels => {
                    "{spinner:.cyan} Checking garbage levels ({pos} / {len} packs)"
                }
                GcTaskKind::DeletingUnusedPacks => {
                    "[{percent} %] [{bar:20.cyan/white}] Deleting unused packs: {pos} / {len}"
                }
                GcTaskKind::RepackingBlobs => {
                    "[{percent} %] [{bar:20.cyan/white}] Repacking blobs: {pos} / {len}"
                }
                GcTaskKind::DeletingOldIndices => {
                    "[{percent} %] [{bar:20.cyan/white}] Deleting old index files: {pos}/{len}"
                }
                GcTaskKind::DeletingObsoletePacks => {
                    "[{percent} %] [{bar:20.cyan/white}] Deleting obsolete pack files: {pos}/{len}"
                }
                _ => "[{percent} %] [{bar:20.cyan/white}] {pos} / {len}",
            };
            pb.set_style(
                ProgressStyle::default_bar()
                    .template(template)
                    .expect("invalid progress bar template for GC bar progress")
                    .progress_chars("=> ")
                    .tick_chars(SPINNER_TICK_CHARS),
            );
            pb
        }
        None => {
            let pb = ProgressBar::new_spinner();
            pb.set_draw_target(default_bar_draw_target());
            let template = match kind {
                GcTaskKind::SearchingReferencedBlobs => {
                    "{spinner:.cyan} Searching referenced blobs: {pos}"
                }
                GcTaskKind::FindingObsoleteBlobs => "{spinner:.cyan} Finding obsolete blobs: {pos}",
                _ => "{spinner:.cyan} {pos}",
            };
            pb.set_style(
                ProgressStyle::default_spinner()
                    .template(template)
                    .expect("invalid progress bar template for GC spinner progress")
                    .tick_chars(SPINNER_TICK_CHARS),
            );
            pb
        }
    };
    new_pb.enable_steady_tick(GlobalOpts::progress_refresh_interval());

    let mut pb_lock = state.pb.lock();
    pb_lock.finish_and_clear();
    *pb_lock = new_pb;
}
