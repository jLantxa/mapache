use std::sync::Arc;

use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use parking_lot::Mutex;

use crate::{
    common::{defaults::UI_RATE_ESTIMATOR_WINDOW, global::GlobalOpts},
    ui::{
        self, SPINNER_TICK_CHARS, default_bar_draw_target, default_progress_style,
        events::{Event, EventSender, GcEvent, GcTaskKind},
        with_custom_elapsed, with_custom_eta,
    },
    utils::rate_estimator::RateEstimator,
};

struct CliGcState {
    pb: Mutex<ProgressBar>,
    rate: Arc<Mutex<RateEstimator>>,
}

impl Drop for CliGcState {
    fn drop(&mut self) {
        let pb = self.pb.lock();
        pb.finish_and_clear();
    }
}

pub fn make_event_sender() -> EventSender {
    let state = Arc::new(CliGcState {
        pb: Mutex::new(ProgressBar::with_draw_target(
            Some(0),
            ProgressDrawTarget::hidden(),
        )),
        rate: Arc::new(Mutex::new(RateEstimator::new(UI_RATE_ESTIMATOR_WINDOW))),
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
                state.rate.lock().observe(pos as f64);
                pb_lock.set_position(pos);
            }
            GcEvent::TaskFinished { .. } => {
                let mut pb_lock = state.pb.lock();
                pb_lock.finish_and_clear();
                *pb_lock = ProgressBar::with_draw_target(Some(0), ProgressDrawTarget::hidden());
                state.rate.lock().reset();
            }
            GcEvent::Warning(ref msg) => {
                ui::cli::warning!("{}", msg);
            }
            GcEvent::Error(ref msg) => {
                ui::cli::error!("{}", msg);
            }
            GcEvent::Log(ref msg) => {
                ui::cli::log!("{}", msg);
            }
            GcEvent::Finished { .. } => {
                let pb = state.pb.lock();
                pb.finish_and_clear();
            }
        }
    })
}

fn start_new_bar(state: &CliGcState, kind: GcTaskKind, total: Option<u64>) {
    state.rate.lock().reset();
    let new_pb = match total {
        Some(len) => {
            let pb = ProgressBar::with_draw_target(Some(len), default_bar_draw_target());
            let template = match kind {
                GcTaskKind::CheckingGarbageLevels => {
                    "{spinner:.cyan} [{custom_elapsed}] Checking garbage levels ({pos} / {len} packs) [ETA: {custom_eta}]"
                }
                GcTaskKind::FindingDuplicateBlobs => {
                    "{spinner:.cyan} [{custom_elapsed}] Checking pack footers for duplicates ({pos} / {len} packs) [ETA: {custom_eta}]"
                }
                GcTaskKind::DeletingUnusedPacks => {
                    "[{percent} %] [{bar:20.cyan/white}] [{custom_elapsed}] Deleting unused packs: {pos} / {len} [ETA: {custom_eta}]"
                }
                GcTaskKind::RepackingBlobs => {
                    "[{percent} %] [{bar:20.cyan/white}] [{custom_elapsed}] Repacking blobs: {pos} / {len} [ETA: {custom_eta}]"
                }
                GcTaskKind::DeletingOldIndices => {
                    "[{percent} %] [{bar:20.cyan/white}] [{custom_elapsed}] Deleting old index files: {pos}/{len} [ETA: {custom_eta}]"
                }
                GcTaskKind::DeletingObsoletePacks => {
                    "[{percent} %] [{bar:20.cyan/white}] [{custom_elapsed}] Deleting obsolete pack files: {pos}/{len} [ETA: {custom_eta}]"
                }
                _ => {
                    "[{percent} %] [{bar:20.cyan/white}] [{custom_elapsed}] {pos} / {len} [ETA: {custom_eta}]"
                }
            };
            let style = with_custom_eta(
                with_custom_elapsed(
                    default_progress_style()
                        .template(template)
                        .expect("invalid progress bar template for GC bar progress")
                        .tick_chars(SPINNER_TICK_CHARS),
                ),
                state.rate.clone(),
            );
            pb.set_style(style);
            pb
        }
        None => {
            let pb = ProgressBar::new_spinner();
            pb.set_draw_target(default_bar_draw_target());
            let template = match kind {
                GcTaskKind::SearchingReferencedBlobs => {
                    "{spinner:.cyan} [{custom_elapsed}] Searching referenced blobs: {pos}"
                }
                GcTaskKind::FindingObsoleteBlobs => {
                    "{spinner:.cyan} [{custom_elapsed}] Finding obsolete blobs: {pos}"
                }
                _ => "{spinner:.cyan} [{custom_elapsed}] {pos}",
            };
            pb.set_style(with_custom_elapsed(
                ProgressStyle::default_spinner()
                    .template(template)
                    .expect("invalid progress bar template for GC spinner progress")
                    .tick_chars(SPINNER_TICK_CHARS),
            ));
            pb
        }
    };
    new_pb.enable_steady_tick(GlobalOpts::progress_refresh_interval());

    let mut pb_lock = state.pb.lock();
    pb_lock.finish_and_clear();
    *pb_lock = new_pb;
}
