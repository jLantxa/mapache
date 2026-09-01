use std::sync::Arc;

use indicatif::ProgressState;
use parking_lot::Mutex;

use crate::{
    common::global::GlobalOpts,
    utils::{self, rate_estimator::RateEstimator},
};

pub mod cli;
pub(crate) mod debug;
pub mod events;
pub mod json;
pub mod tui;

pub(crate) const SPINNER_TICK_CHARS: &str = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏";

/// Returns a `ProgressStyle` pre-configured with the project's standard progress chars.
/// Callers only need to set `.template(...)` and any custom `.with_key(...)`.
pub(crate) fn default_progress_style() -> indicatif::ProgressStyle {
    indicatif::ProgressStyle::default_bar().progress_chars("=> ")
}

/// Returns the default draw target for progress bars, with a preconfigured refresh rate
/// and verbosity.
pub(crate) fn default_bar_draw_target() -> indicatif::ProgressDrawTarget {
    let verbosity = GlobalOpts::verbosity();
    let refresh_interval = GlobalOpts::progress_refresh_interval();

    if verbosity > 0 {
        indicatif::ProgressDrawTarget::stderr_with_hz((1.0 / refresh_interval.as_secs_f64()) as u8)
    } else {
        indicatif::ProgressDrawTarget::hidden()
    }
}

/// Adds a `custom_elapsed` key to the style that formats elapsed time using
/// `utils::pretty_print_duration`.
pub(crate) fn with_custom_elapsed(style: indicatif::ProgressStyle) -> indicatif::ProgressStyle {
    style.with_key(
        "custom_elapsed",
        |state: &ProgressState, w: &mut dyn std::fmt::Write| {
            let _ = w.write_str(&utils::pretty_print_duration(state.elapsed()));
        },
    )
}

/// Adds a `custom_eta` key to the style that computes ETA from a shared `RateEstimator`.
pub(crate) fn with_custom_eta(
    style: indicatif::ProgressStyle,
    rate: Arc<Mutex<RateEstimator>>,
) -> indicatif::ProgressStyle {
    style.with_key(
        "custom_eta",
        move |state: &ProgressState, w: &mut dyn std::fmt::Write| {
            let pos = state.pos() as f64;
            let total = state.len().map(|l| l as f64);
            match rate.lock().eta(pos, total.unwrap_or(pos)) {
                Some(d) => {
                    let _ = w.write_str(&utils::pretty_print_duration(d));
                }
                None => {
                    let _ = w.write_str("--");
                }
            }
        },
    )
}
