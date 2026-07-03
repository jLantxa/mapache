pub mod cli;
pub(crate) mod debug;
pub mod events;
pub mod json;
pub mod tui;

pub(crate) const SPINNER_TICK_CHARS: &str = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏";

/// Returns the default draw target for progress bars, with a preconfigured refresh rate
/// and verbosity.
pub(crate) fn default_bar_draw_target() -> indicatif::ProgressDrawTarget {
    let verbosity = crate::common::global::GlobalOpts::verbosity();
    let refresh_interval = crate::common::global::GlobalOpts::progress_refresh_interval();

    if verbosity > 0 {
        indicatif::ProgressDrawTarget::stderr_with_hz((1.0 / refresh_interval.as_secs_f64()) as u8)
    } else {
        indicatif::ProgressDrawTarget::hidden()
    }
}
