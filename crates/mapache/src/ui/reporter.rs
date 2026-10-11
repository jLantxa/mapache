use std::sync::Arc;

use indicatif::ProgressBar;
use parking_lot::Mutex;

use crate::{
    common::{defaults::UI_RATE_ESTIMATOR_WINDOW, global::GlobalOpts},
    ui::{
        cli::color::Colorize,
        default_bar_draw_target, default_progress_style,
        events::{Event, EventSender, TaskEvent},
        with_custom_elapsed, with_custom_eta,
    },
    utils::rate_estimator::RateEstimator,
};

/// Progress-bar style presets used by the CLI reporter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhaseStyle {
    /// Plain `pos/len` bar.
    Generic,
    /// Metadata files: `files` units.
    Metadata,
    /// Pack verification: includes elapsed time and ETA.
    Packs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerifyMessage {
    Log,
    Heading,
    HeadingError,
    Success,
    Info,
    InfoSoft,
    Repaired,
    Note,
    Failure,
    FinalSuccess,
    Warning,
    Error,
}

/// Sink for verification messages and progress, independent of the renderer.
pub trait VerifyReporter: Send + Sync {
    fn message(&self, kind: VerifyMessage, msg: &str);

    /// Result of a single item check (e.g. a verified snapshot reference).
    fn check(&self, id: &str, pos: usize, total: usize, ok: bool) {
        self.message(
            VerifyMessage::Log,
            &format!("{id} ({pos}/{total}) [{}]", if ok { "OK" } else { "ERROR" }),
        );
    }

    /// Begin a new progress phase. `total` is the item count.
    fn begin_phase(&self, name: &'static str, total: u64, style: PhaseStyle) {
        let _ = (name, total, style);
    }
    /// Report progress within the current phase.
    fn phase_progress(&self, pos: u64, message: Option<&str>) {
        let _ = (pos, message);
    }
    /// Finish the current phase.
    fn end_phase(&self, cancelled: bool) {
        let _ = cancelled;
    }
}

/// CLI reporter: writes styled output to stdout/stderr and drives an
/// `indicatif` progress bar. Lines are printed through `suspend` so they never
/// scramble an active bar.
pub struct CliVerifyReporter {
    verbosity: u32,
    bar: Mutex<Option<ProgressBar>>,
    /// Rate estimator backing the `custom_eta` key, set for [`PhaseStyle::Packs`].
    rate: Mutex<Option<Arc<Mutex<RateEstimator>>>>,
}

impl std::fmt::Debug for CliVerifyReporter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CliVerifyReporter").finish()
    }
}

impl Default for CliVerifyReporter {
    fn default() -> Self {
        Self::new()
    }
}

impl CliVerifyReporter {
    pub fn new() -> Self {
        Self {
            verbosity: GlobalOpts::verbosity(),
            bar: Mutex::new(None),
            rate: Mutex::new(None),
        }
    }

    /// Print a styled line to stdout, temporarily suspending any active bar.
    fn write_out(&self, line: &str) {
        let bar = self.bar.lock();
        match bar.as_ref() {
            Some(bar) => bar.suspend(|| println!("{line}")),
            None => println!("{line}"),
        }
    }

    /// Print a styled line to stderr, temporarily suspending any active bar.
    fn write_err(&self, line: &str) {
        let bar = self.bar.lock();
        match bar.as_ref() {
            Some(bar) => bar.suspend(|| eprintln!("{line}")),
            None => eprintln!("{line}"),
        }
    }
}

impl VerifyReporter for CliVerifyReporter {
    fn message(&self, kind: VerifyMessage, msg: &str) {
        if kind != VerifyMessage::Error && self.verbosity < 1 {
            return;
        }
        let (line, to_stderr) = match kind {
            VerifyMessage::Log => (msg.to_string(), false),
            VerifyMessage::Heading => (msg.bold().to_string(), false),
            VerifyMessage::HeadingError => (msg.bold().red().to_string(), false),
            VerifyMessage::Success => (msg.bold().green().to_string(), false),
            VerifyMessage::Info => (format!("{} {msg}", "[INFO]".green()), false),
            VerifyMessage::InfoSoft => (format!("{} {msg}", "[INFO]".yellow()), false),
            VerifyMessage::Repaired => (format!("{} {msg}", "[REPAIRED]".green().bold()), false),
            VerifyMessage::Note => (msg.dimmed().to_string(), false),
            VerifyMessage::Failure => (msg.bold().on_red().to_string(), false),
            VerifyMessage::FinalSuccess => {
                (format!("\n{} {msg}", "[SUCCESS]".bold().green()), false)
            }
            VerifyMessage::Warning => (format!("{}: {msg}", "Warning".yellow().bold()), false),
            VerifyMessage::Error => (format!("{}: {msg}", "Error".red().bold()), true),
        };
        if to_stderr {
            self.write_err(&line);
        } else {
            self.write_out(&line);
        }
    }

    fn check(&self, id: &str, pos: usize, total: usize, ok: bool) {
        if self.verbosity >= 1 {
            let marker = if ok {
                "[OK]".bold().green()
            } else {
                "[ERROR]".bold().red()
            };
            self.write_out(&format!(
                "{} {} {}",
                id.bold().yellow(),
                format!("({pos}/{total})").dimmed(),
                marker
            ));
        }
    }

    fn begin_phase(&self, _name: &'static str, total: u64, style: PhaseStyle) {
        let bar = ProgressBar::new(total);
        bar.set_draw_target(default_bar_draw_target());
        bar.enable_steady_tick(GlobalOpts::progress_refresh_interval());
        let mut rate = None;
        bar.set_style(match style {
            PhaseStyle::Generic => default_progress_style(),
            PhaseStyle::Metadata => default_progress_style()
                .template("[{elapsed}] [{bar:25.cyan/white}] {pos}/{len} files ({msg})")
                .expect("invalid progress bar template for verify metadata"),
            PhaseStyle::Packs => {
                let est = Arc::new(Mutex::new(RateEstimator::new(UI_RATE_ESTIMATOR_WINDOW)));
                let style = with_custom_eta(
                    with_custom_elapsed(
                        default_progress_style()
                            .template(
                                "[{custom_elapsed}] [{bar:25.cyan/white}] [ETA: {custom_eta}] \
                                 {pos}/{len} packs ({msg})",
                            )
                            .expect("invalid progress bar template for verify pack integrity"),
                    ),
                    est.clone(),
                );
                rate = Some(est);
                style
            }
        });
        bar.set_message("OK");
        *self.rate.lock() = rate;
        *self.bar.lock() = Some(bar);
    }

    fn phase_progress(&self, pos: u64, message: Option<&str>) {
        if let Some(rate) = self.rate.lock().as_ref() {
            rate.lock().observe(pos as f64);
        }
        if let Some(bar) = self.bar.lock().as_ref() {
            if let Some(message) = message {
                bar.set_message(message.to_string());
            }
            bar.set_position(pos);
        }
    }

    fn end_phase(&self, cancelled: bool) {
        *self.rate.lock() = None;
        if let Some(bar) = self.bar.lock().take() {
            if cancelled {
                bar.abandon();
            } else {
                bar.finish();
            }
        }
    }
}

/// TUI reporter: forwards every message and progress update as a [`TaskEvent`].
pub struct EventVerifyReporter {
    sender: EventSender,
}

impl EventVerifyReporter {
    pub fn new(sender: EventSender) -> Self {
        Self { sender }
    }

    fn emit(&self, event: TaskEvent) {
        (self.sender)(Event::Task(event));
    }
}

impl VerifyReporter for EventVerifyReporter {
    fn message(&self, kind: VerifyMessage, msg: &str) {
        self.emit(match kind {
            VerifyMessage::Warning => TaskEvent::Warning(msg.to_string()),
            VerifyMessage::Error | VerifyMessage::Failure => TaskEvent::Error(msg.to_string()),
            _ => TaskEvent::Log(msg.to_string()),
        });
    }
    fn begin_phase(&self, name: &'static str, total: u64, _style: PhaseStyle) {
        self.emit(TaskEvent::Started {
            name,
            total: Some(total),
        });
    }
    fn phase_progress(&self, pos: u64, message: Option<&str>) {
        self.emit(TaskEvent::Progress {
            pos,
            message: message.map(str::to_string),
        });
    }
    fn end_phase(&self, _cancelled: bool) {
        self.emit(TaskEvent::Finished);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_reporter_preserves_message_severity_and_progress() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let captured = events.clone();
        let reporter = EventVerifyReporter::new(Arc::new(move |event| {
            if let Event::Task(event) = event {
                captured.lock().push(event);
            }
        }));
        reporter.message(VerifyMessage::Log, "normal");
        reporter.message(VerifyMessage::Warning, "warning");
        reporter.message(VerifyMessage::Error, "error");
        reporter.message(VerifyMessage::Failure, "failed");
        reporter.message(VerifyMessage::FinalSuccess, "success");
        reporter.begin_phase("test", 3, PhaseStyle::Generic);
        reporter.phase_progress(1, Some("OK"));
        reporter.end_phase(false);
        let events = events.lock();
        assert!(matches!(&events[0], TaskEvent::Log(message) if message == "normal"));
        assert!(matches!(&events[1], TaskEvent::Warning(message) if message == "warning"));
        assert!(matches!(&events[2], TaskEvent::Error(message) if message == "error"));
        assert!(matches!(&events[3], TaskEvent::Error(message) if message == "failed"));
        assert!(matches!(&events[4], TaskEvent::Log(message) if message == "success"));
        assert!(matches!(
            &events[5],
            TaskEvent::Started { total: Some(3), .. }
        ));
        assert!(matches!(&events[6], TaskEvent::Progress { pos: 1, .. }));
        assert!(matches!(&events[7], TaskEvent::Finished));
    }
}
