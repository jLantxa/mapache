use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::Style,
    text::{Line, Span, Text},
    widgets::Paragraph,
};

use crate::{
    common::defaults::UI_RATE_ESTIMATOR_WINDOW,
    ui::{
        events::TaskEvent,
        tui::{theme, widgets::ProgressBar},
    },
    utils::rate_estimator::RateEstimator,
};

use super::Spinner;

pub struct TaskProgressState {
    pub expected_bytes: u64,
    pub processed_bytes: u64,
    pub skipped_bytes: u64,
    pub expected_items: u64,
    pub processed_items: u64,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
    pub current_message: String,
    pub start_time: Instant,
    pub finish_time: Option<Instant>,
    pub scanning: bool,
    pub cancelling: bool,
    pub rate_estimator: RateEstimator,
}

pub struct PhaseProgress {
    current: Option<(&'static str, u64, Option<u64>, Option<String>)>,
    entries: VecDeque<(Style, String)>,
    pub errors: VecDeque<String>,
    pub cancelling: bool,
    spinner: Spinner,
    idle: &'static str,
}

impl PhaseProgress {
    pub fn new(idle: &'static str) -> Self {
        Self {
            current: None,
            entries: VecDeque::new(),
            errors: VecDeque::new(),
            cancelling: false,
            spinner: Spinner::default(),
            idle,
        }
    }

    /// Sets the active phase. Progress updates may omit the total; when the
    /// same phase is updated without one, the previously known total is kept
    /// so a determinate bar is not downgraded to a spinner.
    pub fn set_phase(&mut self, name: &'static str, pos: u64, total: Option<u64>) {
        let total = match (total, &self.current) {
            (None, Some((current, _, known, _))) if *current == name => *known,
            _ => total,
        };
        self.current = Some((name, pos, total, None));
    }

    pub fn handle_event(&mut self, event: TaskEvent) {
        match event {
            TaskEvent::Started { name, total } => self.set_phase(name, 0, total),
            TaskEvent::Progress { pos, message } => {
                let phase = self.current.get_or_insert(("", 0, None, None));
                phase.1 = pos;
                phase.3 = message.filter(|message| message != "OK");
            }
            TaskEvent::Finished => self.current = None,
            TaskEvent::Log(message) if message.trim().is_empty() => {}
            TaskEvent::Log(message) => self.push(theme::THEME.footer, message),
            TaskEvent::Warning(message) => self.push(theme::THEME.warning, message),
            TaskEvent::Error(message) => {
                self.errors.push_back(message.clone());
                if self.errors.len() > 8 {
                    self.errors.pop_front();
                }
                self.push(theme::THEME.error, message);
            }
        }
    }

    fn push(&mut self, style: Style, message: String) {
        self.entries.push_back((style, message));
        if self.entries.len() > 500 {
            self.entries.pop_front();
        }
    }

    fn progress_line(&self) -> Line<'static> {
        let Some((name, pos, total, message)) = &self.current else {
            return Line::from(Span::styled(format!("  {}", self.idle), theme::THEME.info));
        };
        let mut spans = match total.filter(|total| *total > 0) {
            Some(total) => {
                let filled = ((*pos).min(total) as f64 / total as f64 * 30.0) as usize;
                vec![
                    Span::raw("  "),
                    Span::styled("\u{2501}".repeat(filled), theme::THEME.stat_value),
                    Span::styled("\u{2500}".repeat(30 - filled), theme::THEME.footer),
                    Span::raw(format!("  {pos}/{total}  ")),
                ]
            }
            None => vec![Span::styled(
                format!("  {} ", self.spinner.glyph()),
                theme::THEME.info,
            )],
        };
        spans.push(Span::styled(*name, theme::THEME.stat_value));
        if total.is_none_or(|total| total == 0) {
            spans.push(Span::raw(format!("  {pos}")));
            if let Some(message) = message {
                spans.push(Span::raw(format!("  {message}")));
            }
        }
        Line::from(spans)
    }

    pub fn render_bar(&mut self, frame: &mut Frame, area: Rect) {
        self.spinner.tick();
        let mut lines = vec![self.progress_line()];
        if self.cancelling {
            lines.push(Line::from(Span::styled(
                "  Cancelling\u{2026}",
                theme::THEME.warning,
            )));
        }
        frame.render_widget(Paragraph::new(lines).block(theme::block("Progress")), area);
    }

    pub fn render_details(&self, frame: &mut Frame, area: Rect) {
        let start = self
            .entries
            .len()
            .saturating_sub(area.height.saturating_sub(2) as usize);
        let lines: Vec<_> = self
            .entries
            .iter()
            .skip(start)
            .map(|(style, message)| {
                let marker = if *style == theme::THEME.footer {
                    "\u{2022} "
                } else {
                    "! "
                };
                Line::from(vec![
                    Span::styled(marker, *style),
                    Span::raw(message.as_str()),
                ])
            })
            .collect();
        frame.render_widget(Paragraph::new(lines).block(theme::block("Details")), area);
    }
}

impl TaskProgressState {
    pub fn new() -> Self {
        Self {
            expected_bytes: 0,
            processed_bytes: 0,
            skipped_bytes: 0,
            expected_items: 0,
            processed_items: 0,
            errors: Vec::new(),
            warnings: Vec::new(),
            current_message: String::new(),
            start_time: Instant::now(),
            finish_time: None,
            scanning: false,
            cancelling: false,
            rate_estimator: RateEstimator::new(UI_RATE_ESTIMATOR_WINDOW),
        }
    }

    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }

    pub fn add_processed_bytes(&mut self, bytes: u64) {
        self.processed_bytes += bytes;
        self.rate_estimator.observe(self.processed_bytes as f64);
    }

    pub fn add_processed_items(&mut self, items: u64) {
        self.processed_items += items;
    }

    pub fn set_expected(&mut self, items: u64, bytes: u64) {
        self.expected_items = items;
        self.expected_bytes = bytes;
    }

    pub fn add_error(&mut self, error: String) {
        self.errors.push(error);
    }

    pub fn add_warning(&mut self, warning: String) {
        self.warnings.push(warning);
    }

    pub fn set_message(&mut self, msg: String) {
        self.current_message = msg;
    }

    /// Records the completion time. Idempotent, so a duplicate `Finished`
    /// event does not move the timestamp forwards.
    pub fn finish(&mut self) {
        if self.finish_time.is_none() {
            self.finish_time = Some(Instant::now());
        }
    }
}

pub struct TaskProgressWidget<'a> {
    state: &'a TaskProgressState,
    title: String,
}

impl<'a> TaskProgressWidget<'a> {
    pub fn new(state: &'a TaskProgressState, title: impl Into<String>) -> Self {
        Self {
            state,
            title: title.into(),
        }
    }

    pub fn render(&self, frame: &mut Frame, area: Rect) {
        let footer = theme::key_hint_lines(&[("Esc", "cancel"), ("q", "back")], area.width);
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(5),                   // Progress Bar
                Constraint::Min(0),                      // Details
                Constraint::Length(footer.len() as u16), // Footer
            ])
            .split(area);

        let rate = self.state.rate_estimator.rate();
        let eta = if !self.state.scanning
            && self.state.expected_bytes > 0
            && self.state.processed_bytes > 0
        {
            self.state.rate_estimator.eta(
                self.state.processed_bytes as f64,
                self.state.expected_bytes as f64,
            )
        } else {
            None
        };

        let progress_bar = ProgressBar::new()
            .bytes(self.state.processed_bytes, self.state.expected_bytes)
            .items(self.state.processed_items, self.state.expected_items)
            .skipped(self.state.skipped_bytes)
            .elapsed(self.state.elapsed())
            .scanning(self.state.scanning)
            .cancelling(self.state.cancelling)
            .rate(rate)
            .eta(eta);

        frame.render_widget(progress_bar.render(), chunks[0]);

        let mut lines = Vec::new();
        if !self.state.current_message.is_empty() {
            lines.push(Line::from(vec![
                Span::styled("Current: ", Style::default().bold()),
                Span::raw(&self.state.current_message),
            ]));
        }

        if !self.state.errors.is_empty() {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled("Errors:", theme::THEME.error)));
            for err in self.state.errors.iter().rev().take(5) {
                lines.push(Line::from(vec![
                    Span::styled(" ! ", theme::THEME.error),
                    Span::raw(err),
                ]));
            }
        }

        let widget = Paragraph::new(Text::from(lines)).block(theme::block(&self.title));
        frame.render_widget(widget, chunks[1]);

        frame.render_widget(Paragraph::new(Text::from(footer)), chunks[2]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phase_progress_preserves_order_counts_and_severity() {
        let mut progress = PhaseProgress::new("Starting");
        progress.handle_event(TaskEvent::Started {
            name: "phase",
            total: Some(10),
        });
        progress.handle_event(TaskEvent::Progress {
            pos: 2,
            message: Some("OK".into()),
        });
        progress.handle_event(TaskEvent::Warning("older warning".into()));
        progress.handle_event(TaskEvent::Log("".into()));
        progress.handle_event(TaskEvent::Error("newer error".into()));
        progress.cancelling = true;
        let text = crate::ui::tui::test_support::render_text(80, 10, |frame| {
            progress.render_bar(frame, Rect::new(0, 0, 80, 4));
            progress.render_details(frame, Rect::new(0, 4, 80, 6));
        });
        assert!(text.contains("2/10  phase"));
        assert!(text.contains("Cancelling"));
        assert!(text.find("older warning").unwrap() < text.find("newer error").unwrap());
        assert_eq!(progress.entries.len(), 2);
        assert_eq!(progress.errors.front().unwrap(), "newer error");
        assert_eq!(progress.entries.back().unwrap().0, theme::THEME.error);
        progress.handle_event(TaskEvent::Finished);
        assert!(progress.current.is_none());
    }

    fn render_bar_text(progress: &mut PhaseProgress) -> String {
        crate::ui::tui::test_support::render_text(80, 4, |frame| {
            progress.render_bar(frame, Rect::new(0, 0, 80, 4));
        })
    }

    #[test]
    fn phase_progress_keeps_known_total_on_update_without_total() {
        let mut progress = PhaseProgress::new("Starting");
        // GC reports the total when the task starts and omits it on updates.
        progress.set_phase("Repacking blobs", 0, Some(40));
        progress.set_phase("Repacking blobs", 7, None);
        let text = render_bar_text(&mut progress);
        assert!(
            text.contains("7/40"),
            "update without total must keep the determinate bar: {text:?}"
        );
        assert!(text.contains("Repacking blobs"));
    }

    #[test]
    fn phase_progress_new_phase_does_not_inherit_previous_total() {
        let mut progress = PhaseProgress::new("Starting");
        progress.set_phase("First phase", 5, Some(10));
        progress.set_phase("Second phase", 0, None);
        let text = render_bar_text(&mut progress);
        assert!(text.contains("Second phase"));
        assert!(
            !text.contains("0/10"),
            "a new phase without a total must not reuse the previous one: {text:?}"
        );
    }

    #[test]
    fn phase_progress_bounds_log_and_error_history() {
        let mut progress = PhaseProgress::new("Starting");
        for index in 0..510 {
            progress.handle_event(TaskEvent::Error(index.to_string()));
        }
        assert_eq!(progress.entries.len(), 500);
        assert_eq!(progress.entries.front().unwrap().1, "10");
        assert_eq!(progress.errors.len(), 8);
        assert_eq!(progress.errors.front().unwrap(), "502");
    }
}
