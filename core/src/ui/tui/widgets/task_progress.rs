use std::time::{Duration, Instant};

use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::Style,
    text::{Line, Span, Text},
    widgets::Paragraph,
};

use crate::{
    mapache::defaults::UI_RATE_ESTIMATOR_WINDOW,
    ui::tui::{theme, widgets::ProgressBar},
    utils::rate_estimator::RateEstimator,
};

pub struct TaskProgressState {
    pub expected_bytes: u64,
    pub processed_bytes: u64,
    pub expected_items: u64,
    pub processed_items: u64,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
    pub logs: Vec<String>,
    pub current_message: String,
    pub start_time: Instant,
    pub finish_time: Option<Instant>,
    pub scanning: bool,
    pub cancelling: bool,
    pub rate_estimator: RateEstimator,
}

impl TaskProgressState {
    pub fn new() -> Self {
        Self {
            expected_bytes: 0,
            processed_bytes: 0,
            expected_items: 0,
            processed_items: 0,
            errors: Vec::new(),
            warnings: Vec::new(),
            logs: Vec::new(),
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

    pub fn add_log(&mut self, log: String) {
        self.logs.push(log);
    }

    pub fn set_message(&mut self, msg: String) {
        self.current_message = msg;
    }

    pub fn finish(&mut self) {
        self.finish_time = Some(Instant::now());
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
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(5), // Progress Bar
                Constraint::Min(0),    // Details
                Constraint::Length(1), // Footer
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

        let footer = theme::key_hint_footer(&[("Esc", "cancel"), ("q", "quit")]);
        frame.render_widget(Paragraph::new(footer), chunks[2]);
    }
}
