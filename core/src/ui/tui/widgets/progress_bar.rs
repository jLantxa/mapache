use std::time::Duration;

use ratatui::{
    style::{Modifier, Style},
    text::{Line, Span, Text},
    widgets::Paragraph,
};

use crate::{ui::tui::theme, utils};

const BAR_WIDTH: usize = 35;

pub struct ProgressBar {
    percentage: f64,
    processed_bytes: u64,
    expected_bytes: u64,
    processed_items: u64,
    expected_items: u64,
    elapsed: std::time::Duration,
    scanning: bool,
    cancelling: bool,
    rate: f64,
    eta: Option<Duration>,
}

impl ProgressBar {
    pub fn new() -> Self {
        Self {
            percentage: 0.0,
            processed_bytes: 0,
            expected_bytes: 0,
            processed_items: 0,
            expected_items: 0,
            elapsed: std::time::Duration::ZERO,
            scanning: false,
            cancelling: false,
            rate: 0.0,
            eta: None,
        }
    }

    pub fn bytes(mut self, processed: u64, expected: u64) -> Self {
        self.processed_bytes = processed;
        self.expected_bytes = expected;
        if expected > 0 {
            self.percentage = (processed as f64 / expected as f64 * 100.0).min(100.0);
        }
        self
    }

    pub fn rate(mut self, rate: f64) -> Self {
        self.rate = rate;
        self
    }

    pub fn eta(mut self, eta: Option<Duration>) -> Self {
        self.eta = eta;
        self
    }

    pub fn items(mut self, processed: u64, expected: u64) -> Self {
        self.processed_items = processed;
        self.expected_items = expected;
        self
    }

    pub fn elapsed(mut self, elapsed: std::time::Duration) -> Self {
        self.elapsed = elapsed;
        self
    }

    pub fn scanning(mut self, scanning: bool) -> Self {
        self.scanning = scanning;
        self
    }

    pub fn cancelling(mut self, cancelling: bool) -> Self {
        self.cancelling = cancelling;
        self
    }

    pub fn render(&self) -> Paragraph<'static> {
        let filled_style = Style::default()
            .fg(theme::THEME.progress_filled)
            .add_modifier(Modifier::BOLD);
        let empty_style = Style::default().fg(theme::THEME.progress_empty);

        let (bar_spans, info_text) = if self.scanning {
            let mut spans = Vec::new();
            spans.push(Span::styled("\u{2500}".repeat(BAR_WIDTH), empty_style));
            (
                spans,
                format!(
                    "  scanning...  {}",
                    utils::format_size_binary(self.processed_bytes, 3),
                ),
            )
        } else {
            let pct = self.percentage;
            let filled = (pct / 100.0 * BAR_WIDTH as f64) as usize;
            let empty = BAR_WIDTH - filled;
            let mut spans = Vec::new();
            if filled > 0 {
                spans.push(Span::styled("\u{2501}".repeat(filled), filled_style));
            }
            if empty > 0 {
                spans.push(Span::styled("\u{2500}".repeat(empty), empty_style));
            }
            (
                spans,
                format!(
                    "  {:.1}%  {} / {}",
                    pct,
                    utils::format_size_binary(self.processed_bytes, 3),
                    utils::format_size_binary(self.expected_bytes, 3),
                ),
            )
        };

        let mut line_spans = bar_spans;
        let info_style = if self.scanning {
            Style::default()
        } else {
            Style::default()
                .fg(theme::THEME.progress_filled)
                .add_modifier(Modifier::BOLD)
        };
        line_spans.push(Span::styled(info_text, info_style));

        let status_line = if self.cancelling {
            Line::from(Span::styled("  Cancelling...", theme::THEME.warning))
        } else {
            self.build_status_line()
        };

        Paragraph::new(Text::from(vec![Line::from(line_spans), status_line]))
            .block(theme::block("Progress"))
    }

    fn build_status_line(&self) -> Line<'static> {
        let elapsed = Style::default()
            .fg(theme::THEME.teal)
            .add_modifier(Modifier::BOLD);
        let bold = Style::default().add_modifier(Modifier::BOLD);
        let speed_style = Style::default()
            .fg(theme::THEME.green)
            .add_modifier(Modifier::BOLD);
        let eta_style = Style::default()
            .fg(theme::THEME.peach)
            .add_modifier(Modifier::BOLD);
        let spacer = "   ";

        let mut spans: Vec<Span> = Vec::new();

        spans.push(Span::styled(
            format!("[{}]", utils::pretty_print_duration(self.elapsed)),
            elapsed,
        ));

        if !self.scanning
            && self.expected_bytes > 0
            && self.processed_bytes > 0
            && self.rate > 0.0
            && let Some(d) = self.eta
            && d != Duration::ZERO
        {
            spans.push(Span::raw(spacer));
            spans.push(Span::styled("ETA: ", eta_style));
            spans.push(Span::styled(format_duration(d), eta_style));
        }

        let rate_str = utils::format_size_binary(self.rate as u64, 3);
        spans.push(Span::raw(spacer));
        spans.push(Span::styled(rate_str, speed_style));
        spans.push(Span::styled("/s", speed_style));

        if self.expected_items > 0 {
            spans.push(Span::raw(spacer));
            spans.push(Span::styled("Items: ", bold));
            spans.push(Span::raw(format!(
                "{}/{}",
                self.processed_items, self.expected_items
            )));
        } else if self.processed_items > 0 {
            spans.push(Span::raw(spacer));
            spans.push(Span::styled("Items: ", bold));
            spans.push(Span::raw(format!("{}", self.processed_items)));
        }

        Line::from(spans)
    }
}

impl Default for ProgressBar {
    fn default() -> Self {
        Self::new()
    }
}

fn format_duration(d: Duration) -> String {
    let secs = d.as_secs();
    if secs >= 3600 {
        format!("{}h {}m", secs / 3600, (secs % 3600) / 60)
    } else if secs >= 60 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else {
        format!("{}s", secs)
    }
}
