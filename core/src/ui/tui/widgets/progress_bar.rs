use ratatui::{
    style::{Modifier, Style},
    text::{Line, Span, Text},
    widgets::Paragraph,
};

use crate::ui::tui::theme;

const BAR_WIDTH: usize = 35;

pub struct ProgressBar {
    percentage: f64,
    processed_bytes: u64,
    expected_bytes: u64,
    processed_items: u64,
    expected_items: u64,
    elapsed: std::time::Duration,
    scanning: bool,
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
                    crate::utils::format_size_binary(self.processed_bytes, 3),
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
                    crate::utils::format_size_binary(self.processed_bytes, 3),
                    crate::utils::format_size_binary(self.expected_bytes, 3),
                ),
            )
        };

        let elapsed_str = crate::utils::pretty_print_duration(self.elapsed);
        let rate = if self.elapsed.as_secs_f64() > 0.0 {
            self.processed_bytes as f64 / self.elapsed.as_secs_f64()
        } else {
            0.0
        };
        let rate_str = crate::utils::format_size_binary(rate as u64, 3);

        let mut status_parts = vec![format!("[{}]", elapsed_str)];
        if !self.scanning && self.expected_bytes > 0 && self.processed_bytes > 0 {
            let remaining = self.expected_bytes.saturating_sub(self.processed_bytes) as f64;
            if rate > 0.0 {
                let eta_secs = (remaining / rate) as u64;
                status_parts.push(format!("ETA: {}", format_duration_secs(eta_secs)));
            }
        }
        status_parts.push(format!("{}/s", rate_str));

        if self.expected_items > 0 {
            status_parts.push(format!(
                "Items: {}/{}",
                self.processed_items, self.expected_items
            ));
        }

        let status = status_parts.join("   ");

        let mut line_spans = bar_spans;
        line_spans.push(Span::raw(info_text));

        let lines = vec![Line::from(line_spans), Line::from(status)];

        Paragraph::new(Text::from(lines)).block(theme::themed_block("Progress"))
    }
}

impl Default for ProgressBar {
    fn default() -> Self {
        Self::new()
    }
}

fn format_duration_secs(secs: u64) -> String {
    if secs >= 3600 {
        format!("{}h {}m", secs / 3600, (secs % 3600) / 60)
    } else if secs >= 60 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else {
        format!("{}s", secs)
    }
}
