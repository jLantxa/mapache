use std::time::Duration;

use ratatui::{
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::Paragraph,
};

use crate::{ui::tui::theme, utils};

const BAR_WIDTH: usize = 35;
const FILLED_GLYPH: char = '\u{2588}';
const TRACK_GLYPH: char = '\u{2591}';
/// Eighth-block glyphs for the sub-cell leading edge, indexed by eighths.
const PARTIAL_GLYPHS: [char; 8] = [
    ' ', '\u{258F}', '\u{258E}', '\u{258D}', '\u{258C}', '\u{258B}', '\u{258A}', '\u{2589}',
];

/// Builds a smooth progress bar of `width` cells: full blocks, an eighth-block
/// leading edge, and a shaded track. The fill fades from the theme's progress
/// colour towards green, giving the bar a bit of depth without animation.
pub(super) fn bar_spans(ratio: f64, width: usize) -> Vec<Span<'static>> {
    if width == 0 {
        return Vec::new();
    }

    let ratio = ratio.clamp(0.0, 1.0);
    let exact = ratio * width as f64;
    let mut filled = exact.floor() as usize;
    let mut eighths = ((exact - filled as f64) * 8.0).round() as usize;
    if eighths >= 8 {
        filled += 1;
        eighths = 0;
    }
    let filled = filled.min(width);
    let partial = usize::from(eighths > 0 && filled < width);
    let empty = width.saturating_sub(filled + partial);

    let start = theme::THEME.progress_filled;
    let end = theme::THEME.green;
    let mut spans = Vec::with_capacity(width);

    for index in 0..filled {
        spans.push(Span::styled(
            FILLED_GLYPH.to_string(),
            fill_style(gradient(start, end, index, width)),
        ));
    }
    if partial == 1 {
        spans.push(Span::styled(
            PARTIAL_GLYPHS[eighths].to_string(),
            fill_style(gradient(start, end, filled, width)),
        ));
    }
    if empty > 0 {
        spans.push(Span::styled(
            TRACK_GLYPH.to_string().repeat(empty),
            Style::default().fg(theme::THEME.progress_empty),
        ));
    }

    spans
}

fn fill_style(color: Color) -> Style {
    Style::default().fg(color).add_modifier(Modifier::BOLD)
}

/// Interpolates between two RGB colours. Non-RGB themes (256/16/no colour)
/// keep the starting colour, which the theme has already degraded for them.
fn gradient(start: Color, end: Color, position: usize, width: usize) -> Color {
    let t = if width <= 1 {
        0.0
    } else {
        position as f64 / (width - 1) as f64
    };
    match (start, end) {
        (Color::Rgb(sr, sg, sb), Color::Rgb(er, eg, eb)) => {
            let mix =
                |from: u8, to: u8| (from as f64 + (to as f64 - from as f64) * t).round() as u8;
            Color::Rgb(mix(sr, er), mix(sg, eg), mix(sb, eb))
        }
        _ => start,
    }
}

pub struct ProgressBar {
    title: String,
    percentage: f64,
    processed_bytes: u64,
    expected_bytes: u64,

    processed_items: u64,
    expected_items: u64,

    skipped_bytes: u64,
    elapsed: std::time::Duration,

    scanning: bool,
    cancelling: bool,

    rate: f64,
    eta: Option<Duration>,
}

impl ProgressBar {
    pub fn new() -> Self {
        Self {
            title: "Progress".to_string(),
            percentage: 0.0,
            processed_bytes: 0,
            expected_bytes: 0,
            processed_items: 0,
            expected_items: 0,
            skipped_bytes: 0,
            elapsed: std::time::Duration::ZERO,
            scanning: false,
            cancelling: false,
            rate: 0.0,
            eta: None,
        }
    }

    /// Sets the block title so the bar names the operation it belongs to.
    pub fn title(mut self, title: impl Into<String>) -> Self {
        self.title = title.into();
        self
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

    pub fn skipped(mut self, skipped: u64) -> Self {
        self.skipped_bytes = skipped;
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
        let line_spans = if self.scanning || self.expected_bytes == 0 {
            self.scanning_spans()
        } else {
            let pct = self.percentage;
            let mut spans = bar_spans(pct / 100.0, BAR_WIDTH);
            spans.push(Span::styled(
                format!("  {:>5.1}%", pct),
                fill_style(theme::THEME.progress_filled),
            ));
            spans.push(Span::styled("  ", Style::default()));
            spans.push(Span::styled(
                utils::format_size_binary(self.processed_bytes, 3),
                Style::default()
                    .fg(theme::THEME.file_fg)
                    .add_modifier(Modifier::BOLD),
            ));
            spans.push(Span::styled(
                " / ",
                Style::default().fg(theme::THEME.subtext_dim),
            ));
            spans.push(Span::styled(
                utils::format_size_binary(self.expected_bytes, 3),
                Style::default().fg(theme::THEME.subtext),
            ));
            spans
        };

        let status_line = if self.cancelling {
            Line::from(Span::styled("  Cancelling...", theme::THEME.warning))
        } else {
            self.build_status_line()
        };

        Paragraph::new(Text::from(vec![Line::from(line_spans), status_line]))
            .block(theme::block(&self.title))
    }

    /// While the filesystem is still being scanned, keep the post-scan layout
    /// but omit the percentage and any total that has fallen behind processing.
    fn scanning_spans(&self) -> Vec<Span<'static>> {
        let mut spans = bar_spans(0.0, BAR_WIDTH);
        spans.push(Span::raw("  "));
        spans.push(Span::styled(
            utils::format_size_binary(self.processed_bytes, 3),
            Style::default()
                .fg(theme::THEME.file_fg)
                .add_modifier(Modifier::BOLD),
        ));
        if self.expected_bytes > self.processed_bytes {
            spans.push(Span::styled(
                " / ",
                Style::default().fg(theme::THEME.subtext_dim),
            ));
            spans.push(Span::styled(
                utils::format_size_binary(self.expected_bytes, 3),
                Style::default().fg(theme::THEME.subtext),
            ));
        }

        spans
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

        if !self.scanning {
            let rate_str = utils::format_size_binary(self.rate as u64, 3);
            spans.push(Span::raw(spacer));
            spans.push(Span::styled(rate_str, speed_style));
            spans.push(Span::styled("/s", speed_style));
        }

        if self.expected_items > 0 && (!self.scanning || self.expected_items > self.processed_items)
        {
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

        if self.skipped_bytes > 0 {
            spans.push(Span::raw(spacer));
            spans.push(Span::styled("Skipped: ", bold));
            spans.push(Span::raw(utils::format_size_binary(self.skipped_bytes, 3)));
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

#[cfg(test)]
mod tests {
    use super::*;

    fn bar_text(spans: &[Span<'static>]) -> String {
        spans.iter().map(|span| span.content.as_ref()).collect()
    }

    #[test]
    fn rendered_percentage_clamps_above_100() {
        let bar = ProgressBar::new().bytes(200, 100);
        let rendered = crate::ui::tui::test_support::render_text(60, 5, |frame| {
            frame.render_widget(bar.render(), frame.area());
        });
        assert!(
            rendered.contains("100.0%"),
            "bar must clamp the percentage to 100: {rendered:?}"
        );
    }

    #[test]
    fn rendered_without_expected_bytes_shows_only_processed_size() {
        let bar = ProgressBar::new().bytes(50, 0);
        let rendered = crate::ui::tui::test_support::render_text(60, 5, |frame| {
            frame.render_widget(bar.render(), frame.area());
        });
        assert!(
            rendered.contains("50 B"),
            "bar must show the processed size: {rendered:?}"
        );
        assert!(rendered.contains(TRACK_GLYPH));
        assert!(
            !rendered.contains("/ 0 B") && !rendered.contains("0.0%"),
            "bar must not show a total size or percentage when it is unknown: {rendered:?}"
        );
    }

    #[test]
    fn scanning_shows_empty_progress_and_hides_stale_totals() {
        let ahead = ProgressBar::new()
            .scanning(true)
            .rate(100.0)
            .eta(Some(Duration::from_secs(5)))
            .items(5, 10)
            .bytes(500, 1000);
        let ahead = crate::ui::tui::test_support::render_text(100, 5, |frame| {
            frame.render_widget(ahead.render(), frame.area());
        });
        assert!(ahead.contains(TRACK_GLYPH) && !ahead.contains(FILLED_GLYPH));
        assert!(ahead.contains("500 B / 1000 B") && ahead.contains("Items: 5/10"));
        assert!(!ahead.contains('%') && !ahead.contains("ETA") && !ahead.contains("/s"));

        let behind = ProgressBar::new()
            .scanning(true)
            .items(10, 5)
            .bytes(1000, 500);
        let behind = crate::ui::tui::test_support::render_text(100, 5, |frame| {
            frame.render_widget(behind.render(), frame.area());
        });
        assert!(behind.contains("1000 B") && behind.contains("Items: 10"));
        assert!(!behind.contains("/ 500 B") && !behind.contains("10/5"));
    }

    #[test]
    fn rendered_title_names_the_operation() {
        let bar = ProgressBar::new().title("Snapshot Progress").bytes(1, 2);
        let rendered = crate::ui::tui::test_support::render_text(80, 5, |frame| {
            frame.render_widget(bar.render(), frame.area());
        });
        assert!(
            rendered.contains("Snapshot Progress"),
            "bar must render its configured title: {rendered:?}"
        );
    }

    #[test]
    fn bar_spans_keep_the_requested_width_and_clamp_the_ratio() {
        for ratio in [-1.0, 0.0, 0.25, 0.5, 0.9, 1.0, 2.0] {
            let text = bar_text(&bar_spans(ratio, 20));
            assert_eq!(
                text.chars().count(),
                20,
                "ratio {ratio} must still fill the bar width"
            );
        }
    }

    #[test]
    fn bar_spans_use_a_smooth_partial_leading_edge() {
        // Half of 35 cells is 17 full cells plus a half block.
        let text = bar_text(&bar_spans(0.5, 35));
        assert_eq!(text.chars().filter(|&ch| ch == FILLED_GLYPH).count(), 17);
        assert!(
            text.contains('\u{258C}'),
            "half block missing from {text:?}"
        );
        assert_eq!(text.chars().filter(|&ch| ch == TRACK_GLYPH).count(), 17);
    }
}
