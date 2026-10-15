//! The in-TUI log overlay, toggled globally with `Ctrl+L`.
//!
//! It renders the in-memory ring buffer filled by [`crate::ui::debug`], newest
//! record first. The overlay scrolls, so older records stay reachable on small
//! terminals.

use ratatui::{
    Frame,
    layout::Alignment,
    style::{Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, BorderType, Borders, Clear, Paragraph},
};
use tracing::Level;

use crate::ui::{
    debug::LogRecord,
    tui::{
        theme,
        widgets::{centered_rect, wrap_line},
    },
};

fn level_style(level: Level) -> Style {
    let color = match level {
        Level::ERROR => theme::THEME.red,
        Level::WARN => theme::THEME.yellow,
        Level::INFO => theme::THEME.blue,
        Level::DEBUG | Level::TRACE => theme::THEME.subtext_dim,
    };
    Style::default().fg(color).add_modifier(Modifier::BOLD)
}

/// Renders the log overlay as a centred modal over the current screen.
///
/// `scroll` is the overlay's vertical offset; it is clamped in place so it can
/// never point past the content.
pub(crate) fn render(frame: &mut Frame, records: &[LogRecord], scroll: &mut u16) {
    let area = frame.area();

    let mut lines: Vec<Line<'static>> = Vec::new();
    if records.is_empty() {
        lines.push(Line::from(Span::styled(
            "No log records captured.",
            theme::THEME.footer,
        )));
        lines.push(Line::from(Span::styled(
            "Set MAPACHE_DEBUG_LEVEL=trace for more detail.",
            theme::THEME.subtext_dim,
        )));
    } else {
        // Newest first.
        for record in records.iter().rev() {
            lines.push(Line::from(vec![
                Span::styled(
                    record.timestamp.clone(),
                    Style::default().fg(theme::THEME.subtext_dim),
                ),
                Span::raw(" "),
                Span::styled(format!("{:<5}", record.level), level_style(record.level)),
                Span::raw(" "),
                Span::styled(
                    record.target.clone(),
                    Style::default().fg(theme::THEME.subtext_dim),
                ),
            ]));
            lines.push(Line::from(vec![
                Span::raw("  "),
                Span::styled(
                    record.message.clone(),
                    Style::default().fg(theme::THEME.subtext),
                ),
            ]));
        }
    }
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "c clear \u{b7} Esc or Ctrl+L to close",
        theme::THEME.footer,
    )));

    let content_width = lines.iter().map(|l| l.width()).max().unwrap_or(0) as u16;
    let width = (content_width + 4).min(area.width);
    // Leave a column for the scrollbar.
    let wrap_width = (width.saturating_sub(3)).max(1) as usize;

    let mut wrapped: Vec<Line<'static>> = Vec::new();
    for line in &lines {
        wrap_line(line, wrap_width, &mut wrapped);
    }

    let height = (wrapped.len() as u16 + 2).min(area.height);
    let inner_height = height.saturating_sub(2) as usize;
    let max_scroll = wrapped.len().saturating_sub(inner_height) as u16;
    *scroll = (*scroll).min(max_scroll);

    let popup = centered_rect(area, width, height);
    let total = wrapped.len();

    frame.render_widget(Clear, popup);
    frame.render_widget(
        Paragraph::new(Text::from(wrapped))
            .block(
                Block::default()
                    .style(Style::new().bg(theme::THEME.bg))
                    .borders(Borders::ALL)
                    .border_type(BorderType::Rounded)
                    .border_style(theme::THEME.border)
                    .title(Span::styled(" Logs ", theme::THEME.header))
                    .title_alignment(Alignment::Center),
            )
            .scroll((*scroll, 0)),
        popup,
    );

    if max_scroll > 0 {
        theme::render_scrollbar(frame, popup, total, *scroll as usize);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(level: Level, message: &str) -> LogRecord {
        LogRecord {
            level,
            target: "mapache::test".to_string(),
            message: message.to_string(),
            timestamp: "12:00:00".to_string(),
        }
    }

    #[test]
    fn overlay_lists_records_newest_first() {
        let records = vec![
            record(Level::INFO, "older message"),
            record(Level::ERROR, "newer message"),
        ];
        let mut scroll = 0;
        let text = crate::ui::tui::test_support::render_text(80, 24, |frame| {
            render(frame, &records, &mut scroll);
        });

        assert!(text.contains("Logs"));
        let older = text.find("older message").expect("older record shown");
        let newer = text.find("newer message").expect("newer record shown");
        assert!(newer < older, "the newest record should be at the top");
    }

    #[test]
    fn overlay_handles_an_empty_buffer() {
        let mut scroll = 0;
        let text = crate::ui::tui::test_support::render_text(80, 24, |frame| {
            render(frame, &[], &mut scroll);
        });
        assert!(text.contains("No log records captured"));
    }
}
