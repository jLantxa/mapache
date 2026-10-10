//! The help overlay, toggled globally with `?` or `F1`.
//!
//! It combines the bindings that work on every screen with the key hints
//! reported by the active screen (`Screen::help_hints`). Long rows wrap and the
//! overlay scrolls, so every binding stays reachable on small terminals.

use ratatui::{
    Frame,
    layout::Alignment,
    style::Style,
    text::{Line, Span, Text},
    widgets::{Block, BorderType, Borders, Clear, Paragraph},
};

use crate::ui::tui::{
    theme,
    widgets::{centered_rect, wrap_line},
};

/// Bindings that work on every screen, independent of the active screen.
const GLOBAL_HINTS: &[(&str, &str)] = &[
    ("? / F1", "toggle this help"),
    ("Ctrl+C", "quit mapache"),
    ("\u{2191}\u{2193}", "navigate / scroll"),
    ("PgUp/PgDn", "scroll this help"),
    ("Wheel", "scroll with the mouse"),
];

/// Renders the help overlay as a centred modal over the current screen.
///
/// `scroll` is the overlay's vertical offset; it is clamped in place so it can
/// never point past the content.
pub fn render(frame: &mut Frame, screen_hints: &[(&str, &str)], scroll: &mut u16) {
    let area = frame.area();

    let key_width = GLOBAL_HINTS
        .iter()
        .chain(screen_hints.iter())
        .map(|(key, _)| key.chars().count())
        .max()
        .unwrap_or(0);

    let row = move |key: &str, label: &str| {
        Line::from(vec![
            Span::styled(
                format!(" {:width$} ", key, width = key_width),
                theme::THEME.menu_key,
            ),
            Span::styled(
                format!("  {}", label),
                Style::default().fg(theme::THEME.subtext),
            ),
        ])
    };

    let mut lines: Vec<Line<'static>> = Vec::new();
    lines.push(Line::from(Span::styled("Global", theme::THEME.header)));
    for (key, label) in GLOBAL_HINTS {
        lines.push(row(key, label));
    }
    if !screen_hints.is_empty() {
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled("This screen", theme::THEME.header)));
        for (key, label) in screen_hints {
            lines.push(row(key, label));
        }
    }
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        "Esc or ? to close",
        theme::THEME.footer,
    )));

    // Size the popup from the unwrapped content, then wrap every row to the
    // inner width so long labels never run past the right edge.
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

    let block = Block::default()
        .style(Style::new().bg(theme::THEME.bg))
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(theme::THEME.border)
        .title(Span::styled(" Help ", theme::THEME.header))
        .title_alignment(Alignment::Center);

    let total = wrapped.len();
    frame.render_widget(Clear, popup);
    frame.render_widget(
        Paragraph::new(Text::from(wrapped))
            .block(block)
            .scroll((*scroll, 0)),
        popup,
    );

    if max_scroll > 0 {
        theme::render_scrollbar(frame, popup, total, *scroll as usize);
    }
}
