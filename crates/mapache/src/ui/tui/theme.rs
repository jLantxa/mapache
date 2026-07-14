use std::sync::LazyLock;

use ratatui::{
    Frame,
    layout::{Margin, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Gauge, Paragraph, Scrollbar, ScrollbarOrientation, ScrollbarState},
};

/// Standard inner margin for content areas in TUI screens.
pub(crate) const CONTENT_MARGIN: Margin = Margin::new(2, 1);

/// Electric Pastel colour palette used throughout the TUI.
#[expect(dead_code)]
pub(crate) struct Theme {
    // ── Core palette ────────────────────────────────────────────
    pub bg: Color,
    pub surface: Color,
    pub overlay: Color,
    pub subtext_dim: Color,
    pub subtext: Color,
    pub text: Color,
    // Accents
    pub blue: Color,
    pub green: Color,
    pub yellow: Color,
    pub red: Color,
    pub mauve: Color,
    pub peach: Color,
    pub teal: Color,
    pub sky: Color,
    pub pink: Color,
    pub lavender: Color,

    // ── Pre-built styles ─────────────────────────────────────────
    pub header: Style,         // Bold coloured header text
    pub header_surface: Style, // Header on a filled surface
    pub border: Style,         // Default border colour
    pub border_focused: Style, // Border for focussed / active element
    pub selection: Style,      // Selected/highlighted row background
    pub menu_key: Style,       // Keyboard-hint key styling
    pub footer: Style,         // Footer / secondary text
    // Snapshot-table columns
    pub snap_id: Style,
    pub snap_date: Style,
    pub snap_host: Style,
    pub snap_size: Style,
    pub snap_active: Style,
    pub snap_inactive: Style,
    // File-tree
    pub dir_fg: Color,
    pub file_fg: Color,
    pub symlink_fg: Color,
    pub file_size: Style,
    pub breadcrumb: Style,
    // Progress
    pub progress_filled: Color,
    pub progress_empty: Color,
    pub gauge_filled: Style,
    pub gauge_empty: Style,
    // Status colours (use `.fg` on these for Toast, or pass whole Style)
    pub success: Style,
    pub error: Style,
    pub warning: Style,
    pub info: Style,
    // Stat cards
    pub stat_label: Style,
    pub stat_value: Style,
    pub stat_card: Style,
}

// ── Electric Pastel (dark) ──────────────────────────────────────
const ELECTRIC: Theme = {
    use Color::Rgb;
    let bg = Rgb(14, 24, 30);
    let surface = Rgb(22, 34, 42);
    let overlay = Rgb(34, 46, 54);
    let subtext_dim = Rgb(76, 100, 112);
    let subtext = Rgb(132, 155, 167);
    let text = Rgb(228, 235, 240);
    let blue = Rgb(110, 180, 255);
    let green = Rgb(110, 230, 150);
    let yellow = Rgb(255, 215, 110);
    let red = Rgb(255, 110, 135);
    let mauve = Rgb(170, 110, 255);
    let peach = Rgb(255, 170, 110);
    let teal = Rgb(110, 230, 210);
    let sky = Rgb(110, 200, 255);
    let pink = Rgb(255, 110, 175);
    let lavender = Rgb(180, 140, 255);

    Theme {
        bg,
        surface,
        overlay,
        subtext_dim,
        subtext,
        text,
        blue,
        green,
        yellow,
        red,
        mauve,
        peach,
        teal,
        sky,
        pink,
        lavender,
        header: Style::new().fg(teal).add_modifier(Modifier::BOLD),
        header_surface: Style::new()
            .bg(surface)
            .fg(teal)
            .add_modifier(Modifier::BOLD),
        border: Style::new().fg(overlay),
        border_focused: Style::new().fg(teal),
        selection: Style::new().bg(overlay),
        menu_key: Style::new().fg(teal).add_modifier(Modifier::BOLD),
        footer: Style::new().fg(subtext_dim),
        snap_id: Style::new().fg(green),
        snap_date: Style::new().fg(yellow),
        snap_host: Style::new().fg(pink),
        snap_size: Style::new().fg(mauve),
        snap_active: Style::new().fg(green),
        snap_inactive: Style::new().fg(subtext_dim),
        dir_fg: blue,
        file_fg: text,
        symlink_fg: pink,
        file_size: Style::new().fg(subtext_dim),
        breadcrumb: Style::new().fg(teal).add_modifier(Modifier::BOLD),
        progress_filled: teal,
        progress_empty: overlay,
        gauge_filled: Style::new().fg(teal).bg(surface),
        gauge_empty: Style::new().fg(overlay).bg(bg),
        success: Style::new().fg(green).add_modifier(Modifier::BOLD),
        error: Style::new().fg(red).add_modifier(Modifier::BOLD),
        warning: Style::new().fg(yellow).add_modifier(Modifier::BOLD),
        info: Style::new().fg(blue).add_modifier(Modifier::BOLD),
        stat_label: Style::new().fg(subtext),
        stat_value: Style::new().fg(text).add_modifier(Modifier::BOLD),
        stat_card: Style::new().fg(text),
    }
};

pub(crate) static THEME: LazyLock<Theme> = LazyLock::new(|| ELECTRIC);

// ── Convenience helpers ─────────────────────────────────────────

/// A bordered block with consistent styling.
pub fn block(title: &str) -> Block<'static> {
    Block::default()
        .style(Style::new().bg(THEME.bg))
        .borders(Borders::ALL)
        .border_type(ratatui::widgets::BorderType::Rounded)
        .border_style(THEME.border)
        .title(format!(" {} ", title))
        .title_style(THEME.header)
}

/// A block without borders (just a padded surface).
#[expect(dead_code)]
pub fn surface_block(title: &str) -> Block<'static> {
    Block::default()
        .style(Style::new().bg(THEME.surface))
        .title(format!(" {} ", title))
        .title_style(THEME.header)
}

/// A gauge with modern styling.
#[expect(dead_code)]
pub fn gauge<'a>(pct: f64, label: &'a str) -> Gauge<'a> {
    Gauge::default()
        .gauge_style(THEME.gauge_filled)
        .percent((pct * 100.0) as u16)
        .label(label)
}

pub fn scrollbar() -> Scrollbar<'static> {
    Scrollbar::new(ScrollbarOrientation::VerticalRight)
        .begin_symbol(None)
        .end_symbol(None)
        .track_symbol(Some("\u{2502}"))
        .thumb_symbol("\u{2588}")
        .style(THEME.border)
}

pub fn render_scrollbar(frame: &mut Frame, area: Rect, total: usize, position: usize) {
    if total == 0 {
        return;
    }
    let mut state = ScrollbarState::new(total).position(position);
    frame.render_stateful_widget(scrollbar(), area.inner(Margin::new(1, 1)), &mut state);
}

pub fn key_hint(key: &str, label: &str) -> Vec<Span<'static>> {
    vec![
        Span::styled(format!(" {} ", key), THEME.menu_key),
        Span::styled(format!(" {} ", label), Style::new().fg(THEME.subtext)),
    ]
}

pub fn key_hint_footer(hints: &[(&str, &str)]) -> Line<'static> {
    let mut spans = Vec::new();
    for (i, (key, label)) in hints.iter().enumerate() {
        if i > 0 {
            spans.push(Span::styled("  ", Style::new().fg(THEME.subtext_dim)));
        }
        spans.extend(key_hint(key, label));
    }
    Line::from(spans)
}

#[expect(dead_code)]
pub fn separator() -> Paragraph<'static> {
    Paragraph::new(Span::styled(
        "\u{2500}".repeat(80),
        Style::new().fg(THEME.overlay),
    ))
}

pub fn format_tags(tags: impl IntoIterator<Item = impl AsRef<str>>) -> String {
    let mut result = String::new();
    let mut iter = tags.into_iter();
    if let Some(first) = iter.next() {
        result.push_str(first.as_ref());
        for tag in iter {
            result.push_str(", ");
            result.push_str(tag.as_ref());
        }
    }
    result
}

/// Characters used for the loading spinner animation in TUI screens.
pub const SPINNER_CHARS: &[char] = &['\u{25D0}', '\u{25D3}', '\u{25D1}', '\u{25D2}'];
