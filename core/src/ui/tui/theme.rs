use std::sync::LazyLock;

use ratatui::{
    Frame,
    layout::{Margin, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Scrollbar, ScrollbarOrientation, ScrollbarState},
};

#[allow(dead_code)]
pub(crate) struct Theme {
    pub header_fg: Color,
    pub footer_fg: Color,
    pub menu_key: Color,
    pub snapshot_id: Color,
    pub snapshot_date: Color,
    pub snapshot_host: Color,
    pub snapshot_size: Color,
    pub table_header: Color,
    pub border_color: Color,
    pub selected_row_bg: Color,
    pub progress_filled: Color,
    pub progress_empty: Color,
    pub toast_error: Color,
    pub toast_warning: Color,
    pub toast_info: Color,
    pub file_fg: Color,
    pub dir_fg: Color,
    pub symlink_fg: Color,
    pub file_size_fg: Color,
    pub breadcrumb_fg: Color,
    pub style_header: Style,
    pub style_menu_key: Style,
    pub style_selected_row: Style,
    pub style_border: Style,
    pub style_table_header: Style,
    pub style_snapshot_id: Style,
    pub style_snapshot_date: Style,
    pub style_snapshot_host: Style,
    pub style_snapshot_size: Style,
}

const DARK_THEME: Theme = Theme {
    header_fg: Color::Rgb(137, 180, 250),
    footer_fg: Color::Rgb(116, 120, 142),
    menu_key: Color::Rgb(137, 180, 250),
    snapshot_id: Color::Rgb(137, 180, 250),
    snapshot_date: Color::Rgb(166, 227, 161),
    snapshot_host: Color::Rgb(249, 226, 175),
    snapshot_size: Color::Rgb(203, 166, 247),
    table_header: Color::Rgb(205, 214, 244),
    border_color: Color::Rgb(88, 91, 112),
    selected_row_bg: Color::DarkGray,
    progress_filled: Color::Cyan,
    progress_empty: Color::DarkGray,
    toast_error: Color::Red,
    toast_warning: Color::Yellow,
    toast_info: Color::Cyan,
    file_fg: Color::White,
    dir_fg: Color::Cyan,
    symlink_fg: Color::Magenta,
    file_size_fg: Color::DarkGray,
    breadcrumb_fg: Color::Yellow,
    style_header: Style::new()
        .fg(Color::Rgb(137, 180, 250))
        .add_modifier(Modifier::BOLD),
    style_menu_key: Style::new()
        .fg(Color::Rgb(137, 180, 250))
        .add_modifier(Modifier::BOLD),
    style_selected_row: Style::new().bg(Color::DarkGray),
    style_border: Style::new().fg(Color::Rgb(88, 91, 112)),
    style_table_header: Style::new()
        .fg(Color::Rgb(205, 214, 244))
        .add_modifier(Modifier::BOLD)
        .add_modifier(Modifier::REVERSED),
    style_snapshot_id: Style::new().fg(Color::Rgb(137, 180, 250)),
    style_snapshot_date: Style::new().fg(Color::Rgb(166, 227, 161)),
    style_snapshot_host: Style::new().fg(Color::Rgb(249, 226, 175)),
    style_snapshot_size: Style::new().fg(Color::Rgb(203, 166, 247)),
};

const LIGHT_THEME: Theme = Theme {
    header_fg: Color::Rgb(0, 70, 180),
    footer_fg: Color::Rgb(60, 60, 60),
    menu_key: Color::Rgb(0, 70, 180),
    snapshot_id: Color::Rgb(0, 70, 180),
    snapshot_date: Color::Rgb(0, 110, 0),
    snapshot_host: Color::Rgb(170, 80, 0),
    snapshot_size: Color::Rgb(100, 0, 160),
    table_header: Color::Rgb(20, 20, 20),
    border_color: Color::Rgb(80, 80, 80),
    selected_row_bg: Color::Rgb(175, 180, 230),
    progress_filled: Color::Rgb(0, 90, 210),
    progress_empty: Color::Rgb(160, 160, 160),
    toast_error: Color::Rgb(200, 0, 0),
    toast_warning: Color::Rgb(190, 90, 0),
    toast_info: Color::Rgb(0, 70, 180),
    file_fg: Color::Rgb(20, 20, 20),
    dir_fg: Color::Rgb(0, 70, 180),
    symlink_fg: Color::Rgb(160, 0, 100),
    file_size_fg: Color::Rgb(80, 80, 80),
    breadcrumb_fg: Color::Rgb(0, 70, 180),
    style_header: Style::new()
        .fg(Color::Rgb(0, 70, 180))
        .add_modifier(Modifier::BOLD),
    style_menu_key: Style::new()
        .fg(Color::Rgb(0, 70, 180))
        .add_modifier(Modifier::BOLD),
    style_selected_row: Style::new().bg(Color::Rgb(175, 180, 230)),
    style_border: Style::new().fg(Color::Rgb(80, 80, 80)),
    style_table_header: Style::new()
        .fg(Color::Rgb(20, 20, 20))
        .add_modifier(Modifier::BOLD)
        .add_modifier(Modifier::REVERSED),
    style_snapshot_id: Style::new().fg(Color::Rgb(0, 70, 180)),
    style_snapshot_date: Style::new().fg(Color::Rgb(0, 110, 0)),
    style_snapshot_host: Style::new().fg(Color::Rgb(170, 80, 0)),
    style_snapshot_size: Style::new().fg(Color::Rgb(100, 0, 160)),
};

fn is_light_terminal() -> bool {
    std::env::var("COLORFGBG")
        .ok()
        .as_deref()
        .and_then(|v| v.split(';').nth(1))
        .and_then(|bg| bg.parse::<u8>().ok())
        .is_some_and(|bg| bg > 7)
}

/// Must be called before entering raw mode so terminal queries work
pub(crate) fn init() {
    let _ = &*THEME;
}

pub(crate) static THEME: LazyLock<&'static Theme> =
    LazyLock::new(|| match std::env::var("MAPACHE_TUI_THEME").as_deref() {
        Ok("light") => &LIGHT_THEME,
        Ok("dark") => &DARK_THEME,
        _ if is_light_terminal() => &LIGHT_THEME,
        _ => &DARK_THEME,
    });

pub fn scrollbar() -> Scrollbar<'static> {
    Scrollbar::new(ScrollbarOrientation::VerticalRight)
        .begin_symbol(None)
        .end_symbol(None)
        .track_symbol(Some("\u{2502}"))
        .thumb_symbol("\u{2588}")
        .style(THEME.style_border)
}

pub fn themed_block(title: &str) -> Block<'_> {
    Block::default()
        .borders(Borders::ALL)
        .title(format!(" {} ", title))
        .border_style(THEME.style_border)
}

pub fn key_hint(key: &str, label: &str) -> Vec<Span<'static>> {
    vec![
        Span::styled(format!("[{}]", key), THEME.style_menu_key),
        Span::raw(format!(" {}", label)),
    ]
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

pub fn key_hint_footer(hints: &[(&str, &str)]) -> Line<'static> {
    let mut spans = Vec::new();
    for (i, (key, label)) in hints.iter().enumerate() {
        if i > 0 {
            spans.push(Span::raw("    "));
        }
        spans.extend(key_hint(key, label));
    }
    Line::from(spans)
}

pub fn render_scrollbar(frame: &mut Frame, area: Rect, total: usize, position: usize) {
    if total == 0 {
        return;
    }
    let mut state = ScrollbarState::new(total).position(position);
    frame.render_stateful_widget(scrollbar(), area.inner(Margin::new(1, 1)), &mut state);
}
