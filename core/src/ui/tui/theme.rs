use ratatui::{
    Frame,
    layout::{Margin, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Scrollbar, ScrollbarOrientation, ScrollbarState},
};

pub const HEADER_FG: Color = Color::Rgb(137, 180, 250);
pub const FOOTER_FG: Color = Color::Rgb(116, 120, 142);
pub const MENU_KEY: Color = Color::Rgb(137, 180, 250);
pub const SNAPSHOT_ID: Color = Color::Rgb(137, 180, 250);
pub const SNAPSHOT_DATE: Color = Color::Rgb(166, 227, 161);
pub const SNAPSHOT_HOST: Color = Color::Rgb(249, 226, 175);
pub const SNAPSHOT_SIZE: Color = Color::Rgb(203, 166, 247);
pub const TABLE_HEADER: Color = Color::Rgb(205, 214, 244);
pub const BORDER_COLOR: Color = Color::Rgb(88, 91, 112);
pub const SELECTED_ROW_BG: Color = Color::DarkGray;
pub const PROGRESS_FILLED: Color = Color::Cyan;
pub const PROGRESS_EMPTY: Color = Color::DarkGray;
pub const TOAST_ERROR: Color = Color::Red;
pub const TOAST_WARNING: Color = Color::Yellow;
pub const TOAST_INFO: Color = Color::Cyan;

pub const STYLE_HEADER: Style = Style::new().fg(HEADER_FG).add_modifier(Modifier::BOLD);

pub const STYLE_MENU_KEY: Style = Style::new().fg(MENU_KEY).add_modifier(Modifier::BOLD);

pub const STYLE_SELECTED_ROW: Style = Style::new().bg(SELECTED_ROW_BG);

pub const STYLE_BORDER: Style = Style::new().fg(BORDER_COLOR);

pub const STYLE_TABLE_HEADER: Style = Style::new()
    .fg(TABLE_HEADER)
    .add_modifier(Modifier::BOLD)
    .add_modifier(Modifier::REVERSED);

pub const STYLE_SNAPSHOT_ID: Style = Style::new().fg(SNAPSHOT_ID);
pub const STYLE_SNAPSHOT_DATE: Style = Style::new().fg(SNAPSHOT_DATE);
pub const STYLE_SNAPSHOT_HOST: Style = Style::new().fg(SNAPSHOT_HOST);
pub const STYLE_SNAPSHOT_SIZE: Style = Style::new().fg(SNAPSHOT_SIZE);

pub fn scrollbar() -> Scrollbar<'static> {
    Scrollbar::new(ScrollbarOrientation::VerticalRight)
        .begin_symbol(None)
        .end_symbol(None)
        .track_symbol(Some("\u{2502}"))
        .thumb_symbol("\u{2588}")
        .style(STYLE_BORDER)
}

pub fn themed_block(title: &str) -> Block<'_> {
    Block::default()
        .borders(Borders::ALL)
        .title(format!(" {} ", title))
        .border_style(STYLE_BORDER)
}

pub fn key_hint(key: &str, label: &str) -> Vec<Span<'static>> {
    vec![
        Span::styled(format!("[{}]", key), STYLE_MENU_KEY),
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
