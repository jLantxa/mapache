use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Span;
use ratatui::widgets::{Block, Borders, Scrollbar, ScrollbarOrientation};

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

pub fn header_style() -> Style {
    Style::default().fg(HEADER_FG).add_modifier(Modifier::BOLD)
}

pub fn menu_key_style() -> Style {
    Style::default().fg(MENU_KEY).add_modifier(Modifier::BOLD)
}

pub fn selected_row_style() -> Style {
    Style::default().bg(SELECTED_ROW_BG)
}

pub fn border_style() -> Style {
    Style::default().fg(BORDER_COLOR)
}

pub fn themed_block(title: &str) -> Block<'_> {
    Block::default()
        .borders(Borders::ALL)
        .title(format!(" {} ", title))
        .border_style(border_style())
}

pub fn create_scrollbar() -> Scrollbar<'static> {
    Scrollbar::new(ScrollbarOrientation::VerticalRight)
        .begin_symbol(None)
        .end_symbol(None)
        .track_symbol(Some("│"))
        .thumb_symbol("█")
        .style(border_style())
}

pub fn key_hint(key: &str, label: &str) -> Vec<Span<'static>> {
    vec![
        Span::styled(format!("[{}]", key), menu_key_style()),
        Span::raw(format!(" {}", label)),
    ]
}

pub fn format_tags(tags: impl IntoIterator<Item = impl AsRef<str>>) -> String {
    let v: Vec<_> = tags.into_iter().map(|t| t.as_ref().to_string()).collect();
    v.join(", ")
}
