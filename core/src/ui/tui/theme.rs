use ratatui::style::{Color, Modifier, Style};

pub const HEADER_FG: Color = Color::Rgb(137, 180, 250);
pub const FOOTER_FG: Color = Color::Rgb(116, 120, 142);
pub const MENU_KEY: Color = Color::Rgb(137, 180, 250);
pub const MENU_TEXT: Color = Color::Rgb(166, 173, 200);
pub const SNAPSHOT_ID: Color = Color::Rgb(137, 180, 250);
pub const SNAPSHOT_DATE: Color = Color::Rgb(166, 227, 161);
pub const SNAPSHOT_HOST: Color = Color::Rgb(249, 226, 175);
pub const SNAPSHOT_SIZE: Color = Color::Rgb(203, 166, 247);
pub const TABLE_HEADER: Color = Color::Rgb(205, 214, 244);
pub const BORDER_COLOR: Color = Color::Rgb(88, 91, 112);

pub fn header_style() -> Style {
    Style::default().fg(HEADER_FG).add_modifier(Modifier::BOLD)
}

pub fn menu_key_style() -> Style {
    Style::default().fg(MENU_KEY).add_modifier(Modifier::BOLD)
}

pub fn menu_text_style() -> Style {
    Style::default().fg(MENU_TEXT)
}

pub fn selected_row_style() -> Style {
    Style::default().bg(Color::DarkGray)
}

pub fn border_style() -> Style {
    Style::default().fg(BORDER_COLOR)
}
