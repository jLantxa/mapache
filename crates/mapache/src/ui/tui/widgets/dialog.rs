use ratatui::{
    Frame,
    layout::{Alignment, Rect},
    style::Style,
    text::{Line, Span, Text},
    widgets::{Block, BorderType, Borders, Padding, Paragraph, Widget},
};

use crate::ui::tui::{theme, widgets::wrap_line};

const DIALOG_MIN_WIDTH: u16 = 30;
const DIALOG_PADDING: u16 = 2;

pub struct Dialog {
    title: String,
    border_style: Style,
    border_type: BorderType,
    title_style: Style,
    content: Text<'static>,
}

#[expect(dead_code)]
impl Dialog {
    pub fn with_text(
        title: impl Into<String>,
        border_style: Style,
        content: Text<'static>,
    ) -> Self {
        Self {
            title: title.into(),
            border_style,
            border_type: BorderType::Rounded,
            title_style: theme::THEME.header,
            content,
        }
    }

    pub fn border_type(mut self, border_type: BorderType) -> Self {
        self.border_type = border_type;
        self
    }

    pub fn title_style(mut self, style: Style) -> Self {
        self.title_style = style;
        self
    }

    pub fn render(&self, area: Rect, frame: &mut Frame) {
        let overhead = 2 + DIALOG_PADDING * 2;

        let text_width = self
            .content
            .lines
            .iter()
            .map(|l| l.width())
            .max()
            .unwrap_or(0) as u16;
        let natural_width = text_width + overhead;
        let popup_width = natural_width.min(area.width).max(DIALOG_MIN_WIDTH);

        let text_max_width = popup_width.saturating_sub(overhead) as usize;

        let mut wrapped_lines: Vec<Line<'static>> = Vec::new();
        for line in &self.content.lines {
            wrap_line(line, text_max_width, &mut wrapped_lines);
        }

        let line_count = wrapped_lines.len();
        let popup_height = (line_count as u16 + overhead).min(area.height).max(5);

        let x = area.width.saturating_sub(popup_width) / 2;
        let y = area.height.saturating_sub(popup_height) / 2;
        let popup_area = Rect {
            x,
            y,
            width: popup_width,
            height: popup_height,
        };

        let block = Block::default()
            .style(Style::new().bg(theme::THEME.bg))
            .borders(Borders::ALL)
            .border_type(self.border_type)
            .border_style(self.border_style)
            .title(Span::styled(format!(" {} ", self.title), self.title_style))
            .title_alignment(Alignment::Center)
            .padding(Padding::uniform(DIALOG_PADDING));

        let content = Paragraph::new(Text::from(wrapped_lines))
            .alignment(Alignment::Center)
            .block(block);

        ratatui::widgets::Clear.render(popup_area, frame.buffer_mut());
        frame.render_widget(content, popup_area);
    }
}
