use ratatui::{
    Frame,
    layout::{Alignment, Rect},
    style::Style,
    text::{Line, Span, Text},
    widgets::{Block, BorderType, Borders, Padding, Paragraph, Widget},
};

use crate::ui::tui::{
    theme,
    widgets::{centered_rect, wrap_line},
};

const DIALOG_PADDING: u16 = 2;

pub struct Dialog {
    title: String,
    border_style: Style,
    content: Text<'static>,
}

impl Dialog {
    pub fn with_text(
        title: impl Into<String>,
        border_style: Style,
        content: Text<'static>,
    ) -> Self {
        Self {
            title: title.into(),
            border_style,
            content,
        }
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
        // Never exceed the frame; on very narrow terminals the dialog simply
        // gets narrower instead of drawing past the right edge.
        let popup_width = natural_width.min(area.width);

        let text_max_width = popup_width.saturating_sub(overhead) as usize;

        let mut wrapped_lines: Vec<Line<'static>> = Vec::new();
        for line in &self.content.lines {
            wrap_line(line, text_max_width.max(1), &mut wrapped_lines);
        }

        let line_count = wrapped_lines.len();
        let popup_height = (line_count as u16 + overhead).min(area.height);

        let popup_area = centered_rect(area, popup_width, popup_height);

        let block = Block::default()
            .style(Style::new().bg(theme::THEME.bg))
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(self.border_style)
            .title(Span::styled(
                format!(" {} ", self.title),
                theme::THEME.header,
            ))
            .title_alignment(Alignment::Center)
            .padding(Padding::uniform(DIALOG_PADDING));

        let content = Paragraph::new(Text::from(wrapped_lines))
            .alignment(Alignment::Center)
            .block(block);

        ratatui::widgets::Clear.render(popup_area, frame.buffer_mut());
        frame.render_widget(content, popup_area);
    }
}
