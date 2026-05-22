use ratatui::{
    Frame,
    layout::{Alignment, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Padding, Paragraph, Widget},
};

const TOAST_MIN_WIDTH: u16 = 30;
const TOAST_MAX_WIDTH: u16 = 70;
const TOAST_MARGIN: u16 = 2;
const TOAST_PADDING: u16 = 2;
const TOAST_BORDER: u16 = 2;
const TOAST_TITLE_EXTRA: u16 = 2;

pub struct Toast {
    title: String,
    color: Color,
    content: Text<'static>,
}

impl Toast {
    pub fn new(title: impl Into<String>, color: Color, message: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            color,
            content: Text::from(message.into()),
        }
    }

    pub fn with_text(title: impl Into<String>, color: Color, content: Text<'static>) -> Self {
        Self {
            title: title.into(),
            color,
            content,
        }
    }

    pub fn render(&self, area: Rect, frame: &mut Frame) {
        let inner_width = area.width.saturating_sub(TOAST_MARGIN * 2);
        let max_width = TOAST_MAX_WIDTH.min(inner_width).max(TOAST_MIN_WIDTH);

        let title_width = self.title.chars().count() as u16 + TOAST_TITLE_EXTRA;
        let popup_width = title_width
            .max(max_width)
            .min(inner_width)
            .max(TOAST_MIN_WIDTH);
        let text_width = popup_width.saturating_sub(TOAST_BORDER + TOAST_PADDING * 2) as usize;

        let mut wrapped_lines: Vec<Line<'static>> = Vec::new();
        for line in &self.content.lines {
            wrap_line(line, text_width, &mut wrapped_lines);
        }

        let line_count = wrapped_lines.len();
        let popup_height = (line_count as u16 + TOAST_BORDER + TOAST_PADDING * 2)
            .min(area.height.saturating_sub(TOAST_MARGIN * 2))
            .max(5);

        let x = (area.width - popup_width) / 2;
        let y = (area.height - popup_height) / 2;

        let popup_area = Rect {
            x,
            y,
            width: popup_width,
            height: popup_height,
        };

        let style = Style::default().fg(self.color);
        let title_style = Style::default().fg(self.color).add_modifier(Modifier::BOLD);

        let content = Paragraph::new(Text::from(wrapped_lines))
            .alignment(Alignment::Left)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(style)
                    .title(Span::styled(format!(" {} ", self.title), title_style))
                    .title_alignment(Alignment::Center)
                    .padding(Padding::uniform(TOAST_PADDING)),
            );

        ratatui::widgets::Clear.render(popup_area, frame.buffer_mut());
        frame.render_widget(content, popup_area);
    }
}

fn wrap_line(line: &Line<'_>, max_width: usize, out: &mut Vec<Line<'static>>) {
    if line.spans.is_empty() {
        out.push(Line::from(""));
        return;
    }

    let full_text: String = line.spans.iter().map(|s| s.content.as_ref()).collect();
    let full_style = line.spans.first().map(|s| s.style);

    let chars: Vec<char> = full_text.chars().collect();
    let total = chars.len();
    if total == 0 {
        out.push(Line::from(""));
        return;
    }

    let mut start = 0;
    while start < total {
        let end = (start + max_width).min(total);
        let segment: String = chars[start..end].iter().collect();
        if let Some(style) = full_style {
            out.push(Line::from(Span::styled(segment, style)));
        } else {
            out.push(Line::from(segment));
        }
        start = end;
    }
}
