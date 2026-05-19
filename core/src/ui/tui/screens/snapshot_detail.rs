use chrono::Local;
use crossterm::event::KeyCode;
use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Direction, Layout},
    style::Style,
    text::{Line, Span, Text},
    widgets::{Block, Borders, Paragraph, Scrollbar, ScrollbarOrientation, ScrollbarState},
};

use crate::{repository::snapshot::SnapshotEntry, utils};

use crate::ui::tui::theme;

#[derive(Debug)]
pub enum DetailAction {
    Back,
    Quit,
}

pub struct SnapshotDetailScreen {
    entry: SnapshotEntry,
    scroll: usize,
    max_scroll: usize,
}

impl SnapshotDetailScreen {
    pub fn new(entry: SnapshotEntry) -> Self {
        Self {
            entry,
            scroll: 0,
            max_scroll: 0,
        }
    }

    pub fn handle_key(&mut self, key: KeyCode) -> Option<DetailAction> {
        match key {
            KeyCode::Esc => Some(DetailAction::Back),
            KeyCode::Char('q') => Some(DetailAction::Quit),
            KeyCode::Down => {
                self.scroll = (self.scroll + 1).min(self.max_scroll);
                None
            }
            KeyCode::Up => {
                self.scroll = self.scroll.saturating_sub(1);
                None
            }
            _ => None,
        }
    }

    pub fn render(&mut self, frame: &mut Frame) {
        let area = frame.area();
        let inner = area.inner(ratatui::layout::Margin::new(2, 1));

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(1), Constraint::Min(3)])
            .split(inner);

        self.render_title(frame, chunks[0]);

        let content_area = chunks[1];
        let content_height = content_area.height.saturating_sub(2) as usize;

        let mut lines: Vec<Line<'static>> = Vec::new();
        let s = &self.entry.snapshot;

        let mut add_line = |label: Option<&str>, spans: Vec<Span<'static>>| {
            let mut line_spans = Vec::new();
            if let Some(lbl) = label {
                line_spans.push(Span::styled(
                    format!("{:<13}", lbl),
                    Style::default().bold(),
                ));
            }
            line_spans.extend(spans);
            lines.push(Line::from(line_spans));
        };

        add_line(
            Some("ID:"),
            vec![Span::styled(
                self.entry.id.to_hex(),
                Style::default().fg(theme::SNAPSHOT_ID),
            )],
        );

        let ts = utils::pretty_print_timestamp(&s.timestamp, None);
        let elapsed = Local::now() - s.timestamp;
        let ago = utils::pretty_print_duration_chrono(elapsed, 1);
        add_line(
            Some("Date:"),
            vec![
                Span::raw(ts.to_string()),
                Span::raw("  ("),
                Span::styled(
                    format!("{} ago", ago),
                    Style::default().fg(theme::SNAPSHOT_DATE),
                ),
                Span::raw(")"),
            ],
        );

        if let Some(ref parent) = s.parent {
            add_line(
                Some("Parent:"),
                vec![Span::styled(
                    parent.to_short_hex(12),
                    Style::default().fg(theme::SNAPSHOT_ID),
                )],
            );
        }

        add_line(
            Some("Host:"),
            vec![Span::raw(
                s.hostname.as_deref().unwrap_or("(unknown)").to_string(),
            )],
        );
        add_line(
            Some("User:"),
            vec![Span::raw(
                s.username.as_deref().unwrap_or("(unknown)").to_string(),
            )],
        );

        if let Some(ref version) = s.version {
            add_line(Some("Version:"), vec![Span::raw(version.to_string())]);
        }

        add_line(
            Some("Root:"),
            vec![Span::raw(s.root.to_string_lossy().to_string())],
        );

        if let Some(ref desc) = s.description {
            add_line(Some("Description:"), vec![Span::raw(desc.to_string())]);
        }

        let tags = if s.tags.is_empty() {
            "(none)".to_string()
        } else {
            s.tags.iter().cloned().collect::<Vec<_>>().join(", ")
        };
        add_line(Some("Tags:"), vec![Span::raw(tags)]);

        add_line(
            Some("Active:"),
            vec![Span::raw(if self.entry.active { "yes" } else { "no" })],
        );

        add_line(
            None,
            vec![Span::styled(
                "Paths:",
                Style::default().bold().fg(theme::SNAPSHOT_DATE),
            )],
        );

        for p in &s.paths {
            let relative = p
                .strip_prefix(&s.root)
                .unwrap_or(p.as_path())
                .to_string_lossy()
                .to_string();
            add_line(None, vec![Span::raw(format!("  {}", relative))]);
        }

        add_line(
            None,
            vec![Span::styled(
                "Summary:",
                Style::default().bold().fg(theme::SNAPSHOT_DATE),
            )],
        );

        add_line(
            Some("  Size:"),
            vec![Span::raw(utils::format_size_binary(s.size(), 3))],
        );
        add_line(
            Some("  Items:"),
            vec![Span::raw(s.summary.processed_items_count.to_string())],
        );

        self.max_scroll = lines.len().saturating_sub(content_height);

        let total_lines = lines.len();
        let start = self.scroll.min(total_lines.saturating_sub(1));
        let end = (start + content_height).min(total_lines);
        let visible: Vec<_> = if start < end {
            lines
                .iter()
                .skip(start)
                .take(end - start)
                .cloned()
                .collect()
        } else {
            lines.clone()
        };

        let paragraph = Paragraph::new(Text::from(visible))
            .alignment(Alignment::Left)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(" Snapshot ")
                    .border_style(theme::border_style()),
            );

        frame.render_widget(paragraph, content_area);

        if self.max_scroll > 0 {
            let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight)
                .begin_symbol(None)
                .end_symbol(None)
                .track_symbol(Some("│"))
                .thumb_symbol("█")
                .style(theme::border_style());

            let mut scrollbar_state = ScrollbarState::new(total_lines)
                .position(self.scroll)
                .viewport_content_length(content_height);

            frame.render_stateful_widget(
                scrollbar,
                content_area.inner(ratatui::layout::Margin::new(1, 1)),
                &mut scrollbar_state,
            );
        }
    }

    fn render_title(&self, frame: &mut Frame, area: ratatui::layout::Rect) {
        let title = ratatui::text::Line::from(vec![
            Span::styled("[Esc]", Style::default().fg(theme::MENU_KEY).bold()),
            Span::raw(" back"),
            Span::raw("    "),
            Span::styled("[q]", Style::default().fg(theme::MENU_KEY).bold()),
            Span::raw(" close"),
            Span::raw("    "),
            Span::styled("[↑↓]", Style::default().fg(theme::MENU_KEY).bold()),
            Span::raw(" scroll"),
        ])
        .alignment(Alignment::Left);
        frame.render_widget(Paragraph::new(title), area);
    }
}
