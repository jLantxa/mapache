use std::sync::Arc;

use async_trait::async_trait;
use chrono::Local;
use crossterm::event::{KeyCode, KeyEvent};
use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Direction, Layout},
    style::Style,
    text::{Line, Span, Text},
    widgets::{Paragraph, ScrollbarState},
};

use crate::{
    repository::{
        repo::Repository,
        snapshot::{Snapshot, SnapshotEntry, SnapshotEntryList},
    },
    ui::tui::{
        app::{Screen, Transition},
        screens::{file_explorer::FileExplorerScreen, restore::RestoreScreen},
        theme,
    },
    utils,
};

const DEFAULT_PAGE_SIZE: usize = 10;

pub struct SnapshotDetailScreen {
    repo: Arc<Repository>,
    snapshots: SnapshotEntryList,
    current_index: usize,
    scroll: usize,
    max_scroll: usize,
    page_size: usize,
}

impl SnapshotDetailScreen {
    pub fn new(repo: Arc<Repository>, snapshots: SnapshotEntryList, current_index: usize) -> Self {
        Self {
            repo,
            snapshots,
            current_index,
            scroll: 0,
            max_scroll: 0,
            page_size: DEFAULT_PAGE_SIZE,
        }
    }

    pub fn get_snapshot(&self) -> &Snapshot {
        &self.snapshots[self.current_index].snapshot
    }

    pub fn get_entry(&self) -> &SnapshotEntry {
        &self.snapshots[self.current_index]
    }

    fn navigate_snapshot(&mut self, direction: i32) {
        let new_index = if direction < 0 {
            if self.current_index == 0 {
                return;
            }
            self.current_index - 1
        } else {
            if self.current_index >= self.snapshots.len().saturating_sub(1) {
                return;
            }
            self.current_index + 1
        };
        self.current_index = new_index;
        self.scroll = 0;
    }

    fn render_title(&self, frame: &mut Frame, area: ratatui::layout::Rect) {
        let has_prev = self.current_index > 0;
        let has_next = self.current_index < self.snapshots.len().saturating_sub(1);

        let prev_style = if has_prev {
            Style::default().fg(theme::MENU_KEY).bold()
        } else {
            Style::default().fg(theme::FOOTER_FG)
        };
        let next_style = if has_next {
            Style::default().fg(theme::MENU_KEY).bold()
        } else {
            Style::default().fg(theme::FOOTER_FG)
        };

        let title = Line::from(vec![
            Span::styled("[Esc]", Style::default().fg(theme::MENU_KEY).bold()),
            Span::raw(" back"),
            Span::raw("    "),
            Span::styled("[Enter]", Style::default().fg(theme::MENU_KEY).bold()),
            Span::raw(" explore"),
            Span::raw("    "),
            Span::styled("[r]", Style::default().fg(theme::MENU_KEY).bold()),
            Span::raw(" restore"),
            Span::raw("    "),
            Span::styled("<", prev_style),
            Span::raw(" prev"),
            Span::raw("    "),
            Span::styled(">", next_style),
            Span::raw(" next"),
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

#[async_trait]
impl Screen for SnapshotDetailScreen {
    fn render(&mut self, frame: &mut Frame) {
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
        let s = self.get_snapshot();
        let entry = self.get_entry();

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
                entry.id.to_hex(),
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
            theme::format_tags(&s.tags)
        };
        add_line(Some("Tags:"), vec![Span::raw(tags)]);

        add_line(
            Some("Active:"),
            vec![Span::raw(if entry.active { "yes" } else { "no" })],
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
        self.page_size = content_height;

        let paragraph = Paragraph::new(Text::from(lines))
            .alignment(Alignment::Left)
            .block(theme::themed_block("Snapshot"))
            .scroll((self.scroll as u16, 0));

        frame.render_widget(paragraph, content_area);

        if self.max_scroll > 0 {
            let scrollbar = theme::create_scrollbar();

            let mut scrollbar_state = ScrollbarState::new(self.max_scroll + content_height)
                .position(self.scroll)
                .viewport_content_length(content_height);

            frame.render_stateful_widget(
                scrollbar,
                content_area.inner(ratatui::layout::Margin::new(1, 1)),
                &mut scrollbar_state,
            );
        }
    }

    async fn handle_key(&mut self, key: KeyEvent) -> Option<Transition> {
        match key.code {
            KeyCode::Esc => Some(Transition::Pop),
            KeyCode::Char('q') => Some(Transition::Quit),
            KeyCode::Enter => {
                let entry = self.get_entry().clone();
                let tree_id = entry.snapshot.tree;
                match FileExplorerScreen::new(self.repo.clone(), entry, &tree_id).await {
                    Ok(explorer) => Some(Transition::Push(Box::new(explorer))),
                    Err(e) => {
                        tracing::error!("Failed to load file explorer: {:?}", e);
                        None
                    }
                }
            }
            KeyCode::Char('r') => {
                let entry = self.get_entry().clone();
                Some(Transition::Push(Box::new(RestoreScreen::new(
                    self.repo.clone(),
                    entry,
                    None,
                ))))
            }
            KeyCode::Char('<') | KeyCode::Char(',') => {
                self.navigate_snapshot(-1);
                None
            }
            KeyCode::Char('>') | KeyCode::Char('.') => {
                self.navigate_snapshot(1);
                None
            }
            KeyCode::Down => {
                self.scroll = (self.scroll + 1).min(self.max_scroll);
                None
            }
            KeyCode::Up => {
                self.scroll = self.scroll.saturating_sub(1);
                None
            }
            KeyCode::PageDown | KeyCode::Char(' ') => {
                self.scroll = (self.scroll + self.page_size).min(self.max_scroll);
                None
            }
            KeyCode::PageUp => {
                self.scroll = self.scroll.saturating_sub(self.page_size);
                None
            }
            KeyCode::Home => {
                self.scroll = 0;
                None
            }
            KeyCode::End => {
                self.scroll = self.max_scroll;
                None
            }
            _ => None,
        }
    }
}
