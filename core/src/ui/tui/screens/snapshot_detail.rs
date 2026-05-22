use std::sync::Arc;

use anyhow::Result;
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
    snapshots: Arc<SnapshotEntryList>,
    current_index: usize,
    scroll: usize,
    max_scroll: usize,
    page_size: usize,
    cached_lines: Vec<Line<'static>>,
}

impl SnapshotDetailScreen {
    pub fn new(
        repo: Arc<Repository>,
        snapshots: Arc<SnapshotEntryList>,
        current_index: usize,
    ) -> Self {
        Self {
            repo,
            snapshots,
            current_index,
            scroll: 0,
            max_scroll: 0,
            page_size: DEFAULT_PAGE_SIZE,
            cached_lines: Vec::new(),
        }
    }

    fn entry(&self) -> &SnapshotEntry {
        &self.snapshots[self.current_index]
    }

    fn snapshot(&self) -> &Snapshot {
        &self.entry().snapshot
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

    fn build_content_lines(&self) -> Vec<Line<'static>> {
        let s = self.snapshot();
        let entry = self.entry();
        let mut lines = Vec::with_capacity(20);

        lines.push(Line::from(vec![
            Span::styled("ID:          ", Style::default().bold()),
            Span::styled(entry.id.to_hex(), theme::STYLE_SNAPSHOT_ID),
        ]));

        let ts = utils::pretty_print_timestamp(&s.timestamp, None);
        let elapsed = Local::now() - s.timestamp;
        let ago = utils::pretty_print_duration_chrono(elapsed, 1);
        lines.push(Line::from(vec![
            Span::styled("Date:        ", Style::default().bold()),
            Span::raw(ts.to_string()),
            Span::raw("  ("),
            Span::styled(format!("{} ago", ago), theme::STYLE_SNAPSHOT_DATE),
            Span::raw(")"),
        ]));

        if let Some(ref parent) = s.parent {
            lines.push(Line::from(vec![
                Span::styled("Parent:      ", Style::default().bold()),
                Span::styled(parent.to_short_hex(12), theme::STYLE_SNAPSHOT_ID),
            ]));
        }

        lines.push(Line::from(vec![
            Span::styled("Host:        ", Style::default().bold()),
            Span::raw(s.hostname.as_deref().unwrap_or("(unknown)").to_string()),
        ]));
        lines.push(Line::from(vec![
            Span::styled("User:        ", Style::default().bold()),
            Span::raw(s.username.as_deref().unwrap_or("(unknown)").to_string()),
        ]));

        if let Some(ref version) = s.version {
            lines.push(Line::from(vec![
                Span::styled("Version:     ", Style::default().bold()),
                Span::raw(version.to_string()),
            ]));
        }

        lines.push(Line::from(vec![
            Span::styled("Root:        ", Style::default().bold()),
            Span::raw(s.root.to_string_lossy().into_owned()),
        ]));

        if let Some(ref desc) = s.description {
            lines.push(Line::from(vec![
                Span::styled("Description: ", Style::default().bold()),
                Span::raw(desc.to_string()),
            ]));
        }

        let tags = if s.tags.is_empty() {
            "(none)".to_string()
        } else {
            theme::format_tags(&s.tags)
        };
        lines.push(Line::from(vec![
            Span::styled("Tags:        ", Style::default().bold()),
            Span::raw(tags),
        ]));

        lines.push(Line::from(vec![
            Span::styled("Active:      ", Style::default().bold()),
            Span::raw(if entry.active { "yes" } else { "no" }),
        ]));

        lines.push(Line::from(vec![Span::styled(
            "Paths:",
            Style::default().bold().fg(theme::SNAPSHOT_DATE),
        )]));

        for p in &s.paths {
            let relative = p
                .strip_prefix(&s.root)
                .unwrap_or(p.as_path())
                .to_string_lossy()
                .into_owned();
            lines.push(Line::from(vec![Span::raw(format!("  {}", relative))]));
        }

        lines.push(Line::from(vec![Span::styled(
            "Summary:",
            Style::default().bold().fg(theme::SNAPSHOT_DATE),
        )]));

        lines.push(Line::from(vec![
            Span::styled("  Size:      ", Style::default().bold()),
            Span::raw(utils::format_size_binary(s.size(), 3)),
        ]));
        lines.push(Line::from(vec![
            Span::styled("  Items:     ", Style::default().bold()),
            Span::raw(s.summary.processed_items_count.to_string()),
        ]));

        lines
    }

    fn render_title(&self, frame: &mut Frame, area: ratatui::layout::Rect) {
        let has_prev = self.current_index > 0;
        let has_next = self.current_index < self.snapshots.len().saturating_sub(1);

        let prev_style = if has_prev {
            theme::STYLE_MENU_KEY
        } else {
            Style::default().fg(theme::FOOTER_FG)
        };
        let next_style = if has_next {
            theme::STYLE_MENU_KEY
        } else {
            Style::default().fg(theme::FOOTER_FG)
        };

        let title = Line::from(vec![
            Span::styled("[Esc]", theme::STYLE_MENU_KEY),
            Span::raw(" back"),
            Span::raw("    "),
            Span::styled("[Enter]", theme::STYLE_MENU_KEY),
            Span::raw(" explore"),
            Span::raw("    "),
            Span::styled("[r]", theme::STYLE_MENU_KEY),
            Span::raw(" restore"),
            Span::raw("    "),
            Span::styled("<", prev_style),
            Span::raw(" prev"),
            Span::raw("    "),
            Span::styled(">", next_style),
            Span::raw(" next"),
            Span::raw("    "),
            Span::styled("[q]", theme::STYLE_MENU_KEY),
            Span::raw(" close"),
            Span::raw("    "),
            Span::styled("[\u{2191}\u{2193}]", theme::STYLE_MENU_KEY),
            Span::raw(" scroll"),
        ])
        .alignment(Alignment::Left);
        frame.render_widget(Paragraph::new(title), area);
    }

    fn render_content(&mut self, frame: &mut Frame, content_area: ratatui::layout::Rect) {
        let content_height = content_area.height.saturating_sub(2) as usize;
        let line_count = self.cached_lines.len();

        self.max_scroll = line_count.saturating_sub(content_height);
        self.page_size = content_height;

        let paragraph = Paragraph::new(Text::from(self.cached_lines.clone()))
            .alignment(Alignment::Left)
            .block(theme::themed_block("Snapshot"))
            .scroll((self.scroll as u16, 0));

        frame.render_widget(paragraph, content_area);

        if self.max_scroll > 0 {
            let mut scrollbar_state = ScrollbarState::new(self.max_scroll + content_height)
                .position(self.scroll)
                .viewport_content_length(content_height);

            frame.render_stateful_widget(
                theme::scrollbar(),
                content_area.inner(ratatui::layout::Margin::new(1, 1)),
                &mut scrollbar_state,
            );
        }
    }
}

#[async_trait]
impl Screen for SnapshotDetailScreen {
    async fn on_become_active(&mut self) -> Result<()> {
        self.cached_lines = self.build_content_lines();
        self.scroll = 0;
        Ok(())
    }

    fn render(&mut self, frame: &mut Frame) {
        let area = frame.area();
        let inner = area.inner(ratatui::layout::Margin::new(2, 1));

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(1), Constraint::Min(3)])
            .split(inner);

        self.render_title(frame, chunks[0]);
        self.render_content(frame, chunks[1]);
    }

    async fn handle_key(&mut self, key: KeyEvent) -> Option<Transition> {
        match key.code {
            KeyCode::Esc => Some(Transition::Pop),
            KeyCode::Char('q') => Some(Transition::Quit),
            KeyCode::Enter => {
                let entry = self.entry().clone();
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
                let entry = self.entry().clone();
                Some(Transition::Push(Box::new(RestoreScreen::new(
                    self.repo.clone(),
                    entry,
                    None,
                ))))
            }
            KeyCode::Char('<') | KeyCode::Char(',') => {
                self.navigate_snapshot(-1);
                self.cached_lines = self.build_content_lines();
                None
            }
            KeyCode::Char('>') | KeyCode::Char('.') => {
                self.navigate_snapshot(1);
                self.cached_lines = self.build_content_lines();
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
