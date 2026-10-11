use std::sync::Arc;

use async_trait::async_trait;
use chrono::Local;
use crossterm::event::{KeyCode, KeyEvent};
use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Direction, Layout, Margin},
    text::{Line, Span, Text},
    widgets::ScrollbarState,
};

use crate::{
    common::error::Result,
    repository::{
        repo::Repository,
        snapshot::{Snapshot, SnapshotEntry, SnapshotEntryList},
    },
    ui::tui::{
        app::{Screen, Transition},
        screens::{file_explorer::FileExplorerScreen, restore::RestoreScreen},
        theme,
        widgets::{ScrollState, ToastSink, impl_attach_toasts, wrap_line},
    },
    utils,
};

const DEFAULT_PAGE_SIZE: usize = 10;

pub struct SnapshotDetailScreen {
    repo: Arc<Repository>,
    snapshots: Arc<SnapshotEntryList>,
    current_index: usize,
    scroll: ScrollState,
    cached_lines: Vec<Line<'static>>,
    toasts: ToastSink,
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
            scroll: ScrollState {
                page_size: DEFAULT_PAGE_SIZE,
                ..ScrollState::default()
            },
            cached_lines: Vec::new(),
            toasts: ToastSink::default(),
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
        self.scroll.reset();
    }

    fn build_content_lines(&self) -> Vec<Line<'static>> {
        let s = self.snapshot();
        let entry = self.entry();
        let mut lines = Vec::with_capacity(20);
        let lw = 16;

        lines.push(theme::field_spans(
            "ID",
            lw,
            vec![Span::styled(entry.id.to_hex(), theme::THEME.snap_id)],
        ));

        let ts = utils::pretty_print_timestamp(&s.timestamp, None);
        let elapsed = Local::now() - s.timestamp;
        let ago = utils::pretty_print_duration_chrono(elapsed, 1);
        lines.push(theme::field_spans(
            "Date",
            lw,
            vec![
                Span::raw(ts),
                Span::raw("  ("),
                Span::styled(format!("{} ago", ago), theme::THEME.snap_date),
                Span::raw(")"),
            ],
        ));

        if let Some(ref parent) = s.parent {
            lines.push(theme::field_spans(
                "Parent",
                lw,
                vec![Span::styled(parent.to_short_hex(12), theme::THEME.snap_id)],
            ));
        }

        lines.push(theme::field(
            "Host",
            lw,
            s.hostname.as_deref().unwrap_or("(unknown)"),
        ));
        lines.push(theme::field(
            "User",
            lw,
            s.username.as_deref().unwrap_or("(unknown)"),
        ));

        if let Some(ref version) = s.version {
            lines.push(theme::field("Version", lw, version.to_string()));
        }

        lines.push(theme::field(
            "Root",
            lw,
            s.root.to_string_lossy().into_owned(),
        ));

        if let Some(ref desc) = s.description {
            lines.push(theme::field("Description", lw, desc.to_string()));
        }

        lines.push(theme::field(
            "Tags",
            lw,
            if s.tags.is_empty() {
                "(none)".to_string()
            } else {
                theme::format_tags(&s.tags)
            },
        ));

        lines.push(theme::field(
            "Active",
            lw,
            if entry.active { "yes" } else { "no" },
        ));

        let mut paths_iter = s.paths.iter().map(|p| {
            p.strip_prefix(&s.root)
                .unwrap_or(p.as_path())
                .to_string_lossy()
                .into_owned()
        });

        if let Some(first) = paths_iter.next() {
            lines.push(theme::field("Paths", lw, first));
        }
        for relative in paths_iter {
            lines.push(Line::from(vec![
                Span::raw(" ".repeat(lw)),
                Span::raw(relative),
            ]));
        }

        lines.push(Line::from(Span::styled(
            format!("{:lw$}", "Summary", lw = lw),
            theme::THEME.header,
        )));

        lines.push(Line::from(vec![
            Span::styled("  Size", theme::THEME.menu_key),
            Span::raw("  "),
            Span::raw(utils::format_size_binary(s.size(), 3)),
        ]));
        lines.push(Line::from(vec![
            Span::styled("  Items", theme::THEME.menu_key),
            Span::raw(" "),
            Span::raw(s.summary.processed_items_count.to_string()),
        ]));

        lines
    }

    fn render_content(&mut self, frame: &mut Frame, content_area: ratatui::layout::Rect) {
        let block = theme::block("Snapshot");
        let inner = block.inner(content_area);
        // Leave one column for the scrollbar on the right.
        let wrap_width = inner.width.saturating_sub(1).max(1) as usize;

        // Wrap long values (paths, descriptions) so they are readable instead
        // of being clipped at the right edge.
        let mut wrapped: Vec<Line<'static>> = Vec::new();
        for line in &self.cached_lines {
            wrap_line(line, wrap_width, &mut wrapped);
        }

        let content_height = inner.height as usize;
        let line_count = wrapped.len();
        self.scroll.max_offset = line_count.saturating_sub(content_height);
        self.scroll.page_size = content_height;
        self.scroll.offset = self.scroll.offset.min(self.scroll.max_offset);

        let paragraph = ratatui::widgets::Paragraph::new(Text::from(wrapped))
            .alignment(Alignment::Left)
            .block(block)
            .scroll((self.scroll.offset as u16, 0));

        frame.render_widget(paragraph, content_area);

        if self.scroll.max_offset > 0 {
            let mut scrollbar_state = ScrollbarState::new(self.scroll.max_offset + content_height)
                .position(self.scroll.offset)
                .viewport_content_length(content_height);

            frame.render_stateful_widget(
                theme::scrollbar(),
                content_area.inner(Margin::new(1, 1)),
                &mut scrollbar_state,
            );
        }
    }
}

#[async_trait]
impl Screen for SnapshotDetailScreen {
    async fn on_become_active(&mut self) -> Result<()> {
        self.cached_lines = self.build_content_lines();
        self.scroll.reset();
        Ok(())
    }

    fn render(&mut self, frame: &mut Frame) {
        let inner = frame.area().inner(theme::CONTENT_MARGIN);

        let footer = theme::key_hint_lines(&self.help_hints(), inner.width);
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(footer.len() as u16), Constraint::Min(3)])
            .split(inner);

        frame.render_widget(
            ratatui::widgets::Paragraph::new(ratatui::text::Text::from(footer)),
            chunks[0],
        );
        self.render_content(frame, chunks[1]);
    }

    async fn handle_key(&mut self, key: KeyEvent) -> Option<Transition> {
        if self.scroll.handle_key(key.code) {
            return None;
        }

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
                        self.toasts
                            .error(format!("Failed to load file explorer: {e}"));
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
            _ => None,
        }
    }

    fn help_hints(&self) -> Vec<(&'static str, &'static str)> {
        let mut hints = vec![("Esc", "back"), ("Enter", "explore"), ("r", "restore")];
        if self.current_index > 0 {
            hints.push(("<", "prev"));
        }
        if self.current_index < self.snapshots.len().saturating_sub(1) {
            hints.push((">", "next"));
        }
        hints.push(("\u{2191}\u{2193}", "scroll"));
        hints.push(("q", "back"));
        hints
    }

    impl_attach_toasts!(toasts);
}
