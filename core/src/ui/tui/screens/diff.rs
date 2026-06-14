use std::{path::PathBuf, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use crossterm::event::{KeyCode, KeyEvent};
use futures::StreamExt;
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Margin, Rect},
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::{List, ListItem, ListState, Paragraph},
};
use tokio::sync::mpsc;

use crate::{
    fs::tree::{NodeDiff, create_diff_stream},
    repository::{
        repo::Repository,
        snapshot::{DiffCounts, SnapshotEntry, SnapshotEntryList},
    },
    ui::tui::{
        app::{Screen, Transition},
        theme,
        widgets::{StateNavigation, TextInput, TextInputAction},
    },
    utils,
};

enum ChangedKind {
    Content,
    Metadata,
    Type,
}

struct DiffEntry {
    path: PathBuf,
    depth: usize,
    diff: NodeDiff,
    changed_kind: Option<ChangedKind>,
    is_dir: bool,
    has_changes: bool,
    expanded: bool,
}

const SPINNER_CHARS: &[char] = &['\u{25D0}', '\u{25D3}', '\u{25D1}', '\u{25D2}'];

struct DiffLoadResult {
    entries: Vec<DiffEntry>,
    counts: DiffCounts,
}

pub struct DiffScreen {
    repo: Arc<Repository>,
    snapshots: Arc<SnapshotEntryList>,
    source_idx: usize,
    target_idx: usize,
    entries: Vec<DiffEntry>,
    visible: Vec<usize>,
    list_state: ListState,
    counts: DiffCounts,
    loading: bool,
    error: Option<String>,
    show_all: bool,
    last_height: usize,
    spinner_tick: u8,
    filter: Option<TextInput>,
    filter_query: Option<String>,
    rx: mpsc::UnboundedReceiver<Result<DiffLoadResult>>,
}

impl DiffScreen {
    pub fn new(
        repo: Arc<Repository>,
        snapshots: Arc<SnapshotEntryList>,
        source_idx: usize,
        target_idx: usize,
    ) -> Self {
        let (_tx, rx) = mpsc::unbounded_channel();
        Self {
            repo,
            snapshots,
            source_idx,
            target_idx,
            entries: Vec::new(),
            visible: Vec::new(),
            list_state: ListState::default(),
            counts: DiffCounts::default(),
            loading: true,
            error: None,
            show_all: false,
            last_height: 0,
            spinner_tick: 0,
            filter: None,
            filter_query: None,
            rx,
        }
    }

    fn source(&self) -> &SnapshotEntry {
        &self.snapshots[self.source_idx]
    }

    fn target(&self) -> &SnapshotEntry {
        &self.snapshots[self.target_idx]
    }

    async fn diff_entries(
        repo: Arc<Repository>,
        snapshots: Arc<SnapshotEntryList>,
        source_idx: usize,
        target_idx: usize,
    ) -> Result<DiffLoadResult> {
        let src_snap = &snapshots[source_idx].snapshot;
        let tgt_snap = &snapshots[target_idx].snapshot;

        let mut diff_stream = create_diff_stream(repo.clone(), src_snap.tree, tgt_snap.tree)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create diff stream: {}", e))?;
        let mut entries: Vec<DiffEntry> = Vec::new();
        let mut counts = DiffCounts::default();

        while let Some(res) = diff_stream.next().await {
            match res {
                Ok((path, source, target, diff_type)) => {
                    let source_node = source.transpose().ok().flatten();
                    let target_node = target.transpose().ok().flatten();
                    let is_dir = source_node
                        .as_ref()
                        .or(target_node.as_ref())
                        .map(|sn| sn.node.is_dir())
                        .unwrap_or(false);
                    let changed_kind = if diff_type == NodeDiff::Changed {
                        match (source_node.as_ref(), target_node.as_ref()) {
                            (Some(s), Some(t)) => {
                                if s.node.node_type != t.node.node_type {
                                    Some(ChangedKind::Type)
                                } else if s.node.blobs != t.node.blobs {
                                    Some(ChangedKind::Content)
                                } else {
                                    Some(ChangedKind::Metadata)
                                }
                            }
                            _ => None,
                        }
                    } else {
                        None
                    };
                    counts.increment(is_dir, &diff_type);
                    entries.push(DiffEntry {
                        path,
                        depth: 0,
                        diff: diff_type,
                        changed_kind,
                        is_dir,
                        has_changes: diff_type != NodeDiff::Unchanged,
                        expanded: false,
                    });
                }
                Err(e) => {
                    tracing::warn!("Diff stream error: {}", e);
                }
            }
        }

        let depth_base = entries
            .iter()
            .map(|e| e.path.components().count())
            .min()
            .unwrap_or(0);

        for entry in &mut entries {
            entry.depth = entry.path.components().count().saturating_sub(depth_base);
        }

        Self::compute_has_changes(&mut entries);
        Self::expand_changed_dirs(&mut entries);

        Ok(DiffLoadResult { entries, counts })
    }

    fn compute_has_changes(entries: &mut [DiffEntry]) {
        for i in (0..entries.len()).rev() {
            if !entries[i].is_dir {
                continue;
            }
            let depth = entries[i].depth;
            let mut j = i + 1;
            while j < entries.len() && entries[j].depth > depth {
                if entries[j].has_changes {
                    entries[i].has_changes = true;
                    break;
                }
                j += 1;
            }
        }
    }

    fn expand_changed_dirs(entries: &mut [DiffEntry]) {
        for entry in entries.iter_mut() {
            if entry.is_dir && entry.has_changes {
                entry.expanded = true;
            }
        }
    }

    fn expand_filter_dirs(&mut self) {
        let filter_text = self
            .filter
            .as_ref()
            .map(|f| f.text())
            .or(self.filter_query.as_deref())
            .unwrap_or("");
        if filter_text.is_empty() {
            return;
        }
        let q = filter_text.to_lowercase();

        let mut show_entry = vec![false; self.entries.len()];
        for (i, entry) in self.entries.iter().enumerate() {
            if (self.show_all || entry.has_changes)
                && entry.path.to_string_lossy().to_lowercase().contains(&q)
            {
                show_entry[i] = true;
            }
        }
        for i in (0..self.entries.len()).rev() {
            if show_entry[i] {
                let depth = self.entries[i].depth;
                for j in (0..i).rev() {
                    if self.entries[j].depth < depth {
                        show_entry[j] = true;
                        break;
                    }
                }
            }
        }
        for i in 0..self.entries.len() {
            if self.entries[i].is_dir && !self.entries[i].expanded {
                let depth = self.entries[i].depth;
                let has_match = (i + 1..self.entries.len())
                    .take_while(|&j| self.entries[j].depth > depth)
                    .any(|j| show_entry[j]);
                if has_match {
                    self.entries[i].expanded = true;
                }
            }
        }
    }

    fn build_visible(&mut self) {
        let filter_text = self
            .filter
            .as_ref()
            .map(|f| f.text())
            .or(self.filter_query.as_deref())
            .unwrap_or("");
        let query = if filter_text.is_empty() {
            None
        } else {
            Some(filter_text.to_lowercase())
        };

        self.visible.clear();

        if let Some(q) = query {
            let mut show_entry = vec![false; self.entries.len()];

            for (i, entry) in self.entries.iter().enumerate() {
                if (self.show_all || entry.has_changes)
                    && entry.path.to_string_lossy().to_lowercase().contains(&q)
                {
                    show_entry[i] = true;
                }
            }

            for i in (0..self.entries.len()).rev() {
                if show_entry[i] {
                    let depth = self.entries[i].depth;
                    for j in (0..i).rev() {
                        if self.entries[j].depth < depth {
                            show_entry[j] = true;
                            break;
                        }
                    }
                }
            }

            let mut i = 0;
            while i < self.entries.len() {
                if show_entry[i] {
                    self.visible.push(i);
                }

                if self.entries[i].is_dir && !self.entries[i].expanded {
                    let depth = self.entries[i].depth;
                    i += 1;
                    while i < self.entries.len() && self.entries[i].depth > depth {
                        i += 1;
                    }
                } else {
                    i += 1;
                }
            }
        } else {
            let mut i = 0;
            while i < self.entries.len() {
                let entry = &self.entries[i];

                let show = self.show_all || entry.has_changes;

                if show {
                    self.visible.push(i);
                }

                if entry.is_dir && !entry.expanded {
                    let depth = entry.depth;
                    i += 1;
                    while i < self.entries.len() && self.entries[i].depth > depth {
                        i += 1;
                    }
                } else {
                    i += 1;
                }
            }
        }

        if self.visible.is_empty() {
            self.list_state.select(None);
        } else {
            let current = self.list_state.selected().unwrap_or(0);
            if current >= self.visible.len() {
                self.list_state.select(Some(self.visible.len() - 1));
            }
        }
    }

    fn navigate_snapshot(&mut self, direction: i32) {
        let new_src = if direction < 0 {
            if self.source_idx == 0 {
                return;
            }
            self.source_idx - 1
        } else {
            if self.source_idx >= self.snapshots.len().saturating_sub(1) {
                return;
            }
            self.source_idx + 1
        };

        if new_src == self.target_idx {
            return;
        }

        self.source_idx = new_src;
        self.target_idx = if self.source_idx < self.snapshots.len().saturating_sub(1) {
            self.source_idx + 1
        } else if self.source_idx > 0 {
            self.source_idx - 1
        } else {
            return;
        };

        self.loading = true;
        self.entries.clear();
        self.visible.clear();
        self.list_state.select(None);
        self.error = None;
        self.spinner_tick = 0;

        let (tx, rx) = mpsc::unbounded_channel();
        self.rx = rx;

        let repo = self.repo.clone();
        let snapshots = self.snapshots.clone();
        let source_idx = self.source_idx;
        let target_idx = self.target_idx;

        tokio::spawn(async move {
            let result = Self::diff_entries(repo, snapshots, source_idx, target_idx).await;
            let _ = tx.send(result);
        });
    }

    fn selected_entry(&self) -> Option<&DiffEntry> {
        let vis_idx = self.list_state.selected()?;
        let entry_idx = self.visible.get(vis_idx)?;
        self.entries.get(*entry_idx)
    }

    fn toggle_current(&mut self) {
        let Some(entry) = self.selected_entry() else {
            return;
        };
        if !entry.is_dir {
            return;
        }
        let entry_idx = self.visible[self.list_state.selected().unwrap()];
        self.entries[entry_idx].expanded = !self.entries[entry_idx].expanded;
        self.build_visible();
    }

    fn render_header(&self, frame: &mut Frame, area: Rect) {
        let src = self.source();
        let tgt = self.target();

        let lw = 4;
        let src_size = utils::format_size_binary(src.snapshot.size(), 3);
        let tgt_size = utils::format_size_binary(tgt.snapshot.size(), 3);
        let lines = vec![
            Line::from(vec![
                Span::styled(format!("{:lw$}", "From", lw = lw), theme::THEME.menu_key),
                Span::raw("  "),
                Span::styled(src.id.to_short_hex(12), theme::THEME.snap_id),
                Span::styled(" @ ", Style::default().fg(theme::THEME.subtext)),
                Span::styled(
                    src.snapshot.hostname.as_deref().unwrap_or("?"),
                    theme::THEME.snap_host,
                ),
                Span::raw("  "),
                Span::styled(
                    utils::pretty_print_timestamp(&src.snapshot.timestamp, None),
                    theme::THEME.snap_date,
                ),
                Span::raw("  "),
                Span::styled(src_size, theme::THEME.file_size),
            ]),
            Line::from(vec![
                Span::styled(format!("{:lw$}", "To", lw = lw), theme::THEME.menu_key),
                Span::raw("  "),
                Span::styled(tgt.id.to_short_hex(12), theme::THEME.snap_id),
                Span::styled(" @ ", Style::default().fg(theme::THEME.subtext)),
                Span::styled(
                    tgt.snapshot.hostname.as_deref().unwrap_or("?"),
                    theme::THEME.snap_host,
                ),
                Span::raw("  "),
                Span::styled(
                    utils::pretty_print_timestamp(&tgt.snapshot.timestamp, None),
                    theme::THEME.snap_date,
                ),
                Span::raw("  "),
                Span::styled(tgt_size, theme::THEME.file_size),
            ]),
        ];

        let widget = Paragraph::new(lines).style(Style::new().bg(theme::THEME.surface));
        frame.render_widget(widget, area);
    }

    fn render_summary(&self, frame: &mut Frame, area: Rect) {
        let c = &self.counts;
        let total = c.new_files
            + c.new_dirs
            + c.deleted_files
            + c.deleted_dirs
            + c.changed_files
            + c.changed_dirs;
        let line = Line::from(vec![
            Span::raw("  "),
            Span::styled(
                format!("+{}  ", c.new_files + c.new_dirs),
                theme::THEME.success,
            ),
            Span::styled(
                format!("~{}  ", c.changed_files + c.changed_dirs),
                theme::THEME.warning,
            ),
            Span::styled(
                format!("-{}  ", c.deleted_files + c.deleted_dirs),
                theme::THEME.error,
            ),
            Span::styled(
                format!("{} total", total),
                Style::default().fg(theme::THEME.subtext),
            ),
            Span::raw("  "),
            Span::styled(
                format!("{} visible", self.visible.len()),
                Style::default().fg(theme::THEME.subtext_dim),
            ),
        ]);
        let widget = Paragraph::new(line).style(Style::new().bg(theme::THEME.surface));
        frame.render_widget(widget, area);
    }

    fn render_filter(&self, frame: &mut Frame, area: Rect) {
        let Some(input) = &self.filter else { return };
        let filter_text = if input.is_empty() {
            Line::from(Span::styled(" Filter by path... ", theme::THEME.footer))
        } else {
            let text = input.text();
            let cursor = input.cursor();
            let before: String = text.chars().take(cursor).collect();
            let after: String = text.chars().skip(cursor).collect();
            let mut spans = vec![Span::raw(" ")];
            spans.push(Span::raw(before));
            if after.is_empty() {
                spans.push(Span::styled(" ", Style::default().underlined()));
            } else {
                let cursor_char: String = after.chars().take(1).collect();
                let rest: String = after.chars().skip(1).collect();
                spans.push(Span::styled(cursor_char, Style::default().underlined()));
                spans.push(Span::raw(rest));
            }
            Line::from(spans)
        };
        let filter_widget = Paragraph::new(filter_text).block(theme::block("Filter"));
        frame.render_widget(filter_widget, area);
    }

    fn render_tree(&mut self, frame: &mut Frame, area: Rect) {
        if self.entries.is_empty() {
            let msg = if self.show_all {
                "No differences found between snapshots."
            } else {
                "No changes found. Press 'u' to show all entries."
            };
            let widget = Paragraph::new(Line::from(Span::styled(msg, theme::THEME.subtext)))
                .block(theme::block("Changes"));
            frame.render_widget(widget, area);
            return;
        }

        let max_rows = area.height.saturating_sub(2) as usize;
        let display_len = self.visible.len();

        let items: Vec<ListItem<'_>> = self
            .visible
            .iter()
            .map(|&entry_idx| {
                let entry = &self.entries[entry_idx];
                let indent = "  ".repeat(entry.depth);

                let expand = if entry.is_dir {
                    if entry.expanded {
                        " \u{25BC} "
                    } else {
                        " \u{25B6} "
                    }
                } else {
                    "    "
                };

                let (diff_sym, diff_style) = match entry.diff {
                    NodeDiff::New => ("+", theme::THEME.success),
                    NodeDiff::Deleted => ("-", theme::THEME.error),
                    NodeDiff::Changed => match entry.changed_kind {
                        Some(ChangedKind::Type | ChangedKind::Metadata) => (
                            "~",
                            Style::default()
                                .fg(theme::THEME.peach)
                                .add_modifier(Modifier::BOLD),
                        ),
                        _ => ("~", theme::THEME.warning),
                    },
                    NodeDiff::Unchanged => (" ", Style::default().fg(theme::THEME.subtext_dim)),
                };

                let name_style = if entry.is_dir {
                    Style::default()
                        .fg(theme::THEME.dir_fg)
                        .add_modifier(Modifier::BOLD)
                } else {
                    match entry.diff {
                        NodeDiff::New => Style::default().fg(theme::THEME.green),
                        NodeDiff::Deleted => Style::default().fg(theme::THEME.red),
                        NodeDiff::Changed => match entry.changed_kind {
                            Some(ChangedKind::Type | ChangedKind::Metadata) => {
                                Style::default().fg(theme::THEME.peach)
                            }
                            _ => Style::default().fg(theme::THEME.yellow),
                        },
                        NodeDiff::Unchanged => Style::default().fg(theme::THEME.subtext_dim),
                    }
                };

                ListItem::new(Line::from(vec![
                    Span::raw(indent),
                    Span::styled(expand, theme::THEME.subtext_dim),
                    Span::styled(diff_sym, diff_style),
                    Span::raw(" "),
                    Span::styled(entry.path.display().to_string(), name_style),
                ]))
            })
            .collect();

        let title = format!(" Changes ({}) ", display_len);

        let list = List::new(items)
            .block(theme::block(&title))
            .highlight_style(theme::THEME.selection)
            .highlight_symbol("  ");

        frame.render_stateful_widget(list, area, &mut self.list_state);

        if display_len > max_rows {
            theme::render_scrollbar(
                frame,
                area,
                display_len,
                self.list_state.selected().unwrap_or(0),
            );
        }
    }

    fn render_loading(&self, frame: &mut Frame, area: Rect) {
        let spinner = SPINNER_CHARS[(self.spinner_tick as usize) % SPINNER_CHARS.len()];
        let msg = Paragraph::new(Line::from(Span::styled(
            format!(" {} Computing differences... ", spinner),
            theme::THEME.info,
        )))
        .block(theme::block("Changes"));
        frame.render_widget(msg, area);
    }

    fn render_error(&self, frame: &mut Frame, area: Rect) {
        let err_text = self
            .error
            .clone()
            .unwrap_or_else(|| "Unknown error".to_string());
        let msg = Paragraph::new(Line::from(Span::styled(err_text, theme::THEME.error)))
            .block(theme::block("Error"));
        frame.render_widget(msg, area);
    }

    fn render_hints(&self, frame: &mut Frame, area: Rect) {
        let hints = if self.loading {
            theme::key_hint_footer(&[("Esc", "back"), ("q", "quit")])
        } else {
            let mut hints = vec![
                ("Esc", "back"),
                ("\u{2191}\u{2193}", "navigate"),
                ("Enter", "toggle dir"),
                (
                    "u",
                    if self.show_all {
                        "hide unchanged"
                    } else {
                        "show all"
                    },
                ),
                ("/", "filter"),
            ];
            if self.source_idx > 0 {
                hints.push(("<", "prev"));
            }
            if self.source_idx < self.snapshots.len().saturating_sub(1) {
                hints.push((">", "next"));
            }
            hints.push(("q", "quit"));
            theme::key_hint_footer(&hints)
        };
        frame.render_widget(Paragraph::new(hints), area);
    }

    fn handle_filter_key(&mut self, key: KeyCode) {
        let Some(input) = &mut self.filter else {
            return;
        };

        match input.handle_key(key) {
            TextInputAction::Cancel => {
                self.filter = None;
                self.filter_query = None;
                self.build_visible();
            }
            TextInputAction::Confirm => {
                self.filter_query = Some(input.text().to_string());
                self.filter = None;
                self.expand_filter_dirs();
                self.build_visible();
            }
            TextInputAction::Edited => {
                self.expand_filter_dirs();
                self.build_visible();
            }
            TextInputAction::None => {}
        }
    }
}

#[async_trait]
impl Screen for DiffScreen {
    async fn on_become_active(&mut self) -> Result<()> {
        self.repo.reload_master_index().await?;

        let (tx, rx) = mpsc::unbounded_channel();
        self.rx = rx;

        let repo = self.repo.clone();
        let snapshots = self.snapshots.clone();
        let source_idx = self.source_idx;
        let target_idx = self.target_idx;

        tokio::spawn(async move {
            let result = Self::diff_entries(repo, snapshots, source_idx, target_idx).await;
            let _ = tx.send(result);
        });

        Ok(())
    }

    fn render(&mut self, frame: &mut Frame) {
        self.spinner_tick = self.spinner_tick.wrapping_add(1);

        while let Ok(result) = self.rx.try_recv() {
            match result {
                Ok(loaded) => {
                    self.entries = loaded.entries;
                    self.counts = loaded.counts;
                    self.build_visible();
                    if !self.visible.is_empty() {
                        self.list_state.select(Some(0));
                    }
                    self.loading = false;
                }
                Err(e) => {
                    self.error = Some(format!("{:?}", e));
                    self.loading = false;
                }
            }
        }

        let area = frame.area();
        let inner = area.inner(Margin::new(2, 1));

        let detail_h = if self.loading || self.error.is_some() {
            0
        } else {
            1
        };
        let filter_h = if self.filter.is_some() { 3 } else { 0 };

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1),
                Constraint::Length(2),
                Constraint::Length(detail_h),
                Constraint::Min(3),
                Constraint::Length(filter_h),
                Constraint::Length(1),
            ])
            .split(inner);

        self.last_height = chunks[3].height.saturating_sub(2) as usize;

        frame.render_widget(
            Paragraph::new(Line::from(Span::styled("Diff", theme::THEME.menu_key)))
                .style(Style::new().bg(theme::THEME.surface)),
            chunks[0],
        );

        self.render_header(frame, chunks[1]);

        if self.loading {
            self.render_loading(frame, chunks[3]);
        } else if self.error.is_some() {
            self.render_error(frame, chunks[3]);
        } else {
            self.render_summary(frame, chunks[2]);
            self.render_tree(frame, chunks[3]);
        }

        if self.filter.is_some() {
            self.render_filter(frame, chunks[4]);
        }
        self.render_hints(frame, chunks[5]);
    }

    async fn handle_key(&mut self, key: KeyEvent) -> Option<Transition> {
        if self.loading {
            return None;
        }

        if self.filter.is_some() {
            self.handle_filter_key(key.code);
            return None;
        }

        match key.code {
            KeyCode::Esc => {
                if self.filter_query.is_some() {
                    self.filter_query = None;
                    self.build_visible();
                    None
                } else {
                    Some(Transition::Pop)
                }
            }
            KeyCode::Char('q') => Some(Transition::Quit),
            KeyCode::Enter | KeyCode::Right => {
                self.toggle_current();
                None
            }
            KeyCode::Left | KeyCode::Backspace => {
                let entry = self.selected_entry()?;
                if entry.is_dir && entry.expanded {
                    let entry_idx = self.visible[self.list_state.selected().unwrap()];
                    self.entries[entry_idx].expanded = false;
                    self.build_visible();
                }
                None
            }
            KeyCode::Char('u') => {
                self.show_all = !self.show_all;
                self.build_visible();
                None
            }
            KeyCode::Char('/') => {
                let text = self.filter_query.take().unwrap_or_default();
                self.filter = Some(TextInput::with_text(text));
                None
            }
            KeyCode::Char(',') | KeyCode::Char('<') => {
                self.navigate_snapshot(-1);
                None
            }
            KeyCode::Char('.') | KeyCode::Char('>') => {
                self.navigate_snapshot(1);
                None
            }
            KeyCode::Down => {
                self.list_state.next(self.visible.len());
                None
            }
            KeyCode::Up => {
                self.list_state.previous(self.visible.len());
                None
            }
            KeyCode::PageDown => {
                self.list_state
                    .page_next(self.visible.len(), self.last_height);
                None
            }
            KeyCode::PageUp => {
                self.list_state
                    .page_previous(self.visible.len(), self.last_height);
                None
            }
            KeyCode::Home => {
                self.list_state.home(self.visible.len());
                None
            }
            KeyCode::End => {
                self.list_state.end(self.visible.len());
                None
            }
            _ => None,
        }
    }
}
