use std::sync::Arc;

use async_trait::async_trait;
use chrono::Local;
use crossterm::event::{KeyCode, KeyEvent};
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Margin, Rect},
    style::Style,
    text::{Line, Span},
    widgets::{Block, Paragraph, Row, Table, TableState},
};

use crate::{
    commands::{cmd_forget::CmdArgs as ForgetCmdArgs, cmd_snapshot::CmdArgs as SnapshotCmdArgs},
    common::{defaults::SHORT_SNAPSHOT_ID_LEN, error::Result, global::THIS_MAPACHE_VERSION},
    repository::{
        lock::LockHandle,
        repo::Repository,
        snapshot::{SnapshotEntry, SnapshotEntryList, SnapshotStream},
    },
    ui::tui::{
        app::{Screen, Transition},
        screens::{
            diff::DiffScreen, find::FindScreen, forget::ForgetScreen, restore::RestoreScreen,
            snapshot::SnapshotCreateScreen, snapshot_detail::SnapshotDetailScreen,
        },
        theme,
        widgets::{FilterAction, FilterState, StateNavigation},
    },
    utils,
};

const FILTER_INPUT_HEIGHT: u16 = 3;
const HEADER_HEIGHT: u16 = 2;
const MENU_HEIGHT: u16 = 3;

const MENU_ITEMS: &[(char, &str)] = &[
    ('1', "Snapshot"),
    ('2', "Restore"),
    ('3', "Forget"),
    ('4', "Find"),
    ('5', "Diff"),
    ('r', "Refresh"),
    ('q', "Quit"),
];

struct DashboardStats {
    total: usize,
    newest: Option<String>,
}

pub struct DashboardScreen {
    repo: Arc<Repository>,
    lock_handle: Option<LockHandle>,
    repo_path: String,
    repo_id: String,
    snapshot_config: Option<SnapshotCmdArgs>,
    forget_config: Option<ForgetCmdArgs>,
    snapshots: Arc<SnapshotEntryList>,
    filtered_indices: Vec<usize>,
    search_cache: Vec<String>,
    table_state: TableState,
    filter: FilterState,
    last_height: u16,
    stats: DashboardStats,
    diff_source: Option<usize>,
    needs_reload: bool,
}

impl DashboardScreen {
    pub fn new(
        repo: Arc<Repository>,
        lock_handle: Option<LockHandle>,
        repo_path: String,
        repo_id: String,
        snapshot_config: Option<SnapshotCmdArgs>,
        forget_config: Option<ForgetCmdArgs>,
    ) -> Self {
        Self {
            repo,
            lock_handle,
            repo_path,
            repo_id,
            snapshot_config,
            forget_config,
            snapshots: Arc::new(Vec::new()),
            filtered_indices: Vec::new(),
            search_cache: Vec::new(),
            table_state: TableState::default(),
            filter: FilterState::new(),
            last_height: 0,
            stats: DashboardStats {
                total: 0,
                newest: None,
            },
            diff_source: None,
            needs_reload: true,
        }
    }

    pub async fn load_snapshots(&mut self) -> Result<()> {
        // Reload master index first to ensure we see any new data from other processes
        self.repo.reload_master_index().await?;

        // Remember which snapshot was selected so we can restore it after reload
        let selected_id = self
            .table_state
            .selected()
            .and_then(|display_idx| self.display_entry(display_idx).map(|e| e.id));

        let (active_stream, dropped_stream) = futures::try_join!(
            SnapshotStream::new(self.repo.clone()),
            SnapshotStream::dropped(self.repo.clone())
        )?;

        let (active_entries, mut dropped_entries) = futures::try_join!(
            active_stream.collect_entries(true),
            dropped_stream.collect_entries(false)
        )?;

        let mut entries = active_entries;
        entries.append(&mut dropped_entries);

        entries.sort_unstable_by_key(|e| std::cmp::Reverse(e.snapshot.timestamp));
        self.stats = Self::compute_stats(&entries);
        self.snapshots = Arc::new(entries);
        self.update_search_cache();
        self.apply_filter();
        // Restore the previous selection if the snapshot still exists
        if let Some(id) = selected_id
            && let Some(idx) = self.snapshots.iter().position(|e| e.id == id)
            && let Some(display_idx) = self.filtered_indices.iter().position(|&i| i == idx)
        {
            self.table_state.select(Some(display_idx));
        }
        self.diff_source = None;
        self.needs_reload = false;
        Ok(())
    }

    fn compute_stats(entries: &[SnapshotEntry]) -> DashboardStats {
        let newest = entries.first().map(|e| {
            let elapsed = Local::now() - e.snapshot.timestamp;
            format!("{} ago", utils::pretty_print_duration_chrono(elapsed, 1))
        });
        DashboardStats {
            total: entries.len(),
            newest,
        }
    }

    fn update_search_cache(&mut self) {
        self.search_cache = self
            .snapshots
            .iter()
            .map(|e| {
                let mut buf = String::new();
                if let Some(host) = &e.snapshot.hostname {
                    buf.push_str(&host.to_lowercase());
                }
                buf.push(' ');
                for tag in &e.snapshot.tags {
                    buf.push_str(tag);
                    buf.push(' ');
                }
                buf.push_str(&e.snapshot.root.to_string_lossy().to_lowercase());
                buf.push(' ');
                buf.push_str(&e.id.to_hex());
                buf
            })
            .collect();
    }

    fn apply_filter(&mut self) {
        if let Some(query) = self.filter.query() {
            let q = query.to_lowercase();
            if q.is_empty() {
                self.filtered_indices = (0..self.snapshots.len()).collect();
            } else {
                self.filtered_indices = self
                    .search_cache
                    .iter()
                    .enumerate()
                    .filter(|(_, s)| s.contains(&q))
                    .map(|(i, _)| i)
                    .collect();
            }
        } else {
            self.filtered_indices = (0..self.snapshots.len()).collect();
        }

        if !self.filtered_indices.is_empty() {
            self.table_state.select(Some(0));
        } else {
            self.table_state.select(None);
        }
    }

    fn display_len(&self) -> usize {
        self.filtered_indices.len()
    }

    fn display_entry(&self, display_idx: usize) -> Option<&SnapshotEntry> {
        let orig_idx = self.filtered_indices.get(display_idx)?;
        self.snapshots.get(*orig_idx)
    }

    fn handle_filter_key(&mut self, key: KeyCode) {
        match self.filter.handle_key(key) {
            FilterAction::Cancel | FilterAction::Apply => {
                self.apply_filter();
            }
            FilterAction::None => {}
        }
    }

    fn render_top_bar(&self, frame: &mut Frame, area: Rect) {
        let bg = Block::default().style(Style::new().bg(theme::THEME.surface));
        frame.render_widget(&bg, area);

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(1), Constraint::Length(1)])
            .split(area);

        let row1 = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Length(22), Constraint::Min(10)])
            .split(chunks[0]);

        let header = Paragraph::new(Line::from(vec![
            Span::styled("mapache ", theme::THEME.header),
            Span::styled(THIS_MAPACHE_VERSION, theme::THEME.snap_size),
        ]))
        .style(theme::THEME.footer);
        frame.render_widget(header, row1[0]);

        let info = Paragraph::new(Line::from(vec![
            Span::styled(&self.repo_path, theme::THEME.snap_host),
            Span::styled(" [", theme::THEME.footer),
            Span::styled(
                self.repo_id.chars().take(8).collect::<String>(),
                theme::THEME.snap_id,
            ),
            Span::styled("]", theme::THEME.footer),
            Span::styled(format!(" v{}", self.repo.repo_version()), theme::THEME.teal),
        ]))
        .style(theme::THEME.footer);
        frame.render_widget(info, row1[1]);

        let row2 = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Length(22), Constraint::Min(10)])
            .split(chunks[1]);

        let stats_left = Paragraph::new(Line::from(vec![
            Span::styled(self.stats.total.to_string(), theme::THEME.snap_id),
            Span::styled(" snapshots", theme::THEME.footer),
        ]))
        .style(theme::THEME.footer);
        frame.render_widget(stats_left, row2[0]);

        let stats_right = Paragraph::new(Line::from(vec![
            Span::styled("last ", theme::THEME.footer),
            Span::styled(
                self.stats.newest.clone().unwrap_or_else(|| "-".into()),
                theme::THEME.teal,
            ),
        ]))
        .style(theme::THEME.footer);
        frame.render_widget(stats_right, row2[1]);
    }

    fn render_snapshot_list(&self, frame: &mut Frame, area: Rect) {
        let max_rows = area.height.saturating_sub(2) as usize;
        let display_len = self.display_len();

        let title = if self.filter.is_active() {
            format!(" Snapshots ({}/{}) ", display_len, self.snapshots.len())
        } else {
            format!(" Snapshots ({}) ", self.snapshots.len())
        };

        let rows: Vec<Row<'_>> = self
            .filtered_indices
            .iter()
            .map(|&orig_idx| {
                let entry = &self.snapshots[orig_idx];
                let is_source = self.diff_source == Some(orig_idx);
                let active_sym = if is_source {
                    "\u{25c8}"
                } else if entry.active {
                    "\u{25cf}"
                } else {
                    "\u{25cb}"
                };
                let active_style = if is_source {
                    theme::THEME.warning
                } else if entry.active {
                    theme::THEME.snap_active
                } else {
                    theme::THEME.snap_inactive
                };
                let id_str = entry.id.to_short_hex(SHORT_SNAPSHOT_ID_LEN);
                let date = utils::pretty_print_timestamp(&entry.snapshot.timestamp, None);
                let host = entry.snapshot.hostname.as_deref().unwrap_or_default();
                let size = utils::format_size_binary(entry.snapshot.size(), 3);
                let tags = theme::format_tags(&entry.snapshot.tags);

                Row::new(vec![
                    Span::styled(active_sym, active_style),
                    Span::styled(id_str, theme::THEME.snap_id),
                    Span::styled(date, theme::THEME.snap_date),
                    Span::styled(host, theme::THEME.snap_host),
                    Span::styled(format!("{:>13}", size), theme::THEME.snap_size),
                    Span::raw(tags),
                ])
            })
            .collect();

        let header_row = Row::new(vec![
            Span::styled(" ", theme::THEME.menu_key),
            Span::styled("ID", theme::THEME.menu_key),
            Span::styled("Date", theme::THEME.menu_key),
            Span::styled("Host", theme::THEME.menu_key),
            Span::styled(format!("{:>13}", "Size"), theme::THEME.menu_key),
            Span::styled("Tags", theme::THEME.menu_key),
        ]);

        let widths = [
            Constraint::Length(2),
            Constraint::Length(12),
            Constraint::Length(28),
            Constraint::Length(12),
            Constraint::Length(13),
            Constraint::Min(10),
        ];

        let table = Table::new(rows, widths)
            .header(header_row)
            .block(theme::block(&title))
            .row_highlight_style(theme::THEME.selection)
            .highlight_symbol("  ");

        let mut state = self.table_state;
        let selected = self.table_state.selected().unwrap_or(0);
        let max_offset = display_len.saturating_sub(max_rows);
        let offset = selected.saturating_sub(max_rows / 2).min(max_offset);
        *state.offset_mut() = offset;

        frame.render_stateful_widget(table, area, &mut state);

        if display_len > max_rows {
            let mut s = ratatui::widgets::ScrollbarState::new(display_len)
                .position(selected)
                .viewport_content_length(max_rows);
            frame.render_stateful_widget(theme::scrollbar(), area.inner(Margin::new(1, 1)), &mut s);
        }
    }

    fn render_selected_info(&self, frame: &mut Frame, area: Rect) {
        let Some(entry) = self.display_entry(self.table_state.selected().unwrap_or(0)) else {
            return;
        };

        let max_w = area.width.saturating_sub(6) as usize;
        let mut lines = vec![];

        let label_w = 6;

        lines.push(Line::from(vec![
            Span::styled(format!("{:w$}", "ID", w = label_w), theme::THEME.menu_key),
            Span::styled(entry.id.to_hex(), theme::THEME.snap_id),
        ]));

        lines.push(Line::from(vec![
            Span::styled(format!("{:w$}", "Date", w = label_w), theme::THEME.menu_key),
            Span::styled(
                utils::pretty_print_timestamp(&entry.snapshot.timestamp, None),
                theme::THEME.snap_date,
            ),
        ]));

        lines.push(Line::from(vec![
            Span::styled(format!("{:w$}", "Path", w = label_w), theme::THEME.menu_key),
            Span::styled(
                entry.snapshot.root.display().to_string(),
                theme::THEME.footer,
            ),
        ]));

        for (i, p) in entry.snapshot.paths.iter().enumerate() {
            let p_str = p.display().to_string();
            let indent = " ".repeat(label_w);
            if p_str.len() > max_w {
                let truncated: String = p_str.chars().take(max_w.saturating_sub(3)).collect();
                lines.push(Line::from(vec![
                    Span::raw(format!("{}{}", indent, truncated)),
                    Span::raw("\u{2026}"),
                ]));
            } else {
                lines.push(Line::from(vec![Span::raw(format!("{}{}", indent, p_str))]));
            }
            if i >= 4 {
                let remaining = entry.snapshot.paths.len() - i - 1;
                if remaining > 0 {
                    lines.push(Line::from(vec![
                        Span::raw(indent.to_string()),
                        Span::styled(
                            format!("\u{2026} and {} more", remaining),
                            theme::THEME.footer,
                        ),
                    ]));
                }
                break;
            }
        }

        if let Some(desc) = &entry.snapshot.description {
            let desc_trunc: String = desc.chars().take(max_w).collect();
            lines.push(Line::from(vec![
                Span::styled(format!("{:w$}", "Desc", w = label_w), theme::THEME.menu_key),
                Span::raw(desc_trunc),
            ]));
        }

        if !entry.snapshot.tags.is_empty() {
            lines.push(Line::from(vec![
                Span::styled(format!("{:w$}", "Tags", w = label_w), theme::THEME.menu_key),
                Span::raw(theme::format_tags(&entry.snapshot.tags)),
            ]));
        }

        let info = Paragraph::new(lines)
            .block(theme::block("Details"))
            .style(Style::new().bg(theme::THEME.surface));
        frame.render_widget(info, area);
    }

    fn selected_info_height(&self) -> u16 {
        let entry = self.display_entry(self.table_state.selected().unwrap_or(0));
        let Some(entry) = entry else { return 0 };
        let mut h = 3u16;
        if !entry.snapshot.paths.is_empty() {
            h += (entry.snapshot.paths.len().min(5) + 1) as u16;
        }
        if entry.snapshot.description.is_some() {
            h += 1;
        }
        if !entry.snapshot.tags.is_empty() {
            h += 1;
        }
        h + 2
    }

    fn render_diff_status(&self, frame: &mut Frame, area: Rect) {
        let Some(orig_idx) = self.diff_source else {
            return;
        };
        let Some(source) = self.snapshots.get(orig_idx) else {
            return;
        };
        let id_str = source.id.to_short_hex(SHORT_SNAPSHOT_ID_LEN);
        let date = utils::pretty_print_timestamp(&source.snapshot.timestamp, None);
        let host = source.snapshot.hostname.as_deref().unwrap_or_default();
        let text = Line::from(vec![
            Span::styled(" Diff: ", theme::THEME.warning),
            Span::styled(id_str, theme::THEME.snap_id),
            Span::raw(" "),
            Span::styled(date, theme::THEME.snap_date),
            Span::raw(" "),
            Span::styled(host, theme::THEME.snap_host),
            Span::raw(" -- select target and press "),
            Span::styled("5", theme::THEME.menu_key),
            Span::raw(" to diff"),
        ]);
        let block = Block::default().style(Style::new().bg(theme::THEME.surface));
        frame.render_widget(&block, area);
        let inner = area.inner(Margin::new(1, 0));
        frame.render_widget(Paragraph::new(text).style(theme::THEME.footer), inner);
    }

    fn render_filter(&self, frame: &mut Frame, area: Rect) {
        self.filter
            .render(frame, area, "Filter by host, tag, path, or ID");
    }

    fn render_menu(&self, frame: &mut Frame, area: Rect) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(1), Constraint::Length(2)])
            .split(area);

        let menu_text = self.build_menu_text();
        let menu = Paragraph::new(menu_text)
            .style(theme::THEME.footer)
            .alignment(ratatui::layout::Alignment::Left);
        frame.render_widget(menu, chunks[0]);

        let hint_str = if self.diff_source.is_some() {
            Line::from(vec![
                Span::styled("Esc cancel  ", theme::THEME.footer),
                Span::styled("\u{2191}\u{2193} select target  ", theme::THEME.footer),
                Span::styled("5 ", theme::THEME.menu_key),
                Span::styled("diff", theme::THEME.footer),
            ])
        } else {
            Line::from(vec![
                Span::styled("\u{2191}\u{2193} navigate  ", theme::THEME.footer),
                Span::styled("Enter", theme::THEME.menu_key),
                Span::styled(" details  ", theme::THEME.footer),
                Span::styled("/", theme::THEME.menu_key),
                Span::styled(" filter", theme::THEME.footer),
            ])
        };
        let hints = Paragraph::new(hint_str)
            .style(theme::THEME.footer)
            .alignment(ratatui::layout::Alignment::Left);
        frame.render_widget(hints, chunks[1]);
    }

    fn selected_original_index(&self) -> Option<usize> {
        let display_idx = self.table_state.selected()?;
        self.filtered_indices.get(display_idx).copied()
    }

    fn build_menu_text(&self) -> Line<'static> {
        let mut spans = Vec::new();
        for (i, (key, label)) in MENU_ITEMS.iter().enumerate() {
            if i > 0 {
                spans.push(Span::raw("  "));
            }
            spans.extend(theme::key_hint(&key.to_string(), label));
        }

        if let Some(entry) = self.display_entry(self.table_state.selected().unwrap_or(0))
            && !entry.active
        {
            spans.push(Span::raw("  "));
            spans.extend(theme::key_hint("u", "Recall"));
        }

        Line::from(spans)
    }
}

#[async_trait]
impl Screen for DashboardScreen {
    fn render(&mut self, frame: &mut Frame) {
        let area = frame.area();
        let inner_content = area.inner(theme::CONTENT_MARGIN);

        let diff_status_height: u16 = if self.diff_source.is_some() { 2 } else { 1 };

        let has_filter = self.filter.is_active();
        let filter_height = if has_filter { FILTER_INPUT_HEIGHT } else { 0 };
        let info_height = self.selected_info_height();

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(HEADER_HEIGHT),
                Constraint::Length(diff_status_height),
                Constraint::Min(5),
                Constraint::Length(info_height),
                Constraint::Length(filter_height),
                Constraint::Length(MENU_HEIGHT),
            ])
            .split(inner_content);

        self.render_top_bar(frame, chunks[0]);
        if self.diff_source.is_some() {
            self.render_diff_status(frame, chunks[1]);
        }
        self.last_height = chunks[2].height.saturating_sub(2);
        self.render_snapshot_list(frame, chunks[2]);
        if info_height > 0 {
            self.render_selected_info(frame, chunks[3]);
        }
        if has_filter {
            self.render_filter(frame, chunks[4]);
        }
        self.render_menu(frame, chunks[5]);
    }

    async fn handle_key(&mut self, key: KeyEvent) -> Option<Transition> {
        if self.filter.is_active() {
            self.handle_filter_key(key.code);
            return None;
        }

        match key.code {
            KeyCode::Char('q') => Some(Transition::Quit),
            KeyCode::Char('1') => {
                let config = self.snapshot_config.clone();
                self.needs_reload = true;
                Some(Transition::Push(Box::new(SnapshotCreateScreen::new(
                    self.repo.clone(),
                    self.lock_handle.clone(),
                    config,
                ))))
            }
            KeyCode::Char('2') => {
                if let Some(entry) = self.display_entry(self.table_state.selected().unwrap_or(0)) {
                    Some(Transition::Push(Box::new(RestoreScreen::new(
                        self.repo.clone(),
                        entry.clone(),
                        None,
                    ))))
                } else {
                    None
                }
            }
            KeyCode::Char('3') => {
                let config = self.forget_config.clone();
                let active_snapshots: Vec<_> = self
                    .snapshots
                    .iter()
                    .filter(|e| e.active)
                    .cloned()
                    .collect();
                self.needs_reload = true;
                Some(Transition::Push(Box::new(ForgetScreen::new(
                    self.repo.clone(),
                    Arc::new(active_snapshots),
                    config,
                ))))
            }
            KeyCode::Char('4') => {
                let snapshots = self.snapshots.clone();
                Some(Transition::Push(Box::new(FindScreen::new(
                    self.repo.clone(),
                    snapshots,
                ))))
            }
            KeyCode::Char('5') => {
                let display_idx = self.table_state.selected()?;
                let target_orig = self.filtered_indices[display_idx];
                if let Some(source_orig) = self.diff_source.take() {
                    let snapshots = self.snapshots.clone();
                    return Some(Transition::Push(Box::new(DiffScreen::new(
                        self.repo.clone(),
                        snapshots,
                        source_orig,
                        target_orig,
                    ))));
                }
                self.diff_source = Some(target_orig);
                None
            }
            KeyCode::Char('r') => {
                self.needs_reload = true;
                if let Err(e) = self.load_snapshots().await {
                    tracing::error!("Failed to refresh snapshots: {}", e);
                }
                None
            }
            KeyCode::Char('u') => {
                if let Some(entry) = self.display_entry(self.table_state.selected().unwrap_or(0))
                    && !entry.active
                {
                    if let Err(e) = self.repo.recall_dropped_snapshot(&entry.id).await {
                        tracing::error!("Failed to recall snapshot {}: {}", entry.id, e);
                    } else {
                        let _ = self.load_snapshots().await;
                    }
                }
                None
            }
            KeyCode::Char('/') => {
                self.filter.open();
                None
            }
            key if self.table_state.handle_nav_keys(
                key,
                self.display_len(),
                self.last_height as usize,
            ) =>
            {
                None
            }
            KeyCode::Enter => {
                if self.diff_source.is_some() {
                    return None;
                }
                if let Some(orig_idx) = self.selected_original_index() {
                    return Some(Transition::Push(Box::new(SnapshotDetailScreen::new(
                        self.repo.clone(),
                        self.snapshots.clone(),
                        orig_idx,
                    ))));
                }
                None
            }
            KeyCode::Esc => {
                self.diff_source = None;
                None
            }
            _ => None,
        }
    }

    async fn on_become_active(&mut self) -> Result<()> {
        if self.needs_reload {
            self.load_snapshots().await?;
        }
        Ok(())
    }
}
