use std::sync::Arc;

use anyhow::Result;
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
    mapache::{defaults::SHORT_SNAPSHOT_ID_LEN, global::THIS_MAPACHE_VERSION},
    repository::{
        lock::LockHandle,
        repo::Repository,
        snapshot::{SnapshotEntry, SnapshotEntryList, SnapshotStream},
    },
    ui::tui::{
        app::{Screen, Transition},
        screens::{
            forget::ForgetScreen, restore::RestoreScreen, snapshot::SnapshotCreateScreen,
            snapshot_detail::SnapshotDetailScreen,
        },
        theme,
        widgets::{StateNavigation, TextInput, TextInputAction},
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
    ('q', "Quit"),
];

#[allow(dead_code)]
struct DashboardStats {
    total: usize,
    active: usize,
    total_size: u64,
    unique_size: u64,
    oldest: Option<String>,
    newest: Option<String>,
}

pub struct DashboardScreen {
    repo: Arc<Repository>,
    lock_handle: LockHandle,
    repo_path: String,
    repo_id: String,
    snapshot_config: Option<SnapshotCmdArgs>,
    forget_config: Option<ForgetCmdArgs>,
    snapshots: Arc<SnapshotEntryList>,
    filtered_indices: Vec<usize>,
    search_cache: Vec<String>,
    table_state: TableState,
    filter: Option<TextInput>,
    last_height: u16,
    stats: DashboardStats,
}

impl DashboardScreen {
    pub fn new(
        repo: Arc<Repository>,
        lock_handle: LockHandle,
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
            filter: None,
            last_height: 0,
            stats: DashboardStats {
                total: 0,
                active: 0,
                total_size: 0,
                unique_size: 0,
                oldest: None,
                newest: None,
            },
        }
    }

    pub async fn load_snapshots(&mut self) -> Result<()> {
        let active_entries = SnapshotStream::new(self.repo.clone())
            .await?
            .collect_entries(true)
            .await?;

        let mut dropped_entries = SnapshotStream::dropped(self.repo.clone())
            .await?
            .collect_entries(false)
            .await?;

        let mut entries = active_entries;
        entries.append(&mut dropped_entries);

        entries.sort_unstable_by_key(|e| std::cmp::Reverse(e.snapshot.timestamp));
        self.stats = Self::compute_stats(&entries);
        self.snapshots = Arc::new(entries);
        self.update_search_cache();
        self.apply_filter();
        Ok(())
    }

    fn compute_stats(entries: &[SnapshotEntry]) -> DashboardStats {
        let total = entries.len();
        let active = entries.iter().filter(|e| e.active).count();
        let total_size: u64 = entries.iter().map(|e| e.snapshot.size()).sum();
        let unique_size: u64 = entries
            .iter()
            .filter(|e| e.active)
            .map(|e| e.snapshot.size())
            .sum();
        let oldest = entries
            .last()
            .map(|e| utils::pretty_print_timestamp(&e.snapshot.timestamp, None));
        let newest = entries.first().map(|e| {
            let elapsed = Local::now() - e.snapshot.timestamp;
            format!("{} ago", utils::pretty_print_duration_chrono(elapsed, 1))
        });
        DashboardStats {
            total,
            active,
            total_size,
            unique_size,
            oldest,
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
        if let Some(input) = &self.filter {
            let query = input.text().to_lowercase();
            if query.is_empty() {
                self.filtered_indices = (0..self.snapshots.len()).collect();
            } else {
                self.filtered_indices = self
                    .search_cache
                    .iter()
                    .enumerate()
                    .filter(|(_, s)| s.contains(&query))
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
        let Some(input) = &mut self.filter else {
            return;
        };

        match input.handle_key(key) {
            TextInputAction::Cancel => {
                self.filter = None;
                self.apply_filter();
            }
            TextInputAction::Confirm | TextInputAction::Edited => {
                self.apply_filter();
            }
            TextInputAction::None => {}
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

        let title = if self.filter.is_some() {
            format!(" Snapshots ({}/{}) ", display_len, self.snapshots.len())
        } else {
            format!(" Snapshots ({}) ", self.snapshots.len())
        };

        let rows: Vec<Row<'_>> = self
            .filtered_indices
            .iter()
            .map(|&orig_idx| {
                let entry = &self.snapshots[orig_idx];
                let active_sym = if entry.active { "\u{25cf}" } else { "\u{25cb}" };
                let active_style = if entry.active {
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
                    Span::styled(format!("{:>10}", size), theme::THEME.snap_size),
                    Span::raw(tags),
                ])
            })
            .collect();

        let header_row = Row::new(vec![
            Span::styled(" ", theme::THEME.menu_key),
            Span::styled("ID", theme::THEME.menu_key),
            Span::styled("Date", theme::THEME.menu_key),
            Span::styled("Host", theme::THEME.menu_key),
            Span::styled("Size", theme::THEME.menu_key),
            Span::styled("Tags", theme::THEME.menu_key),
        ]);

        let widths = [
            Constraint::Length(2),
            Constraint::Length(12),
            Constraint::Length(28),
            Constraint::Length(12),
            Constraint::Length(10),
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

    fn render_filter(&self, frame: &mut Frame, area: Rect) {
        if let Some(input) = &self.filter {
            let filter_text = if input.is_empty() {
                Line::from(Span::styled(
                    "> Filter by host, tag, path, or ID...",
                    theme::THEME.footer,
                ))
            } else {
                let text = input.text();
                let cursor = input.cursor();
                let before: String = text.chars().take(cursor).collect();
                let after: String = text.chars().skip(cursor).collect();
                let mut spans = vec![Span::raw("> ")];
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

        let hint_str = "\u{2191}\u{2193} navigate  Enter details  / filter".to_string();
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
        let inner_content = area.inner(Margin::new(2, 1));

        self.last_height = inner_content
            .height
            .saturating_sub(HEADER_HEIGHT + MENU_HEIGHT + 1);

        let has_filter = self.filter.is_some();
        let filter_height = if has_filter { FILTER_INPUT_HEIGHT } else { 0 };
        let info_height = self.selected_info_height();

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(HEADER_HEIGHT),
                Constraint::Length(1),
                Constraint::Min(5),
                Constraint::Length(info_height),
                Constraint::Length(filter_height),
                Constraint::Length(MENU_HEIGHT),
            ])
            .split(inner_content);

        self.render_top_bar(frame, chunks[0]);
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
        if self.filter.is_some() {
            self.handle_filter_key(key.code);
            return None;
        }

        match key.code {
            KeyCode::Char('q') => Some(Transition::Quit),
            KeyCode::Char('1') => {
                let config = self.snapshot_config.clone();
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
                Some(Transition::Push(Box::new(ForgetScreen::new(
                    self.repo.clone(),
                    Arc::new(active_snapshots),
                    config,
                ))))
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
                self.filter = Some(TextInput::new());
                None
            }
            KeyCode::Down => {
                self.table_state.next(self.display_len());
                None
            }
            KeyCode::Up => {
                self.table_state.previous(self.display_len());
                None
            }
            KeyCode::PageDown => {
                self.table_state
                    .page_next(self.display_len(), self.last_height as usize);
                None
            }
            KeyCode::PageUp => {
                self.table_state
                    .page_previous(self.display_len(), self.last_height as usize);
                None
            }
            KeyCode::Home => {
                self.table_state.home(self.display_len());
                None
            }
            KeyCode::End => {
                self.table_state.end(self.display_len());
                None
            }
            KeyCode::Enter => {
                if let Some(orig_idx) = self.selected_original_index() {
                    return Some(Transition::Push(Box::new(SnapshotDetailScreen::new(
                        self.repo.clone(),
                        self.snapshots.clone(),
                        orig_idx,
                    ))));
                }
                None
            }
            _ => None,
        }
    }

    async fn on_become_active(&mut self) -> Result<()> {
        self.load_snapshots().await
    }
}
