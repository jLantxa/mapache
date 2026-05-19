use std::sync::Arc;

use anyhow::Result;
use chrono::Local;
use crossterm::event::KeyCode;
use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Direction, Layout, Margin},
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::{
        Block, Borders, Paragraph, Row, Scrollbar, ScrollbarOrientation, ScrollbarState, Table,
        TableState,
    },
};

use crate::{
    mapache::{ID, defaults::SHORT_SNAPSHOT_ID_LEN, global::THIS_MAPACHE_VERSION},
    repository::{
        repo::Repository,
        snapshot::{SnapshotEntry, SnapshotEntryList, SnapshotStream},
    },
    utils,
};

use crate::ui::tui::theme;

#[derive(Debug)]
pub enum DashboardAction {
    Quit,
    Snapshot,
    Restore,
    Stats,
    Verify,
    Forget,
    Clean,
    SnapshotDetail(ID),
}

const MENU_ITEMS: &[(char, &str)] = &[
    ('1', "Snapshot"),
    ('2', "Restore"),
    ('3', "Stats"),
    ('4', "Verify"),
    ('5', "Forget"),
    ('6', "Clean"),
    ('q', "Quit"),
];

const KEY_HINTS: &str = "[↑↓] navigate  [Enter] details  [/] filter  [Esc] clear filter";

pub struct DashboardScreen {
    repo: Arc<Repository>,
    repo_path: String,
    repo_id: String,
    snapshots: SnapshotEntryList,
    filtered_snapshots: SnapshotEntryList,
    table_state: TableState,
    filter: Option<String>,
    filter_cursor: usize,
}

impl DashboardScreen {
    pub fn new(repo: Arc<Repository>, repo_path: String, repo_id: String) -> Self {
        Self {
            repo,
            repo_path,
            repo_id,
            snapshots: Vec::new(),
            filtered_snapshots: Vec::new(),
            table_state: TableState::default(),
            filter: None,
            filter_cursor: 0,
        }
    }

    pub async fn load_snapshots(&mut self) -> Result<()> {
        let mut entries = SnapshotStream::new(self.repo.clone())
            .await?
            .collect_entries(true)
            .await?;
        entries.sort_unstable_by_key(|e| std::cmp::Reverse(e.snapshot.timestamp));
        self.snapshots = entries;
        self.apply_filter();
        Ok(())
    }

    fn apply_filter(&mut self) {
        if let Some(query) = &self.filter {
            let query = query.to_lowercase();
            if query.is_empty() {
                self.filtered_snapshots = self.snapshots.clone();
            } else {
                self.filtered_snapshots = self
                    .snapshots
                    .iter()
                    .filter(|e| {
                        e.snapshot
                            .hostname
                            .as_ref()
                            .is_some_and(|h| h.to_lowercase().contains(&query))
                            || e.snapshot
                                .tags
                                .iter()
                                .any(|t| t.to_lowercase().contains(&query))
                            || e.snapshot
                                .root
                                .to_string_lossy()
                                .to_lowercase()
                                .contains(&query)
                            || e.id.to_hex().to_lowercase().contains(&query)
                    })
                    .cloned()
                    .collect();
            }
        } else {
            self.filtered_snapshots = self.snapshots.clone();
        }

        if !self.filtered_snapshots.is_empty() {
            self.table_state.select(Some(0));
        } else {
            self.table_state.select(None);
        }
    }

    fn display_list(&self) -> &SnapshotEntryList {
        if self.filter.is_some() {
            &self.filtered_snapshots
        } else {
            &self.snapshots
        }
    }

    pub fn handle_key(&mut self, key: KeyCode) -> Option<DashboardAction> {
        if self.filter.is_some() {
            return self.handle_filter_key(key);
        }

        match key {
            KeyCode::Char('q') => Some(DashboardAction::Quit),
            KeyCode::Char('1') => Some(DashboardAction::Snapshot),
            KeyCode::Char('2') => Some(DashboardAction::Restore),
            KeyCode::Char('3') => Some(DashboardAction::Stats),
            KeyCode::Char('4') => Some(DashboardAction::Verify),
            KeyCode::Char('5') => Some(DashboardAction::Forget),
            KeyCode::Char('6') => Some(DashboardAction::Clean),
            KeyCode::Char('/') => {
                self.filter = Some(String::new());
                self.filter_cursor = 0;
                None
            }
            KeyCode::Down => {
                let list = self.display_list();
                if !list.is_empty() {
                    let current = self.table_state.selected().unwrap_or(0);
                    let next = if current >= list.len().saturating_sub(1) {
                        0
                    } else {
                        current + 1
                    };
                    self.table_state.select(Some(next));
                }
                None
            }
            KeyCode::Up => {
                let list = self.display_list();
                if !list.is_empty() {
                    let current = self.table_state.selected().unwrap_or(0);
                    let prev = if current == 0 {
                        list.len().saturating_sub(1)
                    } else {
                        current - 1
                    };
                    self.table_state.select(Some(prev));
                }
                None
            }
            KeyCode::Enter => {
                let list = self.display_list();
                if let Some(idx) = self.table_state.selected()
                    && idx < list.len()
                {
                    return Some(DashboardAction::SnapshotDetail(list[idx].id));
                }
                None
            }
            _ => None,
        }
    }

    fn handle_filter_key(&mut self, key: KeyCode) -> Option<DashboardAction> {
        match key {
            KeyCode::Esc => {
                self.filter = None;
                self.apply_filter();
            }
            KeyCode::Enter => {
                self.apply_filter();
            }
            KeyCode::Backspace => {
                if let Some(ref mut query) = self.filter
                    && self.filter_cursor > 0
                    && !query.is_empty()
                    && let Some((pos, _)) = query.char_indices().nth(self.filter_cursor - 1)
                {
                    query.remove(pos);
                    self.filter_cursor -= 1;
                    self.apply_filter();
                }
            }
            KeyCode::Delete => {
                if let Some(ref mut query) = self.filter
                    && self.filter_cursor < query.chars().count()
                    && !query.is_empty()
                    && let Some((pos, _)) = query.char_indices().nth(self.filter_cursor)
                {
                    query.remove(pos);
                    self.apply_filter();
                }
            }
            KeyCode::Left if self.filter_cursor > 0 => {
                self.filter_cursor -= 1;
            }
            KeyCode::Right => {
                if let Some(ref query) = self.filter
                    && self.filter_cursor < query.chars().count()
                {
                    self.filter_cursor += 1;
                }
            }
            KeyCode::Home => {
                self.filter_cursor = 0;
            }
            KeyCode::End => {
                if let Some(ref query) = self.filter {
                    self.filter_cursor = query.chars().count();
                }
            }
            KeyCode::Char(c) => {
                if let Some(ref mut query) = self.filter {
                    let byte_pos = query
                        .char_indices()
                        .nth(self.filter_cursor)
                        .map(|(i, _)| i)
                        .unwrap_or(query.len());
                    query.insert(byte_pos, c);
                    self.filter_cursor += 1;
                    self.apply_filter();
                }
            }
            _ => {}
        }
        None
    }

    pub fn render(&self, frame: &mut Frame) {
        let has_filter = self.filter.is_some();
        let filter_height = if has_filter { 3 } else { 0 };
        let info_lines = self.info_line_count();

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Min(3),
                Constraint::Length(info_lines as u16),
                Constraint::Length(filter_height),
                Constraint::Length(2),
            ])
            .split(frame.area());

        self.render_header(frame, chunks[0]);
        self.render_snapshot_list(frame, chunks[1]);
        if info_lines > 0 {
            self.render_snapshot_info(frame, chunks[2]);
        }
        if has_filter {
            self.render_filter(frame, chunks[3]);
        }
        self.render_menu(frame, chunks[4]);
    }

    fn render_header(&self, frame: &mut Frame, area: ratatui::layout::Rect) {
        let last_info = self
            .snapshots
            .first()
            .map(|e| {
                let elapsed = Local::now() - e.snapshot.timestamp;
                let ago = utils::pretty_print_duration_chrono(elapsed, 1);
                format!("Last: {} ago", ago)
            })
            .unwrap_or_default();

        let header_text = format!(
            " mapache {}  |  {} snapshots  |  {}\n {} [{}]",
            THIS_MAPACHE_VERSION,
            self.snapshots.len(),
            last_info,
            self.repo_path,
            self.repo_id.chars().take(8).collect::<String>(),
        );
        let header = Paragraph::new(header_text)
            .style(theme::header_style())
            .alignment(Alignment::Left);
        frame.render_widget(header, area);
    }

    fn render_snapshot_list(&self, frame: &mut Frame, area: ratatui::layout::Rect) {
        let max_rows = (area.height.saturating_sub(2)) as usize;
        let display_list = self.display_list();

        let title = if self.filter.is_some() {
            format!(
                " Snapshots ({}/{}) ",
                display_list.len(),
                self.snapshots.len()
            )
        } else {
            format!(" Snapshots ({}) ", self.snapshots.len())
        };

        let rows = self.render_snapshot_rows(display_list);

        let header_row = Row::new(vec![
            Span::styled(" ", Style::default().fg(theme::TABLE_HEADER).bold()),
            Span::styled("ID", Style::default().fg(theme::TABLE_HEADER).bold()),
            Span::styled("Date", Style::default().fg(theme::TABLE_HEADER).bold()),
            Span::styled("Host", Style::default().fg(theme::TABLE_HEADER).bold()),
            Span::styled(
                "Size".to_string(),
                Style::default().fg(theme::TABLE_HEADER).bold(),
            ),
            Span::styled("Tags", Style::default().fg(theme::TABLE_HEADER).bold()),
        ])
        .style(
            Style::default()
                .fg(theme::TABLE_HEADER)
                .add_modifier(Modifier::BOLD | Modifier::REVERSED),
        );

        let table = Table::new(
            rows,
            vec![
                Constraint::Length(1),
                Constraint::Length(12),
                Constraint::Length(28),
                Constraint::Length(12),
                Constraint::Length(14),
                Constraint::Min(8),
            ],
        )
        .header(header_row)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(title)
                .border_style(theme::border_style()),
        )
        .row_highlight_style(theme::selected_row_style())
        .highlight_symbol(">> ");

        let mut state = self.table_state;
        let selected = self.table_state.selected().unwrap_or(0);
        let max_offset = display_list.len().saturating_sub(max_rows);
        let offset = selected.saturating_sub(max_rows / 2).min(max_offset);
        *state.offset_mut() = offset;

        frame.render_stateful_widget(table, area, &mut state);

        if display_list.len() > max_rows {
            let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight)
                .begin_symbol(None)
                .end_symbol(None)
                .track_symbol(Some("│"))
                .thumb_symbol("█")
                .style(theme::border_style());

            let mut scrollbar_state = ScrollbarState::new(display_list.len())
                .position(selected)
                .viewport_content_length(max_rows);

            frame.render_stateful_widget(
                scrollbar,
                area.inner(Margin::new(1, 1)),
                &mut scrollbar_state,
            );
        }
    }

    fn info_line_count(&self) -> usize {
        if self.selected_entry().is_some() {
            5
        } else {
            0
        }
    }

    fn render_snapshot_info(&self, frame: &mut Frame, area: ratatui::layout::Rect) {
        let Some(entry) = self.selected_entry() else {
            return;
        };

        let paths_str =
            self.format_paths(&entry.snapshot.paths, area.width.saturating_sub(9) as usize);

        let lines = vec![
            Line::from(vec![
                Span::styled("ID:    ", Style::default().bold()),
                Span::styled(entry.id.to_hex(), Style::default().fg(theme::SNAPSHOT_ID)),
            ]),
            Line::from(vec![
                Span::styled("Root:  ", Style::default().bold()),
                Span::raw(entry.snapshot.root.to_string_lossy()),
            ]),
            Line::from(vec![
                Span::styled("Paths: ", Style::default().bold()),
                Span::raw(paths_str),
            ]),
        ];

        let info = Paragraph::new(lines).alignment(Alignment::Left).block(
            Block::default()
                .borders(Borders::ALL)
                .title(" Snapshot Info ")
                .border_style(theme::border_style()),
        );
        frame.render_widget(info, area);
    }

    fn render_filter(&self, frame: &mut Frame, area: ratatui::layout::Rect) {
        if let Some(query) = &self.filter {
            let filter_text = if query.is_empty() {
                Line::from(Span::styled(
                    "> Filter by host, tag, path, or ID...",
                    Style::default().fg(ratatui::style::Color::DarkGray),
                ))
            } else {
                Line::from(Span::raw(format!("> {}", query)))
            };
            let filter_widget = Paragraph::new(filter_text).style(Style::default()).block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(" Filter ")
                    .border_style(theme::border_style()),
            );
            frame.render_widget(filter_widget, area);
        }
    }

    fn render_menu(&self, frame: &mut Frame, area: ratatui::layout::Rect) {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(1), Constraint::Length(1)])
            .split(area);

        let menu_text = Self::build_menu_text();
        let menu = Paragraph::new(menu_text)
            .style(Style::default().fg(theme::FOOTER_FG))
            .alignment(Alignment::Left);
        frame.render_widget(menu, chunks[0]);

        let hints = Paragraph::new(KEY_HINTS)
            .style(Style::default().fg(theme::MENU_KEY))
            .alignment(Alignment::Left);
        frame.render_widget(hints, chunks[1]);
    }

    fn selected_entry(&self) -> Option<&crate::repository::snapshot::SnapshotEntry> {
        let list = self.display_list();
        self.table_state.selected().and_then(|idx| list.get(idx))
    }

    pub fn get_entry(&self, id: ID) -> Option<SnapshotEntry> {
        self.snapshots.iter().find(|e| e.id == id).cloned()
    }

    fn render_snapshot_rows<'a>(&self, list: &'a SnapshotEntryList) -> Vec<Row<'a>> {
        list.iter()
            .map(|entry| {
                let active = if entry.active { "*" } else { "-" };
                let id_str = entry.id.to_short_hex(SHORT_SNAPSHOT_ID_LEN);
                let date = utils::pretty_print_timestamp(&entry.snapshot.timestamp, None);
                let host = entry.snapshot.hostname.as_deref().unwrap_or_default();
                let size = utils::format_size_binary(entry.snapshot.size(), 3);
                let tags = entry
                    .snapshot
                    .tags
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ");

                let active_style = if entry.active {
                    Style::default().fg(ratatui::style::Color::Green)
                } else {
                    Style::default().fg(ratatui::style::Color::DarkGray)
                };

                Row::new(vec![
                    Span::styled(active, active_style),
                    Span::styled(id_str, Style::default().fg(theme::SNAPSHOT_ID)),
                    Span::styled(date, Style::default().fg(theme::SNAPSHOT_DATE)),
                    Span::styled(host, Style::default().fg(theme::SNAPSHOT_HOST)),
                    Span::styled(
                        format!("{:>14}", size),
                        Style::default().fg(theme::SNAPSHOT_SIZE),
                    ),
                    Span::raw(tags),
                ])
            })
            .collect()
    }

    fn format_paths(&self, paths: &[std::path::PathBuf], max_width: usize) -> String {
        if paths.is_empty() {
            return "(none)".to_string();
        }

        let mut parts = Vec::new();
        let mut len = 0;
        let suffix = ", ...";

        for p in paths {
            let formatted = format!("\"{}\"", p.display());
            let needed = if parts.is_empty() {
                formatted.len()
            } else {
                formatted.len() + 2
            };

            let limit = if parts.len() + 1 < paths.len() {
                max_width.saturating_sub(suffix.len())
            } else {
                max_width
            };

            if len + needed > limit {
                break;
            }
            len += needed;
            parts.push(formatted);
        }

        let joined = parts.join(", ");
        if parts.len() < paths.len() {
            format!("{}{}", joined, suffix)
        } else {
            joined
        }
    }

    fn build_menu_text() -> Vec<Line<'static>> {
        let mut spans = Vec::new();
        for (i, (key, label)) in MENU_ITEMS.iter().enumerate() {
            if i > 0 {
                spans.push(Span::raw("    "));
            }
            spans.push(Span::styled(
                format!("[{}]", key),
                theme::menu_key_style().bold(),
            ));
            spans.push(Span::styled(
                format!(" {}", label),
                theme::menu_text_style(),
            ));
        }
        vec![Line::from(spans)]
    }
}
