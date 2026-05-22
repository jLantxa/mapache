use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use chrono::Local;
use crossterm::event::{KeyCode, KeyEvent};
use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Direction, Layout, Margin, Rect},
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Paragraph, Row, ScrollbarState, Table, TableState},
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
const HEADER_HEIGHT: u16 = 3;
const MENU_HEIGHT: u16 = 2;

const MENU_ITEMS: &[(char, &str)] = &[
    ('1', "Snapshot"),
    ('2', "Restore"),
    ('3', "Forget"),
    ('q', "Quit"),
];

const KEY_HINTS: &str =
    "[\u{2191}\u{2193}] navigate  [Enter] details  [/] filter  [Esc] clear filter";

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
        }
    }

    pub async fn load_snapshots(&mut self) -> Result<()> {
        let mut entries = SnapshotStream::new(self.repo.clone())
            .await?
            .collect_entries(true)
            .await?;
        entries.sort_unstable_by_key(|e| std::cmp::Reverse(e.snapshot.timestamp));
        self.snapshots = Arc::new(entries);
        self.update_search_cache();
        self.apply_filter();
        Ok(())
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

    fn render_header(&self, frame: &mut Frame, area: Rect) {
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
            .style(theme::STYLE_HEADER)
            .alignment(Alignment::Left);
        frame.render_widget(header, area);
    }

    fn render_snapshot_list(&self, frame: &mut Frame, area: Rect) {
        let max_rows = (area.height.saturating_sub(2)) as usize;
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
                let active = if entry.active { "*" } else { "-" };
                let id_str = entry.id.to_short_hex(SHORT_SNAPSHOT_ID_LEN);
                let date = utils::pretty_print_timestamp(&entry.snapshot.timestamp, None);
                let host = entry.snapshot.hostname.as_deref().unwrap_or_default();
                let size = utils::format_size_binary(entry.snapshot.size(), 3);

                let tags_buf = theme::format_tags(&entry.snapshot.tags);

                let active_style = if entry.active {
                    Style::default().fg(Color::Green)
                } else {
                    Style::default().fg(Color::DarkGray)
                };

                Row::new(vec![
                    Span::styled(active, active_style),
                    Span::styled(id_str, theme::STYLE_SNAPSHOT_ID),
                    Span::styled(date, theme::STYLE_SNAPSHOT_DATE),
                    Span::styled(host, theme::STYLE_SNAPSHOT_HOST),
                    Span::styled(format!("{:>14}", size), theme::STYLE_SNAPSHOT_SIZE),
                    Span::raw(tags_buf),
                ])
            })
            .collect();

        let header_row = Row::new(vec![
            Span::styled(" ", theme::STYLE_TABLE_HEADER),
            Span::styled("ID", theme::STYLE_TABLE_HEADER),
            Span::styled("Date", theme::STYLE_TABLE_HEADER),
            Span::styled("Host", theme::STYLE_TABLE_HEADER),
            Span::styled("Size", theme::STYLE_TABLE_HEADER),
            Span::styled("Tags", theme::STYLE_TABLE_HEADER),
        ])
        .style(theme::STYLE_TABLE_HEADER);

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
        .block(theme::themed_block(&title))
        .row_highlight_style(theme::STYLE_SELECTED_ROW)
        .highlight_symbol(">> ");

        let mut state = self.table_state;
        let selected = self.table_state.selected().unwrap_or(0);
        let max_offset = display_len.saturating_sub(max_rows);
        let offset = selected.saturating_sub(max_rows / 2).min(max_offset);
        *state.offset_mut() = offset;

        frame.render_stateful_widget(table, area, &mut state);

        if display_len > max_rows {
            let mut scrollbar_state = ScrollbarState::new(display_len)
                .position(selected)
                .viewport_content_length(max_rows);

            frame.render_stateful_widget(
                theme::scrollbar(),
                area.inner(Margin::new(1, 1)),
                &mut scrollbar_state,
            );
        }
    }

    fn render_snapshot_info(&self, frame: &mut Frame, area: Rect) {
        let entry = self.selected_entry();
        let info_text = if let Some(e) = entry {
            let mut info = String::with_capacity(256);
            info.push_str("ID: ");
            info.push_str(&e.id.to_hex());
            if let Some(desc) = &e.snapshot.description {
                info.push_str("\nDescription: ");
                info.push_str(desc);
            }
            info.push_str("\nPaths: ");
            Self::format_paths_into(&e.snapshot.paths, &mut info, area.width as usize - 10);
            info
        } else {
            "No snapshot selected".to_string()
        };

        let info = Paragraph::new(info_text)
            .alignment(Alignment::Left)
            .block(theme::themed_block("Snapshot Info"));
        frame.render_widget(info, area);
    }

    fn info_line_count(&self) -> usize {
        let entry = self.selected_entry();
        if let Some(e) = entry {
            let mut count = 2;
            if e.snapshot.description.is_some() {
                count += 1;
            }
            count + 2
        } else {
            3
        }
    }

    fn render_filter(&self, frame: &mut Frame, area: Rect) {
        if let Some(input) = &self.filter {
            let filter_text = if input.is_empty() {
                Line::from(Span::styled(
                    "> Filter by host, tag, path, or ID...",
                    Style::default().fg(Color::DarkGray),
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
            let filter_widget = Paragraph::new(filter_text).block(theme::themed_block("Filter"));
            frame.render_widget(filter_widget, area);
        }
    }

    fn render_menu(&self, frame: &mut Frame, area: Rect) {
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
            .style(theme::STYLE_MENU_KEY)
            .alignment(Alignment::Left);
        frame.render_widget(hints, chunks[1]);
    }

    fn selected_entry(&self) -> Option<&SnapshotEntry> {
        self.table_state
            .selected()
            .and_then(|idx| self.display_entry(idx))
    }

    fn selected_original_index(&self) -> Option<usize> {
        let display_idx = self.table_state.selected()?;
        self.filtered_indices.get(display_idx).copied()
    }

    fn format_paths_into(paths: &[std::path::PathBuf], out: &mut String, max_width: usize) {
        if paths.is_empty() {
            out.push_str("(none)");
            return;
        }

        let suffix = ", ...";
        let mut len = 0;
        let mut count = 0;

        for (i, p) in paths.iter().enumerate() {
            let display = p.display().to_string();
            let formatted_len = display.len() + 2;
            let needed = if i == 0 {
                formatted_len
            } else {
                formatted_len + 2
            };

            let limit = if i + 1 < paths.len() {
                max_width.saturating_sub(suffix.len())
            } else {
                max_width
            };

            if len + needed > limit {
                break;
            }
            len += needed;
            count += 1;
            if i > 0 {
                out.push_str(", ");
            }
            out.push('"');
            out.push_str(&display);
            out.push('"');
        }

        if count < paths.len() {
            out.push_str(suffix);
        }
    }

    fn build_menu_text() -> Vec<Line<'static>> {
        let mut spans = Vec::new();
        for (i, (key, label)) in MENU_ITEMS.iter().enumerate() {
            if i > 0 {
                spans.push(Span::raw("    "));
            }
            spans.extend(theme::key_hint(&key.to_string(), label));
        }
        vec![Line::from(spans)]
    }
}

#[async_trait]
impl Screen for DashboardScreen {
    fn render(&mut self, frame: &mut Frame) {
        let area = frame.area();
        self.last_height = area.height.saturating_sub(HEADER_HEIGHT + MENU_HEIGHT);
        let has_filter = self.filter.is_some();
        let filter_height = if has_filter { FILTER_INPUT_HEIGHT } else { 0 };
        let info_lines = self.info_line_count();

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(HEADER_HEIGHT),
                Constraint::Min(3),
                Constraint::Length(info_lines as u16),
                Constraint::Length(filter_height),
                Constraint::Length(MENU_HEIGHT),
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
                if let Some(entry) = self.selected_entry() {
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
                Some(Transition::Push(Box::new(ForgetScreen::new(
                    self.repo.clone(),
                    self.snapshots.clone(),
                    config,
                ))))
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
