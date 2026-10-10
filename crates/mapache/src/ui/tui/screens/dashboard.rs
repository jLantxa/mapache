use std::sync::Arc;

use async_trait::async_trait;
use chrono::Local;
use crossterm::event::{KeyCode, KeyEvent, MouseButton, MouseEvent};
use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Direction, Layout, Margin, Rect},
    style::Style,
    text::{Line, Span},
    widgets::{Block, Paragraph, Row, Table, TableState},
};

use crate::{
    commands::{cmd_forget::CmdArgs as ForgetCmdArgs, cmd_snapshot::CmdArgs as SnapshotCmdArgs},
    common::{
        defaults::{APP_NAME, SHORT_SNAPSHOT_ID_LEN},
        error::Result,
        global::THIS_MAPACHE_VERSION,
    },
    repository::{
        lock::{LockHandle, LockStatus},
        repo::Repository,
        snapshot::{SnapshotEntry, SnapshotEntryList, SnapshotStream},
    },
    ui::tui::{
        app::{Screen, Transition},
        screens::{
            clean::CleanScreen, diff::DiffScreen, find::FindScreen, forget::ForgetScreen,
            restore::RestoreScreen, snapshot::SnapshotCreateScreen,
            snapshot_detail::SnapshotDetailScreen, stats::StatsScreen, verify::VerifyScreen,
        },
        theme,
        widgets::{
            FilterAction, FilterState, StateNavigation, ToastSink, click_to_index,
            impl_attach_toasts, truncate_with_ellipsis,
        },
    },
    utils,
};

const FILTER_INPUT_HEIGHT: u16 = 3;
const HEADER_HEIGHT: u16 = 2;

/// A dashboard command. Every entry point — accelerators, the focusable menu
/// and the help overlay — is derived from this single list so they cannot
/// drift apart.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum MenuAction {
    Snapshot,
    Restore,
    Forget,
    Find,
    Diff,
    Stats,
    Clean,
    Verify,
    Recall,
    Refresh,
    Quit,
}

#[derive(Clone, Copy)]
struct MenuItem {
    key: &'static str,
    label: &'static str,
    action: MenuAction,
}

const MENU_ITEMS: &[MenuItem] = &[
    MenuItem {
        key: "1",
        label: "Snapshot",
        action: MenuAction::Snapshot,
    },
    MenuItem {
        key: "2",
        label: "Restore",
        action: MenuAction::Restore,
    },
    MenuItem {
        key: "3",
        label: "Forget",
        action: MenuAction::Forget,
    },
    MenuItem {
        key: "4",
        label: "Find",
        action: MenuAction::Find,
    },
    MenuItem {
        key: "5",
        label: "Diff",
        action: MenuAction::Diff,
    },
    MenuItem {
        key: "6",
        label: "Stats",
        action: MenuAction::Stats,
    },
    MenuItem {
        key: "7",
        label: "Clean",
        action: MenuAction::Clean,
    },
    MenuItem {
        key: "8",
        label: "Verify",
        action: MenuAction::Verify,
    },
    MenuItem {
        key: "u",
        label: "Recall",
        action: MenuAction::Recall,
    },
    MenuItem {
        key: "r",
        label: "Refresh",
        action: MenuAction::Refresh,
    },
    MenuItem {
        key: "q",
        label: "Quit",
        action: MenuAction::Quit,
    },
];

/// Clamp a menu cursor to a list of `len` visible entries.
fn clamp_menu_cursor(cursor: usize, len: usize) -> usize {
    if len == 0 { 0 } else { cursor.min(len - 1) }
}

/// Map a shortcut key to its action, ignoring per-selection availability.
fn action_for_key(key: char) -> Option<MenuAction> {
    MENU_ITEMS
        .iter()
        .find(|item| item.key.starts_with(key))
        .map(|item| item.action)
}

/// Column widths for the snapshot table rendered inside `area_width` columns.
///
/// `sym`, `id`, `host` and `size` are fixed; `Date` flexes and the `Tags`
/// column is dropped entirely when the terminal is too narrow, so the table
/// never silently clips a column on an 80-column terminal. Returns the widths
/// plus whether the `Tags` column is present (callers must keep their header
/// and row cells in sync with this flag).
fn snapshot_columns(area_width: u16) -> (Vec<Constraint>, bool) {
    // Block borders (2) plus the highlight symbol (2).
    let usable = area_width.saturating_sub(4);
    let remaining = usable.saturating_sub(2 + 12 + 12 + 13);
    let show_tags = remaining >= 30;
    let date_width = if show_tags {
        remaining.saturating_sub(10).clamp(16, 20)
    } else {
        remaining
    };

    let mut widths = vec![
        Constraint::Length(2),
        Constraint::Length(12),
        Constraint::Length(date_width),
        Constraint::Length(12),
        Constraint::Length(13),
    ];
    if show_tags {
        widths.push(Constraint::Min(10));
    }
    (widths, show_tags)
}

/// Lay out menu items into one or more rows, wrapping at `width`. Every entry
/// point renders through this helper so the menu no longer clips items past
/// the terminal edge. `cursor` is highlighted only while the menu has focus.
fn menu_lines(
    entries: &[MenuItem],
    cursor: usize,
    focused: bool,
    width: u16,
) -> Vec<Line<'static>> {
    // `key_hint` renders " key " + " label ", i.e. key+label+4 columns.
    let widths: Vec<usize> = entries
        .iter()
        .map(|item| item.key.chars().count() + item.label.chars().count() + 4)
        .collect();
    theme::wrap_rows(&widths, width as usize)
        .into_iter()
        .map(|row| {
            let mut spans: Vec<Span<'static>> = Vec::new();
            for (offset, i) in row.clone().enumerate() {
                if offset > 0 {
                    spans.push(Span::raw("  "));
                }
                let item = &entries[i];
                if focused && i == cursor {
                    spans.push(Span::styled(
                        format!(" {} ", item.key),
                        theme::THEME.menu_selected,
                    ));
                    spans.push(Span::styled(
                        format!(" {} ", item.label),
                        theme::THEME.menu_selected,
                    ));
                } else {
                    spans.extend(theme::key_hint(item.key, item.label));
                }
            }
            Line::from(spans)
        })
        .collect()
}

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
    last_snapshot_area: Rect,
    stats: DashboardStats,
    diff_source: Option<usize>,
    needs_reload: bool,
    menu_cursor: usize,
    menu_focused: bool,
    toasts: ToastSink,
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
            last_snapshot_area: Rect::default(),
            stats: DashboardStats {
                total: 0,
                newest: None,
            },
            diff_source: None,
            needs_reload: true,
            menu_cursor: 0,
            menu_focused: false,
            toasts: ToastSink::default(),
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
        let query = self.filter.query().unwrap_or("").to_lowercase();
        self.filtered_indices = if query.is_empty() {
            (0..self.snapshots.len()).collect()
        } else {
            self.search_cache
                .iter()
                .enumerate()
                .filter(|(_, entry)| entry.contains(&query))
                .map(|(i, _)| i)
                .collect()
        };

        self.table_state
            .select((!self.filtered_indices.is_empty()).then_some(0));
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
        const SNAPSHOTS_SUFFIX: &str = " snapshots";
        const LENGTH_MAX: u16 = 32;
        const LENGTH_MIN: u16 = 12;
        const REPO_ID_LEN: usize = 12;
        const LOCK_STATUS_WIDTH: u16 = 16;

        // Both rows share one left-hand column so their right-hand columns line
        // up. It is sized to the wider of the two labels that live in it — the
        // tool name plus version above, and the snapshot count below — plus a
        // column of breathing room. Sizing to the version alone clipped the
        // count once a repo passed ~100k snapshots. The cap keeps an unusually
        // long version string from starving the repository info column.
        let name = format!("{APP_NAME} ");
        let needed = std::cmp::max(
            name.len() + THIS_MAPACHE_VERSION.len(),
            self.stats.total.to_string().len() + SNAPSHOTS_SUFFIX.len(),
        ) as u16;
        let row_constraint = std::cmp::min(needed + 2, LENGTH_MAX);

        let bg = Block::default().style(Style::new().bg(theme::THEME.surface));
        frame.render_widget(&bg, area);

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(1), Constraint::Length(1)])
            .split(area);

        let row1 = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Length(row_constraint),
                Constraint::Min(LENGTH_MIN),
            ])
            .split(chunks[0]);

        let header = Paragraph::new(Line::from(vec![
            Span::styled(name, theme::THEME.header),
            Span::styled(THIS_MAPACHE_VERSION, theme::THEME.snap_size),
        ]))
        .style(theme::THEME.footer);
        frame.render_widget(header, row1[0]);

        let info = Paragraph::new(Line::from(vec![
            Span::styled(&self.repo_path, theme::THEME.snap_host),
            Span::styled(" [", theme::THEME.footer),
            Span::styled(
                self.repo_id.chars().take(REPO_ID_LEN).collect::<String>(),
                theme::THEME.snap_id,
            ),
            Span::styled("]", theme::THEME.footer),
            Span::styled(format!(" v{}", self.repo.repo_version()), theme::THEME.teal),
        ]))
        .style(theme::THEME.footer);
        frame.render_widget(info, row1[1]);

        let row2 = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Length(row_constraint),
                Constraint::Min(0),
                Constraint::Length(LOCK_STATUS_WIDTH),
            ])
            .split(chunks[1]);

        let stats_left = Paragraph::new(Line::from(vec![
            Span::styled(self.stats.total.to_string(), theme::THEME.snap_id),
            Span::styled(SNAPSHOTS_SUFFIX, theme::THEME.footer),
        ]))
        .style(theme::THEME.footer);
        frame.render_widget(stats_left, row2[0]);

        let stats_right = Paragraph::new(Line::from(vec![
            Span::styled("last ", theme::THEME.footer),
            Span::styled(
                self.stats.newest.as_deref().unwrap_or("-"),
                theme::THEME.teal,
            ),
        ]))
        .style(theme::THEME.footer);
        frame.render_widget(stats_right, row2[1]);

        let (lock_text, lock_style) = match self.lock_handle.as_ref().map(LockHandle::status) {
            Some(LockStatus::Shared) => ("lock shared", theme::THEME.success),
            Some(LockStatus::Exclusive) => ("lock exclusive", theme::THEME.warning),
            Some(LockStatus::Released) => ("lock released", theme::THEME.error),
            None => ("lock off", theme::THEME.warning),
        };
        frame.render_widget(
            Paragraph::new(Span::styled(lock_text, lock_style)).alignment(Alignment::Right),
            row2[2],
        );
    }

    fn render_snapshot_list(&self, frame: &mut Frame, area: Rect) {
        let max_rows = area.height.saturating_sub(2) as usize;
        let display_len = self.display_len();

        let title = if self.filter.is_active() {
            format!("Snapshots ({}/{})", display_len, self.snapshots.len())
        } else {
            format!("Snapshots ({})", self.snapshots.len())
        };

        if self.filtered_indices.is_empty() {
            let message = if self.snapshots.is_empty() {
                "No snapshots yet \u{2014} press 1 to create one."
            } else {
                "No snapshots match the current filter."
            };
            theme::empty_state(frame, area, &title, message);
            return;
        }

        let (widths, show_tags) = snapshot_columns(area.width);

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

                let mut cells = vec![
                    Span::styled(active_sym, active_style),
                    Span::styled(id_str, theme::THEME.snap_id),
                    Span::styled(date, theme::THEME.snap_date),
                    Span::styled(host, theme::THEME.snap_host),
                    Span::styled(format!("{:>13}", size), theme::THEME.snap_size),
                ];
                if show_tags {
                    cells.push(Span::raw(theme::format_tags(&entry.snapshot.tags)));
                }
                Row::new(cells)
            })
            .collect();

        let mut header_cells = vec![
            Span::styled(" ", theme::THEME.menu_key),
            Span::styled("ID", theme::THEME.menu_key),
            Span::styled("Date", theme::THEME.menu_key),
            Span::styled("Host", theme::THEME.menu_key),
            Span::styled(format!("{:>13}", "Size"), theme::THEME.menu_key),
        ];
        if show_tags {
            header_cells.push(Span::styled("Tags", theme::THEME.menu_key));
        }
        let header_row = Row::new(header_cells);

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

        lines.push(theme::field_spans(
            "ID",
            label_w,
            vec![Span::styled(entry.id.to_hex(), theme::THEME.snap_id)],
        ));

        lines.push(theme::field_spans(
            "Date",
            label_w,
            vec![Span::styled(
                utils::pretty_print_timestamp(&entry.snapshot.timestamp, None),
                theme::THEME.snap_date,
            )],
        ));

        lines.push(theme::field_spans(
            "Path",
            label_w,
            vec![Span::styled(
                entry.snapshot.root.display().to_string(),
                theme::THEME.footer,
            )],
        ));

        for (i, p) in entry.snapshot.paths.iter().enumerate() {
            let p_str = p.display().to_string();
            let indent = " ".repeat(label_w);
            let shown = truncate_with_ellipsis(&p_str, max_w);
            lines.push(Line::from(vec![Span::raw(format!("{indent}{shown}"))]));
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
            let desc_trunc = truncate_with_ellipsis(desc, max_w);
            lines.push(theme::field("Desc", label_w, desc_trunc));
        }

        if !entry.snapshot.tags.is_empty() {
            lines.push(theme::field(
                "Tags",
                label_w,
                theme::format_tags(&entry.snapshot.tags),
            ));
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

    fn render_menu(&self, frame: &mut Frame, area: Rect, lines: Vec<Line<'static>>) {
        let menu_height = (lines.len() as u16).min(area.height);
        let hint_height = area.height.saturating_sub(menu_height).min(1);
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(menu_height),
                Constraint::Length(hint_height),
            ])
            .split(area);

        frame.render_widget(Paragraph::new(lines).style(theme::THEME.footer), chunks[0]);
        frame.render_widget(
            Paragraph::new(self.build_menu_hint()).style(theme::THEME.footer),
            chunks[1],
        );
    }

    fn build_menu_lines(&self, width: u16) -> Vec<Line<'static>> {
        let entries = self.menu_entries();
        let cursor = clamp_menu_cursor(self.menu_cursor, entries.len());
        menu_lines(&entries, cursor, self.menu_focused, width)
    }

    fn build_menu_hint(&self) -> Line<'static> {
        if self.diff_source.is_some() {
            Line::from(vec![
                Span::styled("Esc cancel  ", theme::THEME.footer),
                Span::styled("\u{2191}\u{2193} select target  ", theme::THEME.footer),
                Span::styled("5 ", theme::THEME.menu_key),
                Span::styled("diff", theme::THEME.footer),
            ])
        } else if self.menu_focused {
            Line::from(vec![
                Span::styled("\u{2190}\u{2192} choose  ", theme::THEME.footer),
                Span::styled("Enter", theme::THEME.menu_key),
                Span::styled(" run  ", theme::THEME.footer),
                Span::styled("Esc", theme::THEME.menu_key),
                Span::styled(" back", theme::THEME.footer),
            ])
        } else {
            Line::from(vec![
                Span::styled("Tab", theme::THEME.menu_key),
                Span::styled(" actions  ", theme::THEME.footer),
                Span::styled("\u{2191}\u{2193} navigate  ", theme::THEME.footer),
                Span::styled("Enter", theme::THEME.menu_key),
                Span::styled(" details  ", theme::THEME.footer),
                Span::styled("/", theme::THEME.menu_key),
                Span::styled(" filter", theme::THEME.footer),
            ])
        }
    }

    /// Visible menu entries: the static list minus actions that do not apply to
    /// the current selection (currently only `Recall`).
    fn menu_entries(&self) -> Vec<MenuItem> {
        MENU_ITEMS
            .iter()
            .copied()
            .filter(|item| self.menu_item_enabled(item.action))
            .collect()
    }

    fn menu_item_enabled(&self, action: MenuAction) -> bool {
        match action {
            MenuAction::Recall => self.selected_is_dropped(),
            _ => true,
        }
    }

    fn selected_is_dropped(&self) -> bool {
        self.display_entry(self.table_state.selected().unwrap_or(0))
            .is_some_and(|entry| !entry.active)
    }

    fn move_menu_cursor(&mut self, delta: isize) {
        let len = self.menu_entries().len();
        if len == 0 {
            self.menu_cursor = 0;
            return;
        }
        let current = self.menu_cursor.min(len - 1) as isize;
        self.menu_cursor = (current + delta).rem_euclid(len as isize) as usize;
    }

    async fn run_action(&mut self, action: MenuAction) -> Option<Transition> {
        match action {
            MenuAction::Snapshot => {
                let config = self.snapshot_config.clone();
                self.needs_reload = true;
                Some(Transition::Push(Box::new(SnapshotCreateScreen::new(
                    self.repo.clone(),
                    self.lock_handle.clone(),
                    config,
                ))))
            }
            MenuAction::Restore => {
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
            MenuAction::Forget => {
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
            MenuAction::Find => {
                let snapshots = self.snapshots.clone();
                Some(Transition::Push(Box::new(FindScreen::new(
                    self.repo.clone(),
                    snapshots,
                ))))
            }
            MenuAction::Diff => {
                let display_idx = self.table_state.selected()?;
                let target_orig = self.filtered_indices.get(display_idx).copied()?;
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
                self.menu_focused = false;
                None
            }
            MenuAction::Stats => Some(Transition::Push(Box::new(StatsScreen::new(
                self.repo.clone(),
            )))),
            MenuAction::Clean => {
                self.needs_reload = true;
                Some(Transition::Push(Box::new(CleanScreen::new(
                    self.repo.clone(),
                    self.lock_handle.clone(),
                ))))
            }
            MenuAction::Verify => Some(Transition::Push(Box::new(VerifyScreen::new(
                self.repo.clone(),
                self.lock_handle.clone(),
            )))),
            MenuAction::Recall => {
                if let Some(entry) = self.display_entry(self.table_state.selected().unwrap_or(0))
                    && !entry.active
                {
                    if let Err(e) = self.repo.recall_dropped_snapshot(&entry.id).await {
                        tracing::error!("Failed to recall snapshot {}: {}", entry.id, e);
                        self.toasts
                            .error(format!("Failed to recall snapshot {}: {e}", entry.id));
                    } else {
                        let _ = self.load_snapshots().await;
                    }
                }
                None
            }
            MenuAction::Refresh => {
                self.needs_reload = true;
                if let Err(e) = self.load_snapshots().await {
                    tracing::error!("Failed to refresh snapshots: {}", e);
                    self.toasts
                        .error(format!("Failed to refresh snapshots: {e}"));
                }
                None
            }
            MenuAction::Quit => Some(Transition::Quit),
        }
    }

    fn selected_original_index(&self) -> Option<usize> {
        let display_idx = self.table_state.selected()?;
        self.filtered_indices.get(display_idx).copied()
    }
}

#[async_trait]
impl Screen for DashboardScreen {
    fn render(&mut self, frame: &mut Frame) {
        let inner_content = frame.area().inner(theme::CONTENT_MARGIN);

        let diff_status_height: u16 = if self.diff_source.is_some() { 2 } else { 1 };

        let has_filter = self.filter.is_active();
        let filter_height = if has_filter { FILTER_INPUT_HEIGHT } else { 0 };
        let info_height = self.selected_info_height();
        let menu_lines = self.build_menu_lines(inner_content.width);
        let menu_height = (menu_lines.len() as u16 + 1).min(inner_content.height);

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(HEADER_HEIGHT),
                Constraint::Length(diff_status_height),
                Constraint::Min(5),
                Constraint::Length(info_height),
                Constraint::Length(filter_height),
                Constraint::Length(menu_height),
            ])
            .split(inner_content);

        self.render_top_bar(frame, chunks[0]);
        if self.diff_source.is_some() {
            self.render_diff_status(frame, chunks[1]);
        }
        self.last_height = chunks[2].height.saturating_sub(2);
        self.last_snapshot_area = chunks[2];
        self.render_snapshot_list(frame, chunks[2]);
        if info_height > 0 {
            self.render_selected_info(frame, chunks[3]);
        }
        if has_filter {
            self.render_filter(frame, chunks[4]);
        }
        self.render_menu(frame, chunks[5], menu_lines);
    }

    async fn handle_key(&mut self, key: KeyEvent) -> Option<Transition> {
        if self.filter.is_active() {
            self.handle_filter_key(key.code);
            return None;
        }

        // Direct accelerators (digits and letter shortcuts) always work, even
        // while the navigable menu has focus.
        if let KeyCode::Char(c) = key.code
            && let Some(action) = action_for_key(c).filter(|action| self.menu_item_enabled(*action))
        {
            return self.run_action(action).await;
        }

        if key.code == KeyCode::Char('/') {
            self.menu_focused = false;
            self.filter.open();
            return None;
        }

        if self.menu_focused {
            match key.code {
                KeyCode::Esc => {
                    self.menu_focused = false;
                }
                KeyCode::Tab | KeyCode::Right => self.move_menu_cursor(1),
                KeyCode::BackTab | KeyCode::Left => self.move_menu_cursor(-1),
                KeyCode::Enter => {
                    let entries = self.menu_entries();
                    let cursor = clamp_menu_cursor(self.menu_cursor, entries.len());
                    if let Some(item) = entries.get(cursor) {
                        return self.run_action(item.action).await;
                    }
                }
                _ => {}
            }
            return None;
        }

        match key.code {
            KeyCode::Tab => {
                self.menu_focused = true;
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
            key if self.table_state.handle_nav_keys(
                key,
                self.display_len(),
                self.last_height as usize,
            ) =>
            {
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

    async fn handle_mouse(&mut self, mouse: MouseEvent) -> bool {
        if self.filter.is_active()
            || self.needs_reload
            || !matches!(
                mouse.kind,
                crossterm::event::MouseEventKind::Down(MouseButton::Left)
            )
        {
            return false;
        }
        // The dashboard scrolls the table by centring the selection, so
        // replay the offset formula the render used before mapping the click.
        let selected = self.table_state.selected().unwrap_or(0);
        let max_rows = self.last_snapshot_area.height.saturating_sub(2) as usize;
        let max_offset = self.display_len().saturating_sub(max_rows);
        let offset = selected.saturating_sub(max_rows / 2).min(max_offset);

        let Some(idx) = click_to_index(mouse.row, offset, self.last_snapshot_area, 1) else {
            return false;
        };
        if idx < self.display_len() {
            self.table_state.select(Some(idx));
            return true;
        }
        false
    }

    fn help_hints(&self) -> Vec<(&'static str, &'static str)> {
        let mut hints: Vec<(&'static str, &'static str)> = MENU_ITEMS
            .iter()
            .filter(|item| self.menu_item_enabled(item.action))
            .map(|item| (item.key, item.label))
            .collect();
        if self.diff_source.is_some() {
            hints.push(("Esc", "cancel diff"));
            hints.push(("\u{2191}\u{2193}", "select target"));
        } else {
            hints.push(("Tab", "actions"));
            hints.push(("\u{2191}\u{2193} j/k", "navigate"));
            hints.push(("Enter", "details"));
            hints.push(("/", "filter"));
        }
        hints
    }

    fn text_input_active(&self) -> bool {
        self.filter.is_active()
    }

    impl_attach_toasts!(toasts);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        backend::{StorageBackend, mock::MockBackend},
        common::defaults::TEST_REPO_CONFIG,
        repository::repo::{Auth, THIS_REPOSITORY_VERSION},
    };
    use crossterm::event::KeyModifiers;
    use zeroize::Zeroizing;

    fn render_header(screen: &DashboardScreen, width: u16) -> String {
        crate::ui::tui::test_support::render_text(width, HEADER_HEIGHT, |frame| {
            screen.render_top_bar(frame, frame.area());
        })
    }

    async fn test_screen() -> Result<DashboardScreen> {
        let auth = Auth {
            username: "test".to_string(),
            password: Zeroizing::new("password".to_string()),
        };
        let backend: Arc<dyn StorageBackend> = Arc::new(MockBackend::new());
        Repository::init(
            THIS_REPOSITORY_VERSION,
            &auth,
            None,
            backend.clone(),
            None,
            false,
        )
        .await?;
        let (repo, _, handle) =
            Repository::try_open_with_lock(&auth, None, backend, TEST_REPO_CONFIG, false, None)
                .await?;
        Ok(DashboardScreen::new(
            repo,
            Some(handle),
            "local:/a/long/repository/path".to_string(),
            "123456789abc".to_string(),
            None,
            None,
        ))
    }

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn menu_text(lines: &[Line<'static>]) -> String {
        lines
            .iter()
            .flat_map(|line| line.spans.iter())
            .map(|span| span.content.as_ref())
            .collect()
    }

    #[test]
    fn menu_wraps_and_keeps_every_action_visible_at_80_cols() {
        for width in [76u16, 40] {
            let lines = menu_lines(MENU_ITEMS, 0, false, width);
            assert!(lines.len() >= 2, "menu should wrap at {width} cols");
            for line in &lines {
                let rendered: String = line
                    .spans
                    .iter()
                    .map(|span| span.content.as_ref())
                    .collect();
                assert!(
                    rendered.chars().count() <= width as usize,
                    "menu row exceeds {width} cols: {rendered:?}"
                );
            }
            // Every command, including the newest screens, stays reachable.
            let text = menu_text(&lines);
            for label in ["Stats", "Clean", "Verify", "Refresh", "Quit"] {
                assert!(text.contains(label), "missing {label} at {width} cols");
            }
        }
    }

    #[test]
    fn menu_highlights_focused_action() {
        let unfocused = menu_lines(MENU_ITEMS, 5, false, 76);
        assert!(
            !unfocused
                .iter()
                .flat_map(|line| line.spans.iter())
                .any(|span| span.style == theme::THEME.menu_selected),
            "unfocused menu must not highlight any action"
        );

        let focused = menu_lines(MENU_ITEMS, 5, true, 76);
        assert!(
            focused
                .iter()
                .flat_map(|line| line.spans.iter())
                .any(|span| span.style == theme::THEME.menu_selected
                    && span.content.contains("Stats")),
            "focused action must carry the selection style"
        );
    }

    #[test]
    fn accelerator_keys_map_to_actions() {
        assert_eq!(action_for_key('1'), Some(MenuAction::Snapshot));
        assert_eq!(action_for_key('6'), Some(MenuAction::Stats));
        assert_eq!(action_for_key('8'), Some(MenuAction::Verify));
        assert_eq!(action_for_key('u'), Some(MenuAction::Recall));
        assert_eq!(action_for_key('r'), Some(MenuAction::Refresh));
        assert_eq!(action_for_key('q'), Some(MenuAction::Quit));
        assert_eq!(action_for_key('/'), None);
        assert_eq!(action_for_key('x'), None);
    }

    fn min_total(widths: &[Constraint]) -> u16 {
        widths
            .iter()
            .map(|constraint| match constraint {
                Constraint::Length(value) | Constraint::Min(value) => *value,
                _ => 0,
            })
            .sum()
    }

    #[test]
    fn snapshot_table_fits_and_drops_tags_when_narrow() {
        let (wide, show_tags) = snapshot_columns(80);
        assert!(show_tags, "Tags should be shown on an 80-column terminal");
        assert!(
            min_total(&wide) <= 80 - 4,
            "columns overflow an 80-column frame: {}",
            min_total(&wide)
        );

        let (narrow, show_tags) = snapshot_columns(60);
        assert!(!show_tags, "Tags should be dropped on a narrow terminal");
        assert!(
            min_total(&narrow) <= 60 - 4,
            "columns overflow a 60-column frame: {}",
            min_total(&narrow)
        );
    }

    #[tokio::test]
    async fn tab_focuses_menu_and_arrows_move_cursor() -> Result<()> {
        let mut screen = test_screen().await?;
        assert!(!screen.menu_focused);
        let _ = screen.handle_key(key(KeyCode::Tab)).await;
        assert!(screen.menu_focused);
        let start = screen.menu_cursor;
        let _ = screen.handle_key(key(KeyCode::Right)).await;
        assert_eq!(screen.menu_cursor, start + 1);
        let _ = screen.handle_key(key(KeyCode::Left)).await;
        assert_eq!(screen.menu_cursor, start);
        let _ = screen.handle_key(key(KeyCode::Esc)).await;
        assert!(!screen.menu_focused);
        Ok(())
    }

    #[tokio::test]
    async fn enter_runs_focused_menu_action() -> Result<()> {
        let mut screen = test_screen().await?;
        let _ = screen.handle_key(key(KeyCode::Tab)).await;
        let entries = screen.menu_entries();
        assert_eq!(
            entries.last().map(|item| item.action),
            Some(MenuAction::Quit)
        );
        screen.menu_cursor = entries.len() - 1;
        let transition = screen.handle_key(key(KeyCode::Enter)).await;
        assert!(matches!(transition, Some(Transition::Quit)));
        Ok(())
    }

    #[tokio::test]
    async fn q_accelerator_still_quits() -> Result<()> {
        let mut screen = test_screen().await?;
        let transition = screen.handle_key(key(KeyCode::Char('q'))).await;
        assert!(matches!(transition, Some(Transition::Quit)));
        Ok(())
    }

    #[tokio::test]
    async fn dashboard_render_shows_all_commands_at_80x24() -> Result<()> {
        let mut screen = test_screen().await?;
        let content = crate::ui::tui::test_support::render_text(80, 24, |frame| {
            screen.render(frame);
        });
        for label in ["Stats", "Clean", "Verify", "Refresh", "Quit"] {
            assert!(
                content.contains(label),
                "missing {label} at 80x24: {content}"
            );
        }
        assert!(
            content.contains("No snapshots yet"),
            "empty dashboard should guide a new user: {content}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn header_shows_session_lock_status_at_narrow_widths() -> Result<()> {
        let auth = Auth {
            username: "test".to_string(),
            password: Zeroizing::new("password".to_string()),
        };
        for (exclusive, expected) in [(false, "lock shared"), (true, "lock exclusive")] {
            let backend: Arc<dyn StorageBackend> = Arc::new(MockBackend::new());
            Repository::init(
                THIS_REPOSITORY_VERSION,
                &auth,
                None,
                backend.clone(),
                None,
                false,
            )
            .await?;
            let (repo, _, handle) = Repository::try_open_with_lock(
                &auth,
                None,
                backend,
                TEST_REPO_CONFIG,
                exclusive,
                None,
            )
            .await?;
            let mut screen = DashboardScreen::new(
                repo,
                Some(handle.clone()),
                "local:/a/long/repository/path".to_string(),
                "123456789abc".to_string(),
                None,
                None,
            );
            screen.stats.total = 123456;
            for width in [40, 80, 120] {
                let text = render_header(&screen, width);
                assert!(
                    text.ends_with(expected),
                    "missing {expected} at width {width}: {text}"
                );
                assert!(text.contains("123456 snapshots"));
            }
            handle.unlock().await;
            assert_eq!(
                screen.lock_handle.as_ref().unwrap().status(),
                LockStatus::Released
            );
            assert!(render_header(&screen, 40).ends_with("lock released"));
            screen.lock_handle = None;
            assert!(render_header(&screen, 40).ends_with("lock off"));
        }
        Ok(())
    }
}
