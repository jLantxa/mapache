use std::{path::PathBuf, sync::Arc};

use async_trait::async_trait;
use crossterm::event::{KeyCode, KeyEvent};
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Margin, Rect},
    style::{Modifier, Style},
    text::{Line, Span},
    widgets::{Block, BorderType, Borders, List, ListItem, ListState, Paragraph},
};
use tokio::sync::mpsc;

use crate::{
    fs::node::Node,
    mapache::{defaults::SHORT_SNAPSHOT_ID_LEN, find_in_snapshot},
    repository::{
        repo::Repository,
        snapshot::{SnapshotEntry, SnapshotEntryList},
    },
    ui::tui::{
        app::{Screen, Transition},
        screens::{file_explorer::FileExplorerScreen, restore::RestoreScreen},
        theme,
        widgets::{StateNavigation, TextInput},
    },
    utils,
};

enum Focus {
    Input,
    Results,
}

struct FindResult {
    snapshot_entry: SnapshotEntry,
    path: PathBuf,
    node: Node,
}

enum SearchUpdate {
    Progress {
        current: usize,
        total: usize,
        matches: usize,
        host: String,
        id: String,
    },
    Done(Vec<FindResult>),
}

const SPINNER_CHARS: &[char] = &['\u{25D0}', '\u{25D3}', '\u{25D1}', '\u{25D2}'];

pub struct FindScreen {
    repo: Arc<Repository>,
    focus: Focus,
    search_input: TextInput,
    results: Vec<FindResult>,
    filtered_indices: Vec<usize>,
    list_state: ListState,
    is_searching: bool,
    search_rx: Option<mpsc::UnboundedReceiver<SearchUpdate>>,
    search_progress: Option<(usize, usize, usize)>,
    status_message: String,
    last_height: usize,
    spinner_tick: u8,
    snapshots: Arc<SnapshotEntryList>,
}

impl FindScreen {
    pub fn new(repo: Arc<Repository>, snapshots: Arc<SnapshotEntryList>) -> Self {
        Self {
            repo,
            snapshots,
            focus: Focus::Input,
            search_input: TextInput::new(),
            results: Vec::new(),
            filtered_indices: Vec::new(),
            list_state: ListState::default(),
            is_searching: false,
            search_rx: None,
            search_progress: None,
            status_message: "Type a glob pattern and press Enter to search".to_string(),
            last_height: 0,
            spinner_tick: 0,
        }
    }

    fn start_search(&mut self) {
        let pattern = self.search_input.text().to_string();
        if pattern.is_empty() {
            self.status_message = "Enter a pattern to search".to_string();
            return;
        }

        let (tx, rx) = mpsc::unbounded_channel();
        self.search_rx = Some(rx);
        self.search_progress = None;
        self.is_searching = true;
        self.spinner_tick = 0;
        self.status_message = format!("Searching for '{}'...", pattern);
        self.results.clear();
        self.filtered_indices.clear();
        self.focus = Focus::Input;

        let repo = self.repo.clone();
        let entries = self.snapshots.clone();
        tokio::spawn(async move {
            let total = entries.len();
            let mut all_results = Vec::new();

            for (i, entry) in entries.iter().enumerate() {
                match find_in_snapshot(repo.clone(), &entry.snapshot, &pattern).await {
                    Ok(found) => {
                        for (path, node) in found {
                            all_results.push(FindResult {
                                snapshot_entry: entry.clone(),
                                path,
                                node,
                            });
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Search failed in snapshot {}: {}", entry.id, e);
                    }
                }

                let _ = tx.send(SearchUpdate::Progress {
                    current: i + 1,
                    total,
                    matches: all_results.len(),
                    host: entry
                        .snapshot
                        .hostname
                        .as_deref()
                        .unwrap_or("-")
                        .to_string(),
                    id: entry.id.to_short_hex(SHORT_SNAPSHOT_ID_LEN),
                });
            }

            let _ = tx.send(SearchUpdate::Done(all_results));
        });
    }

    fn check_search_updates(&mut self) {
        let mut rx = match self.search_rx.take() {
            Some(rx) => rx,
            None => return,
        };

        while let Ok(update) = rx.try_recv() {
            match update {
                SearchUpdate::Progress {
                    current,
                    total,
                    matches,
                    host,
                    id,
                } => {
                    self.search_progress = Some((current, total, matches));
                    self.status_message = format!(
                        "[{}/{}] {} {} - {}",
                        current,
                        total,
                        id,
                        host,
                        utils::format_count(matches, "match", "matches")
                    );
                }
                SearchUpdate::Done(all_results) => {
                    self.results = all_results;
                    self.filtered_indices = (0..self.results.len()).collect();
                    if !self.results.is_empty() {
                        self.list_state.select(Some(0));
                        self.focus = Focus::Results;
                    }
                    let count = self.results.len();
                    let snap_count = self.search_progress.map(|(_, t, _)| t).unwrap_or(0);
                    self.status_message = if count == 0 {
                        "No matches found. Try a different pattern.".to_string()
                    } else {
                        format!("{} match(es) in {} snapshot(s)", count, snap_count)
                    };
                    self.is_searching = false;
                    self.search_progress = None;
                    return;
                }
            }
        }

        self.search_rx = Some(rx);
    }

    fn selected_result(&self) -> Option<&FindResult> {
        let display_idx = self.list_state.selected()?;
        let orig_idx = self.filtered_indices.get(display_idx)?;
        self.results.get(*orig_idx)
    }

    fn display_len(&self) -> usize {
        self.filtered_indices.len()
    }

    fn render_search_bar(&self, frame: &mut Frame, area: Rect) {
        let input = &self.search_input;
        let text = input.text();
        let cursor = input.cursor();
        let is_focused = matches!(self.focus, Focus::Input) && !self.is_searching;

        let (before, at_cursor, after) = if text.is_empty() && !is_focused {
            (
                String::new(),
                String::new(),
                " Press / to search".to_string(),
            )
        } else {
            let before: String = text.chars().take(cursor).collect();
            let rest: String = text.chars().skip(cursor).collect();
            let at = rest.chars().take(1).collect();
            let after: String = rest.chars().skip(1).collect();
            (before, at, after)
        };

        let mut spans = vec![Span::raw("> ")];
        if text.is_empty() && !is_focused {
            spans.push(Span::styled(&after, theme::THEME.subtext_dim));
        } else {
            spans.push(Span::raw(&before));
            if is_focused {
                if at_cursor.is_empty() {
                    spans.push(Span::styled(" ", Style::default().underlined()));
                } else {
                    spans.push(Span::styled(&at_cursor, Style::default().underlined()));
                }
            } else {
                spans.push(Span::raw(&at_cursor));
            }
            spans.push(Span::raw(&after));
        }

        let border_style = if is_focused {
            theme::THEME.border_focused
        } else {
            theme::THEME.border
        };

        let block = Block::default()
            .style(Style::new().bg(theme::THEME.bg))
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(border_style)
            .title(" Pattern ")
            .title_style(theme::THEME.header);

        frame.render_widget(Paragraph::new(Line::from(spans)).block(block), area);
    }

    fn render_status(&self, frame: &mut Frame, area: Rect) {
        let spinner = SPINNER_CHARS[(self.spinner_tick % 4) as usize];

        let (msg, style) = if self.is_searching {
            let msg = format!(" {} {}", spinner, self.status_message);
            (msg, theme::THEME.info)
        } else if self.results.is_empty() && !self.status_message.contains("Type") {
            (self.status_message.clone(), theme::THEME.warning)
        } else {
            (self.status_message.clone(), theme::THEME.footer)
        };

        let widget = Paragraph::new(Line::from(Span::styled(msg, style)))
            .block(Block::default().style(Style::new().bg(theme::THEME.bg)));
        frame.render_widget(widget, area);
    }

    fn render_results(&mut self, frame: &mut Frame, area: Rect) {
        if self.results.is_empty() {
            return;
        }

        let max_rows = area.height.saturating_sub(2) as usize;
        let display_len = self.display_len();

        let title = format!(" Results ({}) ", display_len);

        let items: Vec<ListItem<'_>> = self
            .filtered_indices
            .iter()
            .map(|&orig_idx| {
                let result = &self.results[orig_idx];
                let node = &result.node;
                let path_str = result.path.display().to_string();

                let icon = if node.is_dir() {
                    "\u{25B6} "
                } else if node.is_symlink() {
                    "\u{21C4} "
                } else {
                    "  "
                };

                let name_style = if node.is_dir() {
                    Style::default()
                        .fg(theme::THEME.dir_fg)
                        .add_modifier(Modifier::BOLD)
                } else if node.is_symlink() {
                    Style::default().fg(theme::THEME.symlink_fg)
                } else {
                    Style::default().fg(theme::THEME.file_fg)
                };

                let size_str = if node.is_file() {
                    format!("{:>13}", utils::format_size_binary(node.metadata.size, 3))
                } else {
                    format!("{:>13}", "")
                };

                let id_short = result.snapshot_entry.id.to_short_hex(SHORT_SNAPSHOT_ID_LEN);
                let host = result
                    .snapshot_entry
                    .snapshot
                    .hostname
                    .as_deref()
                    .unwrap_or("-");

                ListItem::new(Line::from(vec![
                    Span::raw(icon),
                    Span::styled(path_str, name_style),
                    Span::raw("    "),
                    Span::styled(id_short, theme::THEME.snap_id),
                    Span::raw("    "),
                    Span::styled(host, theme::THEME.snap_host),
                    Span::raw("    "),
                    Span::styled(size_str, theme::THEME.file_size),
                ]))
            })
            .collect();

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

    fn render_detail(&self, frame: &mut Frame, area: Rect) {
        let Some(result) = self.selected_result() else {
            return;
        };
        let node = &result.node;
        let snap = &result.snapshot_entry.snapshot;

        let mut lines = Vec::with_capacity(3);
        let lw = 10;

        let type_str = if node.is_dir() {
            "Directory"
        } else if node.is_symlink() {
            "Symlink"
        } else {
            "File"
        };

        lines.push(Line::from(vec![
            Span::styled(format!("{:lw$}", "Type", lw = lw), theme::THEME.menu_key),
            Span::styled(type_str, theme::THEME.snap_host),
            Span::raw("  "),
            Span::styled(format!("{:lw$}", "Size", lw = lw), theme::THEME.menu_key),
            Span::styled(
                utils::format_size_binary(node.metadata.size, 2),
                theme::THEME.snap_size,
            ),
        ]));

        let ts = utils::pretty_print_timestamp(&snap.timestamp, None);
        lines.push(Line::from(vec![
            Span::styled(
                format!("{:lw$}", "Snapshot", lw = lw),
                theme::THEME.menu_key,
            ),
            Span::styled(
                result.snapshot_entry.id.to_short_hex(12),
                theme::THEME.snap_id,
            ),
            Span::raw(" @ "),
            Span::styled(
                snap.hostname.as_deref().unwrap_or("?"),
                theme::THEME.snap_host,
            ),
            Span::raw(" "),
            Span::styled(ts, theme::THEME.snap_date),
        ]));

        lines.push(Line::from(vec![
            Span::styled(format!("{:lw$}", "Actions", lw = lw), theme::THEME.menu_key),
            Span::styled("r", theme::THEME.menu_key),
            Span::raw(" restore  "),
            Span::styled("Enter", theme::THEME.menu_key),
            Span::raw(" browse  "),
            Span::styled("/", theme::THEME.menu_key),
            Span::raw(" new search"),
        ]));

        let widget = Paragraph::new(lines).block(theme::block("Details"));
        frame.render_widget(widget, area);
    }

    fn render_hints(&self, frame: &mut Frame, area: Rect) {
        let hints = if self.is_searching {
            theme::key_hint_footer(&[("Esc", "waiting...")])
        } else {
            match self.focus {
                Focus::Input => {
                    theme::key_hint_footer(&[("Esc", "back"), ("Enter", "search"), ("q", "quit")])
                }
                Focus::Results => theme::key_hint_footer(&[
                    ("Esc", "back"),
                    ("\u{2191}\u{2193}", "navigate"),
                    ("Enter", "browse"),
                    ("r", "restore"),
                    ("/", "search"),
                    ("q", "quit"),
                ]),
            }
        };
        frame.render_widget(Paragraph::new(hints), area);
    }
}

#[async_trait]
impl Screen for FindScreen {
    fn render(&mut self, frame: &mut Frame) {
        self.check_search_updates();
        self.spinner_tick = self.spinner_tick.wrapping_add(1);

        let area = frame.area();
        let inner = area.inner(Margin::new(2, 1));

        let has_results = !self.results.is_empty();
        let detail_height = if has_results && self.selected_result().is_some() {
            5u16
        } else {
            0u16
        };

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1),
                Constraint::Length(3),
                Constraint::Length(1),
                Constraint::Min(5),
                Constraint::Length(detail_height),
                Constraint::Length(1),
            ])
            .split(inner);

        self.last_height = chunks[3].height.saturating_sub(2) as usize;

        frame.render_widget(
            Paragraph::new(Line::from(Span::styled(
                " Find in snapshots ",
                theme::THEME.header,
            ))),
            chunks[0],
        );
        self.render_search_bar(frame, chunks[1]);
        self.render_status(frame, chunks[2]);
        self.render_results(frame, chunks[3]);

        if detail_height > 0 {
            self.render_detail(frame, chunks[4]);
        }

        self.render_hints(frame, chunks[5]);
    }

    async fn handle_key(&mut self, key: KeyEvent) -> Option<Transition> {
        if self.is_searching {
            return None;
        }

        match self.focus {
            Focus::Input => match key.code {
                KeyCode::Esc => {
                    if self.results.is_empty() {
                        Some(Transition::Pop)
                    } else {
                        self.focus = Focus::Results;
                        None
                    }
                }
                KeyCode::Enter => {
                    self.start_search();
                    None
                }
                KeyCode::Char('q') => Some(Transition::Quit),
                _ => {
                    self.search_input.handle_key(key.code);
                    None
                }
            },
            Focus::Results => match key.code {
                KeyCode::Esc => Some(Transition::Pop),
                KeyCode::Char('q') => Some(Transition::Quit),
                KeyCode::Char('/') => {
                    self.focus = Focus::Input;
                    None
                }
                KeyCode::Enter => {
                    if let Some(result) = self.selected_result() {
                        let entry = result.snapshot_entry.clone();
                        let tree_id = entry.snapshot.tree;
                        match FileExplorerScreen::new(self.repo.clone(), entry, &tree_id).await {
                            Ok(explorer) => Some(Transition::Push(Box::new(explorer))),
                            Err(e) => {
                                tracing::error!("Failed to open file explorer: {}", e);
                                None
                            }
                        }
                    } else {
                        None
                    }
                }
                KeyCode::Char('r') => {
                    if let Some(result) = self.selected_result() {
                        Some(Transition::Push(Box::new(RestoreScreen::new(
                            self.repo.clone(),
                            result.snapshot_entry.clone(),
                            Some(vec![result.path.clone()]),
                        ))))
                    } else {
                        None
                    }
                }
                KeyCode::Down => {
                    self.list_state.next(self.display_len());
                    None
                }
                KeyCode::Up => {
                    self.list_state.previous(self.display_len());
                    None
                }
                KeyCode::PageDown => {
                    self.list_state
                        .page_next(self.display_len(), self.last_height);
                    None
                }
                KeyCode::PageUp => {
                    self.list_state
                        .page_previous(self.display_len(), self.last_height);
                    None
                }
                KeyCode::Home => {
                    self.list_state.home(self.display_len());
                    None
                }
                KeyCode::End => {
                    self.list_state.end(self.display_len());
                    None
                }
                _ => None,
            },
        }
    }
}
