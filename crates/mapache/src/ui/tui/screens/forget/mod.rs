pub(crate) mod retention;

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Local;
use crossterm::event::{KeyCode, KeyEvent};
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout},
    style::Style,
    text::{Line, Span, Text},
    widgets::{Paragraph, Row, Table, TableState},
};
pub use retention::{RetentionAction, RetentionConfig};

use crate::{
    common::defaults::SHORT_SNAPSHOT_ID_LEN,
    repository::{
        repo::{REPO_DROPPED_EXTENSION, Repository},
        retention::apply_retention_rules,
        snapshot::SnapshotEntryList,
    },
    ui::tui::{
        app::{Screen, Transition},
        theme,
        widgets::{Dialog, FormFieldType, StateNavigation},
    },
    utils,
};

#[derive(Debug, Clone, Copy, PartialEq)]
enum ForgetPhase {
    Selection,
    Retention,
    Confirm,
    Result,
}

struct ForgetSelection {
    bits: Vec<bool>,
}

impl ForgetSelection {
    fn new(len: usize) -> Self {
        Self {
            bits: vec![false; len],
        }
    }

    fn set(&mut self, idx: usize, val: bool) {
        if idx < self.bits.len() {
            self.bits[idx] = val;
        }
    }

    fn get(&self, idx: usize) -> bool {
        self.bits.get(idx).copied().unwrap_or(false)
    }

    fn toggle_all(&mut self) {
        let all_set = self.bits.iter().all(|&v| v);
        for bit in &mut self.bits {
            *bit = !all_set;
        }
    }

    fn count_selected(&self) -> usize {
        self.bits.iter().filter(|&&v| v).count()
    }
}

enum ForgetAction {
    None,
    Quit,
    Pop,
    ExecuteForget,
}

enum ForgetResult {
    Success { removed_count: usize },
    NoDeleted,
}

pub struct ForgetScreen {
    repo: Arc<Repository>,
    phase: ForgetPhase,
    entries: Arc<SnapshotEntryList>,
    selected: ForgetSelection,
    table_state: TableState,
    retention: RetentionConfig,
    result: Option<ForgetResult>,
}

impl ForgetScreen {
    pub fn new(
        repo: Arc<Repository>,
        entries: Arc<SnapshotEntryList>,
        config: Option<crate::commands::cmd_forget::CmdArgs>,
    ) -> Self {
        let len = entries.len();
        let mut selected = ForgetSelection::new(len);

        let retention = RetentionConfig::new(config.as_ref());
        let rules = retention.to_rules();

        if !rules.is_empty() {
            let mut sorted_indices: Vec<_> = (0..entries.len()).collect();
            sorted_indices.sort_unstable_by_key(|&i| entries[i].snapshot.timestamp);
            let sorted_refs: Vec<_> = sorted_indices.iter().map(|&i| &entries[i]).collect();
            let keep_ids = apply_retention_rules(&sorted_refs, &rules, None, Local::now());
            for (i, entry) in entries.iter().enumerate() {
                selected.set(i, !keep_ids.contains(&entry.id));
            }
        }

        let mut table_state = TableState::default();
        if !entries.is_empty() {
            table_state.select(Some(0));
        }

        Self {
            repo,
            phase: ForgetPhase::Selection,
            entries,
            selected,
            table_state,
            retention,
            result: None,
        }
    }

    fn selected_count(&self) -> usize {
        self.selected.count_selected()
    }

    async fn execute_forget(&mut self) {
        let to_remove: Vec<_> = self
            .selected
            .bits
            .iter()
            .enumerate()
            .filter_map(|(i, &v)| if v { Some(i) } else { None })
            .collect();

        if to_remove.is_empty() {
            self.result = Some(ForgetResult::NoDeleted);
        } else {
            let mut removed_count = 0;
            for idx in &to_remove {
                if let Some(entry) = self.entries.get(*idx) {
                    if let Err(e) = self
                        .repo
                        .set_extension(
                            crate::common::ContentIdType::Snapshot,
                            &entry.id,
                            Some(REPO_DROPPED_EXTENSION),
                        )
                        .await
                    {
                        tracing::error!("Failed to forget snapshot {}: {}", entry.id.to_hex(), e);
                    } else {
                        removed_count += 1;
                    }
                }
            }
            if removed_count == 0 {
                self.result = Some(ForgetResult::NoDeleted);
            } else {
                self.result = Some(ForgetResult::Success { removed_count });
            }
        }
        self.phase = ForgetPhase::Result;
    }

    fn handle_selection_key(&mut self, key: KeyEvent) -> ForgetAction {
        match key.code {
            KeyCode::Esc => ForgetAction::Pop,
            KeyCode::Char('q') => ForgetAction::Quit,
            KeyCode::Char(' ') => {
                if let Some(idx) = self.table_state.selected()
                    && idx < self.entries.len()
                {
                    let current = self.selected.get(idx);
                    self.selected.set(idx, !current);
                }
                ForgetAction::None
            }
            KeyCode::Enter => {
                let count = self.selected_count();
                if count > 0 {
                    self.phase = ForgetPhase::Confirm;
                } else {
                    return ForgetAction::ExecuteForget;
                }
                ForgetAction::None
            }
            KeyCode::Char('r') => {
                self.phase = ForgetPhase::Retention;
                ForgetAction::None
            }
            KeyCode::Char('a') => {
                self.selected.toggle_all();
                ForgetAction::None
            }
            key if self
                .table_state
                .handle_nav_keys(key, self.entries.len(), 10) =>
            {
                ForgetAction::None
            }
            _ => ForgetAction::None,
        }
    }

    fn handle_retention_key(&mut self, key: KeyEvent) -> ForgetAction {
        match self.retention.handle_key(key.code) {
            RetentionAction::Apply => {
                let rules = self.retention.to_rules();
                let mut sorted_indices: Vec<_> = (0..self.entries.len()).collect();
                sorted_indices.sort_unstable_by_key(|&i| self.entries[i].snapshot.timestamp);
                let sorted_refs: Vec<_> =
                    sorted_indices.iter().map(|&i| &self.entries[i]).collect();
                let keep_ids = apply_retention_rules(&sorted_refs, &rules, None, Local::now());
                for (i, entry) in self.entries.iter().enumerate() {
                    self.selected.set(i, !keep_ids.contains(&entry.id));
                }
                self.phase = ForgetPhase::Selection;
                ForgetAction::None
            }
            RetentionAction::Cancel => {
                self.phase = ForgetPhase::Selection;
                ForgetAction::None
            }
            RetentionAction::None => match key.code {
                KeyCode::Char('0') => {
                    // Reset form fields
                    for field in self.retention.form.fields_mut() {
                        if let FormFieldType::Text(ref mut input) = field.field_type {
                            input.clear();
                        }
                    }
                    ForgetAction::None
                }
                KeyCode::Char('q') => ForgetAction::Quit,
                _ => ForgetAction::None,
            },
        }
    }

    fn handle_confirm_key(&mut self, key: KeyEvent) -> ForgetAction {
        match key.code {
            KeyCode::Esc => {
                self.phase = ForgetPhase::Selection;
                ForgetAction::None
            }
            KeyCode::Enter => ForgetAction::ExecuteForget,
            KeyCode::Char('q') => ForgetAction::Quit,
            _ => ForgetAction::None,
        }
    }

    fn render_selection(&mut self, frame: &mut Frame) {
        let area = frame.area();
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Min(3),
                Constraint::Length(2),
            ])
            .split(area.inner(theme::CONTENT_MARGIN));

        let selected = self.selected_count();
        let header = Paragraph::new(format!(
            "Forget Snapshots\nSelected: {} / {}",
            selected,
            self.entries.len()
        ))
        .style(theme::THEME.header);
        frame.render_widget(header, chunks[0]);

        let rows: Vec<Row> = self
            .entries
            .iter()
            .enumerate()
            .map(|(idx, e)| {
                let is_selected = self.selected.get(idx);
                let selected_str = if is_selected { "[X]" } else { "[ ]" };
                let id = e.id.to_short_hex(SHORT_SNAPSHOT_ID_LEN);
                let date = utils::pretty_print_timestamp(&e.snapshot.timestamp, None);
                let host = e.snapshot.hostname.as_deref().unwrap_or("");
                let tags = theme::format_tags(&e.snapshot.tags);

                let style = if is_selected {
                    Style::default().fg(theme::THEME.red)
                } else {
                    Style::default()
                };

                Row::new(vec![
                    Span::styled(selected_str, style),
                    Span::styled(id, theme::THEME.snap_id),
                    Span::styled(date, theme::THEME.snap_date),
                    Span::styled(host, theme::THEME.snap_host),
                    Span::raw(tags),
                ])
            })
            .collect();

        let table = Table::new(
            rows,
            vec![
                Constraint::Length(5),
                Constraint::Length(12),
                Constraint::Length(30),
                Constraint::Length(15),
                Constraint::Min(20),
            ],
        )
        .header(Row::new(vec!["", "ID", "Date", "Host", "Tags"]).style(theme::THEME.header))
        .block(theme::block("Snapshots"))
        .row_highlight_style(theme::THEME.selection);

        frame.render_stateful_widget(table, chunks[1], &mut self.table_state);
        theme::render_scrollbar(
            frame,
            chunks[1],
            self.entries.len(),
            self.table_state.selected().unwrap_or(0),
        );

        let footer = theme::key_hint_footer(&[
            ("Space", "toggle"),
            ("Enter", "confirm"),
            ("r", "retention"),
            ("a", "toggle all"),
            ("Esc", "back"),
            ("q", "quit"),
        ]);
        frame.render_widget(Paragraph::new(footer), chunks[2]);
    }

    fn render_confirm(&self, frame: &mut Frame) {
        let text = vec![
            Line::from(vec![
                Span::raw("You are about to forget "),
                Span::styled(self.selected_count().to_string(), theme::THEME.error),
                Span::raw(" snapshots."),
            ]),
            Line::from(""),
            Line::from(Span::styled(
                "THIS ACTION IS NOT EASILY REVERSIBLE.",
                theme::THEME.error,
            )),
            Line::from(""),
            Line::from(vec![
                Span::styled("[Enter]", theme::THEME.menu_key),
                Span::raw(" to proceed, "),
                Span::styled("[Esc]", theme::THEME.menu_key),
                Span::raw(" to cancel"),
            ]),
        ];

        Dialog::with_text("Confirm Forget", theme::THEME.border, Text::from(text))
            .render(frame.area(), frame);
    }

    fn render_result(&self, frame: &mut Frame) {
        let (title, text) = match self.result {
            Some(ForgetResult::Success { removed_count }) => (
                "Forget Result",
                vec![
                    Line::from(Span::styled("SUCCESS", theme::THEME.success)),
                    Line::from(""),
                    Line::from(format!("Successfully forgot {} snapshots.", removed_count)),
                    Line::from(""),
                    Line::from(vec![
                        Span::styled("[Enter/Esc]", theme::THEME.menu_key),
                        Span::raw(" back to dashboard"),
                    ]),
                ],
            ),
            Some(ForgetResult::NoDeleted) => (
                "Forget Result",
                vec![
                    Line::from(Span::styled("NO SNAPSHOTS REMOVED", theme::THEME.warning)),
                    Line::from(""),
                    Line::from("No snapshots were selected or removed."),
                    Line::from(""),
                    Line::from(vec![
                        Span::styled("[Enter/Esc]", theme::THEME.menu_key),
                        Span::raw(" back to selection"),
                    ]),
                ],
            ),
            None => ("Forget Result", vec![Line::from("Unknown state")]),
        };

        Dialog::with_text(title, theme::THEME.border, Text::from(text)).render(frame.area(), frame);
    }
}

#[async_trait]
impl Screen for ForgetScreen {
    fn render(&mut self, frame: &mut Frame) {
        match self.phase {
            ForgetPhase::Selection => self.render_selection(frame),
            ForgetPhase::Retention => {
                let area = frame.area();
                let inner = area.inner(theme::CONTENT_MARGIN);
                self.retention.render(frame, inner);
            }
            ForgetPhase::Confirm => {
                self.render_selection(frame);
                self.render_confirm(frame);
            }
            ForgetPhase::Result => self.render_result(frame),
        }
    }

    async fn handle_key(&mut self, key: KeyEvent) -> Option<Transition> {
        let action = match self.phase {
            ForgetPhase::Selection => self.handle_selection_key(key),
            ForgetPhase::Retention => self.handle_retention_key(key),
            ForgetPhase::Confirm => self.handle_confirm_key(key),
            ForgetPhase::Result => match key.code {
                KeyCode::Enter | KeyCode::Esc => {
                    if let Some(ForgetResult::Success { .. }) = self.result {
                        ForgetAction::Pop
                    } else {
                        self.phase = ForgetPhase::Selection;
                        ForgetAction::None
                    }
                }
                KeyCode::Char('q') => ForgetAction::Quit,
                _ => ForgetAction::None,
            },
        };

        match action {
            ForgetAction::None => None,
            ForgetAction::Quit => Some(Transition::Quit),
            ForgetAction::Pop => Some(Transition::Pop),
            ForgetAction::ExecuteForget => {
                self.execute_forget().await;
                None
            }
        }
    }
}
