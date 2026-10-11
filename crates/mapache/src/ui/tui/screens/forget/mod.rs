pub(crate) mod retention;

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Local;
use crossterm::event::{KeyCode, KeyEvent, MouseButton, MouseEvent};
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::Style,
    text::{Line, Span, Text},
    widgets::{Paragraph, Row, Table, TableState},
};
pub use retention::{RetentionAction, RetentionConfig};

use crate::{
    commands::cmd_forget,
    common::{ContentIdType, defaults::SHORT_SNAPSHOT_ID_LEN},
    repository::{
        repo::{REPO_DROPPED_EXTENSION, Repository},
        retention::build_forget_plan,
        snapshot::SnapshotEntryList,
    },
    ui::tui::{
        app::{Screen, Transition},
        theme,
        widgets::{
            Dialog, FormFieldType, StateNavigation, ToastSink, click_to_index, impl_attach_toasts,
        },
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
        self.bits.fill(!all_set);
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
    last_table_area: Rect,
    retention: RetentionConfig,
    result: Option<ForgetResult>,
    toasts: ToastSink,
}

impl ForgetScreen {
    pub fn new(
        repo: Arc<Repository>,
        entries: Arc<SnapshotEntryList>,
        config: Option<cmd_forget::CmdArgs>,
    ) -> Self {
        let mut table_state = TableState::default();
        if !entries.is_empty() {
            table_state.select(Some(0));
        }

        let mut screen = Self {
            repo,
            phase: ForgetPhase::Selection,
            selected: ForgetSelection::new(entries.len()),
            entries,
            table_state,
            last_table_area: Rect::default(),
            retention: RetentionConfig::new(config.as_ref()),
            result: None,
            toasts: ToastSink::default(),
        };
        screen.apply_retention_to_selection();
        screen
    }

    /// Recomputes the selection from the retention form using the same plan the
    /// CLI uses, so host/tag filters and retention rules behave identically in
    /// both front ends.
    fn apply_retention_to_selection(&mut self) {
        let rules = self.retention.to_rules();
        let filter = self.retention.filter();
        let refs: Vec<&_> = self.entries.iter().collect();
        let plan = build_forget_plan(
            &refs,
            &filter,
            &rules,
            self.retention.keep_min(),
            Local::now(),
        );

        for (i, entry) in self.entries.iter().enumerate() {
            self.selected.set(i, plan.remove.contains(&entry.id));
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
                    let result = if self.retention.force() {
                        self.repo
                            .delete_file(ContentIdType::Snapshot, &entry.id, None)
                            .await
                            .map(|_| ())
                    } else {
                        self.repo
                            .set_extension(
                                ContentIdType::Snapshot,
                                &entry.id,
                                Some(REPO_DROPPED_EXTENSION),
                            )
                            .await
                    };
                    if let Err(e) = result {
                        tracing::error!("Failed to forget snapshot {}: {}", entry.id.to_hex(), e);
                        self.toasts.error(format!(
                            "Failed to forget snapshot {}: {e}",
                            entry.id.to_hex()
                        ));
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
                self.apply_retention_to_selection();
                self.phase = ForgetPhase::Selection;
                ForgetAction::None
            }
            RetentionAction::Cancel => {
                self.phase = ForgetPhase::Selection;
                ForgetAction::None
            }
            RetentionAction::None => match key.code {
                // While a field is being edited every key belongs to the
                // field, so screen-level shortcuts must not fire.
                KeyCode::Char('0') if !self.retention.form.is_editing() => {
                    // Reset form fields
                    for field in self.retention.form.fields_mut() {
                        match &mut field.field_type {
                            FormFieldType::Text(input) => input.clear(),
                            FormFieldType::MultiSelect(items) => items.clear(),
                            FormFieldType::Toggle(value) => *value = false,
                            _ => {}
                        }
                    }
                    ForgetAction::None
                }
                KeyCode::Char('q') if !self.retention.form.is_editing() => ForgetAction::Quit,
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
        let inner = frame.area().inner(theme::CONTENT_MARGIN);
        let footer = theme::key_hint_lines(&self.help_hints(), inner.width);
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Min(3),
                Constraint::Length(footer.len() as u16),
            ])
            .split(inner);

        self.last_table_area = chunks[1];

        let selected = self.selected_count();
        let header = Paragraph::new(format!(
            "Forget Snapshots\nSelected: {} / {}",
            selected,
            self.entries.len()
        ))
        .style(theme::THEME.header);
        frame.render_widget(header, chunks[0]);

        // Derive column widths from the available space so the Tags column is
        // dropped (rather than clipped) on narrow terminals.
        let usable = chunks[1].width.saturating_sub(2);
        let remaining = usable.saturating_sub(5 + 12 + 15);
        let show_tags = remaining >= 30;
        let date_width = if show_tags {
            remaining.saturating_sub(10).clamp(16, 20)
        } else {
            remaining.max(16)
        };

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

                let style = if is_selected {
                    Style::default().fg(theme::THEME.red)
                } else {
                    Style::default()
                };

                let mut cells = vec![
                    Span::styled(selected_str, style),
                    Span::styled(id, theme::THEME.snap_id),
                    Span::styled(date, theme::THEME.snap_date),
                    Span::styled(host, theme::THEME.snap_host),
                ];
                if show_tags {
                    cells.push(Span::raw(theme::format_tags(&e.snapshot.tags)));
                }
                Row::new(cells)
            })
            .collect();

        let mut widths = vec![
            Constraint::Length(5),
            Constraint::Length(12),
            Constraint::Length(date_width),
            Constraint::Length(15),
        ];
        if show_tags {
            widths.push(Constraint::Min(20));
        }

        let mut header_cells = vec!["", "ID", "Date", "Host"];
        if show_tags {
            header_cells.push("Tags");
        }

        if self.entries.is_empty() {
            theme::empty_state(frame, chunks[1], "Snapshots", "No snapshots to forget.");
        } else {
            let table = Table::new(rows, widths)
                .header(Row::new(header_cells).style(theme::THEME.header))
                .block(theme::block("Snapshots"))
                .row_highlight_style(theme::THEME.selection);

            frame.render_stateful_widget(table, chunks[1], &mut self.table_state);
            theme::render_scrollbar(
                frame,
                chunks[1],
                self.entries.len(),
                self.table_state.selected().unwrap_or(0),
            );
        }

        let footer = theme::key_hint_lines(&self.help_hints(), inner.width);
        frame.render_widget(Paragraph::new(Text::from(footer)), chunks[2]);
    }

    fn render_confirm(&self, frame: &mut Frame) {
        let text = vec![
            Line::from(vec![
                Span::raw("You are about to forget "),
                Span::styled(self.selected_count().to_string(), theme::THEME.error),
                Span::raw(" snapshots."),
            ]),
            Line::from(""),
            Line::from(if self.retention.force() {
                "Force enabled: snapshot metadata will be permanently deleted."
            } else {
                "Snapshot metadata will be staged for removal and can be recalled."
            }),
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
                let inner = frame.area().inner(theme::CONTENT_MARGIN);
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

    async fn handle_mouse(&mut self, mouse: MouseEvent) -> bool {
        if self.phase != ForgetPhase::Selection
            || !matches!(
                mouse.kind,
                crossterm::event::MouseEventKind::Down(MouseButton::Left)
            )
        {
            return false;
        }
        // The table has a header row above the items.
        let Some(idx) = click_to_index(
            mouse.row,
            self.table_state.offset(),
            self.last_table_area,
            1,
        ) else {
            return false;
        };
        if idx < self.entries.len() {
            self.table_state.select(Some(idx));
            return true;
        }
        false
    }

    fn help_hints(&self) -> Vec<(&'static str, &'static str)> {
        match self.phase {
            ForgetPhase::Selection => vec![
                ("Space", "toggle"),
                ("Enter", "confirm"),
                ("r", "retention"),
                ("a", "toggle all"),
                ("Esc", "back"),
                ("q", "back"),
            ],
            ForgetPhase::Retention => {
                if self.retention.form.is_editing() {
                    vec![("Enter", "confirm"), ("Esc", "cancel edit")]
                } else {
                    vec![
                        ("Tab", "navigate"),
                        ("Enter", "edit / apply"),
                        ("Space", "toggle"),
                        ("0", "clear fields"),
                        ("Esc", "back"),
                        ("q", "back"),
                    ]
                }
            }
            ForgetPhase::Confirm => vec![("Enter", "proceed"), ("Esc", "cancel"), ("q", "back")],
            ForgetPhase::Result => vec![("Enter", "continue"), ("Esc", "back"), ("q", "back")],
        }
    }

    fn text_input_active(&self) -> bool {
        self.phase == ForgetPhase::Retention && self.retention.form.is_editing()
    }

    impl_attach_toasts!(toasts);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        backend::{Handle, StorageBackend, WriteContents, mock::MockBackend},
        common::{ID, defaults::TEST_REPO_CONFIG, error::Result},
        repository::{
            repo::{Auth, THIS_REPOSITORY_VERSION},
            snapshot::{Snapshot, SnapshotEntry},
        },
    };
    use zeroize::Zeroizing;

    async fn make_screen(force: bool) -> Result<ForgetScreen> {
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
        let (repo, _) =
            Repository::try_open_unlocked(&auth, None, backend.clone(), TEST_REPO_CONFIG).await?;
        let mut entries = Vec::new();
        for snapshot_index in 1..=3 {
            let id = ID::from_bytes([snapshot_index; 32]);
            backend
                .write(
                    &Handle::new(&repo.get_path(ContentIdType::Snapshot, &id)),
                    WriteContents::Owned(vec![snapshot_index]),
                )
                .await?;
            entries.push(SnapshotEntry {
                id,
                snapshot: Snapshot {
                    timestamp: Local::now() - chrono::Duration::minutes(i64::from(snapshot_index)),
                    ..Default::default()
                },
                active: true,
            });
        }
        Ok(ForgetScreen::new(
            repo,
            Arc::new(entries),
            Some(cmd_forget::CmdArgs {
                keep_last: Some(1),
                keep_min: Some(2),
                force,
                ..Default::default()
            }),
        ))
    }

    #[tokio::test]
    async fn keep_min_limits_selection_and_force_controls_staging() -> Result<()> {
        for force in [false, true] {
            let mut screen = make_screen(force).await?;
            assert_eq!(screen.selected_count(), 1);
            let selected_index = screen
                .selected
                .bits
                .iter()
                .position(|selected| *selected)
                .unwrap();
            let removed_id = screen.entries[selected_index].id;
            let path = screen.repo.get_path(ContentIdType::Snapshot, &removed_id);
            screen.execute_forget().await;
            assert!(!screen.repo.backend().path_exists(&path).await);
            assert_eq!(
                screen
                    .repo
                    .backend()
                    .path_exists(&path.with_extension(REPO_DROPPED_EXTENSION))
                    .await,
                !force
            );
            assert_eq!(screen.repo.list_snapshot_ids().await?.len(), 2);
            assert!(matches!(
                screen.result,
                Some(ForgetResult::Success { removed_count: 1 })
            ));
        }
        Ok(())
    }

    #[test]
    fn selection_set_out_of_bounds() {
        let mut s = ForgetSelection::new(3);
        s.set(10, true); // no panic
        assert_eq!(s.count_selected(), 0);
    }

    #[test]
    fn selection_toggle_all_partial() {
        let mut s = ForgetSelection::new(3);
        s.set(0, true);
        s.toggle_all(); // all off → all on (because not all were set)
        assert_eq!(s.count_selected(), 3);
    }
}
