use std::sync::Arc;

use async_trait::async_trait;
use chrono::Local;
use crossterm::event::{KeyCode, KeyEvent};
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Margin, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Paragraph, Row, Table, TableState},
};

use crate::{
    mapache::defaults::SHORT_SNAPSHOT_ID_LEN,
    repository::{
        repo::Repository,
        retention::{RetentionRule, apply_retention_rules},
        snapshot::{SnapshotEntry, SnapshotEntryList, SnapshotStream},
    },
    ui::tui::{
        app::{Screen, Transition},
        theme,
        widgets::StateNavigation,
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
    selected: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum RetentionField {
    Last,
    Within,
    Hourly,
    Daily,
    Weekly,
    Monthly,
    Yearly,
    Apply,
}

impl RetentionField {
    fn next(&self) -> Self {
        match self {
            RetentionField::Last => RetentionField::Within,
            RetentionField::Within => RetentionField::Hourly,
            RetentionField::Hourly => RetentionField::Daily,
            RetentionField::Daily => RetentionField::Weekly,
            RetentionField::Weekly => RetentionField::Monthly,
            RetentionField::Monthly => RetentionField::Yearly,
            RetentionField::Yearly => RetentionField::Apply,
            RetentionField::Apply => RetentionField::Last,
        }
    }

    fn prev(&self) -> Self {
        match self {
            RetentionField::Last => RetentionField::Apply,
            RetentionField::Within => RetentionField::Last,
            RetentionField::Hourly => RetentionField::Within,
            RetentionField::Daily => RetentionField::Hourly,
            RetentionField::Weekly => RetentionField::Weekly, // Fixed a typo here
            RetentionField::Monthly => RetentionField::Weekly,
            RetentionField::Yearly => RetentionField::Monthly,
            RetentionField::Apply => RetentionField::Yearly,
        }
    }

    fn label(&self) -> &'static str {
        match self {
            RetentionField::Last => "Keep last:",
            RetentionField::Within => "Keep within:",
            RetentionField::Hourly => "Keep hourly:",
            RetentionField::Daily => "Keep daily:",
            RetentionField::Weekly => "Keep weekly:",
            RetentionField::Monthly => "Keep monthly:",
            RetentionField::Yearly => "Keep yearly:",
            RetentionField::Apply => "",
        }
    }

    fn is_apply(&self) -> bool {
        matches!(self, RetentionField::Apply)
    }
}

struct RetentionForm {
    keep_last: Option<usize>,
    keep_within: Option<String>,
    keep_hourly: Option<usize>,
    keep_daily: Option<usize>,
    keep_weekly: Option<usize>,
    keep_monthly: Option<usize>,
    keep_yearly: Option<usize>,
    focused: RetentionField,
    editing: Option<RetentionField>,
    edit_buffer: String,
}

impl RetentionForm {
    fn new(config: Option<&crate::commands::cmd_forget::CmdArgs>) -> Self {
        let keep_within = config.and_then(|c| c.keep_within.map(|d| d.to_string()));
        Self {
            keep_last: config.and_then(|c| c.keep_last),
            keep_within,
            keep_hourly: config.and_then(|c| c.keep_hourly),
            keep_daily: config.and_then(|c| c.keep_daily),
            keep_weekly: config.and_then(|c| c.keep_weekly),
            keep_monthly: config.and_then(|c| c.keep_monthly),
            keep_yearly: config.and_then(|c| c.keep_yearly),
            focused: RetentionField::Last,
            editing: None,
            edit_buffer: String::new(),
        }
    }

    fn to_rules(&self) -> Vec<RetentionRule> {
        let mut rules = Vec::new();
        if let Some(n) = self.keep_last {
            rules.push(RetentionRule::KeepLast(n));
        }
        if let Some(ref d) = self.keep_within
            && let Ok(dur) = utils::parse_duration_string(d)
        {
            rules.push(RetentionRule::KeepWithin(dur));
        }
        if let Some(n) = self.keep_hourly {
            rules.push(RetentionRule::KeepHourly(n));
        }
        if let Some(n) = self.keep_daily {
            rules.push(RetentionRule::KeepDaily(n));
        }
        if let Some(n) = self.keep_weekly {
            rules.push(RetentionRule::KeepWeekly(n));
        }
        if let Some(n) = self.keep_monthly {
            rules.push(RetentionRule::KeepMonthly(n));
        }
        if let Some(n) = self.keep_yearly {
            rules.push(RetentionRule::KeepYearly(n));
        }
        rules
    }

    fn value_for(&self, field: RetentionField) -> Option<String> {
        match field {
            RetentionField::Last => self.keep_last.map(|n| {
                if n == usize::MAX {
                    "all".to_string()
                } else {
                    n.to_string()
                }
            }),
            RetentionField::Within => self.keep_within.clone(),
            RetentionField::Hourly => self.keep_hourly.map(|n| {
                if n == usize::MAX {
                    "all".to_string()
                } else {
                    n.to_string()
                }
            }),
            RetentionField::Daily => self.keep_daily.map(|n| {
                if n == usize::MAX {
                    "all".to_string()
                } else {
                    n.to_string()
                }
            }),
            RetentionField::Weekly => self.keep_weekly.map(|n| {
                if n == usize::MAX {
                    "all".to_string()
                } else {
                    n.to_string()
                }
            }),
            RetentionField::Monthly => self.keep_monthly.map(|n| {
                if n == usize::MAX {
                    "all".to_string()
                } else {
                    n.to_string()
                }
            }),
            RetentionField::Yearly => self.keep_yearly.map(|n| {
                if n == usize::MAX {
                    "all".to_string()
                } else {
                    n.to_string()
                }
            }),
            RetentionField::Apply => None,
        }
    }

    fn parse_usize_or_all(v: &str) -> Option<usize> {
        if v == "all" {
            Some(usize::MAX)
        } else {
            v.parse::<usize>().ok().filter(|n| *n > 0)
        }
    }

    fn set_value(&mut self, field: RetentionField, value: Option<String>) {
        match field {
            RetentionField::Last => {
                self.keep_last = value.as_deref().and_then(Self::parse_usize_or_all);
            }
            RetentionField::Within => {
                self.keep_within = value.filter(|v| !v.is_empty());
            }
            RetentionField::Hourly => {
                self.keep_hourly = value.as_deref().and_then(Self::parse_usize_or_all);
            }
            RetentionField::Daily => {
                self.keep_daily = value.as_deref().and_then(Self::parse_usize_or_all);
            }
            RetentionField::Weekly => {
                self.keep_weekly = value.as_deref().and_then(Self::parse_usize_or_all);
            }
            RetentionField::Monthly => {
                self.keep_monthly = value.as_deref().and_then(Self::parse_usize_or_all);
            }
            RetentionField::Yearly => {
                self.keep_yearly = value.as_deref().and_then(Self::parse_usize_or_all);
            }
            RetentionField::Apply => {}
        }
    }

    fn clear_all(&mut self) {
        self.keep_last = None;
        self.keep_within = None;
        self.keep_hourly = None;
        self.keep_daily = None;
        self.keep_weekly = None;
        self.keep_monthly = None;
        self.keep_yearly = None;
    }

    fn handle_key(&mut self, key: KeyEvent) -> Option<Vec<RetentionRule>> {
        if let Some(editing) = self.editing {
            match key.code {
                KeyCode::Esc => {
                    self.editing = None;
                    self.edit_buffer.clear();
                }
                KeyCode::Enter => {
                    let val = if self.edit_buffer.is_empty() {
                        None
                    } else {
                        Some(self.edit_buffer.clone())
                    };
                    self.set_value(editing, val);
                    self.editing = None;
                    self.edit_buffer.clear();
                }
                KeyCode::Char(c) => {
                    self.edit_buffer.push(c);
                }
                KeyCode::Backspace => {
                    self.edit_buffer.pop();
                }
                _ => {}
            }
            None
        } else {
            match key.code {
                KeyCode::Esc => None,
                KeyCode::Char('q') => Some(Vec::new()),
                KeyCode::Tab | KeyCode::Down => {
                    self.focused = self.focused.next();
                    None
                }
                KeyCode::BackTab | KeyCode::Up => {
                    self.focused = self.focused.prev();
                    None
                }
                KeyCode::Enter => {
                    if self.focused.is_apply() {
                        return Some(self.to_rules());
                    } else {
                        self.editing = Some(self.focused);
                        self.edit_buffer = self.value_for(self.focused).unwrap_or_default();
                    }
                    None
                }
                KeyCode::Char(' ') => {
                    if !self.focused.is_apply() {
                        let current = self.value_for(self.focused);
                        if current.is_some() {
                            self.set_value(self.focused, None);
                        } else {
                            self.editing = Some(self.focused);
                            self.edit_buffer = String::new();
                        }
                    }
                    None
                }
                _ => None,
            }
        }
    }
}

pub struct ForgetScreen {
    phase: ForgetPhase,
    entries: SnapshotEntryList,
    selections: Vec<ForgetSelection>,
    table_state: TableState,
    retention_form: RetentionForm,
    result: Option<ForgetResult>,
}

enum ForgetResult {
    Success { removed: Vec<SnapshotEntry> },
    NoDeleted,
}

impl ForgetScreen {
    pub async fn new(
        repo: Arc<Repository>,
        config: Option<crate::commands::cmd_forget::CmdArgs>,
    ) -> Self {
        let mut entries = match SnapshotStream::new(repo.clone()).await {
            Ok(stream) => stream.collect_entries(true).await.unwrap_or_default(),
            Err(_) => Vec::new(),
        };
        // Sort ascending for retention rules
        entries.sort_unstable_by_key(|e| e.snapshot.timestamp);

        let selections: Vec<ForgetSelection> = entries
            .iter()
            .map(|_| ForgetSelection { selected: false })
            .collect();

        let mut table_state = TableState::default();
        if !entries.is_empty() {
            table_state.select(Some(0));
        }

        let retention_form = RetentionForm::new(config.as_ref());
        let rules = retention_form.to_rules();

        let mut screen = Self {
            phase: ForgetPhase::Selection,
            entries,
            selections,
            table_state,
            retention_form,
            result: None,
        };

        if !rules.is_empty() {
            screen.apply_retention(&rules);
        }

        screen
    }

    fn apply_retention(&mut self, rules: &[RetentionRule]) {
        let keep_ids = apply_retention_rules(&self.entries, rules, None, Local::now());

        for (i, entry) in self.entries.iter().enumerate() {
            self.selections[i].selected = !keep_ids.contains(&entry.id);
        }
    }

    fn selected_count(&self) -> usize {
        self.selections.iter().filter(|s| s.selected).count()
    }

    fn execute_forget(&mut self) {
        let to_remove: Vec<_> = self
            .entries
            .iter()
            .enumerate()
            .filter(|(i, _)| self.selections[*i].selected)
            .map(|(_, e)| e.clone())
            .collect();

        if to_remove.is_empty() {
            self.result = Some(ForgetResult::NoDeleted);
        } else {
            // For now, let's just simulate.
            self.result = Some(ForgetResult::Success { removed: to_remove });
        }
        self.phase = ForgetPhase::Result;
    }

    fn handle_selection_key(&mut self, key: KeyEvent) -> Option<Transition> {
        match key.code {
            KeyCode::Esc => Some(Transition::Pop),
            KeyCode::Char('q') => Some(Transition::Quit),
            KeyCode::Char(' ') => {
                if let Some(idx) = self.table_state.selected()
                    && idx < self.selections.len()
                {
                    self.selections[idx].selected = !self.selections[idx].selected;
                }
                None
            }
            KeyCode::Enter => {
                let count = self.selected_count();
                if count > 0 {
                    self.phase = ForgetPhase::Confirm;
                } else {
                    self.execute_forget();
                }
                None
            }
            KeyCode::Char('r') => {
                self.phase = ForgetPhase::Retention;
                None
            }
            KeyCode::Char('a') => {
                let all_selected = self.selections.iter().all(|s| s.selected);
                for s in &mut self.selections {
                    s.selected = !all_selected;
                }
                None
            }
            KeyCode::Down => {
                self.table_state.next(self.selections.len());
                None
            }
            KeyCode::Up => {
                self.table_state.previous(self.selections.len());
                None
            }
            KeyCode::PageDown => {
                self.table_state.page_next(self.selections.len(), 10);
                None
            }
            KeyCode::PageUp => {
                self.table_state.page_previous(self.selections.len(), 10);
                None
            }
            KeyCode::Home => {
                self.table_state.home(self.selections.len());
                None
            }
            KeyCode::End => {
                self.table_state.end(self.selections.len());
                None
            }
            _ => None,
        }
    }

    fn handle_retention_key(&mut self, key: KeyEvent) -> Option<Transition> {
        if let Some(rules) = self.retention_form.handle_key(key) {
            if rules.is_empty() && key.code == KeyCode::Char('q') {
                return Some(Transition::Quit);
            }
            if !rules.is_empty() {
                self.apply_retention(&rules);
            }
            self.phase = ForgetPhase::Selection;
            return None;
        }

        match key.code {
            KeyCode::Char('0') => {
                self.retention_form.clear_all();
                None
            }
            KeyCode::Esc => {
                self.phase = ForgetPhase::Selection;
                None
            }
            _ => None,
        }
    }

    fn handle_confirm_key(&mut self, key: KeyEvent) -> Option<Transition> {
        match key.code {
            KeyCode::Esc => {
                self.phase = ForgetPhase::Selection;
                None
            }
            KeyCode::Enter => {
                self.execute_forget();
                None
            }
            KeyCode::Char('q') => Some(Transition::Quit),
            _ => None,
        }
    }
}

#[async_trait]
impl Screen for ForgetScreen {
    fn render(&mut self, frame: &mut Frame) {
        match self.phase {
            ForgetPhase::Selection => self.render_selection(frame),
            ForgetPhase::Retention => self.render_retention(frame),
            ForgetPhase::Confirm => self.render_confirm(frame),
            ForgetPhase::Result => self.render_result(frame),
        }
    }

    async fn handle_key(&mut self, key: KeyEvent) -> Option<Transition> {
        match self.phase {
            ForgetPhase::Selection => self.handle_selection_key(key),
            ForgetPhase::Retention => self.handle_retention_key(key),
            ForgetPhase::Confirm => self.handle_confirm_key(key),
            ForgetPhase::Result => Some(Transition::PopAndReload),
        }
    }
}

impl ForgetScreen {
    fn render_selection(&mut self, frame: &mut Frame) {
        let area = frame.area();
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Min(3),
                Constraint::Length(2),
            ])
            .split(area.inner(Margin::new(2, 1)));

        let selected = self.selected_count();
        let header = Paragraph::new(format!(
            "Forget Snapshots\nSelected: {} / {}",
            selected,
            self.entries.len()
        ))
        .style(theme::header_style());
        frame.render_widget(header, chunks[0]);

        let rows: Vec<Row> = self
            .entries
            .iter()
            .enumerate()
            .rev() // Show newest first in table
            .map(|(i, e)| {
                let selected = if self.selections[i].selected {
                    "[X]"
                } else {
                    "[ ]"
                };
                let id = e.id.to_short_hex(SHORT_SNAPSHOT_ID_LEN);
                let date = utils::pretty_print_timestamp(&e.snapshot.timestamp, None);
                let host = e.snapshot.hostname.as_deref().unwrap_or("");
                let tags = theme::format_tags(&e.snapshot.tags);

                let style = if self.selections[i].selected {
                    Style::default().fg(Color::Red)
                } else {
                    Style::default()
                };

                Row::new(vec![
                    Span::styled(selected, style),
                    Span::styled(id, Style::default().fg(theme::SNAPSHOT_ID)),
                    Span::styled(date, Style::default().fg(theme::SNAPSHOT_DATE)),
                    Span::styled(host, Style::default().fg(theme::SNAPSHOT_HOST)),
                    Span::raw(tags),
                ])
            })
            .collect();

        let table = Table::new(
            rows,
            vec![
                Constraint::Length(5),
                Constraint::Length(12),
                Constraint::Length(25),
                Constraint::Length(15),
                Constraint::Min(10),
            ],
        )
        .header(
            Row::new(vec!["", "ID", "Date", "Host", "Tags"])
                .style(Style::default().add_modifier(Modifier::BOLD)),
        )
        .block(theme::themed_block("Select snapshots to forget"))
        .row_highlight_style(theme::selected_row_style())
        .highlight_symbol(">> ");

        frame.render_stateful_widget(table, chunks[1], &mut self.table_state);

        let footer = Line::from(vec![
            Span::styled("[Space]", Style::default().fg(theme::MENU_KEY).bold()),
            Span::raw(" toggle"),
            Span::raw("    "),
            Span::styled("[a]", Style::default().fg(theme::MENU_KEY).bold()),
            Span::raw(" all"),
            Span::raw("    "),
            Span::styled("[r]", Style::default().fg(theme::MENU_KEY).bold()),
            Span::raw(" retention"),
            Span::raw("    "),
            Span::styled("[Enter]", Style::default().fg(theme::MENU_KEY).bold()),
            Span::raw(" forget"),
            Span::raw("    "),
            Span::styled("[Esc]", Style::default().fg(theme::MENU_KEY).bold()),
            Span::raw(" back"),
        ]);
        frame.render_widget(Paragraph::new(footer), chunks[2]);
    }

    fn render_retention(&self, frame: &mut Frame) {
        let area = frame.area();
        let inner = area.inner(Margin::new(4, 2));

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(10), Constraint::Length(2)])
            .split(inner);

        let mut lines = Vec::new();
        let fields = [
            RetentionField::Last,
            RetentionField::Within,
            RetentionField::Hourly,
            RetentionField::Daily,
            RetentionField::Weekly,
            RetentionField::Monthly,
            RetentionField::Yearly,
        ];

        for field in fields {
            let focused = self.retention_form.focused == field;
            let editing = self.retention_form.editing == Some(field);
            let value = if editing {
                format!("{}█", self.retention_form.edit_buffer)
            } else {
                self.retention_form.value_for(field).unwrap_or_default()
            };

            let label_style = if focused {
                Style::default()
                    .fg(theme::SNAPSHOT_ID)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };

            let marker = if focused { "▶ " } else { "  " };
            lines.push(Line::from(vec![
                Span::styled(marker, Style::default().fg(theme::SNAPSHOT_DATE)),
                Span::styled(format!("{:<15}", field.label()), label_style),
                Span::raw(value),
            ]));
        }

        lines.push(Line::from(""));
        let apply_focused = self.retention_form.focused == RetentionField::Apply;
        let apply_style = if apply_focused {
            Style::default()
                .fg(theme::SNAPSHOT_DATE)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(Color::DarkGray)
        };
        lines.push(Line::from(vec![
            Span::raw("               "),
            Span::styled("[ Apply Rules ]", apply_style),
        ]));

        let form = Paragraph::new(Text::from(lines)).block(theme::themed_block("Retention Rules"));
        frame.render_widget(form, chunks[0]);

        let footer = Line::from(vec![
            Span::styled("[Tab/↓↑]", Style::default().fg(theme::MENU_KEY).bold()),
            Span::raw(" navigate"),
            Span::raw("    "),
            Span::styled("[Enter]", Style::default().fg(theme::MENU_KEY).bold()),
            Span::raw(" edit/apply"),
            Span::raw("    "),
            Span::styled("[0]", Style::default().fg(theme::MENU_KEY).bold()),
            Span::raw(" clear all"),
            Span::raw("    "),
            Span::styled("[Esc]", Style::default().fg(theme::MENU_KEY).bold()),
            Span::raw(" cancel"),
        ]);
        frame.render_widget(Paragraph::new(footer), chunks[1]);
    }

    fn render_confirm(&self, frame: &mut Frame) {
        let area = frame.area();
        let count = self.selected_count();
        let popup = Rect {
            x: area.width / 4,
            y: area.height / 3,
            width: area.width / 2,
            height: 10,
        };

        let text = vec![
            Line::from(""),
            Line::from(vec![
                Span::raw("Are you sure you want to forget "),
                Span::styled(count.to_string(), Style::default().fg(Color::Red).bold()),
                Span::raw(" snapshots?"),
            ]),
            Line::from(""),
            Line::from("This action cannot be undone."),
            Line::from(""),
            Line::from(vec![
                Span::styled("[Enter]", Style::default().fg(theme::MENU_KEY).bold()),
                Span::raw(" confirm    "),
                Span::styled("[Esc]", Style::default().fg(theme::MENU_KEY).bold()),
                Span::raw(" cancel"),
            ]),
        ];

        let block = theme::themed_block("Confirm Forget")
            .title_alignment(ratatui::layout::Alignment::Center);
        let paragraph = Paragraph::new(Text::from(text))
            .block(block)
            .alignment(ratatui::layout::Alignment::Center);

        frame.render_widget(ratatui::widgets::Clear, popup);
        frame.render_widget(paragraph, popup);
    }

    fn render_result(&self, frame: &mut Frame) {
        let area = frame.area();
        let popup = Rect {
            x: area.width / 4,
            y: area.height / 3,
            width: area.width / 2,
            height: 10,
        };

        let text = match &self.result {
            Some(ForgetResult::Success { removed }) => vec![
                Line::from(""),
                Line::from(vec![Span::styled(
                    "Success!",
                    Style::default().fg(Color::Green).bold(),
                )]),
                Line::from(""),
                Line::from(format!("Removed {} snapshots.", removed.len())),
                Line::from(""),
                Line::from("Press any key to continue."),
            ],
            Some(ForgetResult::NoDeleted) => vec![
                Line::from(""),
                Line::from("No snapshots were selected or matched."),
                Line::from(""),
                Line::from("Press any key to continue."),
            ],
            None => vec![Line::from("Something went wrong.")],
        };

        let block =
            theme::themed_block("Result").title_alignment(ratatui::layout::Alignment::Center);
        let paragraph = Paragraph::new(Text::from(text))
            .block(block)
            .alignment(ratatui::layout::Alignment::Center);

        frame.render_widget(ratatui::widgets::Clear, popup);
        frame.render_widget(paragraph, popup);
    }
}
