use std::cmp::Reverse;
use std::sync::Arc;

use crossterm::event::{KeyCode, KeyEvent};
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Margin, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Paragraph, Row, Table, TableState},
};

use crate::{
    mapache::{ContentIdType, ID, defaults::SHORT_SNAPSHOT_ID_LEN},
    repository::{
        repo::{REPO_DROPPED_EXTENSION, Repository},
        retention::{RetentionRule, apply_retention_rules},
        snapshot::{SnapshotEntry, SnapshotEntryList, SnapshotStream},
    },
    ui::tui::theme,
    utils,
};

#[derive(Debug, Clone, Copy, PartialEq)]
enum ForgetPhase {
    Selection,
    Retention,
    Confirm,
    Result,
}

#[derive(Debug)]
pub enum ForgetAction {
    Back,
    Quit,
}

struct ForgetSelection {
    id: ID,
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
            RetentionField::Weekly => RetentionField::Daily,
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
    repo: Arc<Repository>,
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
        entries.sort_unstable_by_key(|e| Reverse(e.snapshot.timestamp));

        let selections: Vec<ForgetSelection> = entries
            .iter()
            .map(|e| ForgetSelection {
                id: e.id,
                selected: false,
            })
            .collect();

        let mut table_state = TableState::default();
        if !entries.is_empty() {
            table_state.select(Some(0));
        }

        let retention_form = RetentionForm::new(config.as_ref());
        let rules = retention_form.to_rules();

        let mut screen = Self {
            repo,
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

    pub fn handle_key(&mut self, key: KeyEvent) -> Option<ForgetAction> {
        match self.phase {
            ForgetPhase::Selection => self.handle_selection_key(key),
            ForgetPhase::Retention => self.handle_retention_key(key),
            ForgetPhase::Confirm => self.handle_confirm_key(key),
            ForgetPhase::Result => Some(ForgetAction::Back),
        }
    }

    fn handle_selection_key(&mut self, key: KeyEvent) -> Option<ForgetAction> {
        match key.code {
            KeyCode::Esc => Some(ForgetAction::Back),
            KeyCode::Char('q') => Some(ForgetAction::Quit),
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
                if let Some(i) = self.table_state.selected() {
                    let next = if i >= self.selections.len().saturating_sub(1) {
                        0
                    } else {
                        i + 1
                    };
                    self.table_state.select(Some(next));
                }
                None
            }
            KeyCode::Up => {
                if let Some(i) = self.table_state.selected() {
                    let prev = if i == 0 {
                        self.selections.len().saturating_sub(1)
                    } else {
                        i - 1
                    };
                    self.table_state.select(Some(prev));
                }
                None
            }
            KeyCode::PageDown => {
                if let Some(i) = self.table_state.selected() {
                    let next = (i + 10).min(self.selections.len().saturating_sub(1));
                    self.table_state.select(Some(next));
                }
                None
            }
            KeyCode::PageUp => {
                if let Some(i) = self.table_state.selected() {
                    let prev = i.saturating_sub(10);
                    self.table_state.select(Some(prev));
                }
                None
            }
            KeyCode::Home => {
                if !self.selections.is_empty() {
                    self.table_state.select(Some(0));
                }
                None
            }
            KeyCode::End => {
                if !self.selections.is_empty() {
                    self.table_state
                        .select(Some(self.selections.len().saturating_sub(1)));
                }
                None
            }
            _ => None,
        }
    }

    fn handle_retention_key(&mut self, key: KeyEvent) -> Option<ForgetAction> {
        if let Some(rules) = self.retention_form.handle_key(key) {
            if rules.is_empty() && key.code == KeyCode::Char('q') {
                return Some(ForgetAction::Quit);
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

    fn handle_confirm_key(&mut self, key: KeyEvent) -> Option<ForgetAction> {
        match key.code {
            KeyCode::Esc => {
                self.phase = ForgetPhase::Selection;
                None
            }
            KeyCode::Char('q') => Some(ForgetAction::Quit),
            KeyCode::Char('y') => {
                self.execute_forget();
                None
            }
            KeyCode::Char('n') => {
                self.phase = ForgetPhase::Selection;
                None
            }
            _ => None,
        }
    }

    fn apply_retention(&mut self, rules: &[RetentionRule]) {
        let mut sorted: Vec<&SnapshotEntry> = self.entries.iter().collect();
        sorted.sort_by_key(|e| e.snapshot.timestamp);
        let sorted_entries: Vec<SnapshotEntry> = sorted.into_iter().cloned().collect();
        let ids_to_keep = apply_retention_rules(&sorted_entries, rules, None, chrono::Local::now());
        for sel in &mut self.selections {
            sel.selected = !ids_to_keep.contains(&sel.id);
        }
    }

    fn execute_forget(&mut self) {
        let selected_ids: Vec<ID> = self
            .selections
            .iter()
            .filter(|s| s.selected)
            .map(|s| s.id)
            .collect();

        let removed: Vec<SnapshotEntry> = self
            .entries
            .iter()
            .filter(|e| selected_ids.contains(&e.id))
            .cloned()
            .collect();

        let repo = self.repo.clone();
        let removed_ids: Vec<ID> = selected_ids.clone();

        tokio::spawn(async move {
            for id in removed_ids {
                let _ = repo
                    .set_extension(ContentIdType::Snapshot, &id, Some(REPO_DROPPED_EXTENSION))
                    .await;
            }
        });

        if removed.is_empty() {
            self.result = Some(ForgetResult::NoDeleted);
        } else {
            self.result = Some(ForgetResult::Success { removed });
        }
        self.phase = ForgetPhase::Result;
    }

    fn selected_count(&self) -> usize {
        self.selections.iter().filter(|s| s.selected).count()
    }

    fn retention_summary(&self) -> String {
        let rules = self.retention_form.to_rules();
        if rules.is_empty() {
            return String::new();
        }
        let parts: Vec<String> = rules
            .iter()
            .map(|r| match r {
                RetentionRule::KeepLast(n) => {
                    let val = if *n == usize::MAX {
                        "all".to_string()
                    } else {
                        n.to_string()
                    };
                    format!("last:{}", val)
                }
                RetentionRule::KeepWithin(d) => format!("within:{}", d),
                RetentionRule::KeepHourly(n) => {
                    let val = if *n == usize::MAX {
                        "all".to_string()
                    } else {
                        n.to_string()
                    };
                    format!("hourly:{}", val)
                }
                RetentionRule::KeepDaily(n) => {
                    let val = if *n == usize::MAX {
                        "all".to_string()
                    } else {
                        n.to_string()
                    };
                    format!("daily:{}", val)
                }
                RetentionRule::KeepWeekly(n) => {
                    let val = if *n == usize::MAX {
                        "all".to_string()
                    } else {
                        n.to_string()
                    };
                    format!("weekly:{}", val)
                }
                RetentionRule::KeepMonthly(n) => {
                    let val = if *n == usize::MAX {
                        "all".to_string()
                    } else {
                        n.to_string()
                    };
                    format!("monthly:{}", val)
                }
                RetentionRule::KeepYearly(n) => {
                    let val = if *n == usize::MAX {
                        "all".to_string()
                    } else {
                        n.to_string()
                    };
                    format!("yearly:{}", val)
                }
                RetentionRule::KeepTags(tags) => format!(
                    "tags:{}",
                    tags.iter().cloned().collect::<Vec<_>>().join(",")
                ),
            })
            .collect();
        parts.join(", ")
    }

    pub fn render(&mut self, frame: &mut Frame) {
        match self.phase {
            ForgetPhase::Selection => self.render_selection(frame),
            ForgetPhase::Retention => self.render_retention(frame),
            ForgetPhase::Confirm => self.render_confirm(frame),
            ForgetPhase::Result => self.render_result(frame),
        }
    }

    fn render_selection(&mut self, frame: &mut Frame) {
        let area = frame.area();
        let inner = area.inner(Margin::new(2, 1));

        let retention_str = self.retention_summary();
        let has_retention = !retention_str.is_empty();

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1),
                Constraint::Min(5),
                Constraint::Length(if has_retention { 2 } else { 0 }),
                Constraint::Length(1),
            ])
            .split(inner);

        let count = self.selected_count();
        let count_text = if count > 0 {
            format!(
                "{} snapshot(s) selected  │  [Enter] confirm  │  [r] rules",
                count
            )
        } else {
            "No snapshots selected  │  [Enter] confirm  │  [r] rules".to_string()
        };
        frame.render_widget(Paragraph::new(count_text), chunks[0]);

        let rows: Vec<Row> = self
            .entries
            .iter()
            .enumerate()
            .map(|(i, entry)| {
                let sel = &self.selections[i];
                let checkbox = if sel.selected { "[X]" } else { "[ ]" };
                let checkbox_style = if sel.selected {
                    Style::default().fg(Color::Red).bold()
                } else {
                    Style::default().fg(Color::DarkGray)
                };

                let highlight_style = if self.table_state.selected() == Some(i) {
                    Style::default().bg(Color::DarkGray)
                } else {
                    Style::default()
                };

                let id_str = entry.id.to_short_hex(SHORT_SNAPSHOT_ID_LEN);
                let date = utils::pretty_print_timestamp(&entry.snapshot.timestamp, None);
                let host = entry.snapshot.hostname.as_deref().unwrap_or("-");
                let size = utils::format_size_binary(entry.snapshot.size(), 3);

                Row::new([
                    Span::styled(checkbox, checkbox_style),
                    Span::styled(id_str, Style::default().fg(theme::SNAPSHOT_ID)),
                    Span::styled(date, Style::default().fg(theme::SNAPSHOT_DATE)),
                    Span::styled(host, Style::default().fg(theme::SNAPSHOT_HOST)),
                    Span::styled(size, Style::default().fg(theme::SNAPSHOT_SIZE)),
                ])
                .style(highlight_style)
            })
            .collect();

        let table = Table::new(
            rows,
            [
                Constraint::Length(4),
                Constraint::Length(12),
                Constraint::Length(28),
                Constraint::Length(12),
                Constraint::Length(14),
            ],
        )
        .header(
            Row::new([
                Span::styled("", Style::default().fg(theme::TABLE_HEADER).bold()),
                Span::styled("ID", Style::default().fg(theme::TABLE_HEADER).bold()),
                Span::styled("Date", Style::default().fg(theme::TABLE_HEADER).bold()),
                Span::styled("Host", Style::default().fg(theme::TABLE_HEADER).bold()),
                Span::styled("Size", Style::default().fg(theme::TABLE_HEADER).bold()),
            ])
            .style(
                Style::default()
                    .fg(theme::TABLE_HEADER)
                    .add_modifier(Modifier::BOLD | Modifier::REVERSED),
            ),
        )
        .block(theme::themed_block("Select Snapshots to Forget"))
        .row_highlight_style(theme::selected_row_style())
        .highlight_symbol(">> ");

        frame.render_stateful_widget(table, chunks[1], &mut self.table_state);

        if has_retention {
            let retention_text = format!("Retention: {}", retention_str);
            frame.render_widget(
                Paragraph::new(retention_text).style(Style::default().fg(Color::Yellow)),
                chunks[2],
            );
        }

        let footer = theme::key_hints(&[
            ("Space", "toggle"),
            ("a", "select all"),
            ("Enter", "confirm"),
            ("r", "retention"),
            ("Esc", "back"),
            ("q", "quit"),
        ]);
        frame.render_widget(Paragraph::new(footer), chunks[3]);
    }

    fn render_retention(&self, frame: &mut Frame) {
        let area = frame.area();

        let popup_width = 60.min(area.width.saturating_sub(4));
        let popup_height = 20.min(area.height.saturating_sub(4));
        let x = (area.width - popup_width) / 2;
        let y = (area.height - popup_height) / 2;

        let popup_area = Rect {
            x,
            y,
            width: popup_width,
            height: popup_height,
        };

        let fields = [
            RetentionField::Last,
            RetentionField::Within,
            RetentionField::Hourly,
            RetentionField::Daily,
            RetentionField::Weekly,
            RetentionField::Monthly,
            RetentionField::Yearly,
        ];

        let mut lines: Vec<Line> = vec![
            Line::from(Span::styled(
                "Retention Rules",
                Style::default().fg(Color::Cyan).bold(),
            )),
            Line::from(""),
        ];

        for field in &fields {
            let is_focused = self.retention_form.focused == *field;
            let is_editing = self.retention_form.editing == Some(*field);
            let has_value = self.retention_form.value_for(*field).is_some();

            let marker = if is_focused { "▶ " } else { "  " };
            let label_style = if is_focused {
                Style::default().fg(Color::Cyan).bold()
            } else {
                Style::default()
            };

            let value_display = if is_editing {
                let buf = &self.retention_form.edit_buffer;
                format!("[{}█]", buf)
            } else if has_value {
                let val = self.retention_form.value_for(*field).unwrap();
                format!("[{}]", val)
            } else {
                "[off]".to_string()
            };

            let value_style = if has_value || is_editing {
                if is_focused {
                    Style::default().fg(Color::Green).bold()
                } else {
                    Style::default().fg(Color::Green)
                }
            } else {
                Style::default().fg(Color::DarkGray)
            };

            lines.push(Line::from(vec![
                Span::styled(marker, Style::default().fg(Color::Yellow)),
                Span::styled(format!("{:<16}", field.label()), label_style),
                Span::styled(value_display, value_style),
            ]));
        }

        let apply_focused = self.retention_form.focused.is_apply();
        let apply_style = if apply_focused {
            Style::default().fg(Color::Cyan).bold()
        } else {
            Style::default().fg(Color::DarkGray)
        };
        let apply_marker = if apply_focused { "  ◀ ▶" } else { "     " };
        lines.push(Line::from(vec![
            Span::raw("               "),
            Span::styled("[ Apply Rules ]", apply_style),
            Span::styled(apply_marker, Style::default().fg(Color::Yellow)),
        ]));

        lines.push(Line::from(""));
        let hint = if self.retention_form.editing.is_some() {
            vec![
                Line::from(Span::styled(
                    "[Enter] confirm  [Esc] cancel  [Backspace] delete",
                    Style::default().fg(Color::DarkGray),
                )),
            ]
        } else {
            vec![
                Line::from(Span::styled(
                    "[Tab] navigate  [Enter/Space] edit  [0] clear",
                    Style::default().fg(Color::DarkGray),
                )),
                Line::from(Span::styled(
                    "[q] quit  [Esc] back",
                    Style::default().fg(Color::DarkGray),
                )),
            ]
        };
        lines.extend(hint);

        let widget = Paragraph::new(Text::from(lines))
            .block(theme::themed_block("Retention Rules"))
            .alignment(ratatui::layout::Alignment::Left);

        frame.render_widget(ratatui::widgets::Clear, popup_area);
        frame.render_widget(widget, popup_area);
    }

    fn render_confirm(&self, frame: &mut Frame) {
        let area = frame.area();
        let inner = area.inner(Margin::new(2, 1));

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(5), Constraint::Length(1)])
            .split(inner);

        let selected: Vec<&SnapshotEntry> = self
            .selections
            .iter()
            .filter(|s| s.selected)
            .filter_map(|s| self.entries.iter().find(|e| e.id == s.id))
            .collect();

        let max_list = inner.height.saturating_sub(8) as usize;
        let display_list: Vec<&SnapshotEntry> = if selected.len() > max_list {
            selected.iter().take(max_list).copied().collect()
        } else {
            selected.clone()
        };

        let mut lines: Vec<Line> = vec![
            Line::from(Span::styled(
                "Confirm Forget",
                Style::default().fg(Color::Red).bold().underlined(),
            )),
            Line::from(""),
            Line::from(format!(
                "This will mark {} snapshot(s) for deletion:",
                selected.len()
            )),
            Line::from(""),
        ];

        for entry in &display_list {
            let id_str = entry.id.to_short_hex(SHORT_SNAPSHOT_ID_LEN);
            let date = utils::pretty_print_timestamp(&entry.snapshot.timestamp, None);
            lines.push(Line::from(vec![
                Span::styled("  - ", Style::default().fg(Color::Red)),
                Span::styled(id_str, Style::default().fg(theme::SNAPSHOT_ID)),
                Span::raw(format!(" ({})", date)),
            ]));
        }

        if selected.len() > max_list {
            lines.push(Line::from(Span::styled(
                format!("  ... and {} more", selected.len() - max_list),
                Style::default().fg(Color::DarkGray),
            )));
        }

        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            "Press [y] to confirm, [n] to cancel.",
            Style::default().fg(Color::Yellow).bold(),
        )));

        let widget = Paragraph::new(Text::from(lines)).block(theme::themed_block("Confirm"));
        frame.render_widget(widget, chunks[0]);

        let footer =
            theme::key_hints(&[("y", "yes"), ("n", "no"), ("Esc", "cancel"), ("q", "quit")]);
        frame.render_widget(Paragraph::new(footer), chunks[1]);
    }

    fn render_result(&self, frame: &mut Frame) {
        let area = frame.area();
        let _inner = area.inner(Margin::new(2, 1));

        let lines = match &self.result {
            Some(ForgetResult::Success { removed }) => {
                let lines: Vec<Line> = {
                    let mut out: Vec<Line> = vec![
                        Line::from(Span::styled(
                            "Forget Complete",
                            Style::default().fg(Color::Green).bold(),
                        )),
                        Line::from(""),
                        Line::from(format!(
                            "Successfully marked {} snapshot(s) for deletion:",
                            removed.len()
                        )),
                        Line::from(""),
                    ];

                    for entry in removed {
                        let id_str = entry.id.to_short_hex(SHORT_SNAPSHOT_ID_LEN);
                        let date = utils::pretty_print_timestamp(&entry.snapshot.timestamp, None);
                        let host = entry.snapshot.hostname.as_deref().unwrap_or("-");
                        out.push(Line::from(vec![
                            Span::styled("  - ", Style::default().fg(Color::Red)),
                            Span::styled(id_str, Style::default().fg(theme::SNAPSHOT_ID)),
                            Span::raw(format!(" ({}) [{}]", date, host)),
                        ]));
                    }

                    out.push(Line::from(""));
                    out.push(Line::from("Run 'clean' to reclaim storage space."));
                    out
                };
                lines
            }
            Some(ForgetResult::NoDeleted) => vec![
                Line::from(Span::styled(
                    "Success",
                    Style::default().fg(Color::Green).bold(),
                )),
                Line::from(""),
                Line::from("No snapshots were deleted."),
            ],
            None => vec![Line::from("Processing...")],
        };

        let widget = Paragraph::new(Text::from(lines)).block(theme::themed_block("Result"));
        frame.render_widget(widget, _inner);
    }
}
