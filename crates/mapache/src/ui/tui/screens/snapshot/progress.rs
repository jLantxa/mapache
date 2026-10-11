use std::collections::VecDeque;

use crossterm::event::KeyCode;
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::Style,
    text::{Line, Span},
    widgets::{List, ListItem, Paragraph},
};

use crate::{
    common::ID,
    fs::{abbreviate_path, tree::NodeDiff},
    repository::snapshot::SnapshotSummary,
    ui::events::BackupEvent,
    ui::tui::{
        theme,
        widgets::{ProgressBar, TaskProgressState},
    },
};

const MAX_ERRORS: usize = 3;
const MAX_WARNINGS: usize = 3;
const MAX_RECENT_NODES: usize = 5;

#[derive(Debug, Clone)]
pub enum SummaryResult {
    Success {
        summary: Box<SnapshotSummary>,
        snapshot_id: ID,
        duration: std::time::Duration,
    },
    Cancelled,
    Error(String),
    NoChanges,
}

pub struct ProgressState {
    pub core: TaskProgressState,
    recent_nodes: VecDeque<(String, NodeDiff)>,
}

impl ProgressState {
    pub fn new() -> Self {
        Self {
            core: TaskProgressState::new(),
            recent_nodes: VecDeque::new(),
        }
    }

    pub fn handle_event(&mut self, event: BackupEvent) {
        match event {
            BackupEvent::ScanStarted => {
                self.core.scanning = true;
            }
            BackupEvent::ScanProgress { items, bytes } => {
                self.core.expected_items += items;
                self.core.expected_bytes += bytes;
            }
            BackupEvent::ScanFinished { .. } => {
                self.core.scanning = false;
            }
            BackupEvent::NodeProcessing { path, diff, .. } => {
                let path_str = path.to_string_lossy().to_string();
                self.core.set_message(path_str.clone());
                self.recent_nodes.push_back((path_str, diff));
                if self.recent_nodes.len() > MAX_RECENT_NODES {
                    self.recent_nodes.pop_front();
                }
            }
            BackupEvent::NodeProcessed { .. } => {
                self.core.add_processed_items(1);
            }
            BackupEvent::BytesProcessed(bytes) => {
                self.core.add_processed_bytes(bytes);
            }
            BackupEvent::Error(msg) => {
                self.core.add_error(msg);
                if self.core.errors.len() > MAX_ERRORS {
                    self.core.errors.remove(0);
                }
            }
            BackupEvent::Warning(msg) => {
                self.core.add_warning(msg);
                if self.core.warnings.len() > MAX_WARNINGS {
                    self.core.warnings.remove(0);
                }
            }
            BackupEvent::Log(_) => {}
            BackupEvent::Finished(_) => {
                self.core.finish();
            }
        }
    }

    pub fn has_errors(&self) -> bool {
        !self.core.errors.is_empty()
    }

    pub fn has_recent_nodes(&self) -> bool {
        !self.recent_nodes.is_empty()
    }

    pub fn recent_nodes(&self) -> &VecDeque<(String, NodeDiff)> {
        &self.recent_nodes
    }

    pub fn errors(&self) -> &[String] {
        &self.core.errors
    }
}

#[derive(Debug)]
pub enum ProgressAction {
    None,
    Quit,
    Cancel,
}

pub fn render_progress(frame: &mut Frame, state: &ProgressState, dry_run: bool) {
    let inner = frame.area().inner(theme::CONTENT_MARGIN);

    let has_errors = state.has_errors();
    let error_height = if has_errors { 5 } else { 0 };
    let nodes_height = if state.has_recent_nodes() {
        (state.recent_nodes().len() + 2) as u16
    } else {
        0
    };

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(5),
            Constraint::Length(nodes_height),
            Constraint::Length(error_height),
            Constraint::Length(1),
        ])
        .split(inner);

    render_progress_bar(frame, chunks[0], state);
    if state.has_recent_nodes() {
        render_recent_nodes(frame, chunks[1], state);
    }
    if has_errors {
        render_errors(frame, chunks[2], state);
    }
    render_progress_footer(frame, chunks[3], dry_run);
}

fn render_progress_bar(frame: &mut Frame, area: Rect, state: &ProgressState) {
    let rate = state.core.rate_estimator.rate();
    let eta = if !state.core.scanning
        && state.core.expected_bytes > 0
        && state.core.processed_bytes > 0
    {
        state.core.rate_estimator.eta(
            state.core.processed_bytes as f64,
            state.core.expected_bytes as f64,
        )
    } else {
        None
    };

    let progress_bar = ProgressBar::new()
        .bytes(state.core.processed_bytes, state.core.expected_bytes)
        .items(state.core.processed_items, state.core.expected_items)
        .elapsed(state.core.elapsed())
        .scanning(state.core.scanning)
        .cancelling(state.core.cancelling)
        .rate(rate)
        .eta(eta);

    frame.render_widget(progress_bar.render(), area);
}

fn render_recent_nodes(frame: &mut Frame, area: Rect, state: &ProgressState) {
    let max_path_len = (area.width as usize).saturating_sub(4);

    let items: Vec<ListItem> = state
        .recent_nodes()
        .iter()
        .map(|(path, diff)| {
            let (label, color) = match diff {
                NodeDiff::New => ("+", theme::THEME.green),
                NodeDiff::Changed => ("~", theme::THEME.yellow),
                NodeDiff::Deleted => ("-", theme::THEME.red),
                NodeDiff::Unchanged => (" ", theme::THEME.subtext_dim),
            };
            let short_path = abbreviate_path(std::path::Path::new(path), max_path_len);
            ListItem::new(Line::from(vec![
                Span::styled(format!("{} ", label), Style::default().fg(color)),
                Span::raw(short_path),
            ]))
        })
        .collect();

    let list = List::new(items).block(theme::block("Processing"));
    frame.render_widget(list, area);
}

fn render_errors(frame: &mut Frame, area: Rect, state: &ProgressState) {
    let items: Vec<ListItem> = state
        .errors()
        .iter()
        .map(|e| ListItem::new(Line::from(Span::styled(e.clone(), theme::THEME.error))))
        .collect();

    let list = List::new(items).block(theme::block("Errors"));
    frame.render_widget(list, area);
}

fn render_progress_footer(frame: &mut Frame, area: Rect, dry_run: bool) {
    let mut footer = theme::key_hint_footer(&[("Esc", "cancel"), ("q", "back")]);
    if dry_run {
        footer
            .spans
            .insert(0, Span::styled("[DRY RUN] ", theme::THEME.warning));
    }
    frame.render_widget(Paragraph::new(footer), area);
}

pub fn handle_progress_key(key: KeyCode) -> ProgressAction {
    match key {
        KeyCode::Char('q') => ProgressAction::Quit,
        KeyCode::Esc => ProgressAction::Cancel,
        _ => ProgressAction::None,
    }
}
