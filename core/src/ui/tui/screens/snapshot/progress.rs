use std::path::PathBuf;

use crossterm::event::KeyCode;
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Style},
    text::{Line, Span},
    widgets::{List, ListItem, Paragraph},
};
use tokio::sync::mpsc;

use crate::{
    fs::{abbreviate_path, tree::NodeDiff},
    mapache::ID,
    repository::snapshot::SnapshotSummary,
    ui::{
        SnapshotProgressReporter,
        tui::{
            theme,
            widgets::{ProgressBar, TaskProgressState},
        },
    },
};

const MAX_ERRORS: usize = 3;
const MAX_WARNINGS: usize = 3;
const MAX_RECENT_NODES: usize = 5;

pub enum SnapshotEvent {
    ProcessedItem,
    ProcessedBytes(u64),
    AddExpectedItems(u64),
    AddExpectedBytes(u64),
    ScanFinished,
    Error(String),
    Warning(String),
    ProcessingNode(PathBuf, NodeDiff, Option<u64>),
    Completed(Box<Result<SummaryResult, String>>),
}

pub struct TuiSnapshotProgressReporter {
    pub tx: mpsc::UnboundedSender<SnapshotEvent>,
}

impl SnapshotProgressReporter for TuiSnapshotProgressReporter {
    fn processing_node(&self, path: &std::path::Path, diff: NodeDiff, _size_hint: Option<u64>) {
        let _ = self.tx.send(SnapshotEvent::ProcessingNode(
            path.to_path_buf(),
            diff,
            None,
        ));
    }

    fn processed_node(&self, _path: &std::path::Path, _diff: NodeDiff, _size_hint: Option<u64>) {
        let _ = self.tx.send(SnapshotEvent::ProcessedItem);
    }

    fn processed_bytes(&self, bytes: u64) {
        let _ = self.tx.send(SnapshotEvent::ProcessedBytes(bytes));
    }

    fn add_expected_items(&self, val: u64) {
        let _ = self.tx.send(SnapshotEvent::AddExpectedItems(val));
    }

    fn add_expected_bytes(&self, val: u64) {
        let _ = self.tx.send(SnapshotEvent::AddExpectedBytes(val));
    }

    fn scan_finished(&self) {
        let _ = self.tx.send(SnapshotEvent::ScanFinished);
    }

    fn error(&self, msg: &str) {
        let _ = self.tx.send(SnapshotEvent::Error(msg.to_string()));
    }

    fn warning(&self, msg: &str) {
        let _ = self.tx.send(SnapshotEvent::Warning(msg.to_string()));
    }

    fn log(&self, _msg: String) {}

    fn verbose_1(&self, _msg: String) {}

    fn verbose_2(&self, _msg: String) {}

    fn finalize(&self) {}
}

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
    recent_nodes: Vec<(String, NodeDiff)>,
    pub spinner_index: usize,
    pub task_result: Option<Result<SummaryResult, String>>,
}

impl ProgressState {
    pub fn new() -> Self {
        Self {
            core: TaskProgressState::new(),
            recent_nodes: Vec::new(),
            spinner_index: 0,
            task_result: None,
        }
    }

    pub fn handle_event(&mut self, event: SnapshotEvent) {
        match event {
            SnapshotEvent::ProcessedItem => {
                self.core.add_processed_items(1);
            }
            SnapshotEvent::ProcessedBytes(bytes) => {
                self.core.add_processed_bytes(bytes);
            }
            SnapshotEvent::AddExpectedItems(val) => {
                self.core.expected_items += val;
            }
            SnapshotEvent::AddExpectedBytes(val) => {
                self.core.expected_bytes += val;
            }
            SnapshotEvent::ScanFinished => {
                self.core.scanning = false;
            }
            SnapshotEvent::Error(msg) => {
                self.core.add_error(msg);
                if self.core.errors.len() > MAX_ERRORS {
                    self.core.errors.remove(0);
                }
            }
            SnapshotEvent::Warning(msg) => {
                self.core.add_warning(msg);
                if self.core.warnings.len() > MAX_WARNINGS {
                    self.core.warnings.remove(0);
                }
            }
            SnapshotEvent::ProcessingNode(path, diff, _size) => {
                let path_str = path.to_string_lossy().to_string();
                self.core.set_message(path_str.clone());
                self.recent_nodes.push((path_str, diff));
                if self.recent_nodes.len() > MAX_RECENT_NODES {
                    self.recent_nodes.remove(0);
                }
            }
            SnapshotEvent::Completed(result) => {
                self.task_result = Some(*result);
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

    pub fn recent_nodes(&self) -> &[(String, NodeDiff)] {
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

pub fn render_progress(frame: &mut Frame, state: &ProgressState) {
    let area = frame.area();
    let inner = area.inner(ratatui::layout::Margin::new(2, 1));

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
        render_errors(frame, chunks[chunks.len() - 2], state);
    }
    render_progress_footer(frame, chunks[chunks.len() - 1]);
}

fn render_progress_bar(frame: &mut Frame, area: Rect, state: &ProgressState) {
    let progress_bar = ProgressBar::new()
        .bytes(state.core.processed_bytes, state.core.expected_bytes)
        .items(state.core.processed_items, state.core.expected_items)
        .elapsed(state.core.elapsed())
        .scanning(state.core.scanning);

    frame.render_widget(progress_bar.render(), area);
}

fn render_recent_nodes(frame: &mut Frame, area: Rect, state: &ProgressState) {
    let max_path_len = (area.width as usize).saturating_sub(4);

    let items: Vec<ListItem> = state
        .recent_nodes()
        .iter()
        .map(|(path, diff)| {
            let diff_style = match diff {
                NodeDiff::New => Style::default().fg(Color::Green),
                NodeDiff::Changed => Style::default().fg(Color::Yellow),
                NodeDiff::Deleted => Style::default().fg(Color::Red),
                NodeDiff::Unchanged => Style::default().fg(Color::DarkGray),
            };
            let diff_label = match diff {
                NodeDiff::New => "+",
                NodeDiff::Changed => "~",
                NodeDiff::Deleted => "-",
                NodeDiff::Unchanged => " ",
            };
            let short_path = abbreviate_path(std::path::Path::new(path), max_path_len);
            ListItem::new(Line::from(vec![
                Span::styled(format!("{} ", diff_label), diff_style),
                Span::raw(short_path),
            ]))
        })
        .collect();

    let list = List::new(items).block(theme::themed_block("Processing"));
    frame.render_widget(list, area);
}

fn render_errors(frame: &mut Frame, area: Rect, state: &ProgressState) {
    let items: Vec<ListItem> = state
        .errors()
        .iter()
        .map(|e| {
            ListItem::new(Line::from(Span::styled(
                e.clone(),
                Style::default().fg(Color::Red),
            )))
        })
        .collect();

    let list = List::new(items).block(theme::themed_block("Errors"));
    frame.render_widget(list, area);
}

fn render_progress_footer(frame: &mut Frame, area: Rect) {
    let footer = theme::key_hint_footer(&[("Esc", "cancel"), ("q", "quit")]);
    frame.render_widget(Paragraph::new(footer), area);
}

pub fn handle_progress_key(key: KeyCode) -> ProgressAction {
    match key {
        KeyCode::Char('q') => ProgressAction::Quit,
        KeyCode::Esc => ProgressAction::Cancel,
        _ => ProgressAction::None,
    }
}
