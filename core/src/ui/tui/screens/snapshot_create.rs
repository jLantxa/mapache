use std::{
    collections::BTreeSet,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use anyhow::Result;
use async_trait::async_trait;
use crossterm::event::{KeyCode, KeyEvent};
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Cell, List, ListItem, Paragraph, Row, Table},
};
use tokio::sync::mpsc;

use crate::{
    archiver::{self, SnapshotOptions, progress::SnapshotProgress},
    backend::StorageHint,
    commands::{
        EMPTY_TAG_MARK, UseSnapshot, cleanup::CleanupHandler, find_use_snapshot, parse_tags,
    },
    fs::{
        self, abbreviate_path, calculate_lcp,
        filter::{PathFilter, merge_filtered_paths, normalized_exclude_paths},
        tree::NodeDiff,
    },
    mapache::{
        self, ContentIdType, ID,
        defaults::{DEFAULT_SNAPSHOT_PACKERS, DEFAULT_SNAPSHOT_READERS, SHORT_SNAPSHOT_ID_LEN},
    },
    repository::{lock::LockHandle, repo::Repository, snapshot::SnapshotSummary},
    ui::SnapshotProgressReporter,
    utils,
};

use crate::ui::tui::{
    app::{Screen, Transition},
    theme,
};

const PROGRESS_BAR_WIDTH: usize = 35;
const POPUP_WIDTH: u16 = 60;
const POPUP_HEIGHT: u16 = 10;
const POPUP_MARGIN: u16 = 4;
const MAX_ERRORS: usize = 3;
const MAX_WARNINGS: usize = 3;
const MAX_RECENT_NODES: usize = 5;

#[derive(Debug, Clone, Copy, PartialEq)]
enum SnapshotPhase {
    Config,
    Progress,
    Summary,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum FormField {
    Paths,
    Tags,
    Description,
    Exclude,
    AsRoot,
    NoParent,
    Readers,
    Packers,
    Start,
}

impl FormField {
    fn next(&self) -> Self {
        match self {
            FormField::Paths => FormField::Tags,
            FormField::Tags => FormField::Description,
            FormField::Description => FormField::Exclude,
            FormField::Exclude => FormField::AsRoot,
            FormField::AsRoot => FormField::NoParent,
            FormField::NoParent => FormField::Readers,
            FormField::Readers => FormField::Packers,
            FormField::Packers => FormField::Start,
            FormField::Start => FormField::Paths,
        }
    }

    fn prev(&self) -> Self {
        match self {
            FormField::Paths => FormField::Start,
            FormField::Tags => FormField::Paths,
            FormField::Description => FormField::Tags,
            FormField::Exclude => FormField::Description,
            FormField::AsRoot => FormField::Exclude,
            FormField::NoParent => FormField::AsRoot,
            FormField::Readers => FormField::NoParent,
            FormField::Packers => FormField::Readers,
            FormField::Start => FormField::Packers,
        }
    }

    fn is_text_input(&self) -> bool {
        matches!(
            self,
            FormField::Paths | FormField::Tags | FormField::Description | FormField::Exclude
        )
    }

    fn is_number_field(&self) -> bool {
        matches!(self, FormField::Readers | FormField::Packers)
    }

    fn is_toggle(&self) -> bool {
        matches!(self, FormField::AsRoot | FormField::NoParent)
    }

    fn is_start(&self) -> bool {
        matches!(self, FormField::Start)
    }
}

struct SnapshotForm {
    paths: String,
    tags: String,
    description: String,
    exclude: String,
    as_root: bool,
    no_parent: bool,
    readers: u32,
    packers: u32,
}

impl SnapshotForm {
    fn new(config_defaults: Option<&crate::commands::cmd_snapshot::CmdArgs>) -> Self {
        config_defaults
            .map(|cfg| Self {
                paths: cfg
                    .paths
                    .iter()
                    .map(|p| p.to_string_lossy().to_string())
                    .collect::<Vec<_>>()
                    .join(","),
                tags: cfg.tags_str.clone().unwrap_or_default(),
                description: cfg.description.clone().unwrap_or_default(),
                exclude: cfg
                    .exclude
                    .as_ref()
                    .map(|e| e.join(","))
                    .unwrap_or_default(),
                as_root: cfg.as_root.unwrap_or(false),
                no_parent: cfg.no_parent,
                readers: cfg.num_readers.unwrap_or(DEFAULT_SNAPSHOT_READERS) as u32,
                packers: cfg.num_packers.unwrap_or(DEFAULT_SNAPSHOT_PACKERS) as u32,
            })
            .unwrap_or_else(|| Self {
                paths: String::new(),
                tags: String::new(),
                description: String::new(),
                exclude: String::new(),
                as_root: false,
                no_parent: false,
                readers: DEFAULT_SNAPSHOT_READERS as u32,
                packers: DEFAULT_SNAPSHOT_PACKERS as u32,
            })
    }
}

enum SnapshotEvent {
    ProcessedItem,
    ProcessedBytes(u64),
    AddExpectedItems(u64),
    AddExpectedBytes(u64),
    ScanFinished,
    Error(String),
    Warning(String),
    ProcessingNode(PathBuf, NodeDiff, Option<u64>),
    Completed(Box<Result<SummaryResult>>),
}

struct TuiSnapshotProgressReporter {
    tx: mpsc::UnboundedSender<SnapshotEvent>,
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

    fn finalize(&self) {}
}

struct ProgressState {
    expected_bytes: u64,
    processed_bytes: u64,
    expected_items: u64,
    processed_items: u64,
    errors: Vec<String>,
    warnings: Vec<String>,
    current_file: Option<String>,
    recent_nodes: Vec<(String, NodeDiff)>,
    scan_finished: bool,
    spinner_index: usize,
    start_time: Instant,
    task_result: Option<Result<SummaryResult>>,
}

impl ProgressState {
    fn new() -> Self {
        Self {
            expected_bytes: 0,
            processed_bytes: 0,
            expected_items: 0,
            processed_items: 0,
            errors: Vec::new(),
            warnings: Vec::new(),
            current_file: None,
            recent_nodes: Vec::new(),
            scan_finished: false,
            spinner_index: 0,
            start_time: Instant::now(),
            task_result: None,
        }
    }

    fn handle_event(&mut self, event: SnapshotEvent) {
        match event {
            SnapshotEvent::ProcessedItem => {
                self.processed_items += 1;
            }
            SnapshotEvent::ProcessedBytes(bytes) => {
                self.processed_bytes += bytes;
            }
            SnapshotEvent::AddExpectedItems(val) => {
                self.expected_items += val;
            }
            SnapshotEvent::AddExpectedBytes(val) => {
                self.expected_bytes += val;
            }
            SnapshotEvent::ScanFinished => {
                self.scan_finished = true;
            }
            SnapshotEvent::Error(msg) => {
                self.errors.push(msg);
                if self.errors.len() > MAX_ERRORS {
                    self.errors.remove(0);
                }
            }
            SnapshotEvent::Warning(msg) => {
                self.warnings.push(msg);
                if self.warnings.len() > MAX_WARNINGS {
                    self.warnings.remove(0);
                }
            }
            SnapshotEvent::ProcessingNode(path, diff, _size) => {
                let path_str = path.to_string_lossy().to_string();
                self.current_file = Some(path_str.clone());
                self.recent_nodes.push((path_str, diff));
                if self.recent_nodes.len() > MAX_RECENT_NODES {
                    self.recent_nodes.remove(0);
                }
            }
            SnapshotEvent::Completed(result) => {
                self.task_result = Some(*result);
            }
        }
    }

    fn percentage(&self) -> f64 {
        if self.expected_bytes == 0 {
            0.0
        } else {
            (self.processed_bytes as f64 / self.expected_bytes as f64) * 100.0
        }
    }

    fn elapsed(&self) -> std::time::Duration {
        self.start_time.elapsed()
    }

    fn eta(&self) -> Option<std::time::Duration> {
        if self.expected_bytes == 0 || self.processed_bytes == 0 {
            return None;
        }
        let elapsed = self.elapsed();
        let rate = self.processed_bytes as f64 / elapsed.as_secs_f64();
        if rate == 0.0 {
            return None;
        }
        let remaining = self.expected_bytes.saturating_sub(self.processed_bytes) as f64;
        Some(std::time::Duration::from_secs((remaining / rate) as u64))
    }

    fn rate(&self) -> f64 {
        let elapsed = self.elapsed();
        if elapsed.as_secs_f64() == 0.0 {
            0.0
        } else {
            self.processed_bytes as f64 / elapsed.as_secs_f64()
        }
    }

    fn format_duration_secs(secs: u64) -> String {
        if secs >= 3600 {
            format!("{}h {}m", secs / 3600, (secs % 3600) / 60)
        } else if secs >= 60 {
            format!("{}m {}s", secs / 60, secs % 60)
        } else {
            format!("{}s", secs)
        }
    }

    fn eta_display(&self) -> Option<String> {
        self.eta().map(|d| Self::format_duration_secs(d.as_secs()))
    }

    fn elapsed_display(&self) -> String {
        let secs = self.elapsed().as_secs();
        if secs >= 3600 {
            format!("{}h {}m {}s", secs / 3600, (secs % 3600) / 60, secs % 60)
        } else if secs >= 60 {
            format!("{}m {}s", secs / 60, secs % 60)
        } else {
            format!("{}s", secs)
        }
    }
}

#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
enum SummaryResult {
    Success {
        summary: Box<SnapshotSummary>,
        snapshot_id: ID,
        duration: std::time::Duration,
    },
    Cancelled,
    Error(String),
    NoChanges,
}

pub struct SnapshotCreateScreen {
    repo: Arc<Repository>,
    lock_handle: LockHandle,
    phase: SnapshotPhase,
    form: SnapshotForm,
    focused_field: FormField,
    editing: Option<FormField>,
    edit_buffer: String,
    edit_cursor: usize,
    progress: ProgressState,
    shutdown_signal: Arc<AtomicBool>,
    rx: Option<mpsc::UnboundedReceiver<SnapshotEvent>>,
    summary: Option<SummaryResult>,
}

impl SnapshotCreateScreen {
    pub fn new(
        repo: Arc<Repository>,
        lock_handle: LockHandle,
        config_defaults: Option<crate::commands::cmd_snapshot::CmdArgs>,
    ) -> Self {
        Self {
            repo,
            lock_handle,
            phase: SnapshotPhase::Config,
            form: SnapshotForm::new(config_defaults.as_ref()),
            focused_field: FormField::Paths,
            editing: None,
            edit_buffer: String::new(),
            edit_cursor: 0,
            progress: ProgressState::new(),
            shutdown_signal: Arc::new(AtomicBool::new(false)),
            rx: None,
            summary: None,
        }
    }

    fn handle_key_internal(&mut self, key: KeyEvent) -> Option<SnapshotCreateAction> {
        match self.phase {
            SnapshotPhase::Config => self.handle_config_key(key),
            SnapshotPhase::Progress => self.handle_progress_key(key),
            SnapshotPhase::Summary => self.handle_summary_key(key),
        }
    }

    fn handle_config_key(&mut self, key: KeyEvent) -> Option<SnapshotCreateAction> {
        if let Some(editing) = self.editing {
            return self.handle_edit_key(key, editing);
        }

        match key.code {
            KeyCode::Char('q') => Some(SnapshotCreateAction::Quit),
            KeyCode::Esc => Some(SnapshotCreateAction::Cancel),
            KeyCode::Enter => {
                if self.focused_field.is_start() {
                    self.start_snapshot();
                } else if self.focused_field.is_text_input() || self.focused_field.is_number_field()
                {
                    self.start_editing();
                } else if self.focused_field.is_toggle() {
                    self.toggle_bool_field();
                }
                None
            }
            KeyCode::Tab | KeyCode::Down => {
                self.focused_field = self.focused_field.next();
                None
            }
            KeyCode::BackTab | KeyCode::Up => {
                self.focused_field = self.focused_field.prev();
                None
            }
            KeyCode::Char(' ') => {
                if self.focused_field.is_toggle() {
                    self.toggle_bool_field();
                } else if self.focused_field.is_text_input() || self.focused_field.is_number_field()
                {
                    self.start_editing();
                }
                None
            }
            KeyCode::Left => {
                if self.focused_field.is_number_field() {
                    self.decrement_number_field();
                }
                None
            }
            KeyCode::Right => {
                if self.focused_field.is_number_field() {
                    self.increment_number_field();
                }
                None
            }
            _ => None,
        }
    }

    fn start_editing(&mut self) {
        let value = match self.focused_field {
            FormField::Paths => self.form.paths.clone(),
            FormField::Tags => self.form.tags.clone(),
            FormField::Description => self.form.description.clone(),
            FormField::Exclude => self.form.exclude.clone(),
            FormField::Readers => self.form.readers.to_string(),
            FormField::Packers => self.form.packers.to_string(),
            FormField::AsRoot | FormField::NoParent | FormField::Start => String::new(),
        };
        self.edit_cursor = value.chars().count();
        self.edit_buffer = value;
        self.editing = Some(self.focused_field);
    }

    fn handle_edit_key(
        &mut self,
        key: KeyEvent,
        editing: FormField,
    ) -> Option<SnapshotCreateAction> {
        match key.code {
            KeyCode::Esc => {
                self.editing = None;
                self.edit_buffer.clear();
                self.edit_cursor = 0;
                None
            }
            KeyCode::Enter => {
                self.apply_edit(editing);
                None
            }
            KeyCode::Char(c) => {
                self.edit_insert(c);
                None
            }
            KeyCode::Backspace => {
                self.edit_delete_before();
                None
            }
            KeyCode::Delete => {
                self.edit_delete_at();
                None
            }
            KeyCode::Left => {
                if self.edit_cursor > 0 {
                    self.edit_cursor -= 1;
                }
                None
            }
            KeyCode::Right => {
                if self.edit_cursor < self.edit_buffer.chars().count() {
                    self.edit_cursor += 1;
                }
                None
            }
            KeyCode::Home => {
                self.edit_cursor = 0;
                None
            }
            KeyCode::End => {
                self.edit_cursor = self.edit_buffer.chars().count();
                None
            }
            _ => None,
        }
    }

    fn edit_insert(&mut self, c: char) {
        let byte_pos = self
            .edit_buffer
            .char_indices()
            .nth(self.edit_cursor)
            .map(|(i, _)| i)
            .unwrap_or(self.edit_buffer.len());
        self.edit_buffer.insert(byte_pos, c);
        self.edit_cursor += 1;
    }

    fn edit_delete_before(&mut self) {
        if self.edit_cursor == 0 {
            return;
        }
        if let Some((pos, _)) = self.edit_buffer.char_indices().nth(self.edit_cursor - 1) {
            self.edit_buffer.remove(pos);
            self.edit_cursor -= 1;
        }
    }

    fn edit_delete_at(&mut self) {
        if self.edit_cursor < self.edit_buffer.chars().count()
            && let Some((pos, _)) = self.edit_buffer.char_indices().nth(self.edit_cursor)
        {
            self.edit_buffer.remove(pos);
        }
    }

    fn apply_edit(&mut self, field: FormField) {
        match field {
            FormField::Paths => {
                self.form.paths = self.edit_buffer.clone();
            }
            FormField::Tags => {
                self.form.tags = self.edit_buffer.clone();
            }
            FormField::Description => {
                self.form.description = self.edit_buffer.clone();
            }
            FormField::Exclude => {
                self.form.exclude = self.edit_buffer.clone();
            }
            FormField::Readers => {
                if let Ok(n) = self.edit_buffer.trim().parse::<u32>() {
                    self.form.readers = n.max(1);
                }
            }
            FormField::Packers => {
                if let Ok(n) = self.edit_buffer.trim().parse::<u32>() {
                    self.form.packers = n.max(1);
                }
            }
            FormField::AsRoot | FormField::NoParent | FormField::Start => {}
        }
        self.editing = None;
        self.edit_buffer.clear();
        self.edit_cursor = 0;
    }

    fn handle_progress_key(&mut self, key: KeyEvent) -> Option<SnapshotCreateAction> {
        match key.code {
            KeyCode::Char('q') => Some(SnapshotCreateAction::Quit),
            KeyCode::Esc => {
                self.shutdown_signal.store(true, Ordering::SeqCst);
                None
            }
            _ => None,
        }
    }

    fn handle_summary_key(&mut self, key: KeyEvent) -> Option<SnapshotCreateAction> {
        match key.code {
            KeyCode::Char('q') => Some(SnapshotCreateAction::Quit),
            KeyCode::Enter | KeyCode::Esc => Some(SnapshotCreateAction::Done),
            _ => None,
        }
    }

    fn toggle_bool_field(&mut self) {
        match self.focused_field {
            FormField::AsRoot => {
                self.form.as_root = !self.form.as_root;
            }
            FormField::NoParent => {
                self.form.no_parent = !self.form.no_parent;
            }
            _ => {}
        }
    }

    fn increment_number_field(&mut self) {
        match self.focused_field {
            FormField::Readers => {
                self.form.readers = self.form.readers.saturating_add(1);
            }
            FormField::Packers => {
                self.form.packers = self.form.packers.saturating_add(1);
            }
            _ => {}
        }
    }

    fn decrement_number_field(&mut self) {
        match self.focused_field {
            FormField::Readers => {
                self.form.readers = self.form.readers.saturating_sub(1).max(1);
            }
            FormField::Packers => {
                self.form.packers = self.form.packers.saturating_sub(1).max(1);
            }
            _ => {}
        }
    }

    fn start_snapshot(&mut self) {
        if self.form.paths.trim().is_empty() || self.form.readers == 0 || self.form.packers == 0 {
            return;
        }

        let repo = self.repo.clone();
        let lock_handle = self.lock_handle.clone();
        let shutdown_signal = self.shutdown_signal.clone();

        let paths: Vec<PathBuf> = self
            .form
            .paths
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(PathBuf::from)
            .collect();

        let tags_str = self.form.tags.clone();
        let description = if self.form.description.is_empty() {
            None
        } else {
            Some(self.form.description.clone())
        };
        let exclude: Vec<String> = self
            .form
            .exclude
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect();
        let as_root = self.form.as_root;
        let no_parent = self.form.no_parent;
        let num_readers = self.form.readers as usize;
        let num_packers = self.form.packers as usize;

        let (tx, rx) = mpsc::unbounded_channel();
        let reporter = Arc::new(TuiSnapshotProgressReporter { tx });

        self.rx = Some(rx);
        self.phase = SnapshotPhase::Progress;
        self.progress = ProgressState::new();
        self.shutdown_signal.store(false, Ordering::SeqCst);

        tokio::spawn(async move {
            let _ = Self::run_snapshot_task(
                repo,
                lock_handle,
                paths,
                tags_str,
                description,
                exclude,
                as_root,
                no_parent,
                num_readers,
                num_packers,
                reporter,
                shutdown_signal,
            )
            .await;
        });
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_snapshot_task(
        repo: Arc<Repository>,
        lock_handle: LockHandle,
        paths: Vec<PathBuf>,
        tags_str: String,
        description: Option<String>,
        exclude: Vec<String>,
        as_root: bool,
        no_parent: bool,
        num_readers: usize,
        num_packers: usize,
        reporter: Arc<TuiSnapshotProgressReporter>,
        shutdown_signal: Arc<AtomicBool>,
    ) -> Result<SummaryResult> {
        let start = Instant::now();
        repo.reload_master_index().await?;

        let source_paths = if !as_root {
            paths.clone()
        } else {
            if paths.len() != 1 {
                let result =
                    SummaryResult::Error("Only one path can be the snapshot root".to_string());
                let _ = reporter
                    .tx
                    .send(SnapshotEvent::Completed(Box::new(Ok(result.clone()))));
                return Ok(result);
            }
            let root = &paths[0];
            if !root.is_dir() {
                let result =
                    SummaryResult::Error("The snapshot root must be a directory".to_string());
                let _ = reporter
                    .tx
                    .send(SnapshotEvent::Completed(Box::new(Ok(result.clone()))));
                return Ok(result);
            }
            let mut dir = tokio::fs::read_dir(root).await?;
            let mut result = Vec::new();
            while let Some(entry) = dir.next_entry().await? {
                result.push(entry.path());
            }
            result
        };

        let tags_str = if tags_str.is_empty() {
            EMPTY_TAG_MARK.to_string()
        } else {
            tags_str
        };
        let mut tags: BTreeSet<String> = parse_tags(Some(&tags_str));
        tags.retain(|tag| tag != EMPTY_TAG_MARK);

        let mut absolute_source_paths = BTreeSet::new();
        for path in &source_paths {
            match fs::get_absolute_normalized_path(path) {
                Ok(absolute_path) => {
                    let _ = absolute_source_paths.insert(absolute_path);
                }
                Err(e) => {
                    let result =
                        SummaryResult::Error(format!("Error processing path {:?}: {}", path, e));
                    let _ = reporter
                        .tx
                        .send(SnapshotEvent::Completed(Box::new(Ok(result.clone()))));
                    return Ok(result);
                }
            }
        }

        let normalized_excludes: Option<Vec<PathBuf>> = normalized_exclude_paths(
            merge_filtered_paths(
                if exclude.is_empty() {
                    None
                } else {
                    Some(&exclude)
                },
                None,
            )
            .as_ref(),
        )?;
        let path_filter = PathFilter::new(None, normalized_excludes.clone());
        absolute_source_paths.retain(|p| path_filter.allow(p));
        let absolute_source_paths: Vec<PathBuf> = absolute_source_paths.into_iter().collect();

        let snapshot_root_path = calculate_lcp(&absolute_source_paths, false);

        let parent = UseSnapshot::Latest;
        let parent_snapshot_pair: Option<crate::repository::snapshot::SnapshotPair> =
            match no_parent {
                true => None,
                false => match find_use_snapshot(repo.clone(), &parent).await {
                    Ok(Some((id, snapshot))) => {
                        Some(crate::repository::snapshot::SnapshotPair { id, snapshot })
                    }
                    Ok(None) => None,
                    Err(e) => {
                        let result =
                            SummaryResult::Error(format!("Parent snapshot not found: {}", e));
                        let _ = reporter
                            .tx
                            .send(SnapshotEvent::Completed(Box::new(Ok(result.clone()))));
                        return Ok(result);
                    }
                },
            };

        let progress = Arc::new(SnapshotProgress::new());

        let reporter_clone = reporter.clone();
        let cleanup_handler = CleanupHandler::new_with_callback(move || {
            reporter_clone.finalize();
        })?;
        cleanup_handler.add_lock(lock_handle.clone());

        repo.init_pack_saver(num_packers)
            .map_err(|e| anyhow::anyhow!("Failed to initialize pack saver: {}", e))?;

        let snapshot_result = archiver::snapshot(
            repo.clone(),
            SnapshotOptions {
                absolute_source_paths,
                snapshot_root_path,
                exclude_paths: normalized_excludes.unwrap_or_default(),
                parent_snapshot: parent_snapshot_pair.as_ref(),
                tags,
                description,
                no_scan: false,
                with_atime: false,
            },
            num_readers,
            progress.clone(),
            reporter.clone(),
            shutdown_signal.clone(),
        )
        .await;

        let repo_stats = repo
            .flush_and_finalize_pack_saver()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to finalize snapshot: {}", e))?;

        if shutdown_signal.load(Ordering::SeqCst) {
            let result = SummaryResult::Cancelled;
            let _ = reporter
                .tx
                .send(SnapshotEvent::Completed(Box::new(Ok(result.clone()))));
            return Ok(result);
        }

        let mut new_snapshot = match snapshot_result {
            Ok(s) => s,
            Err(e) => {
                let result = SummaryResult::Error(format!("Snapshot failed: {}", e));
                let _ = reporter
                    .tx
                    .send(SnapshotEvent::Completed(Box::new(Ok(result.clone()))));
                return Ok(result);
            }
        };

        let snapshot_report_summary = progress.summary();

        new_snapshot.summary.processed_items_count = snapshot_report_summary.processed_items_count;
        new_snapshot.summary.processed_bytes = snapshot_report_summary.processed_bytes;
        new_snapshot.summary.diff_counts = snapshot_report_summary.diff_counts;
        new_snapshot.summary.raw_bytes = repo_stats.data.raw;
        new_snapshot.summary.encoded_bytes = repo_stats.data.encoded;
        new_snapshot.summary.meta_raw_bytes = repo_stats.meta.raw;
        new_snapshot.summary.meta_encoded_bytes = repo_stats.meta.encoded;
        new_snapshot.summary.total_raw_bytes =
            new_snapshot.summary.raw_bytes + new_snapshot.summary.meta_raw_bytes;
        new_snapshot.summary.total_encoded_bytes =
            new_snapshot.summary.encoded_bytes + new_snapshot.summary.meta_encoded_bytes;
        new_snapshot.summary.data_blobs = repo_stats.blobs;
        new_snapshot.summary.meta_blobs = repo_stats.meta_blobs;

        let should_save = parent_snapshot_pair.is_none()
            || (parent_snapshot_pair.unwrap().snapshot.tree != new_snapshot.tree);

        if should_save {
            let (new_snapshot_id, new_snapshot_size) = repo
                .save_file(
                    &mapache::SaveID::CalculateID,
                    serde_json::to_string(&new_snapshot)?.as_bytes(),
                    StorageHint {
                        is_metadata: true,
                        file_type: ContentIdType::Snapshot,
                    },
                    None,
                )
                .await?;

            new_snapshot.summary.meta_raw_bytes += new_snapshot_size.raw + repo_stats.index.raw;
            new_snapshot.summary.meta_encoded_bytes +=
                new_snapshot_size.encoded + repo_stats.index.encoded;

            let summary_clone = Box::new(new_snapshot.summary.clone());
            let duration = start.elapsed();
            let result = SummaryResult::Success {
                summary: summary_clone,
                snapshot_id: new_snapshot_id,
                duration,
            };
            let _ = reporter
                .tx
                .send(SnapshotEvent::Completed(Box::new(Ok(result.clone()))));
            Ok(result)
        } else {
            let result = SummaryResult::NoChanges;
            let _ = reporter
                .tx
                .send(SnapshotEvent::Completed(Box::new(Ok(result.clone()))));
            Ok(result)
        }
    }

    pub fn check_completion(&mut self) {
        if self.phase != SnapshotPhase::Progress {
            return;
        }

        if let Some(rx) = &mut self.rx {
            while let Ok(event) = rx.try_recv() {
                if let SnapshotEvent::Completed(result) = &event {
                    match result.as_ref() {
                        Ok(summary_result) => {
                            self.summary = Some(summary_result.clone());
                        }
                        Err(e) => {
                            self.summary = Some(SummaryResult::Error(e.to_string()));
                        }
                    }
                    self.phase = SnapshotPhase::Summary;
                }
                self.progress.handle_event(event);
            }
        }
    }

    fn render_config(&self, frame: &mut Frame) {
        let area = frame.area();
        let inner = area.inner(ratatui::layout::Margin::new(2, 1));

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(10), Constraint::Length(1)])
            .split(inner);

        self.render_config_form(frame, chunks[0]);
        self.render_config_footer(frame, chunks[1]);

        if self.editing.is_some() {
            self.render_edit_popup(frame, area);
        }
    }

    fn render_config_form(&self, frame: &mut Frame, area: Rect) {
        let form_items = self.build_form_lines();
        let form = Paragraph::new(Text::from(form_items))
            .block(theme::themed_block("Snapshot Configuration"));
        frame.render_widget(form, area);
    }

    fn build_form_lines(&self) -> Vec<Line<'static>> {
        let mut lines = Vec::new();
        let is_focused = |f: &FormField| self.focused_field == *f;

        let focus_style = Style::default()
            .fg(theme::SNAPSHOT_ID)
            .add_modifier(Modifier::BOLD);
        let normal_style = Style::default();

        let add_text_field =
            |lines: &mut Vec<Line<'static>>, label: &str, value: &str, focused: bool| {
                let label_style = if focused { focus_style } else { normal_style };
                let display = if value.is_empty() {
                    Span::styled("(empty)", Style::default().fg(Color::DarkGray))
                } else {
                    Span::raw(value.to_string())
                };
                let marker = if focused { "▶ " } else { "  " };
                lines.push(Line::from(vec![
                    Span::styled(marker, Style::default().fg(theme::SNAPSHOT_DATE)),
                    Span::styled(format!("{:<13}", label), label_style.bold()),
                    display,
                ]));
            };

        let add_toggle_field =
            |lines: &mut Vec<Line<'static>>, label: &str, value: bool, focused: bool| {
                let label_style = if focused { focus_style } else { normal_style };
                let checkbox = if value { "[X]" } else { "[ ]" };
                let checkbox_style = if focused {
                    Style::default().fg(theme::SNAPSHOT_DATE)
                } else {
                    normal_style
                };
                let marker = if focused { "▶ " } else { "  " };
                lines.push(Line::from(vec![
                    Span::styled(marker, Style::default().fg(theme::SNAPSHOT_DATE)),
                    Span::styled(format!("{:<13}", label), label_style.bold()),
                    Span::styled(checkbox, checkbox_style),
                ]));
            };

        let add_number_field = |lines: &mut Vec<Line<'static>>,
                                label: &str,
                                value: u32,
                                field: FormField,
                                focused: bool,
                                editing: Option<FormField>,
                                edit_buffer: &str| {
            let label_style = if focused { focus_style } else { normal_style };
            let marker = if focused { "▶ " } else { "  " };
            let is_editing = editing == Some(field);
            let value_str = if is_editing {
                format!("[ {}█]", edit_buffer)
            } else {
                format!("[ {} ]", value)
            };
            let value_style = if focused {
                Style::default().fg(theme::SNAPSHOT_DATE)
            } else {
                normal_style
            };
            lines.push(Line::from(vec![
                Span::styled(marker, Style::default().fg(theme::SNAPSHOT_DATE)),
                Span::styled(format!("{:<13}", label), label_style.bold()),
                Span::styled(value_str, value_style),
            ]));
        };

        add_text_field(
            &mut lines,
            "Paths:",
            &self.form.paths,
            is_focused(&FormField::Paths),
        );
        add_text_field(
            &mut lines,
            "Tags:",
            &self.form.tags,
            is_focused(&FormField::Tags),
        );
        add_text_field(
            &mut lines,
            "Description:",
            &self.form.description,
            is_focused(&FormField::Description),
        );
        add_text_field(
            &mut lines,
            "Exclude:",
            &self.form.exclude,
            is_focused(&FormField::Exclude),
        );
        add_toggle_field(
            &mut lines,
            "As root:",
            self.form.as_root,
            is_focused(&FormField::AsRoot),
        );
        add_toggle_field(
            &mut lines,
            "Full scan:",
            self.form.no_parent,
            is_focused(&FormField::NoParent),
        );
        add_number_field(
            &mut lines,
            "Readers:",
            self.form.readers,
            FormField::Readers,
            is_focused(&FormField::Readers),
            self.editing,
            &self.edit_buffer,
        );
        add_number_field(
            &mut lines,
            "Packers:",
            self.form.packers,
            FormField::Packers,
            is_focused(&FormField::Packers),
            self.editing,
            &self.edit_buffer,
        );

        let start_style = if is_focused(&FormField::Start) {
            Style::default().fg(theme::SNAPSHOT_DATE).bold()
        } else {
            Style::default().fg(Color::DarkGray)
        };
        let start_marker = if is_focused(&FormField::Start) {
            " ◀ ▶"
        } else {
            "    "
        };
        lines.push(Line::from(vec![
            Span::raw("               "),
            Span::styled("[ Start Snapshot ]", start_style),
            Span::raw(start_marker),
        ]));

        lines
    }

    fn render_config_footer(&self, frame: &mut Frame, area: Rect) {
        let footer = if self.editing.is_some() {
            Line::from(vec![
                Span::styled("[Enter]", Style::default().fg(theme::MENU_KEY).bold()),
                Span::raw(" confirm"),
                Span::raw("    "),
                Span::styled("[Esc]", Style::default().fg(theme::MENU_KEY).bold()),
                Span::raw(" cancel edit"),
            ])
        } else if self.focused_field.is_number_field() {
            Line::from(vec![
                Span::styled("[Left]", Style::default().fg(theme::MENU_KEY).bold()),
                Span::raw(" dec"),
                Span::raw("    "),
                Span::styled("[Right]", Style::default().fg(theme::MENU_KEY).bold()),
                Span::raw(" inc"),
                Span::raw("    "),
                Span::styled("[Enter]", Style::default().fg(theme::MENU_KEY).bold()),
                Span::raw(" edit"),
                Span::raw("    "),
                Span::styled("[Tab]", Style::default().fg(theme::MENU_KEY).bold()),
                Span::raw(" next"),
                Span::raw("    "),
                Span::styled("[q]", Style::default().fg(theme::MENU_KEY).bold()),
                Span::raw(" quit"),
            ])
        } else {
            Line::from(vec![
                Span::styled("[Tab]", Style::default().fg(theme::MENU_KEY).bold()),
                Span::raw(" next"),
                Span::raw("    "),
                Span::styled("[Enter]", Style::default().fg(theme::MENU_KEY).bold()),
                Span::raw(" edit/start"),
                Span::raw("    "),
                Span::styled("[Space]", Style::default().fg(theme::MENU_KEY).bold()),
                Span::raw(" toggle"),
                Span::raw("    "),
                Span::styled("[Esc]", Style::default().fg(theme::MENU_KEY).bold()),
                Span::raw(" cancel"),
                Span::raw("    "),
                Span::styled("[q]", Style::default().fg(theme::MENU_KEY).bold()),
                Span::raw(" quit"),
            ])
        };
        frame.render_widget(Paragraph::new(footer), area);
    }

    fn render_edit_popup(&self, frame: &mut Frame, _area: Rect) {
        let Some(editing) = &self.editing else {
            return;
        };

        let area = frame.area();
        let popup_width = POPUP_WIDTH.min(area.width.saturating_sub(POPUP_MARGIN));
        let popup_height = POPUP_HEIGHT.min(area.height.saturating_sub(POPUP_MARGIN));
        let x = (area.width - popup_width) / 2;
        let y = (area.height - popup_height) / 2;

        let popup_area = Rect {
            x,
            y,
            width: popup_width,
            height: popup_height,
        };

        let label = match editing {
            FormField::Paths => "Paths",
            FormField::Tags => "Tags",
            FormField::Description => "Description",
            FormField::Exclude => "Exclude",
            FormField::Readers
            | FormField::Packers
            | FormField::AsRoot
            | FormField::NoParent
            | FormField::Start => return,
        };

        let inner_width = (popup_width as usize).saturating_sub(2);
        let lines: Vec<String> = self
            .edit_buffer
            .chars()
            .collect::<Vec<char>>()
            .chunks(inner_width)
            .map(|chunk| chunk.iter().collect())
            .collect();

        let cursor_line = self.edit_cursor / inner_width;
        let cursor_col = self.edit_cursor % inner_width;

        let visible_lines = popup_height.saturating_sub(2) as usize;
        let scroll_start = cursor_line.saturating_sub(visible_lines / 2);
        let visible: Vec<Line<'static>> = lines
            .iter()
            .enumerate()
            .skip(scroll_start)
            .take(visible_lines)
            .map(|(line_idx, line)| {
                let actual_line_idx = line_idx + scroll_start;
                if actual_line_idx == cursor_line {
                    let before: String = line.chars().take(cursor_col).collect();
                    let after: String = line.chars().skip(cursor_col).collect();
                    let cursor_span = if after.is_empty() {
                        Span::styled(" ", Style::default().underlined())
                    } else {
                        Span::styled(after, Style::default().underlined())
                    };
                    Line::from(vec![Span::raw(before), cursor_span])
                } else {
                    Line::from(line.clone())
                }
            })
            .collect();

        let popup_title = format!("Edit {}", label);
        let content = Paragraph::new(Text::from(visible)).block(theme::themed_block(&popup_title));

        frame.render_widget(ratatui::widgets::Clear, popup_area);
        frame.render_widget(content, popup_area);
    }

    fn render_progress(&mut self, frame: &mut Frame) {
        self.progress.spinner_index += 1;

        let area = frame.area();
        let inner = area.inner(ratatui::layout::Margin::new(2, 1));

        let has_errors = !self.progress.errors.is_empty();
        let error_height = if has_errors { 5 } else { 0 };
        let nodes_height = if self.progress.recent_nodes.is_empty() {
            0
        } else {
            (self.progress.recent_nodes.len() + 2) as u16
        };

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(5),
                Constraint::Length(3),
                Constraint::Length(nodes_height),
                Constraint::Length(error_height),
                Constraint::Length(1),
            ])
            .split(inner);

        self.render_progress_bar(frame, chunks[0]);
        self.render_progress_items(frame, chunks[1]);
        if !self.progress.recent_nodes.is_empty() {
            self.render_recent_nodes(frame, chunks[2]);
        }
        if has_errors {
            self.render_errors(frame, chunks[3]);
        }
        self.render_progress_footer(frame, chunks[4]);
    }

    fn render_progress_bar(&self, frame: &mut Frame, area: Rect) {
        let bar_width = PROGRESS_BAR_WIDTH;
        let cyan_style = Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD);
        let white_style = Style::default().fg(Color::DarkGray);

        let (bar_spans, info_text) =
            if self.progress.scan_finished && self.progress.expected_bytes > 0 {
                let pct = self.progress.percentage().min(100.0);
                let filled = (pct / 100.0 * bar_width as f64) as usize;
                let empty = bar_width - filled;
                let mut spans = Vec::new();
                if filled > 0 {
                    spans.push(Span::styled("━".repeat(filled), cyan_style));
                }
                if empty > 0 {
                    spans.push(Span::styled("─".repeat(empty), white_style));
                }
                (
                    spans,
                    format!(
                        "  {:.1}%  {} / {}",
                        pct,
                        utils::format_size_binary(self.progress.processed_bytes, 3),
                        utils::format_size_binary(self.progress.expected_bytes, 3),
                    ),
                )
            } else {
                let pos = self.progress.spinner_index % (bar_width * 2);
                let start = pos.min(bar_width);
                let end = (pos + bar_width / 2)
                    .min(bar_width * 2)
                    .saturating_sub(bar_width);
                let mut spans = Vec::new();
                let mut current_style = white_style;
                let mut current_chars = String::new();
                for i in 0..bar_width {
                    let ch = if i >= start && i <= end { '━' } else { '─' };
                    let style = if ch == '━' { cyan_style } else { white_style };
                    if style != current_style && !current_chars.is_empty() {
                        spans.push(Span::styled(
                            std::mem::take(&mut current_chars),
                            current_style,
                        ));
                    }
                    current_style = style;
                    current_chars.push(ch);
                }
                if !current_chars.is_empty() {
                    spans.push(Span::styled(current_chars, current_style));
                }
                (
                    spans,
                    format!(
                        "  scanning...  {} processed",
                        utils::format_size_binary(self.progress.processed_bytes, 3),
                    ),
                )
            };

        let elapsed_str = self.progress.elapsed_display();
        let mut status_parts = vec![format!("[{}]", elapsed_str)];

        if self.progress.scan_finished
            && let Some(eta_str) = self.progress.eta_display()
        {
            status_parts.push(format!("ETA: {}", eta_str));
        }

        let rate_str = utils::format_size_binary(self.progress.rate() as u64, 3);
        status_parts.push(format!("{}/s", rate_str));

        let status = status_parts.join("  │  ");

        let mut line_spans = bar_spans;
        line_spans.push(Span::raw(info_text));

        let lines = vec![Line::from(line_spans), Line::from(status)];

        let widget = Paragraph::new(Text::from(lines)).block(theme::themed_block("Progress"));
        frame.render_widget(widget, area);
    }

    fn render_progress_items(&self, frame: &mut Frame, area: Rect) {
        let items_text = if self.progress.scan_finished {
            format!(
                "Items: {} / {}  │  Errors: {}",
                self.progress.processed_items,
                self.progress.expected_items,
                self.progress.errors.len(),
            )
        } else {
            format!(
                "Items: {}  │  Errors: {}",
                self.progress.processed_items,
                self.progress.errors.len(),
            )
        };

        let widget = Paragraph::new(items_text).block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(theme::border_style()),
        );
        frame.render_widget(widget, area);
    }

    fn render_recent_nodes(&self, frame: &mut Frame, area: Rect) {
        let max_path_len = (area.width as usize).saturating_sub(4);

        let items: Vec<ListItem> = self
            .progress
            .recent_nodes
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

    fn render_errors(&self, frame: &mut Frame, area: Rect) {
        let items: Vec<ListItem> = self
            .progress
            .errors
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

    fn render_progress_footer(&self, frame: &mut Frame, area: Rect) {
        let footer = Line::from(vec![
            Span::styled("[Esc]", Style::default().fg(theme::MENU_KEY).bold()),
            Span::raw(" cancel"),
            Span::raw("    "),
            Span::styled("[q]", Style::default().fg(theme::MENU_KEY).bold()),
            Span::raw(" quit"),
        ]);
        frame.render_widget(Paragraph::new(footer), area);
    }

    fn render_summary(&self, frame: &mut Frame) {
        let area = frame.area();
        let inner = area.inner(ratatui::layout::Margin::new(2, 1));

        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(10), Constraint::Length(1)])
            .split(inner);

        match &self.summary {
            Some(SummaryResult::Success {
                summary,
                snapshot_id,
                duration,
                ..
            }) => {
                self.render_summary_success(
                    frame,
                    chunks[0],
                    summary.as_ref(),
                    snapshot_id,
                    *duration,
                );
            }
            Some(SummaryResult::Cancelled) => {
                self.render_summary_cancelled(frame, chunks[0]);
            }
            Some(SummaryResult::Error(msg)) => {
                self.render_summary_error(frame, chunks[0], msg);
            }
            Some(SummaryResult::NoChanges) => {
                self.render_summary_no_changes(frame, chunks[0]);
            }
            None => {
                self.render_summary_waiting(frame, chunks[0]);
            }
        }

        self.render_summary_footer(frame, chunks[1]);
    }

    fn render_summary_success(
        &self,
        frame: &mut Frame,
        area: Rect,
        summary: &SnapshotSummary,
        snapshot_id: &ID,
        duration: std::time::Duration,
    ) {
        let inner = area.inner(ratatui::layout::Margin::new(1, 1));
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1),
                Constraint::Length(5),
                Constraint::Length(1),
                Constraint::Length(5),
                Constraint::Length(1),
                Constraint::Length(1),
            ])
            .split(inner);

        frame.render_widget(
            Paragraph::new(Span::styled(
                "Changes since parent snapshot:",
                Style::default().bold(),
            )),
            chunks[0],
        );

        let col_label: u16 = 10;
        let avail = inner.width.saturating_sub(col_label + 4);
        let col_w = (avail / 4).max(6);

        let header = Row::new([
            Cell::from("").style(Style::default()),
            Cell::from("new").style(Style::default().fg(Color::Green).bold()),
            Cell::from("changed").style(Style::default().fg(Color::Yellow).bold()),
            Cell::from("deleted").style(Style::default().fg(Color::Red).bold()),
            Cell::from("unchanged").style(Style::default().bold()),
        ]);

        let files_row = Row::new([
            Cell::from("Files").style(Style::default().bold()),
            Cell::from(summary.diff_counts.new_files.to_string())
                .style(Style::default().fg(Color::Green)),
            Cell::from(summary.diff_counts.changed_files.to_string())
                .style(Style::default().fg(Color::Yellow)),
            Cell::from(summary.diff_counts.deleted_files.to_string())
                .style(Style::default().fg(Color::Red)),
            Cell::from(summary.diff_counts.unchanged_files.to_string()),
        ]);

        let dirs_row = Row::new([
            Cell::from("Dirs").style(Style::default().bold()),
            Cell::from(summary.diff_counts.new_dirs.to_string())
                .style(Style::default().fg(Color::Green)),
            Cell::from(summary.diff_counts.changed_dirs.to_string())
                .style(Style::default().fg(Color::Yellow)),
            Cell::from(summary.diff_counts.deleted_dirs.to_string())
                .style(Style::default().fg(Color::Red)),
            Cell::from(summary.diff_counts.unchanged_dirs.to_string()),
        ]);

        let table = Table::new(
            [header, files_row, dirs_row],
            [
                Constraint::Length(col_label),
                Constraint::Length(col_w),
                Constraint::Length(col_w),
                Constraint::Length(col_w),
                Constraint::Length(col_w),
            ],
        )
        .block(theme::themed_block(""));
        frame.render_widget(table, chunks[1]);

        frame.render_widget(
            Paragraph::new(Span::styled("Data:", Style::default().bold())),
            chunks[2],
        );

        let raw_data_str = utils::format_size_binary(summary.raw_bytes, 3);
        let enc_data_str = utils::format_size_binary(summary.encoded_bytes, 3);
        let raw_meta_str = utils::format_size_binary(summary.meta_raw_bytes, 3);
        let enc_meta_str = utils::format_size_binary(summary.meta_encoded_bytes, 3);
        let raw_total_str = utils::format_size_binary(summary.total_raw_bytes, 3);
        let enc_total_str = utils::format_size_binary(summary.total_encoded_bytes, 3);

        let max_val_len = raw_data_str
            .len()
            .max(enc_data_str.len())
            .max(raw_meta_str.len())
            .max(enc_meta_str.len())
            .max(raw_total_str.len())
            .max(enc_total_str.len());
        let data_col_w: u16 = (max_val_len + 2) as u16;
        let label_w: u16 = 10;

        let data_header = Row::new([
            Cell::from(""),
            Cell::from("Raw").style(Style::default().fg(Color::Yellow).bold()),
            Cell::from("Compressed").style(Style::default().fg(Color::Green).bold()),
        ]);

        let data_rows = [
            Row::new([
                Cell::from("Data").style(Style::default().bold()),
                Cell::from(raw_data_str).style(Style::default().fg(Color::Yellow)),
                Cell::from(enc_data_str).style(Style::default().fg(Color::Green)),
            ]),
            Row::new([
                Cell::from("Metadata").style(Style::default().bold()),
                Cell::from(raw_meta_str).style(Style::default().fg(Color::Yellow)),
                Cell::from(enc_meta_str).style(Style::default().fg(Color::Green)),
            ]),
            Row::new([
                Cell::from("Total").style(Style::default().bold()),
                Cell::from(raw_total_str).style(Style::default().fg(Color::Yellow).bold()),
                Cell::from(enc_total_str).style(Style::default().fg(Color::Green).bold()),
            ]),
        ];

        let data_table = Table::new(
            std::iter::once(data_header).chain(data_rows),
            [
                Constraint::Length(label_w),
                Constraint::Length(data_col_w),
                Constraint::Length(data_col_w),
            ],
        )
        .block(theme::themed_block(""));
        frame.render_widget(data_table, chunks[3]);

        let stats_line = Line::from(vec![Span::raw(format!(
            "Processed {} and {} items in {}",
            utils::format_size_binary(summary.processed_bytes, 3),
            summary.processed_items_count,
            utils::pretty_print_duration(duration),
        ))]);
        frame.render_widget(Paragraph::new(stats_line), chunks[4]);

        let id_line = Line::from(vec![
            Span::styled("Snapshot ID: ", Style::default().bold()),
            Span::styled(
                snapshot_id.to_short_hex(SHORT_SNAPSHOT_ID_LEN),
                Style::default().fg(theme::SNAPSHOT_ID),
            ),
        ]);
        frame.render_widget(Paragraph::new(id_line), chunks[5]);
    }

    fn render_summary_cancelled(&self, frame: &mut Frame, area: Rect) {
        let lines = vec![
            Line::from(vec![Span::styled(
                "Snapshot was cancelled.",
                Style::default()
                    .fg(Color::Yellow)
                    .bold()
                    .add_modifier(Modifier::UNDERLINED),
            )]),
            Line::from(""),
            Line::from("Some data may have been written to the repository."),
            Line::from("You may want to run 'forget' and 'prune' to clean up."),
        ];

        let widget =
            Paragraph::new(Text::from(lines)).block(theme::themed_block("Snapshot Cancelled"));
        frame.render_widget(widget, area);
    }

    fn render_summary_error(&self, frame: &mut Frame, area: Rect, msg: &str) {
        let lines = vec![
            Line::from(vec![Span::styled(
                "Snapshot failed with error:",
                Style::default()
                    .fg(Color::Red)
                    .bold()
                    .add_modifier(Modifier::UNDERLINED),
            )]),
            Line::from(""),
            Line::from(msg.to_string()),
        ];

        let widget =
            Paragraph::new(Text::from(lines)).block(theme::themed_block("Snapshot Failed"));
        frame.render_widget(widget, area);
    }

    fn render_summary_no_changes(&self, frame: &mut Frame, area: Rect) {
        let lines = vec![
            Line::from(vec![Span::styled(
                "No changes detected since parent.",
                Style::default().fg(Color::Yellow).bold(),
            )]),
            Line::from(""),
            Line::from("Snapshot was skipped."),
        ];

        let widget = Paragraph::new(Text::from(lines)).block(theme::themed_block("No Changes"));
        frame.render_widget(widget, area);
    }

    fn render_summary_waiting(&self, frame: &mut Frame, area: Rect) {
        let lines = vec![Line::from("Waiting for snapshot to complete...")];

        let widget = Paragraph::new(Text::from(lines)).block(theme::themed_block("Snapshot"));
        frame.render_widget(widget, area);
    }

    fn render_summary_footer(&self, frame: &mut Frame, area: Rect) {
        let footer = match &self.summary {
            Some(SummaryResult::Success { .. })
            | Some(SummaryResult::Cancelled)
            | Some(SummaryResult::Error(_))
            | Some(SummaryResult::NoChanges) => Line::from(vec![
                Span::styled("[Enter]", Style::default().fg(theme::MENU_KEY).bold()),
                Span::raw(" done"),
                Span::raw("    "),
                Span::styled("[Esc]", Style::default().fg(theme::MENU_KEY).bold()),
                Span::raw(" done"),
                Span::raw("    "),
                Span::styled("[q]", Style::default().fg(theme::MENU_KEY).bold()),
                Span::raw(" quit"),
            ]),
            _ => Line::from(vec![
                Span::styled("[q]", Style::default().fg(theme::MENU_KEY).bold()),
                Span::raw(" quit"),
            ]),
        };
        frame.render_widget(Paragraph::new(footer), area);
    }
}

#[async_trait]
impl Screen for SnapshotCreateScreen {
    fn render(&mut self, frame: &mut Frame) {
        self.check_completion();

        match self.phase {
            SnapshotPhase::Config => self.render_config(frame),
            SnapshotPhase::Progress => self.render_progress(frame),
            SnapshotPhase::Summary => self.render_summary(frame),
        }
    }

    async fn handle_key(&mut self, key: KeyEvent) -> Option<Transition> {
        self.handle_key_internal(key).map(|action| match action {
            SnapshotCreateAction::Quit => Transition::Quit,
            SnapshotCreateAction::Cancel => Transition::Pop,
            SnapshotCreateAction::Done => Transition::PopAndReload,
        })
    }
}

#[derive(Debug)]
pub enum SnapshotCreateAction {
    Quit,
    Cancel,
    Done,
}
