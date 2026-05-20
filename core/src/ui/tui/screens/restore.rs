use std::{
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
    widgets::Paragraph,
};
use tokio::sync::mpsc;

use crate::{
    repository::{repo::Repository, snapshot::SnapshotEntry},
    restorer::{self, RestoreOptions, Strategy},
    ui::RestoreProgressReporter,
    utils,
};

use crate::ui::tui::{
    app::{Screen, Transition},
    theme,
};

#[derive(Debug, Clone, Copy, PartialEq)]
enum RestorePhase {
    Config,
    Progress,
    Summary,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum FormField {
    Target,
    DryRun,
    Delete,
    StripPrefix,
    Strategy,
    Start,
}

impl FormField {
    fn next(&self) -> Self {
        match self {
            FormField::Target => FormField::DryRun,
            FormField::DryRun => FormField::Delete,
            FormField::Delete => FormField::StripPrefix,
            FormField::StripPrefix => FormField::Strategy,
            FormField::Strategy => FormField::Start,
            FormField::Start => FormField::Target,
        }
    }

    fn prev(&self) -> Self {
        match self {
            FormField::Target => FormField::Start,
            FormField::DryRun => FormField::Target,
            FormField::Delete => FormField::DryRun,
            FormField::StripPrefix => FormField::Delete,
            FormField::Strategy => FormField::StripPrefix,
            FormField::Start => FormField::Strategy,
        }
    }

    fn is_text_input(&self) -> bool {
        matches!(self, FormField::Target)
    }

    fn is_toggle(&self) -> bool {
        matches!(
            self,
            FormField::DryRun | FormField::Delete | FormField::StripPrefix
        )
    }

    fn is_strategy(&self) -> bool {
        matches!(self, FormField::Strategy)
    }

    fn is_start(&self) -> bool {
        matches!(self, FormField::Start)
    }
}

enum RestoreEvent {
    ProcessedItem(PathBuf),
    ProcessedBytes(u64),
    SetMessage(String),
    ResizeWorkload(u64, u64),
    Error(String),
    Warning(String),
    Log(String),
    Completed(Box<Result<()>>),
}

struct TuiRestoreProgressReporter {
    tx: mpsc::UnboundedSender<RestoreEvent>,
    error_count: Arc<std::sync::atomic::AtomicU64>,
    warning_count: Arc<std::sync::atomic::AtomicU64>,
}

impl RestoreProgressReporter for TuiRestoreProgressReporter {
    fn set_message(&self, msg: String) {
        let _ = self.tx.send(RestoreEvent::SetMessage(msg));
    }

    fn resize_workload(&self, num_expected_items: u64, num_expected_bytes: u64) {
        let _ = self.tx.send(RestoreEvent::ResizeWorkload(
            num_expected_items,
            num_expected_bytes,
        ));
    }

    fn processed_item(&self, path: &std::path::Path) {
        let _ = self
            .tx
            .send(RestoreEvent::ProcessedItem(path.to_path_buf()));
    }

    fn processed_bytes(&self, bytes: u64) {
        let _ = self.tx.send(RestoreEvent::ProcessedBytes(bytes));
    }

    fn error(&self, msg: &str) {
        self.error_count.fetch_add(1, Ordering::SeqCst);
        let _ = self.tx.send(RestoreEvent::Error(msg.to_string()));
    }

    fn warning(&self, msg: &str) {
        self.warning_count.fetch_add(1, Ordering::SeqCst);
        let _ = self.tx.send(RestoreEvent::Warning(msg.to_string()));
    }

    fn error_count(&self) -> u64 {
        self.error_count.load(Ordering::SeqCst)
    }

    fn warning_count(&self) -> u64 {
        self.warning_count.load(Ordering::SeqCst)
    }

    fn log(&self, msg: String) {
        let _ = self.tx.send(RestoreEvent::Log(msg));
    }

    fn finalize(&self) {}
}

pub struct RestoreScreen {
    repo: Arc<Repository>,
    snapshot: SnapshotEntry,
    target: String,
    dry_run: bool,
    delete: bool,
    strip_prefix: bool,
    strategy: Strategy,
    paths: Option<Vec<PathBuf>>,

    phase: RestorePhase,
    focus: FormField,
    editing: bool,
    edit_cursor: usize,

    // Progress state
    rx: mpsc::UnboundedReceiver<RestoreEvent>,
    tx: mpsc::UnboundedSender<RestoreEvent>,
    expected_items: u64,
    expected_bytes: u64,
    processed_items: u64,
    processed_bytes: u64,
    current_file: String,
    errors: Vec<String>,
    warnings: Vec<String>,
    logs: Vec<String>,
    start_time: Option<Instant>,
    finish_time: Option<Instant>,
    result: Option<Result<()>>,
}

impl RestoreScreen {
    pub fn new(
        repo: Arc<Repository>,
        snapshot: SnapshotEntry,
        paths: Option<Vec<PathBuf>>,
    ) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            repo,
            snapshot,
            target: String::new(),
            dry_run: false,
            delete: false,
            strip_prefix: false,
            strategy: Strategy::Overwrite,
            paths,

            phase: RestorePhase::Config,
            focus: FormField::Target,
            editing: false,
            edit_cursor: 0,

            rx,
            tx,
            expected_items: 0,
            expected_bytes: 0,
            processed_items: 0,
            processed_bytes: 0,
            current_file: String::new(),
            errors: Vec::new(),
            warnings: Vec::new(),
            logs: Vec::new(),
            start_time: None,
            finish_time: None,
            result: None,
        }
    }

    fn start_restore(&mut self) {
        self.phase = RestorePhase::Progress;
        self.start_time = Some(Instant::now());

        let repo = self.repo.clone();
        let snapshot = self.snapshot.snapshot.clone();
        let target = PathBuf::from(&self.target);
        let paths = self.paths.clone();
        let options = RestoreOptions {
            dry_run: self.dry_run,
            strategy: self.strategy.clone(),
            strip_prefix: if self.strip_prefix {
                // Simplified strip prefix: just use the snapshot root if it's a common prefix
                // In a real app we might want more control.
                Some(snapshot.root.clone())
            } else {
                None
            },
            quit_on_error: false,
            preallocate: true,
            verify: false,
        };

        let tx = self.tx.clone();
        let reporter = Arc::new(TuiRestoreProgressReporter {
            tx: tx.clone(),
            error_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            warning_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        });
        let shutdown_signal = Arc::new(AtomicBool::new(false));

        tokio::spawn(async move {
            let result = restorer::restore(
                repo,
                &snapshot,
                &target,
                paths,
                None, // excludes
                options,
                reporter,
                shutdown_signal,
            )
            .await;
            let _ = tx.send(RestoreEvent::Completed(Box::new(result)));
        });
    }

    fn handle_event(&mut self, event: RestoreEvent) {
        match event {
            RestoreEvent::ProcessedItem(path) => {
                self.processed_items += 1;
                self.current_file = path.to_string_lossy().to_string();
            }
            RestoreEvent::ProcessedBytes(bytes) => {
                self.processed_bytes += bytes;
            }
            RestoreEvent::SetMessage(msg) => {
                self.current_file = msg;
            }
            RestoreEvent::ResizeWorkload(items, bytes) => {
                self.expected_items = items;
                self.expected_bytes = bytes;
            }
            RestoreEvent::Error(err) => {
                self.errors.push(err);
            }
            RestoreEvent::Warning(warn) => {
                self.warnings.push(warn);
            }
            RestoreEvent::Log(msg) => {
                self.logs.push(msg);
            }
            RestoreEvent::Completed(res) => {
                self.phase = RestorePhase::Summary;
                self.finish_time = Some(Instant::now());
                self.result = Some(*res);
            }
        }
    }

    fn render_config(&self, frame: &mut Frame, area: Rect) {
        let header_height = if self.paths.is_some() { 4 } else { 3 };
        let inner = area.inner(ratatui::layout::Margin::new(2, 1));
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(header_height), // Snapshot info
                Constraint::Min(0),                // Form
                Constraint::Length(1),             // Hints
            ])
            .split(inner);

        // Snapshot info
        let mut info_text = format!(
            "Restoring Snapshot: {} ({} - {})",
            self.snapshot.id.to_short_hex(8),
            utils::pretty_print_timestamp(&self.snapshot.snapshot.timestamp, None),
            self.snapshot
                .snapshot
                .hostname
                .as_deref()
                .unwrap_or("unknown")
        );
        if let Some(paths) = &self.paths {
            info_text.push_str(&format!("\nPaths: {:?}", paths));
        }

        let info = Paragraph::new(info_text).block(theme::themed_block("Restore Configuration"));
        frame.render_widget(info, chunks[0]);

        // Form
        let form_items = self.build_form_lines();
        let form = Paragraph::new(Text::from(form_items)).block(theme::themed_block("Options"));
        frame.render_widget(form, chunks[1]);

        // Hints
        self.render_config_footer(frame, chunks[2]);
    }

    fn build_form_lines(&self) -> Vec<Line<'static>> {
        let mut lines = Vec::new();
        let is_focused = |f: &FormField| self.focus == *f;

        let focus_style = Style::default()
            .fg(theme::SNAPSHOT_ID)
            .add_modifier(Modifier::BOLD);
        let normal_style = Style::default();

        let add_text_field =
            |lines: &mut Vec<Line<'static>>, label: &str, value: &str, field: FormField| {
                let focused = is_focused(&field);
                let label_style = if focused { focus_style } else { normal_style };
                let marker = if focused { "▶ " } else { "  " };

                let display = if focused && self.editing {
                    let mut s = value.to_string();
                    let byte_pos = s
                        .char_indices()
                        .nth(self.edit_cursor)
                        .map(|(i, _)| i)
                        .unwrap_or(s.len());
                    s.insert(byte_pos, '█');
                    Span::styled(
                        format!("[ {} ]", s),
                        Style::default().fg(theme::SNAPSHOT_DATE),
                    )
                } else if value.is_empty() {
                    Span::styled("(empty)", Style::default().fg(Color::DarkGray))
                } else {
                    Span::raw(value.to_string())
                };

                lines.push(Line::from(vec![
                    Span::styled(marker, Style::default().fg(theme::SNAPSHOT_DATE)),
                    Span::styled(format!("{:<15}", label), label_style.bold()),
                    display,
                ]));
            };

        let add_toggle_field =
            |lines: &mut Vec<Line<'static>>, label: &str, value: bool, field: FormField| {
                let focused = is_focused(&field);
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
                    Span::styled(format!("{:<15}", label), label_style.bold()),
                    Span::styled(checkbox, checkbox_style),
                ]));
            };

        let add_strategy_field =
            |lines: &mut Vec<Line<'static>>, label: &str, strategy: &Strategy, field: FormField| {
                let focused = is_focused(&field);
                let label_style = if focused { focus_style } else { normal_style };
                let marker = if focused { "▶ " } else { "  " };
                let strategy_text = match strategy {
                    Strategy::Fail => "Fail",
                    Strategy::Overwrite => "Overwrite",
                    Strategy::Skip => "Skip",
                    Strategy::Newer => "Keep Newer",
                };
                let style = if focused {
                    Style::default().fg(theme::SNAPSHOT_DATE)
                } else {
                    normal_style
                };
                lines.push(Line::from(vec![
                    Span::styled(marker, Style::default().fg(theme::SNAPSHOT_DATE)),
                    Span::styled(format!("{:<15}", label), label_style.bold()),
                    Span::styled(format!("< {} >", strategy_text), style),
                ]));
            };

        add_text_field(&mut lines, "Target Path:", &self.target, FormField::Target);
        add_toggle_field(&mut lines, "Dry Run:", self.dry_run, FormField::DryRun);
        add_toggle_field(&mut lines, "Delete Extra:", self.delete, FormField::Delete);
        add_toggle_field(
            &mut lines,
            "Strip Prefix:",
            self.strip_prefix,
            FormField::StripPrefix,
        );
        add_strategy_field(
            &mut lines,
            "Conflict resolution strategy:",
            &self.strategy,
            FormField::Strategy,
        );

        lines.push(Line::from(""));

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
            Span::raw("                 "),
            Span::styled("[ Start Restore ]", start_style),
            Span::raw(start_marker),
        ]));

        lines
    }

    fn render_config_footer(&self, frame: &mut Frame, area: Rect) {
        let footer = if self.editing {
            Line::from(vec![
                Span::styled("[Enter]", Style::default().fg(theme::MENU_KEY).bold()),
                Span::raw(" confirm"),
                Span::raw("    "),
                Span::styled("[Esc]", Style::default().fg(theme::MENU_KEY).bold()),
                Span::raw(" cancel edit"),
            ])
        } else if self.focus == FormField::Strategy {
            Line::from(vec![
                Span::styled("[Left/Right]", Style::default().fg(theme::MENU_KEY).bold()),
                Span::raw(" change"),
                Span::raw("    "),
                Span::styled("[Tab/↑↓]", Style::default().fg(theme::MENU_KEY).bold()),
                Span::raw(" navigate"),
                Span::raw("    "),
                Span::styled("[Esc]", Style::default().fg(theme::MENU_KEY).bold()),
                Span::raw(" cancel"),
                Span::raw("    "),
                Span::styled("[q]", Style::default().fg(theme::MENU_KEY).bold()),
                Span::raw(" quit"),
            ])
        } else {
            Line::from(vec![
                Span::styled("[Tab/↑↓]", Style::default().fg(theme::MENU_KEY).bold()),
                Span::raw(" navigate"),
                Span::raw("    "),
                Span::styled("[Enter/Space]", Style::default().fg(theme::MENU_KEY).bold()),
                Span::raw(" edit/toggle/start"),
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

    fn render_progress(&self, frame: &mut Frame, area: Rect) {
        let inner = area.inner(ratatui::layout::Margin::new(2, 1));
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(5), // Progress bar & stats
                Constraint::Min(0),    // Details / Logs
                Constraint::Length(1), // Footer
            ])
            .split(inner);

        self.render_progress_bar(frame, chunks[0]);
        self.render_progress_details(frame, chunks[1]);
        self.render_progress_footer(frame, chunks[2]);
    }

    fn render_progress_bar(&self, frame: &mut Frame, area: Rect) {
        let pct = if self.expected_bytes > 0 {
            (self.processed_bytes as f64 / self.expected_bytes as f64 * 100.0).min(100.0)
        } else if self.expected_items > 0 {
            (self.processed_items as f64 / self.expected_items as f64 * 100.0).min(100.0)
        } else {
            0.0
        };

        let bar_width: usize = 35;
        let filled = ((pct / 100.0) * bar_width as f64).round() as usize;
        let filled = filled.min(bar_width);
        let empty = bar_width.saturating_sub(filled);

        let cyan_style = Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD);
        let white_style = Style::default().fg(Color::DarkGray);

        let info_text = format!(
            "  {:.1}%  {} / {}",
            pct,
            utils::format_size_binary(self.processed_bytes, 1),
            utils::format_size_binary(self.expected_bytes, 1),
        );

        let elapsed = self.start_time.map(|s| s.elapsed()).unwrap_or_default();
        let elapsed_str = utils::pretty_print_duration(elapsed);

        let rate = if elapsed.as_secs_f64() > 0.0 {
            self.processed_bytes as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        };
        let rate_str = format!("{}/s", utils::format_size_binary(rate as u64, 1));

        let status = format!(
            "[{}]  │  {}  │  Items: {}/{}",
            elapsed_str, rate_str, self.processed_items, self.expected_items
        );

        let lines = vec![
            Line::from(vec![
                Span::styled("━".repeat(filled), cyan_style),
                Span::styled("─".repeat(empty), white_style),
                Span::raw(info_text),
            ]),
            Line::from(status),
        ];

        let widget = Paragraph::new(Text::from(lines)).block(theme::themed_block("Progress"));
        frame.render_widget(widget, area);
    }

    fn render_progress_details(&self, frame: &mut Frame, area: Rect) {
        let mut lines = Vec::new();
        lines.push(Line::from(vec![
            Span::styled("Current: ", Style::default().bold()),
            Span::raw(&self.current_file),
        ]));

        if !self.errors.is_empty() {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                "Errors:",
                Style::default().bold().fg(Color::Red),
            )));
            for err in self.errors.iter().rev().take(5) {
                lines.push(Line::from(vec![
                    Span::styled(" ! ", Style::default().fg(Color::Red)),
                    Span::raw(err),
                ]));
            }
        }

        let widget = Paragraph::new(Text::from(lines)).block(theme::themed_block("Details"));
        frame.render_widget(widget, area);
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

    fn render_summary(&self, frame: &mut Frame, area: Rect) {
        let inner = area.inner(ratatui::layout::Margin::new(2, 1));
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(10), Constraint::Length(1)])
            .split(inner);

        let mut lines = Vec::new();
        let title_style = Style::default().bold().add_modifier(Modifier::UNDERLINED);

        if let Some(res) = &self.result {
            match res {
                Ok(_) => {
                    lines.push(Line::from(Span::styled(
                        "RESTORE SUCCESSFUL",
                        Style::default().bold().fg(Color::Green),
                    )));
                }
                Err(e) => {
                    lines.push(Line::from(Span::styled(
                        "RESTORE FAILED",
                        Style::default().bold().fg(Color::Red),
                    )));
                    lines.push(Line::from(format!("Error: {}", e)));
                }
            }
        }
        lines.push(Line::from(""));

        lines.push(Line::from(Span::styled("Statistics:", title_style)));
        lines.push(Line::from(format!(
            "  Items restored: {}",
            self.processed_items
        )));
        lines.push(Line::from(format!(
            "  Bytes restored: {}",
            utils::format_size_binary(self.processed_bytes, 2)
        )));

        if let (Some(start), Some(finish)) = (self.start_time, self.finish_time) {
            let duration = finish.duration_since(start);
            let duration_str = utils::pretty_print_duration(duration);
            lines.push(Line::from(format!("  Duration:       {}", duration_str)));

            let rate = if duration.as_secs_f64() > 0.0 {
                self.processed_bytes as f64 / duration.as_secs_f64()
            } else {
                0.0
            };
            lines.push(Line::from(format!(
                "  Average rate:   {}/s",
                utils::format_size_binary(rate as u64, 2)
            )));
        }

        lines.push(Line::from(""));
        lines.push(Line::from(vec![
            Span::styled("Errors:   ", Style::default().bold()),
            Span::styled(
                self.errors.len().to_string(),
                if self.errors.is_empty() {
                    Style::default()
                } else {
                    Style::default().fg(Color::Red)
                },
            ),
            Span::raw("    "),
            Span::styled("Warnings: ", Style::default().bold()),
            Span::styled(
                self.warnings.len().to_string(),
                if self.warnings.is_empty() {
                    Style::default()
                } else {
                    Style::default().fg(Color::Yellow)
                },
            ),
        ]));

        let summary = Paragraph::new(Text::from(lines)).block(theme::themed_block("Summary"));
        frame.render_widget(summary, chunks[0]);

        let footer = Line::from(vec![
            Span::styled("[Enter/Esc]", Style::default().fg(theme::MENU_KEY).bold()),
            Span::raw(" back to dashboard"),
        ]);
        frame.render_widget(Paragraph::new(footer), chunks[1]);
    }
}

#[async_trait]
impl Screen for RestoreScreen {
    fn render(&mut self, frame: &mut Frame) {
        while let Ok(event) = self.rx.try_recv() {
            self.handle_event(event);
        }

        let area = frame.area();
        match self.phase {
            RestorePhase::Config => self.render_config(frame, area),
            RestorePhase::Progress => self.render_progress(frame, area),
            RestorePhase::Summary => self.render_summary(frame, area),
        }
    }

    async fn handle_key(&mut self, key: KeyEvent) -> Option<Transition> {
        match self.phase {
            RestorePhase::Config => {
                if self.editing {
                    match key.code {
                        KeyCode::Esc => {
                            self.editing = false;
                            None
                        }
                        KeyCode::Enter => {
                            self.editing = false;
                            None
                        }
                        KeyCode::Char(c) => {
                            let byte_pos = self
                                .target
                                .char_indices()
                                .nth(self.edit_cursor)
                                .map(|(i, _)| i)
                                .unwrap_or(self.target.len());
                            self.target.insert(byte_pos, c);
                            self.edit_cursor += 1;
                            None
                        }
                        KeyCode::Backspace => {
                            if self.edit_cursor > 0 {
                                let byte_pos = self
                                    .target
                                    .char_indices()
                                    .nth(self.edit_cursor - 1)
                                    .map(|(i, _)| i)
                                    .unwrap_or(0);
                                self.target.remove(byte_pos);
                                self.edit_cursor -= 1;
                            }
                            None
                        }
                        KeyCode::Delete => {
                            if self.edit_cursor < self.target.chars().count() {
                                let byte_pos = self
                                    .target
                                    .char_indices()
                                    .nth(self.edit_cursor)
                                    .map(|(i, _)| i)
                                    .unwrap_or(self.target.len());
                                self.target.remove(byte_pos);
                            }
                            None
                        }
                        KeyCode::Left => {
                            if self.edit_cursor > 0 {
                                self.edit_cursor -= 1;
                            }
                            None
                        }
                        KeyCode::Right => {
                            if self.edit_cursor < self.target.chars().count() {
                                self.edit_cursor += 1;
                            }
                            None
                        }
                        _ => None,
                    }
                } else {
                    match key.code {
                        KeyCode::Esc => Some(Transition::Pop),
                        KeyCode::Char('q') => Some(Transition::Quit),
                        KeyCode::Tab | KeyCode::Down => {
                            self.focus = self.focus.next();
                            None
                        }
                        KeyCode::Up => {
                            self.focus = self.focus.prev();
                            None
                        }
                        KeyCode::Left => {
                            if self.focus.is_strategy() {
                                self.strategy = match self.strategy {
                                    Strategy::Fail => Strategy::Newer,
                                    Strategy::Overwrite => Strategy::Fail,
                                    Strategy::Skip => Strategy::Overwrite,
                                    Strategy::Newer => Strategy::Skip,
                                };
                            }
                            None
                        }
                        KeyCode::Right => {
                            if self.focus.is_strategy() {
                                self.strategy = match self.strategy {
                                    Strategy::Fail => Strategy::Overwrite,
                                    Strategy::Overwrite => Strategy::Skip,
                                    Strategy::Skip => Strategy::Newer,
                                    Strategy::Newer => Strategy::Fail,
                                };
                            }
                            None
                        }
                        KeyCode::Char(' ') | KeyCode::Enter => {
                            if self.focus.is_start() {
                                if !self.target.is_empty() {
                                    self.start_restore();
                                }
                            } else if self.focus.is_text_input() {
                                self.editing = true;
                                self.edit_cursor = self.target.chars().count();
                            } else if self.focus.is_toggle() {
                                match self.focus {
                                    FormField::DryRun => self.dry_run = !self.dry_run,
                                    FormField::Delete => self.delete = !self.delete,
                                    FormField::StripPrefix => {
                                        self.strip_prefix = !self.strip_prefix
                                    }
                                    _ => {}
                                }
                            } else if self.focus.is_strategy() {
                                self.strategy = match self.strategy {
                                    Strategy::Fail => Strategy::Overwrite,
                                    Strategy::Overwrite => Strategy::Skip,
                                    Strategy::Skip => Strategy::Newer,
                                    Strategy::Newer => Strategy::Fail,
                                };
                            }
                            None
                        }
                        _ => None,
                    }
                }
            }
            RestorePhase::Progress => {
                // Allow cancelling?
                None
            }
            RestorePhase::Summary => match key.code {
                KeyCode::Enter | KeyCode::Esc => Some(Transition::Pop),
                _ => None,
            },
        }
    }
}
