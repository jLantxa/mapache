use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use async_trait::async_trait;
use crossterm::event::{KeyCode, KeyEvent};
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout},
    text::{Line, Span, Text},
    widgets::Paragraph,
};
use tokio::sync::mpsc;

use crate::{
    commands::cmd_verify::{self, CmdArgs},
    repository::{lock::LockHandle, repo::Repository},
    ui::{
        events::{Event, EventSender, TaskEvent},
        reporter::{EventVerifyReporter, VerifyReporter},
        tui::{
            app::{Screen, Transition},
            background::BackgroundTask,
            theme::{self, kv},
            widgets::{Form, FormCommand, FormField, PhaseProgress, ToastSink, impl_attach_toasts},
        },
    },
    utils,
};

#[derive(Debug, Clone, Copy, PartialEq)]
enum VerifyPhase {
    Config,
    Progress,
    Summary,
}

#[derive(Debug)]
enum VerifyResult {
    Done(cmd_verify::VerifySummary),
    Cancelled,
    Error(String),
}

pub struct VerifyScreen {
    repo: Arc<Repository>,
    lock_handle: Option<LockHandle>,
    phase: VerifyPhase,
    form: Form,
    shutdown_signal: Arc<AtomicBool>,
    rx: Option<mpsc::UnboundedReceiver<TaskEvent>>,
    task: Option<BackgroundTask<VerifyResult>>,
    progress: PhaseProgress,
    summary: Option<VerifyResult>,
    toasts: ToastSink,
}

impl VerifyScreen {
    pub fn new(repo: Arc<Repository>, lock_handle: Option<LockHandle>) -> Self {
        let fields = vec![
            FormField::section("Basic"),
            FormField::toggle("Read packs:", false)
                .help("Also download and read every pack, not just the index and snapshots."),
            FormField::section("Advanced"),
            FormField::number("Parallel:", 4)
                .help("Number of packs to verify concurrently.")
                .bounds(1, u32::MAX),
            FormField::number("Sample %:", 0)
                .help("Read only this percentage of the data blobs (0 reads everything).")
                .bounds(0, 100),
            FormField::toggle("Repair (ECC):", false)
                .help("Attempt to repair corrupted data using the stored ECC."),
            FormField::toggle("Fail early:", false)
                .help("Stop at the first error instead of collecting all of them."),
            FormField::action("Run Verify"),
        ];

        let form = Form::new(fields, 13);

        Self {
            repo,
            lock_handle,
            phase: VerifyPhase::Config,
            form,
            shutdown_signal: Arc::new(AtomicBool::new(false)),
            rx: None,
            task: None,
            progress: PhaseProgress::new("Verifying\u{2026}"),
            summary: None,
            toasts: ToastSink::default(),
        }
    }

    fn build_args(&self) -> CmdArgs {
        let read_packs = self
            .form
            .get_toggle_by_label("Read packs:")
            .unwrap_or(false);
        let parallel = self
            .form
            .get_number_by_label("Parallel:")
            .unwrap_or(4)
            .max(1) as usize;
        let sample = self.form.get_number_by_label("Sample %:").unwrap_or(0);
        let repair = self
            .form
            .get_toggle_by_label("Repair (ECC):")
            .unwrap_or(false);
        let fail_early = self
            .form
            .get_toggle_by_label("Fail early:")
            .unwrap_or(false);

        CmdArgs {
            read_packs,
            parallel,
            with_cache: false,
            fail_early,
            sample: if read_packs && sample > 0 {
                Some(f64::from(sample.min(100)))
            } else {
                None
            },
            repair,
            dump_pack_blobs: None,
            hook_args: Default::default(),
        }
    }

    fn start_verify(&mut self) {
        let args = self.build_args();

        let (tx, rx) = mpsc::unbounded_channel();
        let event_sender: EventSender = {
            let tx = tx.clone();
            Arc::new(move |event: Event| {
                if let Event::Task(e) = event {
                    let _ = tx.send(e);
                }
            })
        };

        self.rx = Some(rx);
        self.phase = VerifyPhase::Progress;
        self.progress = PhaseProgress::new("Verifying\u{2026}");
        self.summary = None;
        self.shutdown_signal.store(false, Ordering::SeqCst);

        let repo = self.repo.clone();
        let secure_storage = repo.secure_storage();
        let lock_handle = self.lock_handle.clone();
        let shutdown_signal = self.shutdown_signal.clone();

        self.task = Some(BackgroundTask::spawn(
            self.shutdown_signal.clone(),
            move || async move {
                Ok(run_verify(
                    repo,
                    secure_storage,
                    lock_handle,
                    args,
                    event_sender,
                    shutdown_signal,
                )
                .await)
            },
        ));
    }

    fn check_completion(&mut self) {
        if self.phase != VerifyPhase::Progress {
            return;
        }

        if let Some(rx) = &mut self.rx {
            while let Ok(event) = rx.try_recv() {
                self.progress.handle_event(event);
            }
        }

        if let Some(task) = &mut self.task
            && let Some(result) = task.poll()
        {
            let result = result.unwrap_or_else(VerifyResult::Error);
            if let VerifyResult::Error(e) = &result {
                self.toasts.error(format!("Verify failed: {e}"));
            }
            self.summary = Some(result);
            self.phase = VerifyPhase::Summary;
        }
    }
}

#[async_trait]
impl Screen for VerifyScreen {
    fn render(&mut self, frame: &mut Frame) {
        self.check_completion();

        match self.phase {
            VerifyPhase::Config => self.render_config(frame),
            VerifyPhase::Progress => self.render_progress(frame),
            VerifyPhase::Summary => self.render_summary(frame),
        }
    }

    async fn handle_key(&mut self, key: KeyEvent) -> Option<Transition> {
        match self.phase {
            VerifyPhase::Config => match self.form.command(key.code) {
                FormCommand::Submit => {
                    self.start_verify();
                    None
                }
                FormCommand::Cancel => Some(Transition::Pop),
                FormCommand::None => None,
            },
            VerifyPhase::Progress => match key.code {
                KeyCode::Esc => {
                    self.shutdown_signal.store(true, Ordering::SeqCst);
                    self.progress.cancelling = true;
                    None
                }
                KeyCode::Char('q') => Some(Transition::Quit),
                _ => None,
            },
            VerifyPhase::Summary => match key.code {
                KeyCode::Enter | KeyCode::Esc => Some(Transition::Pop),
                KeyCode::Char('q') => Some(Transition::Quit),
                _ => None,
            },
        }
    }

    fn help_hints(&self) -> Vec<(&'static str, &'static str)> {
        match self.phase {
            VerifyPhase::Config => vec![
                ("Tab", "move"),
                ("Space/Enter", "toggle/run"),
                ("Esc", "back"),
            ],
            VerifyPhase::Progress => vec![("Esc", "cancel"), ("q", "back")],
            VerifyPhase::Summary => vec![("Enter/Esc", "back"), ("q", "back")],
        }
    }

    fn text_input_active(&self) -> bool {
        self.phase == VerifyPhase::Config && self.form.is_editing()
    }

    impl_attach_toasts!(toasts);

    fn request_shutdown(&mut self) {
        self.shutdown_signal.store(true, Ordering::SeqCst);
    }

    async fn shutdown(&mut self) {
        if let Some(task) = &mut self.task {
            task.shutdown().await;
        }
    }
}

impl VerifyScreen {
    fn render_config(&self, frame: &mut Frame) {
        let area = frame.area().inner(theme::CONTENT_MARGIN);
        let footer = theme::key_hint_lines(&self.help_hints(), area.width);
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(6), Constraint::Length(footer.len() as u16)])
            .split(area);

        self.form.render(frame, chunks[0], "Verify Repository");
        frame.render_widget(Paragraph::new(Text::from(footer)), chunks[1]);
    }

    fn render_progress(&mut self, frame: &mut Frame) {
        let area = frame.area().inner(theme::CONTENT_MARGIN);
        let footer = theme::key_hint_lines(&self.help_hints(), area.width);
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(6),
                Constraint::Min(4),
                Constraint::Length(footer.len() as u16),
            ])
            .split(area);

        self.progress.render_bar(frame, chunks[0]);
        self.progress.render_details(frame, chunks[1]);
        frame.render_widget(Paragraph::new(Text::from(footer)), chunks[2]);
    }

    fn render_summary(&self, frame: &mut Frame) {
        let area = frame.area().inner(theme::CONTENT_MARGIN);
        let mut lines: Vec<Line<'static>> = Vec::new();

        match &self.summary {
            Some(VerifyResult::Done(summary)) => {
                if summary.passed {
                    lines.push(Line::from(Span::styled(
                        "Verification passed.",
                        theme::THEME.success,
                    )));
                } else {
                    lines.push(Line::from(Span::styled(
                        "Verification failed!",
                        theme::THEME.error,
                    )));
                }
                lines.push(Line::from(""));
                lines.push(kv(
                    "Duration",
                    utils::pretty_print_duration(summary.duration),
                ));
                lines.push(kv(
                    "Packs processed",
                    utils::format_count(summary.packs_processed, "pack", "packs"),
                ));
                lines.push(kv("Packs corrupt", summary.packs_corrupt.to_string()));
                lines.push(kv("Packs missing", summary.packs_missing.to_string()));
                lines.push(kv("Packs repaired", summary.packs_repaired.to_string()));
                lines.push(kv("Blobs verified", summary.blobs_verified.to_string()));
                lines.push(kv("Blobs dangling", summary.blobs_dangling.to_string()));
                lines.push(kv(
                    "Snapshots verified",
                    utils::format_count(summary.snapshots_verified, "snapshot", "snapshots"),
                ));
                lines.push(kv(
                    "Snapshots corrupt",
                    summary.snapshots_corrupt.to_string(),
                ));
                lines.push(kv(
                    "Metadata corrupt",
                    summary.metadata_files_corrupt.to_string(),
                ));

                if !self.progress.errors.is_empty() {
                    lines.push(Line::from(""));
                    lines.push(Line::from(Span::styled("Errors", theme::THEME.header)));
                    for e in self.progress.errors.iter().rev() {
                        lines.push(Line::from(vec![
                            Span::styled(" ! ", theme::THEME.error),
                            Span::raw(e.clone()),
                        ]));
                    }
                }

                if summary.failed_early {
                    lines.push(Line::from(""));
                    lines.push(Line::from(Span::styled(
                        "Verification was partial (--fail-early).",
                        theme::THEME.warning,
                    )));
                }
            }
            Some(VerifyResult::Cancelled) => {
                lines.push(Line::from(Span::styled(
                    "Verification cancelled.",
                    theme::THEME.warning,
                )));
            }
            Some(VerifyResult::Error(e)) => {
                lines.push(Line::from(Span::styled(
                    format!("Verify failed: {e}"),
                    theme::THEME.error,
                )));
            }
            None => {
                lines.push(Line::from(Span::styled(
                    "Waiting for verification to complete\u{2026}",
                    theme::THEME.info,
                )));
            }
        }

        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            "Press Enter or Esc to return.",
            theme::THEME.footer,
        )));

        frame.render_widget(
            Paragraph::new(Text::from(lines)).block(theme::block("Verification Report")),
            area,
        );
    }
}

// ---------------------------------------------------------------------------
// Background execution
// ---------------------------------------------------------------------------

async fn run_verify(
    repo: Arc<Repository>,
    secure_storage: Arc<crate::repository::storage::SecureStorage>,
    lock_handle: Option<LockHandle>,
    args: CmdArgs,
    event_sender: EventSender,
    shutdown_signal: Arc<AtomicBool>,
) -> VerifyResult {
    let reporter: Arc<dyn VerifyReporter> = Arc::new(EventVerifyReporter::new(event_sender));

    match cmd_verify::run_with_repo(
        repo,
        secure_storage,
        lock_handle,
        &args,
        false,
        reporter,
        Some(shutdown_signal.clone()),
    )
    .await
    {
        Ok(summary) => VerifyResult::Done(summary),
        Err(e) => {
            if shutdown_signal.load(Ordering::Acquire) {
                VerifyResult::Cancelled
            } else {
                VerifyResult::Error(e.to_string())
            }
        }
    }
}
