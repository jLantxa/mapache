use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use async_trait::async_trait;
use crossterm::event::{KeyCode, KeyEvent};
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    text::{Line, Span, Text},
    widgets::Paragraph,
};
use tokio::sync::mpsc;

use crate::{
    commands::cmd_clean,
    repository::{
        lock::{LockHandle, LockStatus},
        repo::Repository,
    },
    ui::{
        events::{Event, EventSender, GcEvent, GcTaskKind, TaskEvent},
        tui::{
            app::{Screen, Transition},
            background::BackgroundTask,
            theme::{self, kv, section},
            widgets::{
                Dialog, Form, FormCommand, FormField, PhaseProgress, ToastSink, impl_attach_toasts,
            },
        },
    },
    utils,
};

#[derive(Debug, Clone, Copy, PartialEq)]
enum CleanPhase {
    Config,
    Confirm,
    Progress,
    Summary,
}

#[derive(Debug)]
enum CleanResult {
    Done(cmd_clean::CleanReport),
    Cancelled,
    Error(String),
}

pub struct CleanScreen {
    repo: Arc<Repository>,
    lock_handle: Option<LockHandle>,
    phase: CleanPhase,
    form: Form,
    shutdown_signal: Arc<AtomicBool>,
    rx: Option<mpsc::UnboundedReceiver<GcEvent>>,
    task: Option<BackgroundTask<CleanResult>>,
    progress: PhaseProgress,
    summary: Option<CleanResult>,
    toasts: ToastSink,
}

impl CleanScreen {
    pub fn new(repo: Arc<Repository>, lock_handle: Option<LockHandle>) -> Self {
        let fields = vec![
            FormField::section("Basic"),
            FormField::toggle("Dry run:", false).help("Show what would be removed or repacked without changing the repository."),
            FormField::section("Advanced"),
            FormField::number("Tolerance %:", 0).help("How much unreferenced space to tolerate before repacking (0 uses the default).").bounds(0, 100),
            FormField::toggle("No repack:", false).help("Only delete unreferenced packs; never repack partially-used ones."),
            FormField::action("Run Clean"),
        ];

        let form = Form::new(fields, 13);

        Self {
            repo,
            lock_handle,
            phase: CleanPhase::Config,
            form,
            shutdown_signal: Arc::new(AtomicBool::new(false)),
            rx: None,
            task: None,
            progress: PhaseProgress::new("Starting garbage collection\u{2026}"),
            summary: None,
            toasts: ToastSink::default(),
        }
    }

    fn is_dry_run(&self) -> bool {
        self.form.get_toggle_by_label("Dry run:").unwrap_or(false)
    }

    fn start_clean(&mut self) {
        let no_repack = self.form.get_toggle_by_label("No repack:").unwrap_or(false);
        let dry_run = self.is_dry_run();
        let tolerance_pct = self.form.get_number_by_label("Tolerance %:").unwrap_or(0) as f32;
        let tolerance = cmd_clean::effective_tolerance(no_repack, tolerance_pct);

        let (tx, rx) = mpsc::unbounded_channel();
        let event_sender: EventSender = {
            let tx = tx.clone();
            Arc::new(move |event: Event| {
                if let Event::Gc(e) = event {
                    let _ = tx.send(e);
                }
            })
        };

        self.rx = Some(rx);
        self.phase = CleanPhase::Progress;
        self.progress = PhaseProgress::new("Starting garbage collection\u{2026}");
        self.summary = None;
        self.shutdown_signal.store(false, Ordering::SeqCst);

        let repo = self.repo.clone();
        let lock_handle = self.lock_handle.clone();
        let shutdown_signal = self.shutdown_signal.clone();

        self.task = Some(BackgroundTask::spawn(
            self.shutdown_signal.clone(),
            move || async move {
                Ok(run_clean(
                    repo,
                    tolerance,
                    dry_run,
                    event_sender,
                    shutdown_signal,
                    lock_handle,
                )
                .await)
            },
        ));
    }

    fn check_completion(&mut self) {
        if self.phase != CleanPhase::Progress {
            return;
        }

        // Drain regular events first so the last progress update is reflected
        // before a completion is acted upon.
        if let Some(rx) = &mut self.rx {
            while let Ok(event) = rx.try_recv() {
                match event {
                    GcEvent::TaskProgress { kind, pos, total } => {
                        self.progress.set_phase(gc_task_label(kind), pos, total)
                    }
                    GcEvent::Warning(message) => {
                        self.progress.handle_event(TaskEvent::Warning(message))
                    }
                    GcEvent::Error(message) => {
                        self.progress.handle_event(TaskEvent::Error(message))
                    }
                    GcEvent::Log(message) => self.progress.handle_event(TaskEvent::Log(message)),
                    GcEvent::TaskFinished { .. } | GcEvent::Finished { .. } => {}
                }
            }
        }

        if let Some(task) = &mut self.task
            && let Some(result) = task.poll()
        {
            let result = result.unwrap_or_else(CleanResult::Error);
            if let CleanResult::Error(e) = &result {
                self.toasts.error(format!("Failed to clean: {e}"));
            }
            self.summary = Some(result);
            self.phase = CleanPhase::Summary;
        }
    }
}

#[async_trait]
impl Screen for CleanScreen {
    fn render(&mut self, frame: &mut Frame) {
        self.check_completion();

        match self.phase {
            CleanPhase::Config | CleanPhase::Confirm => self.render_config(frame),
            CleanPhase::Progress => self.render_progress(frame),
            CleanPhase::Summary => self.render_summary(frame),
        }
    }

    async fn handle_key(&mut self, key: KeyEvent) -> Option<Transition> {
        match self.phase {
            CleanPhase::Config => match self.form.command(key.code) {
                FormCommand::Submit => {
                    if self.is_dry_run() {
                        self.start_clean();
                    } else {
                        self.phase = CleanPhase::Confirm;
                    }
                    None
                }
                FormCommand::Cancel => Some(Transition::Pop),
                FormCommand::None => None,
            },
            CleanPhase::Confirm => match key.code {
                KeyCode::Enter => {
                    self.start_clean();
                    None
                }
                KeyCode::Esc => {
                    self.phase = CleanPhase::Config;
                    None
                }
                KeyCode::Char('q') => Some(Transition::Quit),
                _ => None,
            },
            CleanPhase::Progress => match key.code {
                KeyCode::Esc => {
                    self.shutdown_signal.store(true, Ordering::SeqCst);
                    self.progress.cancelling = true;
                    None
                }
                KeyCode::Char('q') => Some(Transition::Quit),
                _ => None,
            },
            CleanPhase::Summary => match key.code {
                KeyCode::Enter | KeyCode::Esc => Some(Transition::Pop),
                KeyCode::Char('q') => Some(Transition::Quit),
                _ => None,
            },
        }
    }

    fn help_hints(&self) -> Vec<(&'static str, &'static str)> {
        match self.phase {
            CleanPhase::Config => {
                if self.form.is_editing() {
                    vec![("Enter", "confirm"), ("Esc", "cancel edit")]
                } else {
                    vec![
                        ("Tab", "next"),
                        ("Enter", "edit/run"),
                        ("Space", "toggle"),
                        ("Esc", "back"),
                        ("q", "back"),
                    ]
                }
            }
            CleanPhase::Confirm => vec![("Enter", "run"), ("Esc", "cancel")],
            CleanPhase::Progress => vec![("Esc", "cancel"), ("q", "back")],
            CleanPhase::Summary => vec![("Esc", "done"), ("q", "back")],
        }
    }

    fn text_input_active(&self) -> bool {
        self.phase == CleanPhase::Config && self.form.is_editing()
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

impl CleanScreen {
    fn render_config(&self, frame: &mut Frame) {
        let area = frame.area();
        let inner = area.inner(theme::CONTENT_MARGIN);
        let footer = theme::key_hint_lines(&self.help_hints(), inner.width);
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(10), Constraint::Length(footer.len() as u16)])
            .split(inner);

        self.form.render(frame, chunks[0], "Clean Configuration");
        frame.render_widget(Paragraph::new(Text::from(footer)), chunks[1]);

        if self.phase == CleanPhase::Confirm {
            self.render_confirm(frame, area);
        }
    }

    fn render_confirm(&self, frame: &mut Frame, area: Rect) {
        let text = vec![
            Line::from("Garbage collection removes unused objects and merges"),
            Line::from("packs. This modifies the repository."),
            Line::from(""),
            Line::from(vec![
                Span::styled("[Enter]", theme::THEME.menu_key),
                Span::raw(" to run, "),
                Span::styled("[Esc]", theme::THEME.menu_key),
                Span::raw(" to go back"),
            ]),
        ];
        Dialog::with_text("Confirm Clean", theme::THEME.border, Text::from(text))
            .render(area, frame);
    }

    fn render_progress(&mut self, frame: &mut Frame) {
        let inner = frame.area().inner(theme::CONTENT_MARGIN);
        let footer = theme::key_hint_lines(&self.help_hints(), inner.width);
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(5),
                Constraint::Min(0),
                Constraint::Length(footer.len() as u16),
            ])
            .split(inner);

        self.progress.render_bar(frame, chunks[0]);
        self.progress.render_details(frame, chunks[1]);
        frame.render_widget(Paragraph::new(Text::from(footer)), chunks[2]);
    }

    fn render_summary(&self, frame: &mut Frame) {
        let inner = frame.area().inner(theme::CONTENT_MARGIN);
        let footer = theme::key_hint_lines(&self.help_hints(), inner.width);
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(10), Constraint::Length(footer.len() as u16)])
            .split(inner);

        let lines = match &self.summary {
            Some(CleanResult::Done(outcome)) => summary_lines(outcome),
            Some(CleanResult::Cancelled) => vec![
                Line::from(Span::styled("Clean was cancelled.", theme::THEME.error)),
                Line::from(""),
                Line::from("Some objects may have been removed. You may want to run clean again."),
            ],
            Some(CleanResult::Error(msg)) => vec![Line::from(Span::styled(
                format!("Clean failed: {msg}"),
                theme::THEME.error,
            ))],
            None => vec![Line::from("Waiting for clean to complete\u{2026}")],
        };

        frame.render_widget(
            Paragraph::new(Text::from(lines)).block(theme::block("Clean Summary")),
            chunks[0],
        );
        frame.render_widget(Paragraph::new(Text::from(footer)), chunks[1]);
    }
}

fn gc_task_label(kind: GcTaskKind) -> &'static str {
    match kind {
        GcTaskKind::SearchingReferencedBlobs => "Searching referenced blobs",
        GcTaskKind::FindingObsoleteBlobs => "Finding obsolete blobs",
        GcTaskKind::CheckingGarbageLevels => "Checking garbage levels",
        GcTaskKind::FindingDuplicateBlobs => "Finding duplicate blobs",
        GcTaskKind::DeletingUnusedPacks => "Deleting unused packs",
        GcTaskKind::RepackingBlobs => "Repacking blobs",
        GcTaskKind::DeletingOldIndices => "Deleting old index files",
        GcTaskKind::DeletingObsoletePacks => "Deleting obsolete pack files",
    }
}

fn summary_lines(outcome: &cmd_clean::CleanReport) -> Vec<Line<'static>> {
    let mut lines: Vec<Line<'static>> = vec![section("Repository scan")];

    lines.push(kv(
        "Packs total",
        utils::format_count(outcome.total_packs, "pack", "packs"),
    ));
    lines.push(kv("Referenced blobs", outcome.referenced_blobs.to_string()));
    lines.push(kv("Referenced packs", outcome.referenced_packs.to_string()));
    lines.push(kv("Unused packs", outcome.unused_packs.to_string()));
    lines.push(kv("Obsolete packs", outcome.obsolete_packs.to_string()));
    lines.push(kv("Small packs", outcome.small_packs.to_string()));
    lines.push(kv("Tolerated packs", outcome.tolerated_packs.to_string()));
    lines.push(Line::from(""));

    if outcome.dry_run {
        lines.push(Line::from(Span::styled(
            "Dry run \u{2014} no changes were made",
            theme::THEME.warning,
        )));
    } else {
        let net = outcome.deleted_bytes as i64 - outcome.added_bytes as i64;
        lines.push(Line::from(Span::styled("Result", theme::THEME.header)));
        lines.push(kv(
            "Added",
            utils::format_size_binary(outcome.added_bytes, 3),
        ));
        lines.push(kv(
            "Deleted",
            utils::format_size_binary(outcome.deleted_bytes, 3),
        ));
        if net >= 0 {
            lines.push(kv("Net freed", utils::format_size_binary(net as u64, 3)));
        } else {
            lines.push(kv(
                "Net added",
                utils::format_size_binary(net.unsigned_abs(), 3),
            ));
        }
        lines.push(Line::from(""));
    }

    lines.push(kv(
        "Duration",
        utils::pretty_print_duration(outcome.duration),
    ));

    lines
}

// ---------------------------------------------------------------------------
// Background execution
// ---------------------------------------------------------------------------

async fn run_clean(
    repo: Arc<Repository>,
    tolerance: f32,
    dry_run: bool,
    event_sender: EventSender,
    shutdown_signal: Arc<AtomicBool>,
    lock_handle: Option<LockHandle>,
) -> CleanResult {
    let restore_shared = !dry_run
        && lock_handle
            .as_ref()
            .is_some_and(|lock| lock.status() == LockStatus::Shared);
    if !dry_run
        && let Some(lock) = &lock_handle
        && let Err(error) = lock.set_exclusive(true).await
    {
        return CleanResult::Error(error.to_string());
    }

    let result = match cmd_clean::run_scan_and_execute(
        repo,
        tolerance,
        dry_run,
        event_sender,
        shutdown_signal,
        |_plan| {},
    )
    .await
    {
        Ok(report) => CleanResult::Done(report),
        Err(cmd_clean::CleanError::Interrupted) => CleanResult::Cancelled,
        Err(error) => CleanResult::Error(error.to_string()),
    };

    if restore_shared
        && let Some(lock) = &lock_handle
        && let Err(error) = lock.set_exclusive(false).await
    {
        return CleanResult::Error(format!("could not restore shared session lock: {error}"));
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        backend::{StorageBackend, WriteContents, mock::MockBackend},
        common::{BlobType, SaveID, defaults::TEST_REPO_CONFIG, error::Result},
        repository::repo::{Auth, THIS_REPOSITORY_VERSION},
    };
    use zeroize::Zeroizing;

    #[tokio::test]
    async fn gc_rejects_other_owners_and_restores_session_mode() -> Result<()> {
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
        let (repo, _, handle) = Repository::try_open_with_lock(
            &auth,
            None,
            backend.clone(),
            TEST_REPO_CONFIG,
            false,
            None,
        )
        .await?;
        let (_, _, other) =
            Repository::try_open_with_lock(&auth, None, backend, TEST_REPO_CONFIG, false, None)
                .await?;
        repo.init_pack_saver(1)?;
        repo.encode_and_save_blob(
            BlobType::Data,
            WriteContents::Owned(b"unreferenced data".to_vec()),
            SaveID::CalculateID,
        )?;
        repo.flush_and_finalize_pack_saver().await?;
        assert_eq!(repo.list_packs().await?.len(), 1);
        let result = run_clean(
            repo.clone(),
            0.1,
            false,
            Arc::new(|_| {}),
            Arc::new(AtomicBool::new(false)),
            Some(handle.clone()),
        )
        .await;
        assert!(matches!(result, CleanResult::Error(_)));
        assert_eq!(handle.status(), LockStatus::Shared);
        assert_eq!(repo.list_packs().await?.len(), 1);
        other.unlock().await;
        let saw_gc = Arc::new(AtomicBool::new(false));
        let observed = saw_gc.clone();
        let active_lock = handle.clone();
        let reporter: EventSender = Arc::new(move |event| {
            if let Event::Gc(_) = event {
                assert_eq!(active_lock.status(), LockStatus::Exclusive);
                observed.store(true, Ordering::SeqCst);
            }
        });
        let result = run_clean(
            repo.clone(),
            0.1,
            false,
            reporter,
            Arc::new(AtomicBool::new(false)),
            Some(handle.clone()),
        )
        .await;
        assert!(
            matches!(result, CleanResult::Done(outcome) if outcome.unused_packs == 1 && outcome.deleted_bytes > 0)
        );
        assert!(saw_gc.load(Ordering::SeqCst));
        assert!(repo.list_packs().await?.is_empty());
        assert_eq!(handle.status(), LockStatus::Shared);
        assert_eq!(repo.get_locks().await?.len(), 1);
        handle.set_exclusive(true).await?;
        let result = run_clean(
            repo,
            0.1,
            false,
            Arc::new(|_| {}),
            Arc::new(AtomicBool::new(false)),
            Some(handle.clone()),
        )
        .await;
        assert!(matches!(result, CleanResult::Done(_)));
        assert_eq!(handle.status(), LockStatus::Exclusive);
        handle.unlock().await;
        Ok(())
    }
}
