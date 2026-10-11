pub(crate) mod config;
pub(crate) mod progress;
pub(crate) mod summary;

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use async_trait::async_trait;
use crossterm::event::KeyEvent;
use ratatui::Frame;
use tokio::sync::mpsc;

use crate::{
    commands::{self, cmd_snapshot},
    repository::{lock::LockHandle, repo::Repository},
    ui::{
        events::{BackupEvent, Event, EventSender},
        tui::{
            app::{Screen, Transition},
            background::BackgroundTask,
            screens::snapshot::{
                config::{ConfigAction, SnapshotForm, render_config},
                progress::{
                    ProgressAction, ProgressState, SummaryResult, handle_progress_key,
                    render_progress,
                },
                summary::{SummaryAction, handle_summary_key, render_summary},
            },
        },
    },
};

#[derive(Debug, Clone, Copy, PartialEq)]
enum SnapshotPhase {
    Config,
    Progress,
    Summary,
}

pub struct SnapshotCreateScreen {
    repo: Arc<Repository>,
    lock_handle: Option<LockHandle>,
    phase: SnapshotPhase,
    form: SnapshotForm,
    progress: ProgressState,
    shutdown_signal: Arc<AtomicBool>,
    rx: Option<mpsc::UnboundedReceiver<BackupEvent>>,
    task: Option<BackgroundTask<SummaryResult>>,
    summary: Option<SummaryResult>,
}

impl SnapshotCreateScreen {
    pub fn new(
        repo: Arc<Repository>,
        lock_handle: Option<LockHandle>,
        config_defaults: Option<commands::cmd_snapshot::CmdArgs>,
    ) -> Self {
        Self {
            repo,
            lock_handle,
            phase: SnapshotPhase::Config,
            form: SnapshotForm::new(config_defaults.as_ref()),
            progress: ProgressState::new(),
            shutdown_signal: Arc::new(AtomicBool::new(false)),
            rx: None,
            task: None,
            summary: None,
        }
    }

    fn start_snapshot(&mut self) {
        if !self.form.validate() {
            return;
        }

        let repo = self.repo.clone();
        let lock_handle = self.lock_handle.clone();
        let shutdown_signal = self.shutdown_signal.clone();
        let options = self.form.to_snapshot_options();
        let dry_run = self.form.dry_run();
        let parent = match self.form.parent() {
            Ok(parent) => parent,
            Err(_) => return,
        };
        let no_parent = self
            .form
            .form
            .get_toggle_by_label("No parent:")
            .unwrap_or(false);

        let (tx, rx) = mpsc::unbounded_channel();
        let event_sender: EventSender = {
            let tx = tx.clone();
            Arc::new(move |event: Event| {
                if let Event::Backup(e) = event {
                    let _ = tx.send(e);
                }
            })
        };

        self.rx = Some(rx);
        self.phase = SnapshotPhase::Progress;
        self.progress = ProgressState::new();
        self.progress.core.scanning = !options.no_scan;
        self.shutdown_signal.store(false, Ordering::SeqCst);

        self.task = Some(BackgroundTask::spawn_async(
            self.shutdown_signal.clone(),
            async move {
                let repo = if dry_run {
                    repo.for_dry_run()
                        .await
                        .map_err(|error| error.to_string())?
                } else {
                    repo
                };
                repo.reset_stats();

                let parent_snapshot_pair =
                    cmd_snapshot::resolve_parent_snapshot(repo.clone(), no_parent, parent)
                        .await
                        .map_err(|error| error.to_string())?;

                let result = cmd_snapshot::run_with_repo(
                    repo,
                    lock_handle,
                    options,
                    event_sender,
                    parent_snapshot_pair,
                    Some(shutdown_signal.clone()),
                )
                .await;
                let summary_result = match result {
                    Ok(cmd_snapshot::SnapshotOutcome::Saved(completion)) => {
                        SummaryResult::Success {
                            summary: Box::new(completion.summary),
                            snapshot_id: completion.snapshot_id,
                            duration: completion.duration,
                        }
                    }
                    Ok(cmd_snapshot::SnapshotOutcome::SkippedNoChanges) => SummaryResult::NoChanges,
                    Ok(cmd_snapshot::SnapshotOutcome::Interrupted) => SummaryResult::Cancelled,
                    Err(e) => {
                        if shutdown_signal.load(Ordering::SeqCst) {
                            SummaryResult::Cancelled
                        } else {
                            SummaryResult::Error(e.to_string())
                        }
                    }
                };
                Ok(summary_result)
            },
        ));
    }

    pub fn check_completion(&mut self) {
        if self.phase != SnapshotPhase::Progress {
            return;
        }

        // Process regular events from mpsc first
        if let Some(rx) = &mut self.rx {
            while let Ok(event) = rx.try_recv() {
                self.progress.handle_event(event);
            }
        }

        if let Some(task) = &mut self.task
            && let Some(result) = task.poll()
        {
            match result {
                Ok(summary_result) => {
                    self.summary = Some(summary_result);
                }
                Err(e) => {
                    self.summary = Some(SummaryResult::Error(e));
                }
            }
            self.phase = SnapshotPhase::Summary;
        }
    }
}

#[async_trait]
impl Screen for SnapshotCreateScreen {
    fn render(&mut self, frame: &mut Frame) {
        self.check_completion();

        match self.phase {
            SnapshotPhase::Config => render_config(frame, &self.form),
            SnapshotPhase::Progress => {
                render_progress(frame, &self.progress, self.form.dry_run());
            }
            SnapshotPhase::Summary => render_summary(frame, &self.summary, self.form.dry_run()),
        }
    }

    async fn handle_key(&mut self, key: KeyEvent) -> Option<Transition> {
        match self.phase {
            SnapshotPhase::Config => match self.form.handle_key(key.code) {
                ConfigAction::Cancel => Some(Transition::Pop),
                ConfigAction::Start => {
                    self.start_snapshot();
                    None
                }
                ConfigAction::None => None,
            },
            SnapshotPhase::Progress => match handle_progress_key(key.code) {
                ProgressAction::Quit => Some(Transition::Quit),
                ProgressAction::Cancel => {
                    self.shutdown_signal.store(true, Ordering::SeqCst);
                    self.progress.core.cancelling = true;
                    None
                }
                ProgressAction::None => None,
            },
            SnapshotPhase::Summary => match handle_summary_key(key.code) {
                SummaryAction::Quit => Some(Transition::Quit),
                SummaryAction::Done => Some(Transition::Pop),
                SummaryAction::None => None,
            },
        }
    }

    fn help_hints(&self) -> Vec<(&'static str, &'static str)> {
        match self.phase {
            SnapshotPhase::Config => {
                if self.form.form.is_editing() {
                    vec![("Enter", "confirm"), ("Esc", "cancel edit")]
                } else {
                    vec![
                        ("Tab", "next"),
                        ("Enter", "edit/start"),
                        ("Space", "toggle"),
                        ("Esc", "cancel"),
                        ("q", "back"),
                    ]
                }
            }
            SnapshotPhase::Progress => vec![("Esc", "cancel"), ("q", "back")],
            SnapshotPhase::Summary => vec![("Enter/Esc", "done"), ("q", "back")],
        }
    }

    fn text_input_active(&self) -> bool {
        self.phase == SnapshotPhase::Config && self.form.form.is_editing()
    }

    fn request_shutdown(&mut self) {
        self.shutdown_signal.store(true, Ordering::SeqCst);
    }

    async fn shutdown(&mut self) {
        if let Some(task) = &mut self.task {
            task.shutdown().await;
        }
    }
}
