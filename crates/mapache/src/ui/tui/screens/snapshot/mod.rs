pub(crate) mod config;
pub(crate) mod progress;
pub(crate) mod summary;

use std::sync::{Arc, atomic::Ordering};

use async_trait::async_trait;
use crossterm::event::KeyEvent;
use ratatui::Frame;

use crate::{
    commands::{self, cmd_snapshot},
    repository::{lock::LockHandle, repo::Repository},
    ui::{
        events::{BackupEvent, Event, filtered_event_channel},
        tui::{
            app::{Screen, Transition},
            background::BackgroundTask,
            screens::{
                TaskSession,
                snapshot::{
                    config::{ConfigAction, SnapshotForm, render_config},
                    progress::{ProgressState, SummaryResult, render_progress},
                    summary::render_summary,
                },
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
    session: TaskSession<BackupEvent, SummaryResult>,
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
            session: TaskSession::new(),
            summary: None,
        }
    }

    fn start_snapshot(&mut self) {
        if !self.form.validate() {
            return;
        }

        let repo = self.repo.clone();
        let lock_handle = self.lock_handle.clone();
        let worker_signal = self.session.signal();
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

        let (event_sender, rx) = filtered_event_channel(|event| match event {
            Event::Backup(e) => Some(e),
            _ => None,
        });

        self.session.begin(rx);
        self.phase = SnapshotPhase::Progress;
        self.progress = ProgressState::new();
        self.progress.core.scanning = !options.no_scan;

        self.session.set_task(BackgroundTask::spawn_async(
            self.session.signal(),
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
                    Some(worker_signal.clone()),
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
                        if worker_signal.load(Ordering::SeqCst) {
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

        let result = {
            let Self {
                session, progress, ..
            } = self;
            session.poll(|event| progress.handle_event(event))
        };

        if let Some(result) = result {
            self.summary = Some(match result {
                Ok(summary_result) => summary_result,
                Err(e) => SummaryResult::Error(e),
            });
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
            SnapshotPhase::Progress => self
                .session
                .handle_progress_key(key.code, &mut self.progress.core.cancelling),
            SnapshotPhase::Summary => self.session.handle_summary_key(key.code),
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
        self.session.request_shutdown();
    }

    async fn shutdown(&mut self) {
        self.session.shutdown().await;
    }
}
