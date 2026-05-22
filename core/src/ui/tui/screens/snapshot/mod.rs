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
    commands::cmd_snapshot,
    repository::{lock::LockHandle, repo::Repository},
    ui::tui::app::{Screen, Transition},
};

use config::{ConfigAction, SnapshotForm, render_config};
use progress::{
    ProgressAction, ProgressState, SnapshotEvent, SummaryResult, TuiSnapshotProgressReporter,
    handle_progress_key, render_progress,
};
use summary::{SummaryAction, handle_summary_key, render_summary};

#[derive(Debug, Clone, Copy, PartialEq)]
enum SnapshotPhase {
    Config,
    Progress,
    Summary,
}

pub struct SnapshotCreateScreen {
    repo: Arc<Repository>,
    lock_handle: LockHandle,
    phase: SnapshotPhase,
    form: SnapshotForm,
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
            progress: ProgressState::new(),
            shutdown_signal: Arc::new(AtomicBool::new(false)),
            rx: None,
            summary: None,
        }
    }

    fn start_snapshot(&mut self) {
        if !self.form.paths_can_start() {
            return;
        }

        let repo = self.repo.clone();
        let lock_handle = self.lock_handle.clone();
        let shutdown_signal = self.shutdown_signal.clone();
        let cmd_args = self.form.to_cmd_args();

        let (tx, rx) = mpsc::unbounded_channel();
        let reporter = Arc::new(TuiSnapshotProgressReporter { tx: tx.clone() });

        self.rx = Some(rx);
        self.phase = SnapshotPhase::Progress;
        self.progress = ProgressState::new();
        self.progress.core.scanning = true;
        self.shutdown_signal.store(false, Ordering::SeqCst);

        tokio::spawn(async move {
            let result =
                cmd_snapshot::run_with_repo(repo, lock_handle, &cmd_args, reporter, false).await;
            let summary_result = match result {
                Ok(Some(completion)) => SummaryResult::Success {
                    summary: Box::new(completion.summary),
                    snapshot_id: completion.snapshot_id,
                    duration: completion.duration,
                },
                Ok(None) => SummaryResult::NoChanges,
                Err(e) => {
                    if shutdown_signal.load(Ordering::SeqCst) {
                        SummaryResult::Cancelled
                    } else {
                        SummaryResult::Error(e.to_string())
                    }
                }
            };
            let _ = tx.send(SnapshotEvent::Completed(Box::new(Ok(summary_result))));
        });
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
}

#[async_trait]
impl Screen for SnapshotCreateScreen {
    fn render(&mut self, frame: &mut Frame) {
        self.check_completion();

        match self.phase {
            SnapshotPhase::Config => render_config(frame, &self.form),
            SnapshotPhase::Progress => {
                self.progress.spinner_index += 1;
                render_progress(frame, &self.progress);
            }
            SnapshotPhase::Summary => render_summary(frame, &self.summary),
        }
    }

    async fn handle_key(&mut self, key: KeyEvent) -> Option<Transition> {
        match self.phase {
            SnapshotPhase::Config => match self.form.handle_key(key.code) {
                ConfigAction::Quit => Some(Transition::Quit),
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
}
