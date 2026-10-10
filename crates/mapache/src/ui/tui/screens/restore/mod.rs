pub(crate) mod config;
pub(crate) mod progress;
pub(crate) mod summary;

use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use async_trait::async_trait;
use crossterm::event::{KeyCode, KeyEvent};
use ratatui::{
    Frame,
    text::{Line, Text},
};
use tokio::sync::mpsc;

use crate::{
    commands::cmd_restore,
    repository::{
        repo::Repository,
        snapshot::{SnapshotEntry, SnapshotPair},
    },
    ui::{
        events::{Event, EventSender, RestoreEvent},
        tui::{
            app::{Screen, Transition},
            background::BackgroundTask,
            screens::restore::config::{ConfigAction, RestoreConfig},
            theme,
            widgets::{Dialog, TaskProgressState},
        },
    },
};

#[derive(Debug, Clone, Copy, PartialEq)]
enum RestorePhase {
    Config,
    Confirm,
    Progress,
    Summary,
}

pub struct RestoreScreen {
    repo: Arc<Repository>,
    config: RestoreConfig,
    phase: RestorePhase,
    progress: TaskProgressState,

    rx: mpsc::UnboundedReceiver<RestoreEvent>,
    tx: mpsc::UnboundedSender<RestoreEvent>,
    task: Option<BackgroundTask<()>>,
    result: Option<Result<(), String>>,
    shutdown_signal: Arc<AtomicBool>,
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
            config: RestoreConfig::new(snapshot, paths),
            phase: RestorePhase::Config,
            progress: TaskProgressState::new(),

            rx,
            tx,
            task: None,
            result: None,
            shutdown_signal: Arc::new(AtomicBool::new(false)),
        }
    }

    fn start_restore(&mut self) {
        self.phase = RestorePhase::Progress;
        self.progress.start_time = Instant::now();

        let repo = self.repo.clone();
        let snapshot_pair = SnapshotPair {
            id: self.config.snapshot.id,
            snapshot: self.config.snapshot.snapshot.clone(),
        };
        let args = self.config.to_args();

        let tx = self.tx.clone();
        let reporter: EventSender = {
            let tx = tx.clone();
            Arc::new(move |event: Event| {
                if let Event::Restore(e) = event {
                    let _ = tx.send(e);
                }
            })
        };
        let shutdown_signal = self.shutdown_signal.clone();
        self.shutdown_signal.store(false, Ordering::SeqCst);

        self.task = Some(BackgroundTask::spawn_async(
            self.shutdown_signal.clone(),
            async move {
                let result = cmd_restore::run_with_repo(
                    repo,
                    None,
                    &args,
                    reporter,
                    snapshot_pair,
                    Some(shutdown_signal),
                )
                .await;
                let _ = tx.send(RestoreEvent::Finished);
                result.map_err(|error| error.to_string())
            },
        ));
    }
}

#[async_trait]
impl Screen for RestoreScreen {
    fn render(&mut self, frame: &mut Frame) {
        if let Some(task) = &mut self.task
            && let Some(result) = task.poll()
        {
            self.phase = RestorePhase::Summary;
            self.result = Some(result);
        }

        while let Ok(event) = self.rx.try_recv() {
            progress::handle_event(&mut self.progress, event);
        }

        let area = frame.area();
        match self.phase {
            RestorePhase::Config => self.config.render(frame, area),
            RestorePhase::Confirm => {
                self.config.render(frame, area);
                let root_warning = if self.config.to_args().no_preserve_root.unwrap_or(false) {
                    "Root protection is disabled."
                } else {
                    "The target root directory remains protected."
                };
                Dialog::with_text(
                    "Confirm Restore + Delete",
                    theme::THEME.warning,
                    Text::from(vec![
                        Line::from(format!("Target: {}", self.config.get_target().display())),
                        Line::from("Target items absent from the snapshot will be deleted."),
                        Line::from(root_warning),
                        Line::from("Enter: confirm   Esc: back"),
                    ]),
                )
                .render(area, frame);
            }
            RestorePhase::Progress => progress::render_progress(frame, area, &self.progress),
            RestorePhase::Summary => {
                summary::render_summary(frame, area, &self.progress, &self.result)
            }
        }
    }

    async fn handle_key(&mut self, key: KeyEvent) -> Option<Transition> {
        match self.phase {
            RestorePhase::Config => match self.config.handle_key(key.code) {
                ConfigAction::Start => {
                    if self.config.to_args().delete.unwrap_or(false) && !self.config.get_dry_run() {
                        self.phase = RestorePhase::Confirm;
                    } else {
                        self.start_restore();
                    }
                    None
                }
                ConfigAction::Cancel => Some(Transition::Pop),
                ConfigAction::None => None,
            },
            RestorePhase::Confirm => match key.code {
                KeyCode::Enter => {
                    self.start_restore();
                    None
                }
                KeyCode::Esc => {
                    self.phase = RestorePhase::Config;
                    None
                }
                KeyCode::Char('q') => Some(Transition::Quit),
                _ => None,
            },
            RestorePhase::Progress => match key.code {
                KeyCode::Esc => {
                    self.shutdown_signal.store(true, Ordering::SeqCst);
                    self.progress.cancelling = true;
                    None
                }
                KeyCode::Char('q') => Some(Transition::Quit),
                _ => None,
            },
            RestorePhase::Summary => match key.code {
                KeyCode::Enter | KeyCode::Esc => Some(Transition::Pop),
                KeyCode::Char('q') => Some(Transition::Quit),
                _ => None,
            },
        }
    }

    fn help_hints(&self) -> Vec<(&'static str, &'static str)> {
        match self.phase {
            RestorePhase::Config => {
                if self.config.form.is_editing() {
                    vec![("Enter", "confirm"), ("Esc", "cancel edit")]
                } else {
                    vec![
                        ("Tab/\u{2191}\u{2193}", "navigate"),
                        ("Enter/Space", "edit/toggle/start"),
                        ("Esc", "cancel"),
                        ("q", "back"),
                    ]
                }
            }
            RestorePhase::Progress => vec![("Esc", "cancel"), ("q", "back")],
            RestorePhase::Confirm => vec![("Enter", "confirm"), ("Esc", "back"), ("q", "back")],
            RestorePhase::Summary => vec![("Enter/Esc", "back"), ("q", "back")],
        }
    }

    fn text_input_active(&self) -> bool {
        self.phase == RestorePhase::Config && self.config.form.is_editing()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        backend::{StorageBackend, mock::MockBackend},
        common::{ID, defaults::TEST_REPO_CONFIG, error::Result},
        repository::{
            repo::{Auth, THIS_REPOSITORY_VERSION},
            snapshot::Snapshot,
        },
        ui::tui::widgets::{FormFieldType, TextInput},
    };
    use crossterm::event::KeyModifiers;
    use zeroize::Zeroizing;

    #[tokio::test]
    async fn delete_requires_confirmation_before_starting_restore() -> Result<()> {
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
        let (repo, _) =
            Repository::try_open_unlocked(&auth, None, backend, TEST_REPO_CONFIG).await?;
        let mut screen = RestoreScreen::new(
            repo,
            SnapshotEntry {
                id: ID::default(),
                snapshot: Snapshot::default(),
                active: true,
            },
            None,
        );
        for field in screen.config.form.fields_mut() {
            if field.label == "Target Path:" {
                field.field_type =
                    FormFieldType::Text(TextInput::with_text("restore-target".to_string()));
            }
        }
        screen.config.form.focus_field("Delete:");
        screen.config.form.handle_key(KeyCode::Enter);
        screen.config.form.focus_field("Paths:");
        screen.config.form.handle_key(KeyCode::BackTab);
        screen
            .handle_key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE))
            .await;
        assert_eq!(screen.phase, RestorePhase::Confirm);
        assert!(screen.task.is_none());
        let text = crate::ui::tui::test_support::render_text(100, 30, |frame| {
            screen.render(frame);
        });
        assert!(text.contains("Confirm Restore + Delete"));
        assert!(text.contains("root directory remains protected"));
        screen
            .handle_key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE))
            .await;
        assert_eq!(screen.phase, RestorePhase::Config);
        assert_eq!(screen.config.to_args().delete, Some(true));
        Ok(())
    }
}
