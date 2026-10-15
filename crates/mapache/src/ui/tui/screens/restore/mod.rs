pub(crate) mod config;
pub(crate) mod progress;
pub(crate) mod summary;

use std::{path::PathBuf, sync::Arc, time::Instant};

use async_trait::async_trait;
use crossterm::event::{KeyCode, KeyEvent};
use ratatui::{
    Frame,
    text::{Line, Text},
};

use crate::{
    commands::cmd_restore,
    repository::{
        repo::Repository,
        snapshot::{SnapshotEntry, SnapshotPair},
    },
    ui::{
        events::{Event, RestoreEvent, emit_event, filtered_event_channel},
        tui::{
            app::{Screen, Transition},
            background::BackgroundTask,
            screens::{
                TaskSession,
                restore::config::{ConfigAction, RestoreConfig},
            },
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
    session: TaskSession<RestoreEvent, ()>,
    result: Option<Result<(), String>>,
}

impl RestoreScreen {
    pub fn new(
        repo: Arc<Repository>,
        snapshot: SnapshotEntry,
        paths: Option<Vec<PathBuf>>,
    ) -> Self {
        Self {
            repo,
            config: RestoreConfig::new(snapshot, paths),
            phase: RestorePhase::Config,
            progress: TaskProgressState::new(),
            session: TaskSession::new(),
            result: None,
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

        let (event_sender, rx) = filtered_event_channel(|event| match event {
            Event::Restore(e) => Some(e),
            _ => None,
        });
        self.session.begin(rx);

        let worker_signal = self.session.signal();
        let finished = event_sender.clone();

        self.session.set_task(BackgroundTask::spawn_async(
            self.session.signal(),
            async move {
                let result = cmd_restore::run_with_repo(
                    repo,
                    None,
                    &args,
                    event_sender,
                    snapshot_pair,
                    Some(worker_signal),
                )
                .await;
                emit_event(&finished, Event::Restore(RestoreEvent::Finished));
                result.map_err(|error| error.to_string())
            },
        ));
    }
}

#[async_trait]
impl Screen for RestoreScreen {
    fn render(&mut self, frame: &mut Frame) {
        let result = {
            let Self {
                session, progress, ..
            } = self;
            session.poll(|event| progress::handle_event(progress, event))
        };

        if let Some(result) = result {
            self.phase = RestorePhase::Summary;
            self.result = Some(result);
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
                _ => None,
            },
            RestorePhase::Progress => self
                .session
                .handle_progress_key(key.code, &mut self.progress.cancelling),
            RestorePhase::Summary => self.session.handle_summary_key(key.code),
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
        self.session.request_shutdown();
    }

    async fn shutdown(&mut self) {
        self.session.shutdown().await;
    }

    fn wants_animation(&self) -> bool {
        self.phase == RestorePhase::Progress
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
        assert!(!screen.session.is_running());

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
