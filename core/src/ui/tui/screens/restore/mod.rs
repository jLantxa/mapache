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
use ratatui::Frame;
use tokio::sync::{mpsc, oneshot};

use crate::{
    repository::{repo::Repository, snapshot::SnapshotEntry},
    restorer::{self, RestoreOptions},
    ui::{
        events::{Event, EventSender, RestoreEvent},
        tui::{
            app::{Screen, Transition},
            screens::restore::config::{ConfigAction, RestoreConfig},
            widgets::TaskProgressState,
        },
    },
};

#[derive(Debug, Clone, Copy, PartialEq)]
enum RestorePhase {
    Config,
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
    result_rx: Option<oneshot::Receiver<Option<String>>>,
    result: Option<Option<String>>,
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
            result_rx: None,
            result: None,
            shutdown_signal: Arc::new(AtomicBool::new(false)),
        }
    }

    fn start_restore(&mut self) {
        self.phase = RestorePhase::Progress;
        self.progress.start_time = Instant::now();

        let repo = self.repo.clone();
        let snapshot = self.config.snapshot.snapshot.clone();
        let target = self.config.get_target();
        let options = RestoreOptions {
            dry_run: self.config.get_dry_run(),
            strategy: self.config.get_strategy(),
            strip_prefix: if self.config.get_strip_prefix() {
                Some(snapshot.root.clone())
            } else {
                None
            },
            quit_on_error: false,
            preallocate: true,
            verify: false,
            include: self.config.get_include(),
            exclude: self.config.get_exclude(),
        };

        let tx = self.tx.clone();
        let reporter: EventSender = {
            let tx = tx.clone();
            Arc::new(move |event: Event| {
                if let Event::Restore(e) = event {
                    let _ = tx.send(e);
                }
            })
        };
        let (result_tx, result_rx) = oneshot::channel();
        self.result_rx = Some(result_rx);
        let shutdown_signal = self.shutdown_signal.clone();
        self.shutdown_signal.store(false, Ordering::SeqCst);

        tokio::spawn(async move {
            let result =
                restorer::restore(repo, &snapshot, &target, options, reporter, shutdown_signal)
                    .await;
            let _ = tx.send(RestoreEvent::Finished);
            let _ = result_tx.send(match result {
                Ok(_) => None,
                Err(e) => Some(e.to_string()),
            });
        });
    }
}

#[async_trait]
impl Screen for RestoreScreen {
    fn render(&mut self, frame: &mut Frame) {
        // Check for completion
        if let Some(rx) = &mut self.result_rx
            && let Ok(result) = rx.try_recv()
        {
            self.phase = RestorePhase::Summary;
            self.result = Some(result);
            self.result_rx = None;
        }

        while let Ok(event) = self.rx.try_recv() {
            progress::handle_event(&mut self.progress, event);
        }

        let area = frame.area();
        match self.phase {
            RestorePhase::Config => self.config.render(frame, area),
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
                    if !self.config.get_target().as_os_str().is_empty() {
                        self.start_restore();
                    }
                    None
                }
                ConfigAction::Cancel => Some(Transition::Pop),
                ConfigAction::None => {
                    if key.code == KeyCode::Char('q') {
                        Some(Transition::Quit)
                    } else {
                        None
                    }
                }
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
}
