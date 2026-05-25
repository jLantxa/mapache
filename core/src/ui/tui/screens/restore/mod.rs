pub(crate) mod config;
pub(crate) mod progress;
pub(crate) mod summary;

use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Instant,
};

use async_trait::async_trait;
use crossterm::event::{KeyCode, KeyEvent};
use ratatui::Frame;
use tokio::sync::mpsc;

use crate::{
    repository::{repo::Repository, snapshot::SnapshotEntry},
    restorer::{self, RestoreOptions},
    ui::{
        RestoreProgressReporter,
        tui::{
            app::{Screen, Transition},
            widgets::TaskProgressState,
        },
    },
};

use config::{ConfigAction, RestoreConfig};
use progress::RestoreEvent;

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
    shutdown_signal: Arc<AtomicBool>,
    result: Option<Option<String>>,
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
            shutdown_signal: Arc::new(AtomicBool::new(false)),
            result: None,
        }
    }

    fn start_restore(&mut self) {
        self.phase = RestorePhase::Progress;
        self.progress.start_time = Instant::now();

        let repo = self.repo.clone();
        let snapshot = self.config.snapshot.snapshot.clone();
        let target = self.config.get_target();
        let paths = self.config.paths.clone();
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
        };

        let tx = self.tx.clone();
        let reporter = Arc::new(TuiRestoreProgressReporter {
            tx: tx.clone(),
            error_count: AtomicU64::new(0),
            warning_count: AtomicU64::new(0),
        });
        let shutdown_signal = self.shutdown_signal.clone();
        self.shutdown_signal.store(false, Ordering::SeqCst);

        tokio::spawn(async move {
            let result = restorer::restore(
                repo,
                &snapshot,
                &target,
                paths,
                None,
                options,
                reporter,
                shutdown_signal,
            )
            .await;
            let event = match result {
                Ok(_) => RestoreEvent::Completed(None),
                Err(e) => RestoreEvent::Completed(Some(e.to_string())),
            };
            let _ = tx.send(event);
        });
    }
}

struct TuiRestoreProgressReporter {
    tx: mpsc::UnboundedSender<RestoreEvent>,
    error_count: AtomicU64,
    warning_count: AtomicU64,
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
        self.error_count.fetch_add(1, Ordering::Relaxed);
        let _ = self.tx.send(RestoreEvent::Error(msg.to_string()));
    }

    fn warning(&self, msg: &str) {
        self.warning_count.fetch_add(1, Ordering::Relaxed);
        let _ = self.tx.send(RestoreEvent::Warning(msg.to_string()));
    }

    fn error_count(&self) -> u64 {
        self.error_count.load(Ordering::Relaxed)
    }

    fn warning_count(&self) -> u64 {
        self.warning_count.load(Ordering::Relaxed)
    }

    fn log(&self, msg: String) {
        let _ = self.tx.send(RestoreEvent::Log(msg));
    }

    fn verbose_1(&self, msg: String) {
        let _ = self.tx.send(RestoreEvent::Verbose1(msg));
    }

    fn verbose_2(&self, msg: String) {
        let _ = self.tx.send(RestoreEvent::Verbose2(msg));
    }

    fn finalize(&self) {}
}

#[async_trait]
impl Screen for RestoreScreen {
    fn render(&mut self, frame: &mut Frame) {
        while let Ok(event) = self.rx.try_recv() {
            if let RestoreEvent::Completed(ref res) = event {
                self.phase = RestorePhase::Summary;
                self.result = Some(res.clone());
            }
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
