use std::{sync::Arc, time::Duration};

use anyhow::Result;
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use ratatui::{Terminal, backend::Backend};

use crate::repository::{lock::LockHandle, repo::Repository, storage::SecureStorage};

use crate::ui::tui::screens::{
    dashboard::{DashboardAction, DashboardScreen},
    snapshot_detail::{DetailAction, SnapshotDetailScreen},
};

enum ActiveScreen {
    Dashboard,
    SnapshotDetail(Box<SnapshotDetailScreen>),
}

pub struct App {
    dashboard: DashboardScreen,
    active: ActiveScreen,
    should_quit: bool,
}

impl App {
    pub fn new(
        repo: Arc<Repository>,
        _secure_storage: Arc<SecureStorage>,
        _lock_handle: LockHandle,
        repo_path: String,
    ) -> Self {
        let repo_id = repo.manifest().id().to_hex();
        Self {
            dashboard: DashboardScreen::new(repo, repo_path, repo_id),
            active: ActiveScreen::Dashboard,
            should_quit: false,
        }
    }

    pub async fn run<B: Backend>(&mut self, terminal: &mut Terminal<B>) -> Result<()>
    where
        <B as Backend>::Error: Send + Sync + 'static,
    {
        self.dashboard.load_snapshots().await?;

        while !self.should_quit {
            terminal.draw(|frame| self.render(frame))?;

            if event::poll(Duration::from_millis(100))?
                && let Event::Key(key) = event::read()?
                && key.kind == KeyEventKind::Press
            {
                self.handle_key(key.code);
            }
        }

        Ok(())
    }

    fn handle_key(&mut self, key: KeyCode) {
        match &mut self.active {
            ActiveScreen::Dashboard => {
                if let Some(action) = self.dashboard.handle_key(key) {
                    match action {
                        DashboardAction::Quit => self.should_quit = true,
                        DashboardAction::SnapshotDetail(id) => {
                            if let Some(entry) = self.dashboard.get_entry(id) {
                                self.active = ActiveScreen::SnapshotDetail(Box::new(
                                    SnapshotDetailScreen::new(entry),
                                ));
                            }
                        }
                        DashboardAction::Snapshot
                        | DashboardAction::Restore
                        | DashboardAction::Stats
                        | DashboardAction::Verify
                        | DashboardAction::Forget
                        | DashboardAction::Clean => {}
                    }
                }
            }
            ActiveScreen::SnapshotDetail(screen) => {
                if let Some(action) = screen.handle_key(key) {
                    match action {
                        DetailAction::Back => self.active = ActiveScreen::Dashboard,
                        DetailAction::Quit => self.should_quit = true,
                    }
                }
            }
        }
    }

    fn render(&mut self, frame: &mut ratatui::Frame) {
        match &mut self.active {
            ActiveScreen::Dashboard => self.dashboard.render(frame),
            ActiveScreen::SnapshotDetail(screen) => screen.render(frame),
        }
    }
}
