use anyhow::Result;
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use ratatui::Terminal;
use ratatui::backend::Backend;
use std::sync::Arc;
use std::time::Duration;

use crate::mapache::ID;
use crate::repository::lock::LockHandle;
use crate::repository::repo::Repository;
use crate::repository::storage::SecureStorage;

use super::screens::dashboard::DashboardScreen;

#[derive(Debug, Clone, PartialEq)]
enum AppScreen {
    Dashboard,
}

pub struct App {
    current_screen: AppScreen,
    dashboard: DashboardScreen,
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
            current_screen: AppScreen::Dashboard,
            dashboard: DashboardScreen::new(repo, repo_path, repo_id),
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
        match self.current_screen {
            AppScreen::Dashboard => {
                if let Some(action) = self.dashboard.handle_key(key) {
                    match action {
                        DashboardAction::Quit => self.should_quit = true,
                        DashboardAction::Snapshot
                        | DashboardAction::Restore
                        | DashboardAction::Stats
                        | DashboardAction::Verify
                        | DashboardAction::Forget
                        | DashboardAction::Clean
                        | DashboardAction::SnapshotDetail(_) => {}
                    }
                }
            }
        }
    }

    fn render(&self, frame: &mut ratatui::Frame) {
        match self.current_screen {
            AppScreen::Dashboard => self.dashboard.render(frame),
        }
    }
}

#[derive(Debug)]
pub enum DashboardAction {
    Quit,
    Snapshot,
    Restore,
    Stats,
    Verify,
    Forget,
    Clean,
    #[allow(dead_code)]
    SnapshotDetail(ID),
}
