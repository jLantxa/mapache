use std::{sync::Arc, time::Duration};

use anyhow::Result;
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use ratatui::{Terminal, backend::Backend};

use crate::repository::{lock::LockHandle, repo::Repository, storage::SecureStorage};

use crate::ui::tui::screens::{
    dashboard::{DashboardAction, DashboardScreen},
    file_explorer::{FileExplorerAction, FileExplorerScreen},
    snapshot_detail::{DetailAction, SnapshotDetailScreen},
};

enum ActiveScreen {
    Dashboard,
    SnapshotDetail(Box<SnapshotDetailScreen>),
    FileExplorer(Box<FileExplorerScreen>, Box<SnapshotDetailScreen>),
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
                && key.kind != KeyEventKind::Release
            {
                self.handle_key(key.code).await;
            }
        }

        Ok(())
    }

    async fn handle_key(&mut self, key: KeyCode) {
        match &mut self.active {
            ActiveScreen::Dashboard => {
                if let Some(action) = self.dashboard.handle_key(key) {
                    match action {
                        DashboardAction::Quit => self.should_quit = true,
                        DashboardAction::SnapshotDetail(_id) => {
                            if let Some(index) = self.dashboard.get_current_index() {
                                let snapshots = self.dashboard.get_snapshots().clone();
                                self.active = ActiveScreen::SnapshotDetail(Box::new(
                                    SnapshotDetailScreen::new(snapshots, index),
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
                        DetailAction::Explore => {
                            let root_tree_id = screen.get_snapshot().tree;
                            match FileExplorerScreen::new(self.dashboard.get_repo(), &root_tree_id)
                                .await
                            {
                                Ok(explorer) => {
                                    // Move the current detail screen into the FileExplorer state
                                    if let ActiveScreen::SnapshotDetail(detail_screen) =
                                        std::mem::replace(&mut self.active, ActiveScreen::Dashboard)
                                    {
                                        self.active = ActiveScreen::FileExplorer(
                                            Box::new(explorer),
                                            detail_screen,
                                        );
                                    }
                                }
                                Err(e) => {
                                    tracing::error!("Failed to load file explorer: {:?}", e);
                                }
                            }
                        }
                        DetailAction::PrevSnapshot | DetailAction::NextSnapshot => {
                            // The screen already updated its current_index, just reset scroll
                        }
                    }
                }
            }
            ActiveScreen::FileExplorer(screen, _detail_screen) => {
                if let Some(action) = screen.handle_key(key).await {
                    match action {
                        FileExplorerAction::Back => {
                            // Restore the detail screen
                            if let ActiveScreen::FileExplorer(_, detail) =
                                std::mem::replace(&mut self.active, ActiveScreen::Dashboard)
                            {
                                self.active = ActiveScreen::SnapshotDetail(detail);
                            }
                        }
                        FileExplorerAction::Quit => self.should_quit = true,
                    }
                }
            }
        }
    }

    fn render(&mut self, frame: &mut ratatui::Frame) {
        match &mut self.active {
            ActiveScreen::Dashboard => self.dashboard.render(frame),
            ActiveScreen::SnapshotDetail(screen) => screen.render(frame),
            ActiveScreen::FileExplorer(screen, _) => screen.render(frame),
        }
    }
}
