use std::{sync::Arc, time::Duration};

use anyhow::Result;
use crossterm::event::{self, Event, KeyEvent, KeyEventKind};
use ratatui::{Terminal, backend::Backend};

use crate::repository::{lock::LockHandle, repo::Repository};

use crate::ui::tui::screens::{
    dashboard::{DashboardAction, DashboardScreen},
    file_explorer::{FileExplorerAction, FileExplorerScreen},
    snapshot_create::{SnapshotCreateAction, SnapshotCreateScreen},
    snapshot_detail::{DetailAction, SnapshotDetailScreen},
};

use crate::commands::cmd_snapshot::CmdArgs as SnapshotCmdArgs;

enum ActiveScreen {
    Dashboard,
    SnapshotDetail(Box<SnapshotDetailScreen>),
    FileExplorer(Box<FileExplorerScreen>, Box<SnapshotDetailScreen>),
    SnapshotCreate(Box<SnapshotCreateScreen>),
}

pub struct App {
    dashboard: DashboardScreen,
    active: ActiveScreen,
    should_quit: bool,
    repo: Arc<Repository>,
    lock_handle: LockHandle,
    snapshot_config: Option<SnapshotCmdArgs>,
}

impl App {
    pub fn new(
        repo: Arc<Repository>,
        _secure_storage: Arc<crate::repository::storage::SecureStorage>,
        lock_handle: LockHandle,
        repo_path: String,
        snapshot_config: Option<SnapshotCmdArgs>,
    ) -> Self {
        let repo_id = repo.manifest().id().to_hex();
        Self {
            dashboard: DashboardScreen::new(repo.clone(), repo_path, repo_id),
            active: ActiveScreen::Dashboard,
            should_quit: false,
            repo,
            lock_handle,
            snapshot_config,
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
                self.handle_key(key).await;
            }
        }

        Ok(())
    }

    fn transition_to(&mut self, next: ActiveScreen) {
        self.active = next;
    }

    async fn handle_key(&mut self, key: KeyEvent) {
        match &mut self.active {
            ActiveScreen::Dashboard => {
                if let Some(action) = self.dashboard.handle_key(key.code) {
                    match action {
                        DashboardAction::Quit => self.should_quit = true,
                        DashboardAction::SnapshotDetail(_id) => {
                            if let Some(index) = self.dashboard.get_current_index() {
                                let snapshots = self.dashboard.get_snapshots().clone();
                                self.transition_to(ActiveScreen::SnapshotDetail(Box::new(
                                    SnapshotDetailScreen::new(snapshots, index),
                                )));
                            }
                        }
                        DashboardAction::Snapshot => {
                            let config = self.snapshot_config.clone();
                            self.transition_to(ActiveScreen::SnapshotCreate(Box::new(
                                SnapshotCreateScreen::new(
                                    self.repo.clone(),
                                    self.lock_handle.clone(),
                                    config,
                                ),
                            )));
                        }
                        DashboardAction::Restore
                        | DashboardAction::Stats
                        | DashboardAction::Verify
                        | DashboardAction::Forget
                        | DashboardAction::Clean => {}
                    }
                }
            }
            ActiveScreen::SnapshotDetail(screen) => {
                if let Some(action) = screen.handle_key(key.code) {
                    match action {
                        DetailAction::Back => self.transition_to(ActiveScreen::Dashboard),
                        DetailAction::Quit => self.should_quit = true,
                        DetailAction::Explore => {
                            let root_tree_id = screen.get_snapshot().tree;
                            match FileExplorerScreen::new(self.dashboard.get_repo(), &root_tree_id)
                                .await
                            {
                                Ok(explorer) => {
                                    if let ActiveScreen::SnapshotDetail(detail_screen) =
                                        std::mem::replace(&mut self.active, ActiveScreen::Dashboard)
                                    {
                                        self.transition_to(ActiveScreen::FileExplorer(
                                            Box::new(explorer),
                                            detail_screen,
                                        ));
                                    }
                                }
                                Err(e) => {
                                    tracing::error!("Failed to load file explorer: {:?}", e);
                                }
                            }
                        }
                        DetailAction::PrevSnapshot | DetailAction::NextSnapshot => {}
                    }
                }
            }
            ActiveScreen::FileExplorer(screen, _detail_screen) => {
                if let Some(action) = screen.handle_key(key.code).await {
                    match action {
                        FileExplorerAction::Back => {
                            if let ActiveScreen::FileExplorer(_, detail) =
                                std::mem::replace(&mut self.active, ActiveScreen::Dashboard)
                            {
                                self.transition_to(ActiveScreen::SnapshotDetail(detail));
                            }
                        }
                        FileExplorerAction::Quit => self.should_quit = true,
                    }
                }
            }
            ActiveScreen::SnapshotCreate(screen) => {
                if let Some(action) = screen.handle_key(key) {
                    match action {
                        SnapshotCreateAction::Quit => self.should_quit = true,
                        SnapshotCreateAction::Cancel | SnapshotCreateAction::Done => {
                            self.transition_to(ActiveScreen::Dashboard);
                            let _ = self.dashboard.load_snapshots().await;
                        }
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
            ActiveScreen::SnapshotCreate(screen) => screen.render(frame),
        }
    }
}
