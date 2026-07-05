use std::{sync::Arc, time::Duration};

use crate::common::error::{MapacheError, Result};
use async_trait::async_trait;
use crossterm::event::{self, Event, KeyEvent, KeyEventKind};
use ratatui::{Frame, Terminal, backend::Backend, style::Style, widgets::Block};

use crate::{
    commands::{cmd_forget::CmdArgs as ForgetCmdArgs, cmd_snapshot::CmdArgs as SnapshotCmdArgs},
    repository::{lock::LockHandle, repo::Repository},
    ui::tui::{screens::dashboard::DashboardScreen, theme},
};

#[async_trait]
pub trait Screen: Send {
    fn render(&mut self, frame: &mut Frame);
    async fn handle_key(&mut self, key: KeyEvent) -> Option<Transition>;
    async fn on_become_active(&mut self) -> Result<()> {
        Ok(())
    }
}

pub enum Transition {
    Push(Box<dyn Screen>),
    Pop,
    Quit,
}

pub struct App {
    stack: Vec<Box<dyn Screen>>,
    should_quit: bool,
}

impl App {
    pub fn new(
        repo: Arc<Repository>,
        lock_handle: Option<LockHandle>,
        repo_path: String,
        snapshot_config: Option<SnapshotCmdArgs>,
        forget_config: Option<ForgetCmdArgs>,
    ) -> Self {
        let repo_id = repo.manifest().id().to_hex();
        let dashboard = DashboardScreen::new(
            repo,
            lock_handle,
            repo_path,
            repo_id,
            snapshot_config,
            forget_config,
        );
        Self {
            stack: vec![Box::new(dashboard)],
            should_quit: false,
        }
    }

    pub async fn run<B: Backend>(&mut self, terminal: &mut Terminal<B>) -> Result<()>
    where
        <B as Backend>::Error: Send + Sync + 'static,
    {
        if let Some(screen) = self.stack.last_mut() {
            screen.on_become_active().await?;
        }

        while !self.should_quit {
            terminal
                .draw(|frame| self.render(frame))
                .map_err(|e| MapacheError::Internal(format!("terminal error: {e}")))?;

            if event::poll(Duration::from_millis(100))?
                && let Event::Key(key) = event::read()?
                && key.kind != KeyEventKind::Release
            {
                self.handle_key(key).await;
            }
        }

        Ok(())
    }

    async fn handle_key(&mut self, key: KeyEvent) {
        let transition = if let Some(active) = self.stack.last_mut() {
            active.handle_key(key).await
        } else {
            None
        };

        if let Some(t) = transition {
            match t {
                Transition::Push(s) => {
                    self.stack.push(s);
                    if let Some(active) = self.stack.last_mut()
                        && let Err(e) = active.on_become_active().await
                    {
                        tracing::error!("Failed to activate screen: {}", e);
                    }
                }
                Transition::Pop => {
                    self.stack.pop();
                    if self.stack.is_empty() {
                        self.should_quit = true;
                    } else if let Some(active) = self.stack.last_mut()
                        && let Err(e) = active.on_become_active().await
                    {
                        tracing::error!("Failed to activate screen: {}", e);
                    }
                }
                Transition::Quit => self.should_quit = true,
            }
        }
    }

    fn render(&mut self, frame: &mut Frame) {
        frame.render_widget(
            Block::default().style(Style::default().bg(theme::THEME.bg)),
            frame.area(),
        );

        if let Some(active) = self.stack.last_mut() {
            active.render(frame);
        }
    }
}
