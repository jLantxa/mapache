use anyhow::Result;
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use std::io;
use std::sync::Arc;

use crate::repository::lock::LockHandle;
use crate::repository::repo::Repository;
use crate::repository::storage::SecureStorage;

mod app;
mod screens;
mod theme;
mod widgets;

pub async fn run(
    repo: Arc<Repository>,
    secure_storage: Arc<SecureStorage>,
    lock_handle: LockHandle,
    repo_path: String,
) -> Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = app::App::new(repo, secure_storage, lock_handle, repo_path)
        .run(&mut terminal)
        .await;

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    result
}
