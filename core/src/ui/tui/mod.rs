use std::{io, sync::Arc};

use anyhow::Result;
use crossterm::{
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{Terminal, backend::CrosstermBackend};

use crate::repository::{lock::LockHandle, repo::Repository, storage::SecureStorage};

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
