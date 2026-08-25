use std::{io, sync::Arc};

use crossterm::{
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{Terminal, backend::CrosstermBackend};

use crate::{
    commands::{cmd_forget, cmd_snapshot},
    common::error::Result,
    repository::{lock::LockHandle, repo::Repository},
};

mod app;
mod screens;
mod theme;
mod widgets;

struct TerminalGuard;

impl TerminalGuard {
    fn enter() -> Result<Self> {
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen)?;
        Ok(TerminalGuard)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
    }
}

pub async fn run(
    repo: Arc<Repository>,
    lock_handle: Option<LockHandle>,
    repo_path: String,
    snapshot_config: Option<cmd_snapshot::CmdArgs>,
    forget_config: Option<cmd_forget::CmdArgs>,
) -> Result<()> {
    let _guard = TerminalGuard::enter()?;
    let backend = CrosstermBackend::new(io::stdout());
    let mut terminal = Terminal::new(backend)?;

    let result = app::App::new(repo, lock_handle, repo_path, snapshot_config, forget_config)
        .run(&mut terminal)
        .await;

    terminal.show_cursor()?;

    result
}
