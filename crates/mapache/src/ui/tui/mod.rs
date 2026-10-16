use std::{io, sync::Arc};

use crossterm::{
    event::{DisableMouseCapture, EnableMouseCapture},
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
mod background;
mod help;
mod logs;
mod screens;
#[cfg(test)]
mod test_support;
mod theme;
mod widgets;

pub use theme::{ColorMode, ThemeVariant};

/// Look-and-feel options resolved from the command line.
#[derive(Clone, Copy, Debug)]
pub struct Appearance {
    pub variant: ThemeVariant,
    pub color: ColorMode,
}

struct TerminalGuard;

impl TerminalGuard {
    fn enter() -> Result<Self> {
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
        Ok(TerminalGuard)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen, DisableMouseCapture);
    }
}

pub async fn run(
    repo: Arc<Repository>,
    lock_handle: Option<LockHandle>,
    repo_path: String,
    snapshot_config: Option<cmd_snapshot::CmdArgs>,
    forget_config: Option<cmd_forget::CmdArgs>,
    appearance: Appearance,
) -> Result<()> {
    theme::init(appearance.variant, appearance.color);

    let _guard = TerminalGuard::enter()?;
    let backend = CrosstermBackend::new(io::stdout());
    let mut terminal = Terminal::new(backend)?;

    let result = app::App::new(repo, lock_handle, repo_path, snapshot_config, forget_config)
        .run(&mut terminal)
        .await;

    terminal.show_cursor()?;

    result
}
