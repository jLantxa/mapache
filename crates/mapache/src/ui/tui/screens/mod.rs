use crossterm::event::KeyCode;

pub mod clean;
pub mod dashboard;
pub mod diff;
pub mod file_explorer;
pub mod find;
pub mod forget;
pub mod restore;
pub mod snapshot;
pub mod snapshot_detail;
mod snapshot_row;
pub mod stats;
mod task;
pub mod verify;

pub(crate) use snapshot_row::snapshot_cells;
pub(crate) use task::TaskSession;

/// A screen's response to a key pressed while a background task is running.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProgressKey {
    /// Not a progress-view shortcut.
    None,
    /// `Esc`: ask the running task to stop.
    Cancel,
    /// `q`: quit the application.
    Quit,
}

impl ProgressKey {
    /// Shared `Esc`/`q` handling for the progress phase of an action screen.
    pub(crate) fn from_key(key: KeyCode) -> Self {
        match key {
            KeyCode::Char('q') => Self::Quit,
            KeyCode::Esc => Self::Cancel,
            _ => Self::None,
        }
    }
}

/// A screen's response to a key pressed on a finished task's summary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SummaryKey {
    /// Not a summary-view shortcut.
    None,
    /// `Enter`/`Esc`: dismiss the summary and go back.
    Done,
    /// `q`: quit the application.
    Quit,
}

impl SummaryKey {
    /// Shared `Enter`/`Esc`/`q` handling for the summary phase.
    pub(crate) fn from_key(key: KeyCode) -> Self {
        match key {
            KeyCode::Char('q') => Self::Quit,
            KeyCode::Enter | KeyCode::Esc => Self::Done,
            _ => Self::None,
        }
    }
}
