use std::{path::PathBuf, time::Instant};

use ratatui::{Frame, layout::Rect};

use crate::ui::tui::widgets::{TaskProgressState, TaskProgressWidget};

pub enum RestoreEvent {
    ProcessedItem(PathBuf),
    ProcessedBytes(u64),
    SetMessage(String),
    ResizeWorkload(u64, u64),
    Error(String),
    Warning(String),
    Log(String),
    Verbose1(String),
    Verbose2(String),
    Completed(Option<String>),
}

pub fn handle_event(state: &mut TaskProgressState, event: RestoreEvent) {
    match event {
        RestoreEvent::ProcessedItem(_path) => {
            state.add_processed_items(1);
        }
        RestoreEvent::ProcessedBytes(bytes) => {
            state.add_processed_bytes(bytes);
        }
        RestoreEvent::SetMessage(msg) => {
            state.set_message(msg);
        }
        RestoreEvent::ResizeWorkload(items, bytes) => {
            // Reset items counter from planning phase
            state.processed_items = 0;
            // Reset start time so ETA doesn't include planning phase (0 bytes/sec)
            state.start_time = Instant::now();
            state.set_expected(items, bytes);
        }
        RestoreEvent::Error(err) => {
            state.add_error(err);
        }
        RestoreEvent::Warning(warn) => {
            state.add_warning(warn);
        }
        RestoreEvent::Log(msg) => {
            state.add_log(msg);
        }
        RestoreEvent::Verbose1(msg) => {
            state.add_log(msg);
        }
        RestoreEvent::Verbose2(msg) => {
            state.add_log(msg);
        }
        RestoreEvent::Completed(_) => {
            state.finish();
        }
    }
}

pub fn render_progress(frame: &mut Frame, area: Rect, state: &TaskProgressState) {
    let widget = TaskProgressWidget::new(state, "Restore Progress");
    let inner = area.inner(ratatui::layout::Margin::new(2, 1));
    widget.render(frame, inner);
}
