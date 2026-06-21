use std::time::Instant;

use ratatui::{Frame, layout::Rect};

use crate::ui::events::RestoreEvent;
use crate::ui::tui::widgets::{TaskProgressState, TaskProgressWidget};

pub fn handle_event(state: &mut TaskProgressState, event: RestoreEvent) {
    match event {
        RestoreEvent::Planning => {
            state.set_message("Planning...".to_string());
        }
        RestoreEvent::NodeVisited(_) => {}
        RestoreEvent::PlanBuilt {
            total_items,
            total_bytes,
        } => {
            state.processed_items = 0;
            state.start_time = Instant::now();
            state.set_expected(total_items, total_bytes);
            state.set_message("Restoring...".to_string());
        }
        RestoreEvent::ItemProcessed(_) => {
            state.add_processed_items(1);
        }
        RestoreEvent::BytesProcessed(bytes) => {
            state.add_processed_bytes(bytes);
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
        RestoreEvent::Finished => {
            state.finish();
        }
    }
}

pub fn render_progress(frame: &mut Frame, area: Rect, state: &TaskProgressState) {
    let widget = TaskProgressWidget::new(state, "Restore Progress");
    let inner = area.inner(ratatui::layout::Margin::new(2, 1));
    widget.render(frame, inner);
}
