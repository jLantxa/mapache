//! Shared helpers for TUI tests.
//!
//! Rendering into a `TestBackend` terminal and read back its contents is a
//! pattern repeated across the widget and screen tests. These helpers keep that
//! boilerplate in one place.

use ratatui::{Terminal, backend::TestBackend};

/// Renders `draw` into a `width` x `height` test terminal and returns the
/// visible text with rows concatenated.
pub fn render_text(width: u16, height: u16, draw: impl FnOnce(&mut ratatui::Frame)) -> String {
    let mut terminal = Terminal::new(TestBackend::new(width, height)).unwrap();
    terminal.draw(draw).unwrap();
    buffer_text(&terminal)
}

/// The text currently held in a `TestBackend` terminal's buffer.
pub fn buffer_text(terminal: &Terminal<TestBackend>) -> String {
    terminal
        .backend()
        .buffer()
        .content
        .iter()
        .map(|cell| cell.symbol())
        .collect()
}
