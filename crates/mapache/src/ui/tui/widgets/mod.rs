use crossterm::event::KeyCode;
use ratatui::{
    layout::Rect,
    style::Style,
    text::{Line, Span},
    widgets::{ListState, TableState},
};

use crate::ui::tui::theme;

mod dialog;
mod form;
mod progress_bar;
mod task_progress;
mod text_input;
mod toast;

pub use dialog::Dialog;
pub use form::{Form, FormCommand, FormField, FormFieldType};
pub use progress_bar::ProgressBar;
pub use task_progress::{PhaseProgress, TaskProgressState, TaskProgressWidget};
pub use text_input::{FilterAction, FilterState, TextInput, TextInputAction};
pub use toast::{Toast, ToastQueue, ToastSink};

/// Frame index for the glyph animated while a screen waits on a background
/// task. Screens advance it once per rendered frame and read the glyph when
/// drawing their loading indicator.
#[derive(Default, Clone, Copy)]
pub(crate) struct Spinner {
    frame: usize,
}

impl Spinner {
    /// Advances one frame and returns the glyph to draw.
    pub fn tick(&mut self) -> char {
        self.frame = self.frame.wrapping_add(1);
        self.glyph()
    }

    /// Returns the glyph for the current frame without advancing.
    pub fn glyph(&self) -> char {
        theme::SPINNER_CHARS[self.frame % theme::SPINNER_CHARS.len()]
    }

    pub fn reset(&mut self) {
        self.frame = 0;
    }
}

/// Vertical scroll offset for a text view together with the bounds needed to
/// clamp it. Screens update `page_size`/`max_offset` from their rendered area
/// each frame and route navigation keys through [`ScrollState::handle_key`].
#[derive(Default, Clone, Copy)]
pub(crate) struct ScrollState {
    pub offset: usize,
    pub max_offset: usize,
    pub page_size: usize,
}

impl ScrollState {
    /// Scrolls back to the top.
    pub fn reset(&mut self) {
        self.offset = 0;
    }

    /// Applies the shared scroll keys, returning `true` when the key was
    /// consumed. Arrows move one row; the page keys move a full viewport.
    pub fn handle_key(&mut self, key: KeyCode) -> bool {
        match key {
            KeyCode::Down => self.offset = (self.offset + 1).min(self.max_offset),
            KeyCode::Up => self.offset = self.offset.saturating_sub(1),
            KeyCode::PageDown | KeyCode::Char(' ') => {
                self.offset = (self.offset + self.page_size).min(self.max_offset)
            }
            KeyCode::PageUp => self.offset = self.offset.saturating_sub(self.page_size),
            KeyCode::Home => self.offset = 0,
            KeyCode::End => self.offset = self.max_offset,
            _ => return false,
        }
        true
    }
}

/// The largest `width` x `height` rect centred inside `area`, clamped to it.
pub(crate) fn centered_rect(area: Rect, width: u16, height: u16) -> Rect {
    let width = width.min(area.width);
    let height = height.min(area.height);
    Rect {
        x: area.x + area.width.saturating_sub(width) / 2,
        y: area.y + area.height.saturating_sub(height) / 2,
        width,
        height,
    }
}

/// Wraps a single `Line` into multiple lines respecting `max_width` characters.
/// Each span keeps its own style, so wrapped key/value rows do not collapse to
/// a single colour.
pub(crate) fn wrap_line(line: &Line<'_>, max_width: usize, out: &mut Vec<Line<'static>>) {
    let max_width = max_width.max(1);
    if line.spans.is_empty() {
        out.push(Line::from(""));
        return;
    }

    let mut lines: Vec<Line<'static>> = Vec::new();
    let mut spans: Vec<Span<'static>> = Vec::new();
    let mut current = String::new();
    let mut current_style: Option<Style> = None;
    let mut width = 0usize;

    for span in &line.spans {
        for ch in span.content.chars() {
            if width == max_width {
                flush_span(&mut spans, &mut current, current_style);
                lines.push(Line::from(std::mem::take(&mut spans)));
                width = 0;
                current_style = None;
            }
            if current_style != Some(span.style) {
                flush_span(&mut spans, &mut current, current_style);
                current_style = Some(span.style);
            }
            current.push(ch);
            width += 1;
        }
    }
    flush_span(&mut spans, &mut current, current_style);
    if !spans.is_empty() {
        lines.push(Line::from(spans));
    }
    if lines.is_empty() {
        lines.push(Line::from(""));
    }
    out.extend(lines);
}

fn flush_span(spans: &mut Vec<Span<'static>>, current: &mut String, style: Option<Style>) {
    if !current.is_empty() {
        spans.push(Span::styled(
            std::mem::take(current),
            style.unwrap_or_default(),
        ));
    }
}

/// Cuts `text` down to at most `max_chars` characters, appending an ellipsis
/// when it was shortened, so long text cannot push a row off-screen.
pub(crate) fn truncate_with_ellipsis(text: &str, max_chars: usize) -> String {
    let count = text.chars().count();
    if count <= max_chars {
        return text.to_string();
    }
    let keep = max_chars.saturating_sub(1);
    let mut shortened: String = text.chars().take(keep).collect();
    shortened.push('\u{2026}');
    shortened
}

/// Splits a comma-separated value into trimmed, non-empty entries.
pub(crate) fn split_csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(String::from)
        .collect()
}

/// Maps the absolute `row` of a left click to the index of the item shown
/// there in a list or table rendered inside `area`.
///
/// `area` includes the widget's bordered block, and `header_rows` counts any
/// non-item rows above the items (0 for a plain `List`, 1 for a `Table` with
/// a header). The widget's current scroll `offset` shifts the mapping so a
/// click lands on the right item even after scrolling. Returns `None` when
/// the click missed the items entirely.
pub(crate) fn click_to_index(
    row: u16,
    offset: usize,
    area: Rect,
    header_rows: u16,
) -> Option<usize> {
    let first_item_row = area.y.saturating_add(1).saturating_add(header_rows);
    let row_in_list = row.checked_sub(first_item_row)?;
    let capacity = area.height.saturating_sub(2).saturating_sub(header_rows);
    if row_in_list >= capacity {
        return None;
    }
    Some(offset + row_in_list as usize)
}

pub trait StateNavigation {
    fn next(&mut self, len: usize);
    fn previous(&mut self, len: usize);
    fn page_next(&mut self, len: usize, page_size: usize);
    fn page_previous(&mut self, len: usize, page_size: usize);
    fn home(&mut self, len: usize);
    fn end(&mut self, len: usize);

    /// Handles common navigation key events
    /// (Down/Up/j/k, PageDown/PageUp, Home/End/g/G).
    /// Returns `true` if the key was handled, `false` if it should be passed through.
    fn handle_nav_keys(
        &mut self,
        key: crossterm::event::KeyCode,
        len: usize,
        page_size: usize,
    ) -> bool {
        use crossterm::event::KeyCode;
        match key {
            KeyCode::Down | KeyCode::Char('j') => {
                self.next(len);
                true
            }
            KeyCode::Up | KeyCode::Char('k') => {
                self.previous(len);
                true
            }
            KeyCode::PageDown => {
                self.page_next(len, page_size);
                true
            }
            KeyCode::PageUp => {
                self.page_previous(len, page_size);
                true
            }
            KeyCode::Home | KeyCode::Char('g') => {
                self.home(len);
                true
            }
            KeyCode::End | KeyCode::Char('G') => {
                self.end(len);
                true
            }
            _ => false,
        }
    }
}

macro_rules! impl_state_navigation {
    ($ty:ty) => {
        impl StateNavigation for $ty {
            fn next(&mut self, len: usize) {
                if len == 0 {
                    return;
                }
                let i = match self.selected() {
                    Some(i) => {
                        if i >= len.saturating_sub(1) {
                            0
                        } else {
                            i + 1
                        }
                    }
                    None => 0,
                };
                self.select(Some(i));
            }

            fn previous(&mut self, len: usize) {
                if len == 0 {
                    return;
                }
                let i = match self.selected() {
                    Some(i) => {
                        if i == 0 {
                            len.saturating_sub(1)
                        } else {
                            i - 1
                        }
                    }
                    None => 0,
                };
                self.select(Some(i));
            }

            fn page_next(&mut self, len: usize, page_size: usize) {
                if len == 0 {
                    return;
                }
                let i = match self.selected() {
                    Some(i) => (i + page_size).min(len.saturating_sub(1)),
                    None => 0,
                };
                self.select(Some(i));
            }

            fn page_previous(&mut self, len: usize, page_size: usize) {
                if len == 0 {
                    return;
                }
                let i = match self.selected() {
                    Some(i) => i.saturating_sub(page_size),
                    None => 0,
                };
                self.select(Some(i));
            }

            fn home(&mut self, len: usize) {
                if len > 0 {
                    self.select(Some(0));
                }
            }

            fn end(&mut self, len: usize) {
                if len > 0 {
                    self.select(Some(len.saturating_sub(1)));
                }
            }
        }
    };
}

impl_state_navigation!(TableState);
impl_state_navigation!(ListState);

/// Implements `Screen::attach_toasts` for a screen that stores its toast sink
/// in the named field of type `ToastSink`.
macro_rules! impl_attach_toasts {
    ($field:ident) => {
        fn attach_toasts(&mut self, sink: $crate::ui::tui::widgets::ToastSink) {
            self.$field = sink;
        }
    };
}
pub(crate) use impl_attach_toasts;

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::style::{Color, Modifier};

    fn plain(lines: &[Line<'static>]) -> Vec<String> {
        lines
            .iter()
            .map(|line| line.spans.iter().map(|s| s.content.as_ref()).collect())
            .collect()
    }

    #[test]
    fn truncate_with_ellipsis_shortens_only_overflowing_text() {
        assert_eq!(truncate_with_ellipsis("short.txt", 20), "short.txt");
        assert_eq!(truncate_with_ellipsis("abcdef", 6), "abcdef");
        assert_eq!(truncate_with_ellipsis("abcdefgh", 5), "abcd\u{2026}");
        // Multi-byte characters count as one column each, not one byte.
        assert_eq!(
            truncate_with_ellipsis("caf\u{e9}\u{e9}\u{e9}", 3),
            "ca\u{2026}"
        );
    }

    #[test]
    fn split_csv_trims_and_drops_empty_entries() {
        assert_eq!(split_csv(" a , b ,, c "), ["a", "b", "c"]);
        assert!(split_csv("").is_empty());
        assert!(split_csv(" , ").is_empty());
    }

    #[test]
    fn wrap_line_splits_at_the_width() {
        let line = Line::from("abcdef");
        let mut out = Vec::new();
        wrap_line(&line, 3, &mut out);
        assert_eq!(plain(&out), vec!["abc", "def"]);
    }

    #[test]
    fn wrap_line_preserves_span_styles() {
        let line = Line::from(vec![
            Span::styled("ab", Style::default().fg(Color::Red)),
            Span::styled("cdef", Style::default().add_modifier(Modifier::BOLD)),
        ]);
        let mut out = Vec::new();
        wrap_line(&line, 3, &mut out);

        assert_eq!(plain(&out), vec!["abc", "def"]);
        assert_eq!(out[0].spans[0].style.fg, Some(Color::Red));
        assert_eq!(
            out[0].spans[1].style,
            Style::default().add_modifier(Modifier::BOLD)
        );
        assert_eq!(
            out[1].spans[0].style,
            Style::default().add_modifier(Modifier::BOLD)
        );
    }

    #[test]
    fn wrap_line_handles_empty_lines() {
        let mut out = Vec::new();
        wrap_line(&Line::from(""), 5, &mut out);
        assert_eq!(plain(&out), vec![String::new()]);
    }

    #[test]
    fn click_to_index_maps_cursor_row_to_item() {
        // A bordered block 12 rows tall: rows 0 and 11 are borders, the items
        // fill rows 1..=10 with no header.
        let area = Rect::new(0, 10, 30, 12);
        assert_eq!(click_to_index(10, 0, area, 0), None, "top border");
        assert_eq!(click_to_index(11, 0, area, 0), Some(0));
        assert_eq!(click_to_index(20, 0, area, 0), Some(9));
        assert_eq!(click_to_index(21, 0, area, 0), None, "bottom border");

        // After scrolling, the offset shifts the mapping downward.
        assert_eq!(click_to_index(13, 5, area, 0), Some(7));

        // A table with a header: the header is one more non-item row.
        assert_eq!(click_to_index(13, 0, Rect::new(0, 10, 30, 12), 1), Some(1));
        assert_eq!(
            click_to_index(11, 0, Rect::new(0, 10, 30, 12), 1),
            None,
            "header row"
        );
    }
}
