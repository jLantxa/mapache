use ratatui::{
    text::{Line, Span},
    widgets::{ListState, TableState},
};

mod dialog;
mod form;
mod progress_bar;
mod task_progress;
mod text_input;
mod toast;

pub use dialog::Dialog;
pub use form::{Form, FormAction, FormField, FormFieldType};
pub use progress_bar::ProgressBar;
pub use task_progress::{TaskProgressState, TaskProgressWidget};
pub use text_input::{FilterAction, TextInput, TextInputAction};
pub use toast::Toast;

/// Wraps a single `Line` into multiple lines respecting `max_width` characters.
pub(crate) fn wrap_line(line: &Line<'_>, max_width: usize, out: &mut Vec<Line<'static>>) {
    if line.spans.is_empty() {
        out.push(Line::from(""));
        return;
    }

    let full_text: String = line.spans.iter().map(|s| s.content.as_ref()).collect();
    let full_style = line.spans.first().map(|s| s.style);

    let chars: Vec<char> = full_text.chars().collect();
    let total = chars.len();
    if total == 0 {
        out.push(Line::from(""));
        return;
    }

    let mut start = 0;
    while start < total {
        let end = (start + max_width).min(total);
        let segment: String = chars[start..end].iter().collect();
        if let Some(style) = full_style {
            out.push(Line::from(Span::styled(segment, style)));
        } else {
            out.push(Line::from(segment));
        }
        start = end;
    }
}

pub trait StateNavigation {
    fn next(&mut self, len: usize);
    fn previous(&mut self, len: usize);
    fn page_next(&mut self, len: usize, page_size: usize);
    fn page_previous(&mut self, len: usize, page_size: usize);
    fn home(&mut self, len: usize);
    fn end(&mut self, len: usize);

    /// Handles common navigation key events (Down/Up/PageDown/PageUp/Home/End).
    /// Returns `true` if the key was handled, `false` if it should be passed through.
    fn handle_nav_keys(
        &mut self,
        key: crossterm::event::KeyCode,
        len: usize,
        page_size: usize,
    ) -> bool {
        use crossterm::event::KeyCode;
        match key {
            KeyCode::Down => {
                self.next(len);
                true
            }
            KeyCode::Up => {
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
            KeyCode::Home => {
                self.home(len);
                true
            }
            KeyCode::End => {
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
