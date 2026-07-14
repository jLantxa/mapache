use crossterm::event::KeyCode;
use ratatui::{
    style::Style,
    text::{Line, Span},
};

pub enum TextInputAction {
    None,
    Edited,
    Cancel,
    Confirm,
}

/// Result of handling a filter key event.
pub enum FilterAction {
    None,
    Apply,
    Cancel,
}

pub struct TextInput {
    buffer: String,
    cursor: usize,
}

impl TextInput {
    pub fn new() -> Self {
        Self {
            buffer: String::new(),
            cursor: 0,
        }
    }

    pub fn with_text(text: String) -> Self {
        let cursor = text.chars().count();
        Self {
            buffer: text,
            cursor,
        }
    }

    pub fn text(&self) -> &str {
        &self.buffer
    }

    pub fn clear(&mut self) {
        self.buffer.clear();
        self.cursor = 0;
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub fn cursor(&self) -> usize {
        self.cursor
    }

    pub fn handle_key(&mut self, key: KeyCode) -> TextInputAction {
        match key {
            KeyCode::Esc => TextInputAction::Cancel,
            KeyCode::Enter => TextInputAction::Confirm,
            KeyCode::Char(c) => {
                self.insert_char(c);
                TextInputAction::Edited
            }
            KeyCode::Backspace => {
                self.delete_before();
                TextInputAction::Edited
            }
            KeyCode::Delete => {
                self.delete_at();
                TextInputAction::Edited
            }
            KeyCode::Left => {
                self.cursor_left();
                TextInputAction::Edited
            }
            KeyCode::Right => {
                self.cursor_right();
                TextInputAction::Edited
            }
            KeyCode::Home => {
                self.cursor_home();
                TextInputAction::Edited
            }
            KeyCode::End => {
                self.cursor_end();
                TextInputAction::Edited
            }
            _ => TextInputAction::None,
        }
    }

    pub fn insert_char(&mut self, c: char) {
        let byte_pos = self
            .buffer
            .char_indices()
            .nth(self.cursor)
            .map(|(i, _)| i)
            .unwrap_or(self.buffer.len());
        self.buffer.insert(byte_pos, c);
        self.cursor += 1;
    }

    pub fn delete_before(&mut self) {
        if self.cursor == 0 {
            return;
        }
        if let Some((pos, _)) = self.buffer.char_indices().nth(self.cursor - 1) {
            self.buffer.remove(pos);
            self.cursor -= 1;
        }
    }

    pub fn delete_at(&mut self) {
        if self.cursor < self.buffer.chars().count()
            && let Some((pos, _)) = self.buffer.char_indices().nth(self.cursor)
        {
            self.buffer.remove(pos);
        }
    }

    pub fn cursor_left(&mut self) {
        if self.cursor > 0 {
            self.cursor -= 1;
        }
    }

    pub fn cursor_right(&mut self) -> bool {
        if self.cursor < self.buffer.chars().count() {
            self.cursor += 1;
            true
        } else {
            false
        }
    }

    pub fn cursor_home(&mut self) {
        self.cursor = 0;
    }

    pub fn cursor_end(&mut self) {
        self.cursor = self.buffer.chars().count();
    }

    /// Renders the input text as a `Line` with the cursor shown as an underlined character.
    pub fn render_line(&self, prefix: &str) -> Line<'static> {
        let text = self.text();
        let cursor = self.cursor();
        let before: String = text.chars().take(cursor).collect();
        let after: String = text.chars().skip(cursor).collect();
        let mut spans = vec![Span::raw(prefix.to_string())];
        spans.push(Span::raw(before));
        if after.is_empty() {
            spans.push(Span::styled(" ", Style::default().underlined()));
        } else {
            let cursor_char: String = after.chars().take(1).collect();
            let rest: String = after.chars().skip(1).collect();
            spans.push(Span::styled(cursor_char, Style::default().underlined()));
            spans.push(Span::raw(rest));
        }
        Line::from(spans)
    }

    /// Handles filter-specific key events: Cancel clears the filter, Confirm/Edited triggers re-filter.
    pub fn handle_filter_key(&mut self, key: KeyCode) -> FilterAction {
        match self.handle_key(key) {
            TextInputAction::Cancel => FilterAction::Cancel,
            TextInputAction::Confirm | TextInputAction::Edited => FilterAction::Apply,
            TextInputAction::None => FilterAction::None,
        }
    }
}

impl Default for TextInput {
    fn default() -> Self {
        Self::new()
    }
}
