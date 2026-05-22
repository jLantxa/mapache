use crossterm::event::KeyCode;

pub enum TextInputAction {
    None,
    Edited,
    Cancel,
    Confirm,
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
}

impl Default for TextInput {
    fn default() -> Self {
        Self::new()
    }
}
