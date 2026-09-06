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
}

impl Default for TextInput {
    fn default() -> Self {
        Self::new()
    }
}

/// Manages the lifecycle of a text filter: opening, editing, confirming, and cancelling.
///
/// The filter has two states:
/// - **Editing**: `input` is `Some(TextInput)` — the user is typing.
/// - **Idle**: `input` is `None` — `query` holds the last confirmed filter text (if any).
pub struct FilterState {
    input: Option<TextInput>,
    query: Option<String>,
}

impl FilterState {
    pub fn new() -> Self {
        Self {
            input: None,
            query: None,
        }
    }

    /// Returns `true` while the user is actively editing the filter.
    pub fn is_active(&self) -> bool {
        self.input.is_some()
    }

    /// The last confirmed filter query, or `None` if no filter is applied.
    pub fn query(&self) -> Option<&str> {
        self.query.as_deref()
    }

    /// Returns `true` if a filter query has been confirmed.
    pub fn has_query(&self) -> bool {
        self.query.is_some()
    }

    /// Returns the text currently being typed, if the filter is active.
    /// Returns `None` when idle.
    pub fn active_text(&self) -> Option<&str> {
        self.input.as_ref().map(|i| i.text())
    }

    /// Opens the filter input for editing. If a query already exists, it is
    /// pre-filled so the user can refine it.
    pub fn open(&mut self) {
        let text = self.query.clone().unwrap_or_default();
        self.input = Some(TextInput::with_text(text));
    }

    /// Clears the filter entirely (both editing state and committed query).
    pub fn clear(&mut self) {
        self.input = None;
        self.query = None;
    }

    /// Handles a key event while the filter is active.
    ///
    /// Returns:
    /// - `FilterAction::Apply` on **Enter** — the query was committed.
    /// - `FilterAction::Cancel` on **Esc** — editing was cancelled.
    /// - `FilterAction::None` otherwise.
    ///
    /// After receiving `Apply` or `Cancel`, call `is_active()` and `query()`
    /// to inspect the new state, then re-render / re-filter as needed.
    pub fn handle_key(&mut self, key: crossterm::event::KeyCode) -> FilterAction {
        let input = match self.input.as_mut() {
            Some(i) => i,
            None => return FilterAction::None,
        };

        match input.handle_key(key) {
            TextInputAction::Cancel => {
                self.input = None;
                FilterAction::Cancel
            }
            TextInputAction::Confirm => {
                let text = input.text().to_string();
                self.input = None;
                if text.is_empty() {
                    self.query = None;
                } else {
                    self.query = Some(text);
                }
                FilterAction::Apply
            }
            TextInputAction::Edited => FilterAction::None,
            TextInputAction::None => FilterAction::None,
        }
    }

    /// Renders the filter input bar.
    pub fn render(
        &self,
        frame: &mut ratatui::Frame,
        area: ratatui::layout::Rect,
        placeholder: &str,
    ) {
        let Some(input) = &self.input else {
            return;
        };

        use ratatui::widgets::Paragraph;
        let text = if input.is_empty() {
            ratatui::text::Line::from(ratatui::text::Span::styled(
                format!("> {}...", placeholder),
                super::super::theme::THEME.footer,
            ))
        } else {
            input.render_line("> ")
        };
        let widget = Paragraph::new(text).block(super::super::theme::block("Filter"));
        frame.render_widget(widget, area);
    }
}

impl Default for FilterState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::KeyCode;

    fn key(c: char) -> KeyCode {
        KeyCode::Char(c)
    }

    // ── TextInput ───────────────────────────────────────────────

    #[test]
    fn text_input_insert_and_read() {
        let mut input = TextInput::new();
        assert!(input.is_empty());
        input.insert_char('h');
        input.insert_char('i');
        assert_eq!(input.text(), "hi");
        assert_eq!(input.cursor(), 2);
    }

    #[test]
    fn text_input_delete_before() {
        let mut input = TextInput::with_text("ab".into());
        input.cursor_end();
        input.delete_before();
        assert_eq!(input.text(), "a");
        assert_eq!(input.cursor(), 1);
        input.delete_before();
        assert_eq!(input.text(), "");
        input.delete_before(); // no-op at 0
        assert_eq!(input.text(), "");
    }

    #[test]
    fn text_input_cursor_movement() {
        let mut input = TextInput::with_text("abc".into());
        assert_eq!(input.cursor(), 3);
        input.cursor_home();
        assert_eq!(input.cursor(), 0);
        input.cursor_right();
        assert_eq!(input.cursor(), 1);
        input.cursor_left();
        assert_eq!(input.cursor(), 0);
        input.cursor_left(); // no-op at 0
        assert_eq!(input.cursor(), 0);
    }

    // ── FilterState ─────────────────────────────────────────────

    #[test]
    fn filter_state_open_prefills_existing_query() {
        let mut f = FilterState::new();
        f.open();
        f.handle_key(key('h'));
        f.handle_key(key('i'));
        f.handle_key(KeyCode::Enter); // confirms "hi"
        assert_eq!(f.query(), Some("hi"));

        f.open(); // re-opens with prefill
        assert!(f.is_active());
        assert_eq!(f.active_text(), Some("hi"));
    }

    #[test]
    fn filter_state_confirm_sets_query() {
        let mut f = FilterState::new();
        f.open();
        f.handle_key(key('a'));
        f.handle_key(key('b'));
        let action = f.handle_key(KeyCode::Enter);
        assert!(matches!(action, FilterAction::Apply));
        assert!(!f.is_active());
        assert_eq!(f.query(), Some("ab"));
    }

    #[test]
    fn filter_state_confirm_empty_clears_query() {
        let mut f = FilterState::new();
        f.open();
        let action = f.handle_key(KeyCode::Enter);
        assert!(matches!(action, FilterAction::Apply));
        assert!(!f.has_query());
    }

    #[test]
    fn filter_state_cancel_preserves_existing_query() {
        let mut f = FilterState::new();
        // set a query first
        f.open();
        f.handle_key(key('o'));
        f.handle_key(key('l'));
        f.handle_key(KeyCode::Enter);
        assert_eq!(f.query(), Some("ol"));

        // open, type something, cancel
        f.open();
        f.handle_key(key('x'));
        f.handle_key(KeyCode::Esc);

        // old query preserved
        assert_eq!(f.query(), Some("ol"));
    }
}
