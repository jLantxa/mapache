use crossterm::event::KeyCode;
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::Style,
    text::{Line, Span, Text},
    widgets::{Cell, Paragraph, Row, Table},
};

use crate::ui::tui::{
    theme,
    widgets::{TextInput, TextInputAction},
};

pub enum FormFieldType {
    Text(TextInput),
    Toggle(bool),
    Choice(usize, Vec<String>),
    Number(u32),
    /// A list of tags/patterns edited one entry at a time and rendered as
    /// chips. Editing starts empty; Enter commits the entry.
    MultiSelect(Vec<String>),
    Action(String),
    Label(String),
}

pub struct FormField {
    pub label: String,
    pub field_type: FormFieldType,
    help: Option<String>,
    error: Option<String>,
    number_bounds: (u32, u32),
}

#[derive(PartialEq, Eq)]
enum FormValue {
    Text(String),
    Toggle(bool),
    Choice(usize),
    Number(u32),
    MultiSelect(Vec<String>),
    Other,
}

impl FormField {
    pub fn new(label: impl Into<String>, field_type: FormFieldType) -> Self {
        Self {
            label: label.into(),
            field_type,
            help: None,
            error: None,
            number_bounds: (0, u32::MAX),
        }
    }

    pub fn text(label: impl Into<String>, value: impl Into<String>) -> Self {
        Self::new(
            label,
            FormFieldType::Text(TextInput::with_text(value.into())),
        )
    }

    pub fn list(label: impl Into<String>, items: Vec<String>) -> Self {
        Self::new(label, FormFieldType::MultiSelect(items))
    }

    pub fn toggle(label: impl Into<String>, value: bool) -> Self {
        Self::new(label, FormFieldType::Toggle(value))
    }

    pub fn number(label: impl Into<String>, value: u32) -> Self {
        Self::new(label, FormFieldType::Number(value))
    }

    pub fn choice(label: impl Into<String>, value: usize, options: Vec<String>) -> Self {
        Self::new(label, FormFieldType::Choice(value, options))
    }

    pub fn section(title: impl Into<String>) -> Self {
        Self::new("", FormFieldType::Label(title.into()))
    }

    pub fn action(title: impl Into<String>) -> Self {
        Self::new("", FormFieldType::Action(title.into()))
    }

    pub fn help(mut self, text: impl Into<String>) -> Self {
        self.help = Some(text.into());
        self
    }

    pub fn bounds(mut self, min: u32, max: u32) -> Self {
        self.number_bounds = (min, max.max(min));
        self
    }

    fn value(&self) -> FormValue {
        match &self.field_type {
            FormFieldType::Text(input) => FormValue::Text(input.text().to_string()),
            FormFieldType::Toggle(value) => FormValue::Toggle(*value),
            FormFieldType::Choice(value, _) => FormValue::Choice(*value),
            FormFieldType::Number(value) => FormValue::Number(*value),
            FormFieldType::MultiSelect(items) => FormValue::MultiSelect(items.clone()),
            FormFieldType::Action(_) | FormFieldType::Label(_) => FormValue::Other,
        }
    }

    fn restore_value(&mut self, initial: &FormValue) {
        match (&mut self.field_type, initial) {
            (FormFieldType::Text(input), FormValue::Text(value)) => {
                *input = TextInput::with_text(value.clone())
            }
            (FormFieldType::Toggle(current), FormValue::Toggle(value)) => *current = *value,
            (FormFieldType::Choice(current, _), FormValue::Choice(value)) => *current = *value,
            (FormFieldType::Number(current), FormValue::Number(value)) => *current = *value,
            (FormFieldType::MultiSelect(current), FormValue::MultiSelect(value)) => {
                current.clone_from(value)
            }
            _ => {}
        }
    }
}

/// Cycles a choice index, wrapping at either end. A choice with no options is
/// left unchanged (nothing to select).
fn step_choice(value: &mut usize, options_len: usize, forward: bool) {
    if options_len == 0 {
        return;
    }
    *value = if forward {
        (*value + 1) % options_len
    } else if *value == 0 {
        options_len - 1
    } else {
        *value - 1
    };
}

pub enum FormAction {
    None,
    Submit,
    Cancel,
    Edited,
}

/// Screen-level intent produced by driving a [`Form`] with one key.
///
/// [`Form`] owns dirty-state confirmation: a `Cancel` only surfaces once the
/// user has confirmed discarding unsaved edits, so screens can react to it
/// unconditionally.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FormCommand {
    /// The form consumed the key, or it is not a screen command.
    None,
    /// The user submitted the form.
    Submit,
    /// The user asked to abandon the form.
    Cancel,
}

/// Message shown on the form's help line for the focused field.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FormMessage<'a> {
    Help(&'a str),
    Error(&'a str),
    Warning(&'a str),
}

pub struct Form {
    fields: Vec<FormField>,
    focus: usize,
    editing: bool,
    label_width: usize,
    /// Scratch buffer used while editing a numeric field.
    number_edit: Option<String>,
    /// Scratch editor used while adding an entry to a `MultiSelect` field.
    multi_edit: Option<TextInput>,
    /// Whether the numeric edit buffer still holds the pre-edit value, so the
    /// next typed digit replaces it instead of appending.
    number_edit_fresh: bool,
    /// Whether validation errors are currently displayed. Set by
    /// [`Form::reveal_errors`] on a failed submit and cleared on the next edit.
    show_errors: bool,
    dirty: bool,
    initial_values: Vec<FormValue>,
    /// Whether the user has pressed Esc once on a dirty form and must press it
    /// again to confirm discarding the changes.
    confirm_discard: bool,
}

impl Form {
    pub fn new(fields: Vec<FormField>, label_width: usize) -> Self {
        let initial_values = fields.iter().map(FormField::value).collect();
        // Section labels are decoration, so start on the first usable field.
        let focus = fields
            .iter()
            .position(|field| !matches!(field.field_type, FormFieldType::Label(_)))
            .unwrap_or(0);
        Self {
            fields,
            focus,
            editing: false,
            label_width,
            show_errors: false,
            dirty: false,
            initial_values,
            confirm_discard: false,
            number_edit: None,
            multi_edit: None,
            number_edit_fresh: false,
        }
    }

    /// Attach a validation error to the field with the given label. It is only
    /// displayed after [`Form::reveal_errors`]. Returns `true` if the field was
    /// found.
    pub fn set_error(&mut self, label: &str, text: impl Into<String>) -> bool {
        let Some(field) = self.fields.iter_mut().find(|field| field.label == label) else {
            return false;
        };
        field.error = Some(text.into());
        true
    }

    /// Clear all validation errors and hide the error line. Called whenever the
    /// user edits the form, so a stale message never lingers.
    pub fn clear_errors(&mut self) {
        for field in &mut self.fields {
            field.error = None;
        }
        self.show_errors = false;
    }

    /// Reveal validation errors in place of the help line. Call after a failed
    /// submit; also [`Form::focus_field`] the offending field so it is visible.
    pub fn reveal_errors(&mut self) {
        self.show_errors = true;
    }

    /// Move focus to the field with the given label, if it exists.
    pub fn focus_field(&mut self, label: &str) -> bool {
        match self.fields.iter().position(|field| field.label == label) {
            Some(index) if !matches!(self.fields[index].field_type, FormFieldType::Label(_)) => {
                self.focus = index;
                true
            }
            _ => false,
        }
    }

    /// One-line message to show below the table for the focused field: a
    /// discard warning while confirming, otherwise the validation error (once
    /// errors are revealed), otherwise the field's help text.
    fn focused_message(&self) -> Option<FormMessage<'_>> {
        if self.confirm_discard {
            return Some(FormMessage::Warning(
                "Unsaved changes — press Esc again to discard",
            ));
        }
        let field = self.fields.get(self.focus)?;
        if self.show_errors
            && let Some(error) = &field.error
        {
            return Some(FormMessage::Error(error.as_str()));
        }
        field.help.as_deref().map(FormMessage::Help)
    }

    /// Next field to focus, skipping non-interactive [`FormFieldType::Label`]
    /// section rows. Returns the current focus when every field is a label.
    fn next_focus(&self, forward: bool) -> usize {
        let len = self.fields.len();
        if len == 0 {
            return 0;
        }
        let mut index = self.focus;
        for _ in 0..len {
            index = if forward {
                (index + 1) % len
            } else if index == 0 {
                len - 1
            } else {
                index - 1
            };
            if !matches!(self.fields[index].field_type, FormFieldType::Label(_)) {
                return index;
            }
        }
        self.focus
    }

    pub fn fields_mut(&mut self) -> &mut [FormField] {
        &mut self.fields
    }

    pub fn checkpoint(&mut self) {
        self.initial_values = self.fields.iter().map(FormField::value).collect();
        self.dirty = false;
        self.confirm_discard = false;
        self.clear_errors();
    }

    fn discard_changes(&mut self) {
        for (field, initial) in self.fields.iter_mut().zip(&self.initial_values) {
            field.restore_value(initial);
        }
        self.dirty = false;
        self.confirm_discard = false;
        self.clear_errors();
    }

    /// Look up a field by its exact label.
    fn field_by_label(&self, label: &str) -> Option<&FormField> {
        self.fields.iter().find(|field| field.label == label)
    }

    /// Look up a text field by its exact label.
    ///
    /// Screens address fields by label rather than position, since their field
    /// lists grow and gain section headers over time.
    pub fn get_text_by_label(&self, label: &str) -> Option<&str> {
        match &self.field_by_label(label)?.field_type {
            FormFieldType::Text(input) => Some(input.text()),
            _ => None,
        }
    }

    /// Look up a toggle field by its exact label.
    pub fn get_toggle_by_label(&self, label: &str) -> Option<bool> {
        match self.field_by_label(label)?.field_type {
            FormFieldType::Toggle(value) => Some(value),
            _ => None,
        }
    }

    /// Look up a choice field by its exact label.
    pub fn get_choice_by_label(&self, label: &str) -> Option<usize> {
        match self.field_by_label(label)?.field_type {
            FormFieldType::Choice(value, _) => Some(value),
            _ => None,
        }
    }

    /// Look up a number field by its exact label.
    pub fn get_number_by_label(&self, label: &str) -> Option<u32> {
        match self.field_by_label(label)?.field_type {
            FormFieldType::Number(value) => Some(value),
            _ => None,
        }
    }

    /// Look up a multi-select field by its exact label.
    pub fn get_multi_by_label(&self, label: &str) -> Option<&[String]> {
        match &self.field_by_label(label)?.field_type {
            FormFieldType::MultiSelect(items) => Some(items.as_slice()),
            _ => None,
        }
    }

    /// Handle a key while editing a numeric field. Returns the resulting action.
    fn handle_number_edit(&mut self, key: KeyCode) -> FormAction {
        let index = self.focus;
        let (min, max) = self.fields[index].number_bounds;

        match key {
            KeyCode::Enter => {
                let value = self
                    .number_edit
                    .take()
                    .and_then(|buf| buf.parse::<u32>().ok())
                    .map(|v| v.clamp(min, max));
                let changed = match value {
                    Some(v) => match self.fields.get_mut(index) {
                        Some(FormField {
                            field_type: FormFieldType::Number(current),
                            ..
                        }) => {
                            let changed = *current != v;
                            *current = v;
                            changed
                        }
                        _ => false,
                    },
                    None => false,
                };
                self.editing = false;
                self.number_edit_fresh = false;
                if changed {
                    FormAction::Edited
                } else {
                    FormAction::None
                }
            }
            KeyCode::Esc => {
                self.number_edit = None;
                self.editing = false;
                self.number_edit_fresh = false;
                FormAction::None
            }
            KeyCode::Backspace => {
                if let Some(buf) = self.number_edit.as_mut() {
                    buf.pop();
                }
                self.number_edit_fresh = false;
                FormAction::Edited
            }
            KeyCode::Char(c) if c.is_ascii_digit() => {
                let fresh = self.number_edit_fresh;
                if let Some(buf) = self.number_edit.as_mut() {
                    if fresh {
                        buf.clear();
                    }
                    if buf.len() < 10 {
                        buf.push(c);
                    }
                }
                self.number_edit_fresh = false;
                FormAction::Edited
            }
            _ => FormAction::None,
        }
    }

    /// Commit the MultiSelect edit buffer as a new entry, if it is non-empty.
    /// Returns `true` when an entry was added.
    fn commit_multi_input(&mut self) -> bool {
        let value = match self.multi_edit.as_mut() {
            Some(input) => {
                let value = input.text().trim().to_string();
                if value.is_empty() {
                    return false;
                }
                input.clear();
                value
            }
            None => return false,
        };

        if let FormFieldType::MultiSelect(items) = &mut self.fields[self.focus].field_type {
            if items.contains(&value) {
                return false;
            }
            items.push(value);
            return true;
        }
        false
    }

    /// Handle a key while editing a `MultiSelect` field.
    fn handle_multiselect_edit(&mut self, key: KeyCode) -> FormAction {
        match key {
            KeyCode::Esc => {
                self.multi_edit = None;
                self.editing = false;
                FormAction::None
            }
            KeyCode::Enter => {
                let committed = self.commit_multi_input();
                self.multi_edit = None;
                self.editing = false;
                if committed {
                    FormAction::Edited
                } else {
                    FormAction::None
                }
            }
            KeyCode::Backspace => {
                let editing_empty = self
                    .multi_edit
                    .as_ref()
                    .is_none_or(|input| input.is_empty());
                if editing_empty {
                    // Backspace on an empty entry removes the last chip.
                    if let FormFieldType::MultiSelect(items) =
                        &mut self.fields[self.focus].field_type
                        && !items.is_empty()
                    {
                        items.pop();
                        return FormAction::Edited;
                    }
                    FormAction::None
                } else {
                    if let Some(input) = self.multi_edit.as_mut() {
                        input.delete_before();
                    }
                    FormAction::Edited
                }
            }
            _ => match self.multi_edit.as_mut() {
                Some(input) => match input.handle_key(key) {
                    TextInputAction::Edited => FormAction::Edited,
                    _ => FormAction::None,
                },
                None => FormAction::None,
            },
        }
    }

    /// Dispatch a key.
    ///
    /// Retained changes mark the form dirty; editing clears a previously revealed
    /// validation error. When the form is dirty, a first `Esc` only asks for
    /// confirmation (by setting the warning line) and returns
    /// [`FormAction::None`]; pressing Esc again returns [`FormAction::Cancel`].
    /// Any other key cancels the pending confirmation.
    pub fn handle_key(&mut self, key: KeyCode) -> FormAction {
        if key != KeyCode::Esc {
            self.confirm_discard = false;
        }

        let action = self.handle_key_inner(key);
        self.dirty = self
            .fields
            .iter()
            .zip(&self.initial_values)
            .any(|(field, initial)| field.value() != *initial);
        if matches!(action, FormAction::Edited) {
            self.clear_errors();
            self.confirm_discard = false;
        }

        if matches!(action, FormAction::Cancel) && self.dirty {
            if self.confirm_discard {
                self.discard_changes();
                return FormAction::Cancel;
            }
            self.confirm_discard = true;
            return FormAction::None;
        }

        action
    }

    /// Routes `key` through the form and reports the screen-level intent.
    ///
    /// Plain `q` is deliberately not handled here: the application owns the
    /// "q goes back" shortcut so it keeps working while a field has focus.
    pub fn command(&mut self, key: KeyCode) -> FormCommand {
        match self.handle_key(key) {
            FormAction::Submit => FormCommand::Submit,
            FormAction::Cancel => FormCommand::Cancel,
            FormAction::None | FormAction::Edited => FormCommand::None,
        }
    }

    fn handle_key_inner(&mut self, key: KeyCode) -> FormAction {
        if self.editing {
            let is_number = matches!(
                &self.fields[self.focus].field_type,
                FormFieldType::Number(_)
            );
            if is_number {
                return self.handle_number_edit(key);
            }
            if matches!(
                &self.fields[self.focus].field_type,
                FormFieldType::MultiSelect(_)
            ) {
                return self.handle_multiselect_edit(key);
            }
            let field = &mut self.fields[self.focus];
            match &mut field.field_type {
                FormFieldType::Text(input) => {
                    let before_len = input.text().len();
                    if matches!(
                        input.handle_key(key),
                        TextInputAction::Confirm | TextInputAction::Cancel
                    ) {
                        self.editing = false;
                    }
                    if input.text().len() != before_len {
                        FormAction::Edited
                    } else {
                        FormAction::None
                    }
                }
                _ => {
                    self.editing = false;
                    FormAction::None
                }
            }
        } else {
            match key {
                KeyCode::Esc => FormAction::Cancel,
                KeyCode::Tab | KeyCode::Down => {
                    self.focus = self.next_focus(true);
                    FormAction::None
                }
                KeyCode::BackTab | KeyCode::Up => {
                    self.focus = self.next_focus(false);
                    FormAction::None
                }
                KeyCode::Left => {
                    let field = &mut self.fields[self.focus];
                    match &mut field.field_type {
                        FormFieldType::Choice(val, options) => {
                            step_choice(val, options.len(), false);
                            FormAction::Edited
                        }
                        FormFieldType::Number(val) => {
                            let (min, _) = field.number_bounds;
                            *val = val.saturating_sub(1).max(min);
                            FormAction::Edited
                        }
                        _ => FormAction::None,
                    }
                }
                KeyCode::Right => {
                    let field = &mut self.fields[self.focus];
                    match &mut field.field_type {
                        FormFieldType::Choice(val, options) => {
                            step_choice(val, options.len(), true);
                            FormAction::Edited
                        }
                        FormFieldType::Number(val) => {
                            let (_, max) = field.number_bounds;
                            *val = val.saturating_add(1).min(max);
                            FormAction::Edited
                        }
                        _ => FormAction::None,
                    }
                }
                KeyCode::Char(' ') | KeyCode::Enter => {
                    let field = &mut self.fields[self.focus];
                    match &mut field.field_type {
                        FormFieldType::Text(_) => {
                            self.editing = true;
                            FormAction::None
                        }
                        FormFieldType::Toggle(val) => {
                            *val = !*val;
                            FormAction::Edited
                        }
                        FormFieldType::Number(val) => {
                            self.editing = true;
                            self.number_edit = Some(val.to_string());
                            self.number_edit_fresh = true;
                            FormAction::None
                        }
                        FormFieldType::MultiSelect(_) => {
                            self.editing = true;
                            self.multi_edit = Some(TextInput::new());
                            FormAction::None
                        }
                        FormFieldType::Action(_) => FormAction::Submit,
                        FormFieldType::Label(_) => FormAction::None,
                        FormFieldType::Choice(val, options) => {
                            step_choice(val, options.len(), true);
                            FormAction::Edited
                        }
                    }
                }
                _ => FormAction::None,
            }
        }
    }

    pub fn render(&self, frame: &mut Frame, area: Rect, title: &str) {
        // Reserve a line at the bottom for the focused field's help/error.
        let message = self.focused_message();
        let (table_area, message_area) = if message.is_some() && area.height > 1 {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Min(1), Constraint::Length(1)])
                .split(area);
            (chunks[0], Some(chunks[1]))
        } else {
            (area, None)
        };

        self.render_table(frame, table_area, title);

        if let (Some(message), Some(message_area)) = (message, message_area) {
            let (marker, marker_style, text_style, text) = match message {
                FormMessage::Error(text) => (" ! ", theme::THEME.error, theme::THEME.error, text),
                FormMessage::Warning(text) => {
                    (" ! ", theme::THEME.warning, theme::THEME.warning, text)
                }
                FormMessage::Help(text) => (" ? ", theme::THEME.menu_key, Style::default(), text),
            };
            let line = Line::from(vec![
                Span::styled(marker, marker_style),
                Span::styled(text.to_string(), text_style),
            ]);
            frame.render_widget(Paragraph::new(line), message_area);
        }
    }

    fn render_rows(&self) -> Vec<Row<'_>> {
        let focus_style = Style::default().fg(theme::THEME.green);

        let mut rows: Vec<Row<'_>> = Vec::with_capacity(self.fields.len());
        for (i, field) in self.fields.iter().enumerate() {
            let focused = i == self.focus;
            let row_style = if focused {
                theme::THEME.selection
            } else {
                Style::default()
            };
            // Value cells share this style: the focus colour, or the default.
            let value_style = if focused {
                focus_style
            } else {
                Style::default()
            };

            let label_cell = if focused {
                Cell::from(Span::styled(
                    format!(" {} ", field.label),
                    theme::THEME.header.bold(),
                ))
            } else {
                Cell::from(Span::styled(
                    format!(" {} ", field.label),
                    Style::default().bold(),
                ))
            };

            let value_cell = match &field.field_type {
                FormFieldType::Text(input) => {
                    let text = if input.text().is_empty() {
                        Span::styled("(empty)", theme::THEME.footer)
                    } else if focused {
                        Span::styled(input.text(), focus_style)
                    } else {
                        Span::raw(input.text())
                    };
                    Cell::from(text)
                }
                FormFieldType::Toggle(val) => {
                    let checkbox = if *val { "[X]" } else { "[ ]" };
                    Cell::from(Span::styled(checkbox, value_style))
                }
                FormFieldType::Choice(val, options) => {
                    Cell::from(Span::styled(format!("< {} >", options[*val]), value_style))
                }
                FormFieldType::Number(val) => {
                    let text = if self.editing && focused {
                        let buf = self.number_edit.as_deref().unwrap_or("");
                        format!("[ {buf}_ ]")
                    } else {
                        format!("[ {val} ]")
                    };
                    Cell::from(Span::styled(text, value_style))
                }
                FormFieldType::MultiSelect(items) => {
                    let mut spans: Vec<Span<'_>> = Vec::new();
                    for item in items {
                        if !spans.is_empty() {
                            spans.push(Span::raw(" "));
                        }
                        spans.push(Span::styled(format!("[{item}]"), value_style));
                    }
                    if self.editing && focused {
                        let buf = self
                            .multi_edit
                            .as_ref()
                            .map(|input| input.text())
                            .unwrap_or("");
                        if !spans.is_empty() {
                            spans.push(Span::raw(" "));
                        }
                        spans.push(Span::styled(format!("{buf}_"), value_style));
                    }
                    if spans.is_empty() {
                        Cell::from(Span::styled("(none)", theme::THEME.footer))
                    } else {
                        Cell::from(Text::from(Line::from(spans)))
                    }
                }
                FormFieldType::Action(label) => {
                    if focused {
                        Cell::from(Span::styled(format!(" {} ", label), theme::THEME.success))
                    } else {
                        Cell::from(Span::styled(
                            format!(" {} ", label),
                            theme::THEME.subtext_dim,
                        ))
                    }
                }
                FormFieldType::Label(text) => {
                    Cell::from(Span::styled(text.clone(), Style::default().bold()))
                }
            };

            let row = Row::new(vec![label_cell, value_cell]).style(row_style);
            rows.push(row);
        }
        rows
    }

    /// The half-open range of field indices visible in a table `height` rows
    /// tall (borders excluded), shifted so the focused field is always shown.
    fn visible_range(&self, height: u16) -> (usize, usize) {
        let visible = height.saturating_sub(2).max(1) as usize;
        let start = self.focus.saturating_add(1).saturating_sub(visible);
        (start, start + visible)
    }

    fn render_table(&self, frame: &mut Frame, area: Rect, title: &str) {
        let rows = self.render_rows();

        // On short terminals only render the window of rows that fits, shifted
        // so the focused field stays visible. Rows are uniformly one line tall.
        let (start, end) = self.visible_range(area.height);
        let window: Vec<Row<'_>> = rows.into_iter().skip(start).take(end - start).collect();

        let table = Table::new(
            window,
            [
                Constraint::Length(self.label_width as u16 + 2),
                Constraint::Min(20),
            ],
        )
        .block(theme::block(title))
        .column_spacing(1);

        frame.render_widget(table, area);
    }

    pub fn is_editing(&self) -> bool {
        self.editing
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::KeyCode;

    fn key(c: char) -> KeyCode {
        KeyCode::Char(c)
    }

    fn make_form() -> Form {
        let fields = vec![
            FormField::text("Name", "default"),
            FormField::toggle("Flag", false),
            FormField::choice("Mode", 0, vec!["A".into(), "B".into(), "C".into()]),
            FormField::number("Count", 5),
            FormField::action("Go"),
        ];
        Form::new(fields, 10)
    }

    #[test]
    fn inline_metadata_preserves_help_and_numeric_limits() {
        let mut form = Form::new(
            vec![
                FormField::number("Count", 5)
                    .help("Bounded count")
                    .bounds(5, 7),
            ],
            10,
        );
        assert_eq!(
            form.focused_message(),
            Some(FormMessage::Help("Bounded count"))
        );
        form.handle_key(KeyCode::Left);
        assert_eq!(form.get_number_by_label("Count"), Some(5));
        form.handle_key(KeyCode::Enter);
        form.handle_key(key('9'));
        form.handle_key(KeyCode::Enter);
        assert_eq!(form.get_number_by_label("Count"), Some(7));
        form.handle_key(KeyCode::Right);
        assert_eq!(form.get_number_by_label("Count"), Some(7));
        for _ in 0..3 {
            form.handle_key(KeyCode::Left);
        }
        assert_eq!(form.get_number_by_label("Count"), Some(5));
    }

    #[test]
    fn form_tab_cycles_forward() {
        let mut f = make_form();
        f.handle_key(KeyCode::Tab);
        // focus moved to field 1 (verify via get_toggle — only field 1 is a toggle)
        // We can't directly check focus, but toggling field 1 should work
        f.handle_key(key(' ')); // toggle field 1
        assert_eq!(f.get_toggle_by_label("Flag"), Some(true));
    }

    #[test]
    fn form_backtab_cycles_backward() {
        let mut f = make_form();
        f.handle_key(KeyCode::BackTab);
        // focus should be on last field (Action) — pressing Enter submits
        assert!(matches!(f.handle_key(KeyCode::Enter), FormAction::Submit));
    }

    #[test]
    fn form_choice_left_right() {
        let mut f = make_form();
        f.handle_key(KeyCode::Tab);
        f.handle_key(KeyCode::Tab); // focus on Choice field
        assert_eq!(f.get_choice_by_label("Mode"), Some(0));

        f.handle_key(KeyCode::Right);
        assert_eq!(f.get_choice_by_label("Mode"), Some(1));

        f.handle_key(KeyCode::Right);
        assert_eq!(f.get_choice_by_label("Mode"), Some(2));

        f.handle_key(KeyCode::Right); // wraps around
        assert_eq!(f.get_choice_by_label("Mode"), Some(0));

        f.handle_key(KeyCode::Left); // wraps backward
        assert_eq!(f.get_choice_by_label("Mode"), Some(2));
    }

    #[test]
    fn form_text_editing_mode() {
        let mut f = make_form();
        // focus on field 0 (Text), Enter to start editing
        f.handle_key(KeyCode::Enter);
        assert!(f.is_editing());

        // typing while editing
        f.handle_key(key('x'));
        assert!(f.is_editing());

        // Enter confirms editing
        f.handle_key(KeyCode::Enter);
        assert!(!f.is_editing());
        assert_eq!(f.get_text_by_label("Name"), Some("defaultx"));
    }

    #[test]
    fn form_text_editing_cancel() {
        let mut f = make_form();
        f.handle_key(KeyCode::Enter); // start editing
        f.handle_key(key('z'));
        f.handle_key(KeyCode::Esc); // stops editing (text is kept)
        assert!(!f.is_editing());
        // TextInput Esc stops editing but doesn't clear the buffer —
        // the form treats this as Edited (field value changed)
        assert_eq!(f.get_text_by_label("Name"), Some("defaultz"));
    }

    #[test]
    fn form_get_returns_none_for_wrong_type() {
        let f = make_form();
        assert!(f.get_text_by_label("Flag").is_none()); // Flag is a toggle
        assert!(f.get_toggle_by_label("Name").is_none()); // Name is text
        assert!(f.get_choice_by_label("Name").is_none());
        assert!(f.get_number_by_label("Name").is_none());
    }

    #[test]
    fn form_unknown_label_returns_none() {
        let f = make_form();
        assert!(f.get_text_by_label("Missing").is_none());
        assert!(f.get_toggle_by_label("Missing").is_none());
    }

    /// Move focus to the `Count` number field (index 3).
    fn focus_number(f: &mut Form) {
        f.handle_key(KeyCode::Tab);
        f.handle_key(KeyCode::Tab);
        f.handle_key(KeyCode::Tab);
    }

    #[test]
    fn form_number_typed_digits_replace_and_commit() {
        let mut f = make_form();
        focus_number(&mut f);
        f.handle_key(KeyCode::Enter); // start editing
        assert!(f.is_editing());

        // The first digit replaces the pre-edit value (5) instead of appending.
        assert!(matches!(f.handle_key(key('9')), FormAction::Edited));
        assert!(matches!(f.handle_key(key('8')), FormAction::Edited));
        assert!(matches!(f.handle_key(KeyCode::Enter), FormAction::Edited));
        assert!(!f.is_editing());
        assert_eq!(f.get_number_by_label("Count"), Some(98));
    }

    #[test]
    fn form_number_backspace_edits_buffer() {
        let mut f = make_form();
        focus_number(&mut f);
        f.handle_key(KeyCode::Enter);
        f.handle_key(KeyCode::Backspace); // clears the fresh value
        f.handle_key(key('7'));
        f.handle_key(key('3'));
        f.handle_key(KeyCode::Enter);
        assert_eq!(f.get_number_by_label("Count"), Some(73));
    }

    #[test]
    fn form_number_esc_cancels_edit() {
        let mut f = make_form();
        focus_number(&mut f);
        f.handle_key(KeyCode::Enter);
        f.handle_key(key('1'));
        f.handle_key(key('2'));
        f.handle_key(KeyCode::Esc);
        assert!(!f.is_editing());
        assert_eq!(f.get_number_by_label("Count"), Some(5));
    }

    #[test]
    fn form_navigation_skips_label_rows() {
        let fields = vec![
            FormField::text("A", String::new()),
            FormField::section("Section"),
            FormField::toggle("B", false),
        ];
        let mut f = Form::new(fields, 10);
        assert_eq!(f.focus, 0);

        f.handle_key(KeyCode::Tab);
        assert_eq!(f.focus, 2, "Tab must skip the label row");

        f.handle_key(KeyCode::Tab);
        assert_eq!(f.focus, 0, "navigation wraps around");

        f.handle_key(KeyCode::BackTab);
        assert_eq!(f.focus, 2, "BackTab skips the label backwards");
        assert!(!f.focus_field("Missing"));
        assert_eq!(f.focus, 2);
    }

    #[test]
    fn form_scroll_keeps_focused_field_visible() {
        let mut f = make_form(); // 5 fields
        // A 4-row-tall table shows 2 bordered content rows.
        assert_eq!(f.visible_range(4), (0, 2));

        f.focus = 4;
        assert_eq!(f.visible_range(4), (3, 5));

        // A tall table never scrolls (height 20 => 18 content rows).
        assert_eq!(f.visible_range(20), (0, 18));
    }

    #[test]
    fn form_error_revealed_in_place_of_help_and_cleared_on_edit() {
        let mut f = Form::new(
            vec![
                FormField::text("Name", "default"),
                FormField::number("Count", 5).help("How many items to process."),
            ],
            10,
        );
        assert_eq!(f.focused_message(), None);
        assert!(f.set_error("Count", "Must be positive"));
        assert!(f.focus_field("Count"));
        // Errors are hidden until a failed submit reveals them.
        assert!(matches!(
            f.focused_message(),
            Some(FormMessage::Help("How many items to process."))
        ));
        f.reveal_errors();
        assert!(matches!(
            f.focused_message(),
            Some(FormMessage::Error("Must be positive"))
        ));
        // Editing the field clears the stale error and falls back to help.
        f.handle_key(KeyCode::Right);
        assert!(matches!(
            f.focused_message(),
            Some(FormMessage::Help("How many items to process."))
        ));
    }

    #[test]
    fn form_multiselect_commit_and_remove() {
        let fields = vec![FormField::list("Tags:", vec![])];
        let mut f = Form::new(fields, 10);
        assert!(f.get_multi_by_label("Tags:").unwrap().is_empty());
        assert!(f.get_multi_by_label("Missing:").is_none());
        assert!(f.get_text_by_label("Tags:").is_none());

        // Enter starts editing, typing fills the entry, Enter commits it.
        assert!(matches!(f.handle_key(KeyCode::Enter), FormAction::None));
        f.handle_key(KeyCode::Char('a'));
        f.handle_key(KeyCode::Char('b'));
        assert!(matches!(f.handle_key(KeyCode::Enter), FormAction::Edited));
        assert_eq!(
            f.get_multi_by_label("Tags:").unwrap(),
            ["ab".to_string()].as_slice()
        );

        // A second entry can be added the same way.
        f.handle_key(KeyCode::Enter);
        f.handle_key(KeyCode::Char('c'));
        f.handle_key(KeyCode::Char('d'));
        f.handle_key(KeyCode::Enter);
        assert_eq!(
            f.get_multi_by_label("Tags:").unwrap(),
            ["ab".to_string(), "cd".to_string()]
        );

        // Enter to edit, then Backspace on the empty entry removes the last chip.
        f.handle_key(KeyCode::Enter);
        assert!(matches!(
            f.handle_key(KeyCode::Backspace),
            FormAction::Edited
        ));
        assert_eq!(
            f.get_multi_by_label("Tags:").unwrap(),
            ["ab".to_string()].as_slice()
        );

        // Esc discards a partial entry without touching the committed ones.
        f.handle_key(KeyCode::Enter);
        f.handle_key(KeyCode::Char('x'));
        assert!(matches!(f.handle_key(KeyCode::Esc), FormAction::None));
        assert_eq!(
            f.get_multi_by_label("Tags:").unwrap(),
            ["ab".to_string()].as_slice()
        );
    }

    #[test]
    fn form_cancel_clean_is_immediate() {
        let mut f = make_form();
        assert!(matches!(f.handle_key(KeyCode::Esc), FormAction::Cancel));
        assert!(f.focused_message().is_none(), "no warning for a clean form");
    }

    #[test]
    fn form_esc_on_dirty_form_requires_confirmation() {
        let mut f = make_form();
        // Focus starts on the first (Text) field: open it, type, then confirm.
        f.handle_key(KeyCode::Char(' ')); // start editing
        f.handle_key(key('x')); // edit -> dirty
        f.handle_key(KeyCode::Enter); // leave edit mode
        assert!(f.dirty);

        // First Esc warns instead of cancelling.
        assert!(matches!(f.handle_key(KeyCode::Esc), FormAction::None));
        assert!(matches!(f.focused_message(), Some(FormMessage::Warning(_))));
        // A second Esc confirms the discard.
        assert!(matches!(f.handle_key(KeyCode::Esc), FormAction::Cancel));
    }

    #[test]
    fn form_dirty_warning_cleared_by_other_keys() {
        let mut f = make_form();
        f.handle_key(KeyCode::Char(' ')); // start editing
        f.handle_key(key('x')); // dirty
        f.handle_key(KeyCode::Enter); // leave edit mode
        f.handle_key(KeyCode::Esc); // reveal warning
        assert!(matches!(f.focused_message(), Some(FormMessage::Warning(_))));
        f.handle_key(KeyCode::Tab); // any other key cancels the discard
        assert!(!matches!(
            f.focused_message(),
            Some(FormMessage::Warning(_))
        ));
        // A later Esc warns again rather than cancelling.
        assert!(matches!(f.handle_key(KeyCode::Esc), FormAction::None));
    }

    #[test]
    fn form_reverting_changes_allows_immediate_cancel() {
        let mut f = make_form();
        f.handle_key(KeyCode::Char(' ')); // start editing the text field
        f.handle_key(key('x')); // dirty
        f.handle_key(KeyCode::Enter); // leave edit mode
        assert!(f.dirty);
        f.handle_key(KeyCode::Enter);
        f.handle_key(KeyCode::Backspace);
        f.handle_key(KeyCode::Enter);
        assert!(!f.dirty);
        assert!(matches!(f.handle_key(KeyCode::Esc), FormAction::Cancel));
    }

    #[test]
    fn form_unchanged_text_and_cancelled_number_stay_clean() {
        let mut form = make_form();
        form.handle_key(KeyCode::Enter);
        form.handle_key(KeyCode::Left);
        form.handle_key(KeyCode::Enter);
        assert!(matches!(form.handle_key(KeyCode::Esc), FormAction::Cancel));

        focus_number(&mut form);
        form.handle_key(KeyCode::Enter);
        form.handle_key(key('9'));
        form.handle_key(KeyCode::Esc);
        assert!(!form.dirty);
        assert!(matches!(form.handle_key(KeyCode::Esc), FormAction::Cancel));
    }

    #[test]
    fn confirmed_discard_restores_last_checkpoint() {
        let mut form = make_form();
        form.focus_field("Flag");
        form.handle_key(KeyCode::Enter);
        form.checkpoint();
        form.handle_key(KeyCode::Enter);
        assert_eq!(form.get_toggle_by_label("Flag"), Some(false));
        assert!(matches!(form.handle_key(KeyCode::Esc), FormAction::None));
        assert!(matches!(form.handle_key(KeyCode::Esc), FormAction::Cancel));
        assert_eq!(form.get_toggle_by_label("Flag"), Some(true));
        assert!(!form.dirty);
    }

    #[test]
    fn form_multiselect_preserves_commas_and_cancelled_entries_stay_clean() {
        let mut form = Form::new(vec![FormField::list("Exclude:", vec![])], 10);
        form.handle_key(KeyCode::Enter);
        form.handle_key(key('x'));
        form.handle_key(KeyCode::Esc);
        assert!(matches!(form.handle_key(KeyCode::Esc), FormAction::Cancel));

        form.handle_key(KeyCode::Enter);
        for character in "{a,b}".chars() {
            form.handle_key(key(character));
        }
        form.handle_key(KeyCode::Enter);
        assert_eq!(
            form.get_multi_by_label("Exclude:").unwrap(),
            &["{a,b}".to_string()]
        );
        form.handle_key(KeyCode::Enter);
        for character in "{a,b}".chars() {
            form.handle_key(key(character));
        }
        assert!(matches!(form.handle_key(KeyCode::Enter), FormAction::None));
        assert_eq!(form.get_multi_by_label("Exclude:").unwrap().len(), 1);
    }

    #[test]
    fn form_command_defers_cancel_until_discard_is_confirmed() {
        // The shared driver used by every configuration screen: a dirty form's
        // first Esc must not reach the screen as a Cancel.
        let mut form = make_form();
        form.handle_key(KeyCode::Char(' ')); // start editing
        form.handle_key(key('x')); // dirty
        form.handle_key(KeyCode::Enter); // leave edit mode

        assert!(matches!(form.command(KeyCode::Esc), FormCommand::None));
        assert!(matches!(form.command(KeyCode::Esc), FormCommand::Cancel));
    }

    #[test]
    fn form_command_reports_submit_and_plain_exits() {
        let mut action_form = Form::new(vec![FormField::action("Go")], 10);
        assert!(matches!(
            action_form.command(KeyCode::Enter),
            FormCommand::Submit
        ));

        let mut form = make_form();
        assert!(matches!(form.command(KeyCode::Esc), FormCommand::Cancel));
        assert!(matches!(form.command(KeyCode::Tab), FormCommand::None));
    }

    #[test]
    fn form_renders_sections_chips_errors_and_discard_warning() {
        let mut form = Form::new(
            vec![
                FormField::section("Basic"),
                FormField::list("Tags:", vec!["daily".into()]),
                FormField::section("Advanced"),
                FormField::toggle("Flag", false),
            ],
            10,
        );
        let render = |form: &Form, height| {
            crate::ui::tui::test_support::render_text(80, height, |frame| {
                form.render(frame, frame.area(), "Options");
            })
        };
        let text = render(&form, 10);
        for expected in ["Basic", "Advanced", "[daily]"] {
            assert!(text.contains(expected), "missing {expected}: {text}");
        }
        form.set_error("Tags:", "Tag required");
        form.reveal_errors();
        assert!(render(&form, 5).contains("Tag required"));
        form.focus_field("Flag");
        form.handle_key(key(' '));
        form.handle_key(KeyCode::Esc);
        let text = render(&form, 5);
        assert!(text.contains("press Esc again to discard"));
        assert!(text.contains("[X]"));
    }
}
