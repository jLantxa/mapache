use crossterm::event::KeyCode;
use ratatui::{
    Frame,
    layout::Rect,
    style::{Color, Style},
    text::{Line, Span, Text},
    widgets::Paragraph,
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
    Action(String),
}

pub struct FormField {
    pub label: String,
    pub field_type: FormFieldType,
}

pub enum FormAction {
    None,
    Submit,
    Cancel,
    Edited,
}

pub struct Form {
    fields: Vec<FormField>,
    focus: usize,
    editing: bool,
    label_width: usize,
}

impl Form {
    pub fn new(fields: Vec<FormField>, label_width: usize) -> Self {
        Self {
            fields,
            focus: 0,
            editing: false,
            label_width,
        }
    }

    pub fn fields_mut(&mut self) -> &mut [FormField] {
        &mut self.fields
    }

    pub fn get_text(&self, index: usize) -> Option<&str> {
        match self.fields.get(index)?.field_type {
            FormFieldType::Text(ref input) => Some(input.text()),
            _ => None,
        }
    }

    pub fn get_toggle(&self, index: usize) -> Option<bool> {
        match self.fields.get(index)?.field_type {
            FormFieldType::Toggle(val) => Some(val),
            _ => None,
        }
    }

    pub fn get_choice(&self, index: usize) -> Option<usize> {
        match self.fields.get(index)?.field_type {
            FormFieldType::Choice(val, _) => Some(val),
            _ => None,
        }
    }

    pub fn get_number(&self, index: usize) -> Option<u32> {
        match self.fields.get(index)?.field_type {
            FormFieldType::Number(val) => Some(val),
            _ => None,
        }
    }

    pub fn handle_key(&mut self, key: KeyCode) -> FormAction {
        if self.editing {
            let field = &mut self.fields[self.focus];
            match &mut field.field_type {
                FormFieldType::Text(input) => match input.handle_key(key) {
                    TextInputAction::Confirm | TextInputAction::Cancel => {
                        self.editing = false;
                        FormAction::Edited
                    }
                    TextInputAction::Edited => FormAction::Edited,
                    TextInputAction::None => FormAction::None,
                },
                FormFieldType::Number(_val) => {
                    // Simple number editing could be improved, but for now let's use the same logic
                    // as the screens: they usually use a temporary TextInput for numbers too.
                    // For now, let's just handle Enter/Esc to stop editing if it was somehow triggered.
                    match key {
                        KeyCode::Enter | KeyCode::Esc => {
                            self.editing = false;
                            FormAction::None
                        }
                        _ => FormAction::None,
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
                    self.focus = (self.focus + 1) % self.fields.len();
                    FormAction::None
                }
                KeyCode::BackTab | KeyCode::Up => {
                    self.focus = if self.focus == 0 {
                        self.fields.len() - 1
                    } else {
                        self.focus - 1
                    };
                    FormAction::None
                }
                KeyCode::Left => {
                    let field = &mut self.fields[self.focus];
                    match &mut field.field_type {
                        FormFieldType::Choice(val, options) => {
                            *val = if *val == 0 {
                                options.len() - 1
                            } else {
                                *val - 1
                            };
                            FormAction::Edited
                        }
                        FormFieldType::Number(val) => {
                            *val = val.saturating_sub(1);
                            FormAction::Edited
                        }
                        _ => FormAction::None,
                    }
                }
                KeyCode::Right => {
                    let field = &mut self.fields[self.focus];
                    match &mut field.field_type {
                        FormFieldType::Choice(val, options) => {
                            *val = (*val + 1) % options.len();
                            FormAction::Edited
                        }
                        FormFieldType::Number(val) => {
                            *val = val.saturating_add(1);
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
                        FormFieldType::Number(_) => {
                            self.editing = true;
                            FormAction::None
                        }
                        FormFieldType::Action(_) => FormAction::Submit,
                        FormFieldType::Choice(val, options) => {
                            *val = (*val + 1) % options.len();
                            FormAction::Edited
                        }
                    }
                }
                _ => FormAction::None,
            }
        }
    }

    pub fn render(&self, frame: &mut Frame, area: Rect, title: &str) {
        let mut lines = Vec::new();
        let marker = "\u{25b6} ";
        let unfocused_marker = "  ";

        for (i, field) in self.fields.iter().enumerate() {
            let focused = i == self.focus;
            let m = if focused { marker } else { unfocused_marker };
            let label_style = if focused {
                theme::STYLE_HEADER.bold()
            } else {
                Style::default().bold()
            };

            let mut spans = vec![
                Span::styled(m, theme::STYLE_SNAPSHOT_DATE),
                Span::styled(
                    format!("{:<width$}", field.label, width = self.label_width),
                    label_style,
                ),
            ];

            match &field.field_type {
                FormFieldType::Text(input) => {
                    let text = if input.text().is_empty() {
                        Span::styled("(empty)", Style::default().fg(Color::DarkGray))
                    } else {
                        Span::raw(input.text())
                    };
                    spans.push(text);
                }
                FormFieldType::Toggle(val) => {
                    let checkbox = if *val { "[X]" } else { "[ ]" };
                    let style = if focused {
                        Style::default().fg(theme::SNAPSHOT_DATE)
                    } else {
                        Style::default()
                    };
                    spans.push(Span::styled(checkbox, style));
                }
                FormFieldType::Choice(val, options) => {
                    let style = if focused {
                        Style::default().fg(theme::SNAPSHOT_DATE)
                    } else {
                        Style::default()
                    };
                    let text = format!("< {} >", options[*val]);
                    spans.push(Span::styled(text, style));
                }
                FormFieldType::Number(val) => {
                    let style = if focused {
                        Style::default().fg(theme::SNAPSHOT_DATE)
                    } else {
                        Style::default()
                    };
                    let text = if self.editing && focused {
                        // This is a bit simplified, usually we'd want a real cursor here
                        format!("[ {}_ ]", val)
                    } else {
                        format!("[ {} ]", val)
                    };
                    spans.push(Span::styled(text, style));
                }
                FormFieldType::Action(label) => {
                    let style = if focused {
                        Style::default().fg(theme::SNAPSHOT_DATE).bold()
                    } else {
                        Style::default().fg(Color::DarkGray).bold()
                    };
                    // Clear the previous spans for Action buttons to center them or just style them differently
                    spans = vec![
                        Span::styled(m, theme::STYLE_SNAPSHOT_DATE),
                        Span::styled(label, style),
                    ];
                }
            }

            lines.push(Line::from(spans));
        }

        let widget = Paragraph::new(Text::from(lines)).block(theme::themed_block(title));
        frame.render_widget(widget, area);
    }

    pub fn is_editing(&self) -> bool {
        self.editing
    }
}
