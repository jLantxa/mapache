use crossterm::event::KeyCode;
use ratatui::{
    Frame,
    layout::{Constraint, Rect},
    style::Style,
    text::Span,
    widgets::{Cell, Row, Table},
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
                FormFieldType::Number(_val) => match key {
                    KeyCode::Enter | KeyCode::Esc => {
                        self.editing = false;
                        FormAction::None
                    }
                    _ => FormAction::None,
                },
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
        let focus_style = Style::default().fg(theme::THEME.green);

        let mut rows: Vec<Row<'_>> = Vec::with_capacity(self.fields.len());

        for (i, field) in self.fields.iter().enumerate() {
            let focused = i == self.focus;
            let row_style = if focused {
                theme::THEME.selection
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
                    let style = if focused {
                        focus_style
                    } else {
                        Style::default()
                    };
                    Cell::from(Span::styled(checkbox, style))
                }
                FormFieldType::Choice(val, options) => {
                    let style = if focused {
                        focus_style
                    } else {
                        Style::default()
                    };
                    Cell::from(Span::styled(format!("< {} >", options[*val]), style))
                }
                FormFieldType::Number(val) => {
                    let style = if focused {
                        focus_style
                    } else {
                        Style::default()
                    };
                    let text = if self.editing && focused {
                        format!("[ {}_ ]", val)
                    } else {
                        format!("[ {} ]", val)
                    };
                    Cell::from(Span::styled(text, style))
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
            };

            let row = Row::new(vec![label_cell, value_cell]).style(row_style);
            rows.push(row);
        }

        let table = Table::new(
            rows,
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
