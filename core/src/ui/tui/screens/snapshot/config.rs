use std::path::PathBuf;

use crossterm::event::KeyCode;
use ratatui::{
    Frame,
    layout::Rect,
    text::{Line, Span},
    widgets::Paragraph,
};

use crate::{
    commands::EMPTY_TAG_MARK,
    mapache::defaults::{DEFAULT_SNAPSHOT_PACKERS, DEFAULT_SNAPSHOT_READERS},
    ui::tui::{
        theme,
        widgets::{Form, FormAction, FormField, FormFieldType, TextInput},
    },
};

pub struct SnapshotForm {
    pub form: Form,
}

impl SnapshotForm {
    pub fn new(config_defaults: Option<&crate::commands::cmd_snapshot::CmdArgs>) -> Self {
        let fields = vec![
            FormField {
                label: "Paths:".to_string(),
                field_type: FormFieldType::Text(TextInput::with_text(
                    config_defaults
                        .map(|cfg| {
                            cfg.paths
                                .iter()
                                .map(|p| p.to_string_lossy().to_string())
                                .collect::<Vec<_>>()
                                .join(",")
                        })
                        .unwrap_or_default(),
                )),
            },
            FormField {
                label: "Tags:".to_string(),
                field_type: FormFieldType::Text(TextInput::with_text(
                    config_defaults
                        .and_then(|cfg| cfg.tags_str.clone())
                        .unwrap_or_default(),
                )),
            },
            FormField {
                label: "Description:".to_string(),
                field_type: FormFieldType::Text(TextInput::with_text(
                    config_defaults
                        .and_then(|cfg| cfg.description.clone())
                        .unwrap_or_default(),
                )),
            },
            FormField {
                label: "Exclude:".to_string(),
                field_type: FormFieldType::Text(TextInput::with_text(
                    config_defaults
                        .and_then(|cfg| cfg.exclude.as_ref().map(|e| e.join(",")))
                        .unwrap_or_default(),
                )),
            },
            FormField {
                label: "As root:".to_string(),
                field_type: FormFieldType::Toggle(
                    config_defaults.and_then(|cfg| cfg.as_root).unwrap_or(false),
                ),
            },
            FormField {
                label: "No parent:".to_string(),
                field_type: FormFieldType::Toggle(
                    config_defaults.map(|cfg| cfg.no_parent).unwrap_or(false),
                ),
            },
            FormField {
                label: "Readers:".to_string(),
                field_type: FormFieldType::Number(
                    config_defaults
                        .and_then(|cfg| cfg.num_readers)
                        .unwrap_or(DEFAULT_SNAPSHOT_READERS) as u32,
                ),
            },
            FormField {
                label: "Packers:".to_string(),
                field_type: FormFieldType::Number(
                    config_defaults
                        .and_then(|cfg| cfg.num_packers)
                        .unwrap_or(DEFAULT_SNAPSHOT_PACKERS) as u32,
                ),
            },
            FormField {
                label: "Start Snapshot".to_string(),
                field_type: FormFieldType::Action("Start Snapshot".to_string()),
            },
        ];

        Self {
            form: Form::new(fields, 13),
        }
    }

    pub fn paths_can_start(&self) -> bool {
        !self.form.get_text(0).unwrap_or("").trim().is_empty()
    }

    pub fn handle_key(&mut self, key: KeyCode) -> ConfigAction {
        match self.form.handle_key(key) {
            FormAction::Submit => ConfigAction::Start,
            FormAction::Cancel => ConfigAction::Cancel,
            _ => {
                if key == KeyCode::Char('q') {
                    ConfigAction::Quit
                } else {
                    ConfigAction::None
                }
            }
        }
    }

    pub fn to_cmd_args(&self) -> crate::commands::cmd_snapshot::CmdArgs {
        let paths: Vec<PathBuf> = self
            .form
            .get_text(0)
            .unwrap_or("")
            .split([',', '\n'])
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(PathBuf::from)
            .collect();

        let tags_str = if self.form.get_text(1).unwrap_or("").is_empty() {
            EMPTY_TAG_MARK.to_string()
        } else {
            self.form.get_text(1).unwrap_or("").to_string()
        };

        let description = if self.form.get_text(2).unwrap_or("").is_empty() {
            None
        } else {
            Some(self.form.get_text(2).unwrap_or("").to_string())
        };

        let exclude: Vec<String> = self
            .form
            .get_text(3)
            .unwrap_or("")
            .split([',', '\n'])
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .map(String::from)
            .collect();

        crate::commands::cmd_snapshot::CmdArgs {
            paths,
            as_root: Some(self.form.get_toggle(4).unwrap_or(false)),
            exclude: if exclude.is_empty() {
                None
            } else {
                Some(exclude)
            },
            exclude_file: None,
            tags_str: Some(tags_str),
            description,
            no_parent: self.form.get_toggle(5).unwrap_or(false),
            no_scan: Some(false),
            skip_if_unchanged: Some(false),
            parent: None,
            num_readers: Some(
                self.form
                    .get_number(6)
                    .unwrap_or(DEFAULT_SNAPSHOT_READERS as u32) as usize,
            ),
            num_packers: Some(
                self.form
                    .get_number(7)
                    .unwrap_or(DEFAULT_SNAPSHOT_PACKERS as u32) as usize,
            ),
            dry_run: false,
            with_atime: Some(false),
        }
    }
}

pub enum ConfigAction {
    None,
    Quit,
    Cancel,
    Start,
}

pub fn render_config(frame: &mut Frame, form: &SnapshotForm) {
    let area = frame.area();
    let inner = area.inner(ratatui::layout::Margin::new(2, 1));

    let chunks = ratatui::layout::Layout::default()
        .direction(ratatui::layout::Direction::Vertical)
        .constraints([
            ratatui::layout::Constraint::Min(10),
            ratatui::layout::Constraint::Length(1),
        ])
        .split(inner);

    form.form.render(frame, chunks[0], "Snapshot Configuration");
    render_footer(frame, chunks[1], form);
}

fn render_footer(frame: &mut Frame, area: Rect, form: &SnapshotForm) {
    let footer = if form.form.is_editing() {
        Line::from(vec![
            Span::styled("[Enter]", theme::STYLE_MENU_KEY),
            Span::raw(" confirm"),
            Span::raw("    "),
            Span::styled("[Esc]", theme::STYLE_MENU_KEY),
            Span::raw(" cancel edit"),
        ])
    } else {
        Line::from(vec![
            Span::styled("[Tab]", theme::STYLE_MENU_KEY),
            Span::raw(" next"),
            Span::raw("    "),
            Span::styled("[Enter]", theme::STYLE_MENU_KEY),
            Span::raw(" edit/start"),
            Span::raw("    "),
            Span::styled("[Space]", theme::STYLE_MENU_KEY),
            Span::raw(" toggle"),
            Span::raw("    "),
            Span::styled("[Esc]", theme::STYLE_MENU_KEY),
            Span::raw(" cancel"),
            Span::raw("    "),
            Span::styled("[q]", theme::STYLE_MENU_KEY),
            Span::raw(" quit"),
        ])
    };
    frame.render_widget(Paragraph::new(footer), area);
}
