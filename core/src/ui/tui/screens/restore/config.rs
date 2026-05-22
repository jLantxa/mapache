use std::path::PathBuf;

use crossterm::event::KeyCode;
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    text::{Line, Span},
    widgets::Paragraph,
};

use crate::{
    repository::snapshot::SnapshotEntry,
    restorer::Strategy,
    ui::tui::{
        theme,
        widgets::{Form, FormAction, FormField, FormFieldType, TextInput},
    },
    utils,
};

pub enum ConfigAction {
    None,
    Start,
    Cancel,
}

pub struct RestoreConfig {
    pub form: Form,
    pub snapshot: SnapshotEntry,
    pub paths: Option<Vec<PathBuf>>,
}

impl RestoreConfig {
    pub fn new(snapshot: SnapshotEntry, paths: Option<Vec<PathBuf>>) -> Self {
        let fields = vec![
            FormField {
                label: "Target Path:".to_string(),
                field_type: FormFieldType::Text(TextInput::new()),
            },
            FormField {
                label: "Dry Run:".to_string(),
                field_type: FormFieldType::Toggle(false),
            },
            FormField {
                label: "Delete Extra:".to_string(),
                field_type: FormFieldType::Toggle(false),
            },
            FormField {
                label: "Strip Prefix:".to_string(),
                field_type: FormFieldType::Toggle(false),
            },
            FormField {
                label: "Conflict strategy:".to_string(),
                field_type: FormFieldType::Choice(
                    1, // Default to Overwrite
                    vec![
                        "Fail".to_string(),
                        "Overwrite".to_string(),
                        "Skip".to_string(),
                        "Keep Newer".to_string(),
                    ],
                ),
            },
            FormField {
                label: "Start Restore".to_string(),
                field_type: FormFieldType::Action("Start Restore".to_string()),
            },
        ];

        Self {
            form: Form::new(fields, 18),
            snapshot,
            paths,
        }
    }

    pub fn handle_key(&mut self, key: KeyCode) -> ConfigAction {
        match self.form.handle_key(key) {
            FormAction::Submit => ConfigAction::Start,
            FormAction::Cancel => ConfigAction::Cancel,
            _ => ConfigAction::None,
        }
    }

    pub fn render(&self, frame: &mut Frame, area: Rect) {
        let header_height = if self.paths.is_some() { 4 } else { 3 };
        let inner = area.inner(ratatui::layout::Margin::new(2, 1));
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(header_height),
                Constraint::Min(0),
                Constraint::Length(1),
            ])
            .split(inner);

        let mut info_text = format!(
            "Restoring Snapshot: {} ({} - {})",
            self.snapshot.id.to_short_hex(8),
            utils::pretty_print_timestamp(&self.snapshot.snapshot.timestamp, None),
            self.snapshot
                .snapshot
                .hostname
                .as_deref()
                .unwrap_or("unknown")
        );
        if let Some(paths) = &self.paths {
            info_text.push_str(&format!("\nPaths: {:?}", paths));
        }

        let info = Paragraph::new(info_text).block(theme::themed_block("Restore Configuration"));
        frame.render_widget(info, chunks[0]);

        self.form.render(frame, chunks[1], "Options");

        self.render_footer(frame, chunks[2]);
    }

    fn render_footer(&self, frame: &mut Frame, area: Rect) {
        let footer = if self.form.is_editing() {
            Line::from(vec![
                Span::styled("[Enter]", theme::STYLE_MENU_KEY),
                Span::raw(" confirm"),
                Span::raw("    "),
                Span::styled("[Esc]", theme::STYLE_MENU_KEY),
                Span::raw(" cancel edit"),
            ])
        } else {
            Line::from(vec![
                Span::styled("[Tab/\u{2191}\u{2193}]", theme::STYLE_MENU_KEY),
                Span::raw(" navigate"),
                Span::raw("    "),
                Span::styled("[Enter/Space]", theme::STYLE_MENU_KEY),
                Span::raw(" edit/toggle/start"),
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

    pub fn get_target(&self) -> PathBuf {
        PathBuf::from(self.form.get_text(0).unwrap_or(""))
    }

    pub fn get_dry_run(&self) -> bool {
        self.form.get_toggle(1).unwrap_or(false)
    }

    pub fn get_strip_prefix(&self) -> bool {
        self.form.get_toggle(3).unwrap_or(false)
    }

    pub fn get_strategy(&self) -> Strategy {
        match self.form.get_choice(4).unwrap_or(1) {
            0 => Strategy::Fail,
            1 => Strategy::Overwrite,
            2 => Strategy::Skip,
            3 => Strategy::Newer,
            _ => Strategy::Overwrite,
        }
    }
}
