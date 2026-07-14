use std::path::PathBuf;

use crossterm::event::KeyCode;
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
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
                label: "Include:".to_string(),
                field_type: FormFieldType::Text(TextInput::new()),
            },
            FormField {
                label: "Exclude:".to_string(),
                field_type: FormFieldType::Text(TextInput::new()),
            },
            FormField {
                label: "".to_string(),
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
        let inner = area.inner(theme::CONTENT_MARGIN);
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

        let info = Paragraph::new(info_text).block(theme::block("Restore Configuration"));
        frame.render_widget(info, chunks[0]);

        self.form.render(frame, chunks[1], "Options");

        self.render_footer(frame, chunks[2]);
    }

    fn render_footer(&self, frame: &mut Frame, area: Rect) {
        let footer = if self.form.is_editing() {
            theme::key_hint_footer(&[("Enter", "confirm"), ("Esc", "cancel edit")])
        } else {
            theme::key_hint_footer(&[
                ("Tab/\u{2191}\u{2193}", "navigate"),
                ("Enter/Space", "edit/toggle/start"),
                ("Esc", "cancel"),
                ("q", "quit"),
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
        self.form.get_toggle(2).unwrap_or(false)
    }

    pub fn get_strategy(&self) -> Strategy {
        match self.form.get_choice(3).unwrap_or(1) {
            0 => Strategy::Fail,
            1 => Strategy::Overwrite,
            2 => Strategy::Skip,
            3 => Strategy::Newer,
            _ => Strategy::Overwrite,
        }
    }

    fn parse_paths(text: &str) -> Option<Vec<PathBuf>> {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return None;
        }
        Some(
            trimmed
                .split([',', '\n'])
                .map(|s| PathBuf::from(s.trim()))
                .filter(|p| !p.as_os_str().is_empty())
                .collect(),
        )
    }

    pub fn get_include(&self) -> Option<Vec<PathBuf>> {
        Self::parse_paths(self.form.get_text(4).unwrap_or("")).or_else(|| self.paths.clone())
    }

    pub fn get_exclude(&self) -> Option<Vec<PathBuf>> {
        Self::parse_paths(self.form.get_text(5).unwrap_or(""))
    }
}
