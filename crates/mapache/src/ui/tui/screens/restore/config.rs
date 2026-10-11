use std::path::PathBuf;

use crossterm::event::KeyCode;
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    widgets::Paragraph,
};

use crate::{
    commands::cmd_restore,
    fs::filter::read_filtered_paths_from_file,
    repository::snapshot::SnapshotEntry,
    restorer::Strategy,
    ui::tui::{
        theme,
        widgets::{Form, FormCommand, FormField},
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
}

impl RestoreConfig {
    pub fn new(snapshot: SnapshotEntry, paths: Option<Vec<PathBuf>>) -> Self {
        let fields = vec![
            FormField::section("Basic"),
            FormField::list("Paths:",
                    paths
                        .as_deref()
                        .unwrap_or_default()
                        .iter()
                        .map(|path| path.to_string_lossy().into_owned())
                        .collect()).help("Paths to restore (empty restores all). Enter adds a path; Backspace removes the last."),
            FormField::text("Target Path:", "").help("Required destination directory for restored files."),
            FormField::toggle("Dry Run:", false).help("Report what would be restored without writing anything."),
            FormField::section("Advanced"),
            FormField::toggle("Strip Prefix:", false).help("Strip the snapshot's root prefix from restored paths."),
            FormField::toggle("Verify Content:", false).help("Verify each restored file against its checksum."),
            FormField::choice("Conflict strategy:",
                    1, // Default to Overwrite
                    vec![
                        "Fail".to_string(),
                        "Overwrite".to_string(),
                        "Skip".to_string(),
                        "Keep Newer".to_string(),
                    ]).help("How to handle existing files: fail, overwrite, skip or keep the newer one."),
            FormField::list("Include:", Vec::new()).help("Matching globs override Paths. Press Enter to add one; Backspace removes the last."),
            FormField::list("Exclude:", Vec::new()).help("Do not restore matching globs. Press Enter to add one; Backspace removes the last."),
            FormField::text("Include file:", "").help("Read additional include patterns from this file, one per line."),
            FormField::text("Exclude file:", "").help("Read additional exclude patterns from this file, one per line."),
            FormField::toggle("Delete:", false).help("After restoring, delete target items absent from the snapshot; asks for confirmation."),
            FormField::toggle("No preserve root:", false).help("Allow deletion in the target root directory; requires Delete."),
            FormField::toggle("Quit on error:", false).help("Stop restoring immediately when an error occurs."),
            FormField::toggle("Sparse:", false).help("Create sparse files instead of preallocating disk space."),
            FormField::action("Start Restore"),
        ];

        let form = Form::new(fields, 18);

        Self { form, snapshot }
    }

    pub fn handle_key(&mut self, key: KeyCode) -> ConfigAction {
        match self.form.command(key) {
            FormCommand::Submit => {
                if self.validate() {
                    ConfigAction::Start
                } else {
                    ConfigAction::None
                }
            }
            FormCommand::Cancel => ConfigAction::Cancel,
            FormCommand::None => ConfigAction::None,
        }
    }

    pub fn render(&self, frame: &mut Frame, area: Rect) {
        let inner = area.inner(theme::CONTENT_MARGIN);
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Min(0),
                Constraint::Length(1),
            ])
            .split(inner);

        let info_text = format!(
            "Restoring Snapshot: {} ({} - {})",
            self.snapshot.id.to_short_hex(8),
            utils::pretty_print_timestamp(&self.snapshot.snapshot.timestamp, None),
            self.snapshot
                .snapshot
                .hostname
                .as_deref()
                .unwrap_or("unknown")
        );
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
                ("q", "back"),
            ])
        };
        frame.render_widget(Paragraph::new(footer), area);
    }

    pub fn get_target(&self) -> PathBuf {
        PathBuf::from(self.form.get_text_by_label("Target Path:").unwrap_or(""))
    }

    fn file_path(&self, label: &str) -> Option<PathBuf> {
        self.form
            .get_text_by_label(label)
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
    }

    pub fn to_args(&self) -> cmd_restore::CmdArgs {
        cmd_restore::CmdArgs {
            target: Some(self.get_target()),
            dry_run: self.get_dry_run(),
            strip_prefix: Some(self.get_strip_prefix()),
            verify: Some(self.get_verify()),
            strategy: Some(self.get_strategy()),
            include: self.get_include().map(|paths| {
                paths
                    .iter()
                    .map(|path| path.to_string_lossy().into_owned())
                    .collect()
            }),
            exclude: self.get_exclude().map(|paths| {
                paths
                    .iter()
                    .map(|path| path.to_string_lossy().into_owned())
                    .collect()
            }),
            include_file: self.file_path("Include file:"),
            exclude_file: self.file_path("Exclude file:"),
            delete: self.form.get_toggle_by_label("Delete:"),
            no_preserve_root: self.form.get_toggle_by_label("No preserve root:"),
            quit_on_error: self.form.get_toggle_by_label("Quit on error:"),
            sparse: self.form.get_toggle_by_label("Sparse:"),
            ..Default::default()
        }
    }

    fn validate(&mut self) -> bool {
        self.form.clear_errors();
        let error = if self.get_target().as_os_str().is_empty() {
            Some(("Target Path:", "Target path required".to_string()))
        } else if self
            .form
            .get_toggle_by_label("No preserve root:")
            .unwrap_or(false)
            && !self.form.get_toggle_by_label("Delete:").unwrap_or(false)
        {
            Some((
                "No preserve root:",
                "No preserve root requires Delete".to_string(),
            ))
        } else {
            ["Include file:", "Exclude file:"]
                .into_iter()
                .find_map(|label| {
                    self.file_path(label)
                        .and_then(|path| read_filtered_paths_from_file(&path).err())
                        .map(|error| (label, error.to_string()))
                })
        };
        if let Some((label, error)) = error {
            self.form.set_error(label, error);
            self.form.focus_field(label);
            self.form.reveal_errors();
            return false;
        }
        true
    }

    pub fn get_dry_run(&self) -> bool {
        self.form.get_toggle_by_label("Dry Run:").unwrap_or(false)
    }

    pub fn get_strip_prefix(&self) -> bool {
        self.form
            .get_toggle_by_label("Strip Prefix:")
            .unwrap_or(false)
    }

    pub fn get_verify(&self) -> bool {
        self.form
            .get_toggle_by_label("Verify Content:")
            .unwrap_or(false)
    }

    pub fn get_strategy(&self) -> Strategy {
        match self
            .form
            .get_choice_by_label("Conflict strategy:")
            .unwrap_or(1)
        {
            0 => Strategy::Fail,
            1 => Strategy::Overwrite,
            2 => Strategy::Skip,
            3 => Strategy::Newer,
            _ => Strategy::Overwrite,
        }
    }

    /// Read a [`crate::ui::tui::widgets::FormFieldType::MultiSelect`]
    /// field as a path list. Returns `None` when the field is missing or empty.
    fn multi_paths(&self, label: &str) -> Option<Vec<PathBuf>> {
        let items = self.form.get_multi_by_label(label)?;
        if items.is_empty() {
            return None;
        }
        Some(
            items
                .iter()
                .map(|item| PathBuf::from(item.as_str()))
                .collect(),
        )
    }

    pub fn get_include(&self) -> Option<Vec<PathBuf>> {
        self.multi_paths("Include:")
            .or_else(|| self.multi_paths("Paths:"))
    }

    pub fn get_exclude(&self) -> Option<Vec<PathBuf>> {
        self.multi_paths("Exclude:")
    }
}

#[cfg(test)]
mod tests {
    use crate::ui::tui::widgets::{FormFieldType, TextInput};
    use crate::{common::ID, repository::snapshot::Snapshot};

    use super::*;

    fn make_snapshot_entry() -> SnapshotEntry {
        SnapshotEntry {
            id: ID::default(),
            snapshot: Snapshot {
                timestamp: chrono::Local::now(),
                root: PathBuf::from("/"),
                hostname: Some("test-host".to_string()),
                ..Default::default()
            },
            active: true,
        }
    }

    // --- get_strategy tests ---

    #[test]
    fn get_strategy_out_of_range_fallback() {
        let mut rc = RestoreConfig::new(make_snapshot_entry(), None);
        let field = rc
            .form
            .fields_mut()
            .iter_mut()
            .find(|field| field.label == "Conflict strategy:")
            .unwrap();
        if let FormFieldType::Choice(idx, _) = &mut field.field_type {
            *idx = 99;
        }
        assert_eq!(rc.get_strategy(), Strategy::Overwrite);
    }

    // --- get_include tests ---

    #[test]
    fn get_include_empty_falls_back_to_paths() {
        let paths = Some(vec![PathBuf::from("/data")]);
        let rc = RestoreConfig::new(make_snapshot_entry(), paths);
        let include = rc.get_include().unwrap();
        assert_eq!(include, vec![PathBuf::from("/data")]);
    }

    #[test]
    fn get_include_empty_no_paths_returns_none() {
        let rc = RestoreConfig::new(make_snapshot_entry(), None);
        assert!(rc.get_include().is_none());
    }

    #[test]
    fn get_include_multiselect_overrides_paths() {
        let mut rc = RestoreConfig::new(make_snapshot_entry(), Some(vec![PathBuf::from("/data")]));
        let field = rc
            .form
            .fields_mut()
            .iter_mut()
            .find(|field| field.label == "Include:")
            .unwrap();
        if let FormFieldType::MultiSelect(ref mut items) = field.field_type {
            *items = vec!["*.txt".to_string(), "docs/".to_string()];
        }
        let include = rc.get_include().unwrap();
        assert_eq!(
            include,
            vec![PathBuf::from("*.txt"), PathBuf::from("docs/")]
        );
    }

    #[test]
    fn selected_path_chips_can_be_added_and_removed() {
        let mut config = RestoreConfig::new(
            make_snapshot_entry(),
            Some(vec![PathBuf::from("data, archives")]),
        );
        assert_eq!(
            config.form.get_multi_by_label("Paths:").unwrap(),
            &["data, archives".to_string()]
        );
        config.handle_key(KeyCode::Enter);
        for character in "other data".chars() {
            config.handle_key(KeyCode::Char(character));
        }
        config.handle_key(KeyCode::Enter);
        assert_eq!(
            config.get_include().unwrap(),
            vec![PathBuf::from("data, archives"), PathBuf::from("other data")]
        );
        config.handle_key(KeyCode::Enter);
        config.handle_key(KeyCode::Backspace);
        config.handle_key(KeyCode::Backspace);
        config.handle_key(KeyCode::Esc);
        assert!(config.get_include().is_none());
    }

    #[test]
    fn restore_renders_selected_paths_as_chips() {
        let config = RestoreConfig::new(
            make_snapshot_entry(),
            Some(vec![PathBuf::from("data, archives")]),
        );
        let text = crate::ui::tui::test_support::render_text(80, 20, |frame| {
            config.render(frame, frame.area());
        });
        assert!(text.contains("[data, archives]"));
        assert!(text.contains("Paths:"));
    }

    fn set_text(config: &mut RestoreConfig, label: &str, value: &str) {
        let field = config
            .form
            .fields_mut()
            .iter_mut()
            .find(|field| field.label == label)
            .unwrap();
        if let FormFieldType::Text(input) = &mut field.field_type {
            *input = TextInput::with_text(value.to_string());
        } else {
            panic!("expected text field {label}");
        }
    }

    #[test]
    fn advanced_restore_fields_reach_command_args() {
        let mut config = RestoreConfig::new(make_snapshot_entry(), None);
        assert!(config.get_exclude().is_none());
        for label in ["Delete:", "No preserve root:", "Quit on error:", "Sparse:"] {
            config.form.focus_field(label);
            config.form.handle_key(KeyCode::Enter);
        }
        set_text(&mut config, "Include file:", "include.txt");
        set_text(&mut config, "Exclude file:", "exclude.txt");
        let args = config.to_args();
        assert_eq!(args.include_file, Some(PathBuf::from("include.txt")));
        assert_eq!(args.exclude_file, Some(PathBuf::from("exclude.txt")));
        assert_eq!(args.delete, Some(true));
        assert_eq!(args.no_preserve_root, Some(true));
        assert_eq!(args.quit_on_error, Some(true));
        assert_eq!(args.sparse, Some(true));
    }

    #[test]
    fn validation_rejects_missing_target_and_unsafe_delete_options() {
        let mut config = RestoreConfig::new(make_snapshot_entry(), None);
        assert!(!config.validate());
        set_text(&mut config, "Target Path:", "restore-target");
        assert!(config.validate());
        config.form.focus_field("No preserve root:");
        config.form.handle_key(KeyCode::Enter);
        assert!(!config.validate());
        config.form.focus_field("Delete:");
        config.form.handle_key(KeyCode::Enter);
        assert!(config.validate());
    }

    #[test]
    fn validation_rejects_unreadable_filter_files() {
        let mut config = RestoreConfig::new(make_snapshot_entry(), None);
        let temporary = tempfile::TempDir::new().unwrap();
        set_text(&mut config, "Target Path:", "restore-target");
        let path = temporary.path().join("filters.txt");
        set_text(&mut config, "Include file:", &path.to_string_lossy());
        assert!(!config.validate());
        std::fs::write(&path, "data\n").unwrap();
        assert!(config.validate());
        set_text(
            &mut config,
            "Exclude file:",
            &temporary.path().join("missing.txt").to_string_lossy(),
        );
        assert!(!config.validate());
    }
}
