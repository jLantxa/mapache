use std::path::PathBuf;

use crossterm::event::KeyCode;
use ratatui::{Frame, layout::Rect, widgets::Paragraph};

use crate::{
    commands::{self, cmd_snapshot::SnapshotRunOptions},
    common::defaults::{DEFAULT_SNAPSHOT_PACKERS, DEFAULT_SNAPSHOT_READERS},
    ui::tui::{
        theme,
        widgets::{Form, FormCommand, FormField, split_csv},
    },
};

pub struct SnapshotForm {
    pub form: Form,
}

impl SnapshotForm {
    pub fn new(config_defaults: Option<&commands::cmd_snapshot::CmdArgs>) -> Self {
        let fields = vec![
            FormField::section("Basic"),
            FormField::list("Paths:",
                    config_defaults
                        .map(|cfg| {
                            cfg.paths
                                .iter()
                                .map(|p| p.to_string_lossy().to_string())
                                .collect()
                        })
                        .unwrap_or_default()).help("One path per chip. Enter adds a path; Backspace in an empty editor removes the last."),
            FormField::list("Tags:",
                    config_defaults
                        .and_then(|cfg| cfg.tags_str.as_deref())
                        .map(split_csv)
                        .unwrap_or_default()).help("Optional tags. Press Enter to add the typed tag as a chip; Backspace removes the last."),
            FormField::text("Description:",
                    config_defaults
                        .and_then(|cfg| cfg.description.clone())
                        .unwrap_or_default()).help("Optional free-form description for this snapshot."),
            FormField::toggle("Dry run:",
                    config_defaults.map(|cfg| cfg.dry_run).unwrap_or(false)).help("Simulate the snapshot without writing repository data."),
            FormField::section("Advanced"),
            FormField::list("Exclude:",
                    config_defaults
                        .and_then(|cfg| cfg.exclude.clone())
                        .unwrap_or_default()).help("Glob patterns to exclude. Press Enter to add a pattern; Backspace removes the last."),
            FormField::text("Exclude file:",
                    config_defaults
                        .and_then(|cfg| cfg.exclude_file.as_ref())
                        .map(|path| path.to_string_lossy().into_owned())
                        .unwrap_or_default()).help("Read exclude patterns from this file, one per line."),
            FormField::toggle("As root:",
                    config_defaults.and_then(|cfg| cfg.as_root).unwrap_or(false)).help("Store paths relative to the filesystem root."),
            FormField::toggle("No parent:",
                    config_defaults.map(|cfg| cfg.no_parent).unwrap_or(false)).help("Do not create an incremental snapshot."),
            FormField::text("Parent:",
                    config_defaults
                        .and_then(|cfg| cfg.parent.as_ref())
                        .map(ToString::to_string)
                        .unwrap_or_default()).help("Parent snapshot ID or 'latest'; empty selects the parent automatically."),
            FormField::toggle("No scan:",
                    config_defaults.and_then(|cfg| cfg.no_scan).unwrap_or(false)).help("Skip the preliminary filesystem scan."),
            FormField::toggle("One filesystem:",
                    config_defaults
                        .and_then(|cfg| cfg.one_file_system)
                        .unwrap_or(false)).help("Do not cross filesystem boundaries while traversing paths."),
            FormField::toggle("Fail on skipped:",
                    config_defaults
                        .and_then(|cfg| cfg.fail_on_skipped)
                        .unwrap_or(false)).help("Fail instead of saving a snapshot when source items are skipped."),
            FormField::toggle("Skip unchanged:",
                    config_defaults
                        .and_then(|cfg| cfg.skip_if_unchanged)
                        .unwrap_or(false)).help("Do not save a snapshot when there are no changes from its parent."),
            FormField::toggle("With atime:",
                    config_defaults
                        .and_then(|cfg| cfg.with_atime)
                        .unwrap_or(false)).help("Store access times in snapshot metadata."),
            FormField::number("Readers:",
                    config_defaults
                        .and_then(|cfg| cfg.num_readers)
                        .unwrap_or(DEFAULT_SNAPSHOT_READERS) as u32).help("Number of parallel readers used to scan the filesystem.").bounds(1, u32::MAX),
            FormField::number("Packers:",
                    config_defaults
                        .and_then(|cfg| cfg.num_packers)
                        .unwrap_or(DEFAULT_SNAPSHOT_PACKERS) as u32).help("Number of parallel packers used to write blobs.").bounds(1, u32::MAX),
            FormField::action("Start Snapshot"),
        ];
        let form = Form::new(fields, 17);

        Self { form }
    }

    /// Validate the form before submitting. On failure the offending field is
    /// focused and its error revealed; returns `true` when safe to start.
    pub fn validate(&mut self) -> bool {
        self.form.clear_errors();

        if self.paths().is_empty() {
            self.form.set_error("Paths:", "Paths required");
            self.form.focus_field("Paths:");
            self.form.reveal_errors();
            return false;
        }

        let parent_error = if self.form.get_toggle_by_label("No parent:").unwrap_or(false)
            && !self
                .form
                .get_text_by_label("Parent:")
                .unwrap_or("")
                .trim()
                .is_empty()
        {
            Some("Parent and No parent cannot be combined".to_string())
        } else {
            self.parent().err()
        };
        if let Some(error) = parent_error {
            self.form.set_error("Parent:", error);
            self.form.focus_field("Parent:");
            self.form.reveal_errors();
            return false;
        }
        true
    }

    pub fn parent(&self) -> Result<Option<commands::UseSnapshot>, String> {
        let value = self.form.get_text_by_label("Parent:").unwrap_or("").trim();
        if value.is_empty() {
            Ok(None)
        } else {
            value
                .parse()
                .map(Some)
                .map_err(|error| format!("Invalid parent: {error}"))
        }
    }

    pub fn dry_run(&self) -> bool {
        self.form.get_toggle_by_label("Dry run:").unwrap_or(false)
    }

    pub fn handle_key(&mut self, key: KeyCode) -> ConfigAction {
        match self.form.command(key) {
            FormCommand::Submit => ConfigAction::Start,
            FormCommand::Cancel => ConfigAction::Cancel,
            FormCommand::None => ConfigAction::None,
        }
    }

    fn paths(&self) -> Vec<PathBuf> {
        self.form
            .get_multi_by_label("Paths:")
            .unwrap_or_default()
            .iter()
            .filter(|path| !path.trim().is_empty())
            .map(PathBuf::from)
            .collect()
    }

    pub fn to_snapshot_options(&self) -> SnapshotRunOptions {
        let paths = self.paths();

        let tags: Vec<String> = self
            .form
            .get_multi_by_label("Tags:")
            .map(|items| items.to_vec())
            .unwrap_or_default();
        let tags = if tags.is_empty() {
            None
        } else {
            Some(tags.join(", "))
        };

        let description = match self.form.get_text_by_label("Description:") {
            Some(text) if !text.is_empty() => Some(text.to_string()),
            _ => None,
        };

        let exclude: Vec<String> = self
            .form
            .get_multi_by_label("Exclude:")
            .map(|items| items.to_vec())
            .unwrap_or_default();

        SnapshotRunOptions {
            paths,
            as_root: self.form.get_toggle_by_label("As root:").unwrap_or(false),
            exclude: if exclude.is_empty() {
                None
            } else {
                Some(exclude)
            },
            exclude_file: self
                .form
                .get_text_by_label("Exclude file:")
                .filter(|value| !value.is_empty())
                .map(PathBuf::from),
            tags,
            description,
            no_scan: self.form.get_toggle_by_label("No scan:").unwrap_or(false),
            one_file_system: self
                .form
                .get_toggle_by_label("One filesystem:")
                .unwrap_or(false),
            fail_on_skipped: self
                .form
                .get_toggle_by_label("Fail on skipped:")
                .unwrap_or(false),
            skip_if_unchanged: self
                .form
                .get_toggle_by_label("Skip unchanged:")
                .unwrap_or(false),
            with_atime: self
                .form
                .get_toggle_by_label("With atime:")
                .unwrap_or(false),
            stdin: false,
            num_readers: self
                .form
                .get_number_by_label("Readers:")
                .unwrap_or(DEFAULT_SNAPSHOT_READERS as u32) as usize,
            num_packers: self
                .form
                .get_number_by_label("Packers:")
                .unwrap_or(DEFAULT_SNAPSHOT_PACKERS as u32) as usize,
        }
    }
}

pub enum ConfigAction {
    None,
    Cancel,
    Start,
}

pub fn render_config(frame: &mut Frame, form: &SnapshotForm) {
    let inner = frame.area().inner(theme::CONTENT_MARGIN);

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
        theme::key_hint_footer(&[("Enter", "confirm"), ("Esc", "cancel edit")])
    } else {
        theme::key_hint_footer(&[
            ("Tab", "next"),
            ("Enter", "edit/start"),
            ("Space", "toggle"),
            ("Esc", "cancel"),
            ("q", "back"),
        ])
    };
    frame.render_widget(Paragraph::new(footer), area);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ui::tui::widgets::FormFieldType;

    fn make_form_with_paths(paths: &[&str]) -> SnapshotForm {
        let mut form = SnapshotForm::new(None);
        let fields = form.form.fields_mut();
        let field = fields
            .iter_mut()
            .find(|field| field.label == "Paths:")
            .unwrap();
        if let FormFieldType::MultiSelect(items) = &mut field.field_type {
            *items = paths.iter().map(|path| path.to_string()).collect();
        }
        form
    }

    #[test]
    fn path_chips_can_be_added_and_removed() {
        let mut form = SnapshotForm::new(None);
        form.handle_key(KeyCode::Enter);
        for character in "data, archives".chars() {
            form.handle_key(KeyCode::Char(character));
        }
        form.handle_key(KeyCode::Enter);
        assert_eq!(
            form.to_snapshot_options().paths,
            vec![PathBuf::from("data, archives")]
        );
        assert!(form.validate());
        form.handle_key(KeyCode::Enter);
        form.handle_key(KeyCode::Backspace);
        form.handle_key(KeyCode::Esc);
        assert!(!form.validate());
    }

    #[test]
    fn to_snapshot_options_exclude_and_tags_from_multiselect() {
        let mut form = SnapshotForm::new(None);
        for field in form.form.fields_mut() {
            if let FormFieldType::MultiSelect(items) = &mut field.field_type {
                *items = match field.label.as_str() {
                    "Exclude:" => vec!["*.log".to_string(), "target/".to_string()],
                    "Tags:" => vec!["daily".to_string(), "linux".to_string()],
                    _ => Vec::new(),
                };
            }
        }
        let opts = form.to_snapshot_options();
        let exclude = opts.exclude.unwrap();
        assert_eq!(exclude, vec!["*.log".to_string(), "target/".to_string()]);
        assert_eq!(opts.tags.as_deref(), Some("daily, linux"));
    }

    #[test]
    fn typing_q_in_a_field_does_not_quit() {
        let mut form = SnapshotForm::new(None);
        // Enter edit mode on the "Paths" field.
        assert!(matches!(
            form.handle_key(KeyCode::Enter),
            ConfigAction::None
        ));
        assert!(form.form.is_editing());
        // `q` must be inserted into the input, not quit the app.
        assert!(matches!(
            form.handle_key(KeyCode::Char('q')),
            ConfigAction::None
        ));
        form.handle_key(KeyCode::Enter);
        assert_eq!(
            form.form.get_multi_by_label("Paths:").unwrap(),
            &["q".to_string()]
        );
    }

    #[test]
    fn validation_rejects_empty_path_lists() {
        for paths in [&[][..], &[""][..], &["   "][..]] {
            let mut form = make_form_with_paths(paths);
            assert!(!form.validate());
            assert!(form.to_snapshot_options().paths.is_empty());
        }
        assert!(make_form_with_paths(&["data"]).validate());
    }

    #[test]
    fn validation_renders_paths_required_after_submit() {
        let mut form = SnapshotForm::new(None);
        form.form.handle_key(KeyCode::BackTab);
        assert!(matches!(
            form.handle_key(KeyCode::Enter),
            ConfigAction::Start
        ));
        assert!(!form.validate());
        let text = crate::ui::tui::test_support::render_text(80, 16, |frame| {
            render_config(frame, &form);
        });
        assert!(text.contains("Paths required"));
        assert!(text.contains("Paths:"));
        assert!(text.contains("Basic"));
        assert!(text.contains("Advanced"));
    }

    #[test]
    fn configured_paths_render_as_separate_chips() {
        let defaults = commands::cmd_snapshot::CmdArgs {
            paths: vec![PathBuf::from("data, archives"), PathBuf::from("other data")],
            ..Default::default()
        };
        let form = SnapshotForm::new(Some(&defaults));
        assert_eq!(form.to_snapshot_options().paths, defaults.paths);
        let text = crate::ui::tui::test_support::render_text(100, 18, |frame| {
            render_config(frame, &form);
        });
        assert!(text.contains("[data, archives] [other data]"));
    }

    #[test]
    fn advanced_snapshot_defaults_reach_run_options() {
        let defaults = commands::cmd_snapshot::CmdArgs {
            paths: vec![PathBuf::from("data")],
            exclude_file: Some(PathBuf::from("exclude.txt")),
            parent: Some(commands::UseSnapshot::Latest),
            no_scan: Some(true),
            one_file_system: Some(true),
            fail_on_skipped: Some(true),
            skip_if_unchanged: Some(true),
            with_atime: Some(true),
            dry_run: true,
            ..Default::default()
        };
        let mut form = SnapshotForm::new(Some(&defaults));
        assert!(form.validate());
        assert!(form.dry_run());
        assert_eq!(form.parent().unwrap().unwrap().to_string(), "latest");
        let options = form.to_snapshot_options();
        assert_eq!(options.exclude_file, defaults.exclude_file);
        assert!(options.no_scan && options.one_file_system && options.fail_on_skipped);
        assert!(options.skip_if_unchanged && options.with_atime);
        form.form.focus_field("No parent:");
        form.form.handle_key(KeyCode::Enter);
        assert!(!form.validate());
    }
}
