use crossterm::event::KeyCode;
use ratatui::{Frame, layout::Rect};

use crate::{
    commands::cmd_forget,
    repository::retention::{ForgetFilter, RetentionOptions, RetentionRule},
    ui::tui::widgets::{Form, FormCommand, FormField, split_csv},
    utils,
};

pub struct RetentionConfig {
    pub form: Form,
}

impl RetentionConfig {
    pub fn new(config: Option<&cmd_forget::CmdArgs>) -> Self {
        let hosts: Vec<String> = config.map(|c| c.hosts.clone()).unwrap_or_default();
        let tags_filter: Vec<String> = config
            .and_then(|c| c.tags_str.as_deref())
            .map(split_csv)
            .unwrap_or_default();
        let keep_tags: Vec<String> = config
            .and_then(|c| c.keep_tags.as_deref())
            .map(split_csv)
            .unwrap_or_default();

        let fields = vec![
            FormField::section("Basic"),
            FormField::text("Keep last:",
                    config
                        .and_then(|c| c.keep_last.map(|n| n.to_string()))
                        .unwrap_or_default()).help("Keep the N most recent snapshots. Use 'all' to keep every one."),
            FormField::text("Keep within:",
                    config
                        .and_then(|c| {
                            c.keep_within
                                .map(|duration| format!("{}s", duration.num_seconds()))
                        })
                        .unwrap_or_default()).help("Keep snapshots newer than a duration, e.g. '7d', '12h', '1w'."),
            FormField::text("Keep hourly:",
                    config
                        .and_then(|c| c.keep_hourly.map(|n| n.to_string()))
                        .unwrap_or_default()).help("Keep the latest snapshot of each of the last N hours ('all' for every hour)."),
            FormField::text("Keep daily:",
                    config
                        .and_then(|c| c.keep_daily.map(|n| n.to_string()))
                        .unwrap_or_default()).help("Keep the latest snapshot of each of the last N days ('all' for every day)."),
            FormField::text("Keep weekly:",
                    config
                        .and_then(|c| c.keep_weekly.map(|n| n.to_string()))
                        .unwrap_or_default()).help("Keep the latest snapshot of each of the last N weeks ('all' for every week)."),
            FormField::text("Keep monthly:",
                    config
                        .and_then(|c| c.keep_monthly.map(|n| n.to_string()))
                        .unwrap_or_default()).help("Keep the latest snapshot of each of the last N months ('all' for every month)."),
            FormField::text("Keep yearly:",
                    config
                        .and_then(|c| c.keep_yearly.map(|n| n.to_string()))
                        .unwrap_or_default()).help("Keep the latest snapshot of each of the last N years ('all' for every year)."),
            FormField::list("Keep tags:", keep_tags).help("Always keep snapshots carrying any of these tags. Enter adds a tag; Backspace removes the last."),
            FormField::text("Keep min:",
                    config
                        .and_then(|args| args.keep_min)
                        .map(|count| count.to_string())
                        .unwrap_or_default()).help("Keep at least N matching snapshots after applying retention rules ('all' keeps every one)."),
            FormField::section("Advanced - Filters / Deletion"),
            FormField::list("Hosts:", hosts).help("Only consider snapshots from these hosts. Enter adds a host; Backspace removes the last."),
            FormField::list("Tags (filter):", tags_filter).help("Only consider snapshots carrying all of these tags. Enter adds one; Backspace removes the last."),
            FormField::toggle("Force:", config.map(|args| args.force).unwrap_or(false)).help("Permanently delete selected snapshot metadata instead of staging it for removal."),
            FormField::action("Apply Rules"),
        ];

        let form = Form::new(fields, 15);

        Self { form }
    }

    pub fn handle_key(&mut self, key: KeyCode) -> RetentionAction {
        match self.form.command(key) {
            FormCommand::Submit if self.validate() => {
                self.form.checkpoint();
                RetentionAction::Apply
            }
            FormCommand::Cancel => RetentionAction::Cancel,
            _ => RetentionAction::None,
        }
    }

    fn validate(&mut self) -> bool {
        self.form.clear_errors();
        for label in [
            "Keep last:",
            "Keep within:",
            "Keep hourly:",
            "Keep daily:",
            "Keep weekly:",
            "Keep monthly:",
            "Keep yearly:",
            "Keep min:",
        ] {
            let value = self.form.get_text_by_label(label).unwrap_or("");
            if value.is_empty() {
                continue;
            }
            let error = if label == "Keep within:" {
                utils::parse_duration_string(value)
                    .err()
                    .map(|error| error.to_string())
            } else {
                cmd_forget::parse_retention_number(value).err()
            };
            if let Some(error) = error {
                self.form.set_error(label, error);
                self.form.focus_field(label);
                self.form.reveal_errors();
                return false;
            }
        }
        true
    }

    pub fn render(&self, frame: &mut Frame, area: Rect) {
        self.form.render(frame, area, "Retention Rules");
    }

    pub fn hosts(&self) -> Vec<String> {
        self.form
            .get_multi_by_label("Hosts:")
            .map(|items| items.to_vec())
            .unwrap_or_default()
    }

    pub fn tags_filter(&self) -> Vec<String> {
        self.form
            .get_multi_by_label("Tags (filter):")
            .map(|items| items.to_vec())
            .unwrap_or_default()
    }

    pub fn keep_min(&self) -> Option<usize> {
        self.keep_count("Keep min:")
    }

    pub fn force(&self) -> bool {
        self.form.get_toggle_by_label("Force:").unwrap_or(false)
    }

    /// The host/tag pre-filters grouped for the shared `build_forget_plan`.
    pub fn filter(&self) -> ForgetFilter {
        ForgetFilter {
            hosts: self.hosts(),
            tags: self.tags_filter().into_iter().collect(),
        }
    }

    /// Parse a keep-count field. Returns `None` for an empty or invalid entry;
    /// `all` maps to [`usize::MAX`].
    fn keep_count(&self, label: &str) -> Option<usize> {
        let value = self.form.get_text_by_label(label)?;
        if value.is_empty() {
            return None;
        }
        cmd_forget::parse_retention_number(value).ok()
    }

    pub fn to_rules(&self) -> Vec<RetentionRule> {
        let keep_tags: std::collections::BTreeSet<String> = self
            .form
            .get_multi_by_label("Keep tags:")
            .map(|items| items.iter().cloned().collect())
            .unwrap_or_default();

        RetentionOptions {
            keep_last: self.keep_count("Keep last:"),
            keep_within: self
                .form
                .get_text_by_label("Keep within:")
                .and_then(|value| utils::parse_duration_string(value).ok()),
            keep_yearly: self.keep_count("Keep yearly:"),
            keep_monthly: self.keep_count("Keep monthly:"),
            keep_weekly: self.keep_count("Keep weekly:"),
            keep_daily: self.keep_count("Keep daily:"),
            keep_hourly: self.keep_count("Keep hourly:"),
            keep_tags: (!keep_tags.is_empty()).then_some(keep_tags),
        }
        .to_rules()
    }
}

pub enum RetentionAction {
    None,
    Apply,
    Cancel,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ui::tui::widgets::{FormFieldType, TextInput};

    fn make_config(keep_last: &str) -> RetentionConfig {
        let mut rc = RetentionConfig::new(None);
        let field = rc
            .form
            .fields_mut()
            .iter_mut()
            .find(|field| field.label == "Keep last:")
            .unwrap();
        if let FormFieldType::Text(ref mut input) = field.field_type {
            *input = TextInput::with_text(keep_last.into());
        }
        rc
    }

    fn make_config_with_field(label: &str, value: &str) -> RetentionConfig {
        let mut rc = RetentionConfig::new(None);
        let field = rc
            .form
            .fields_mut()
            .iter_mut()
            .find(|field| field.label == label)
            .unwrap();
        if let FormFieldType::Text(ref mut input) = field.field_type {
            *input = TextInput::with_text(value.into());
        }
        rc
    }

    fn set_multi_by_label(rc: &mut RetentionConfig, label: &str, values: &[&str]) {
        let field = rc
            .form
            .fields_mut()
            .iter_mut()
            .find(|f| f.label == label)
            .unwrap_or_else(|| panic!("field {label:?} not found"));
        if let FormFieldType::MultiSelect(ref mut items) = field.field_type {
            *items = values.iter().map(|v| v.to_string()).collect();
        } else {
            panic!("field {label:?} is not a multiselect field");
        }
    }

    #[test]
    fn filters_are_read_by_label_not_position() {
        // Regression test: the form gained section-header label rows, so reading
        // hosts/tags by positional index silently picked the wrong fields.
        let mut rc = RetentionConfig::new(None);
        set_multi_by_label(&mut rc, "Hosts:", &["server-a", "server-b"]);
        set_multi_by_label(&mut rc, "Tags (filter):", &["release", "important"]);

        assert_eq!(
            rc.hosts(),
            vec!["server-a".to_string(), "server-b".to_string()]
        );
        assert_eq!(
            rc.tags_filter(),
            vec!["release".to_string(), "important".to_string()]
        );

        let filter = rc.filter();
        assert_eq!(
            filter.hosts,
            vec!["server-a".to_string(), "server-b".to_string()]
        );
        assert_eq!(filter.tags.len(), 2);
        assert!(filter.tags.contains("release"));
        assert!(filter.tags.contains("important"));
    }

    #[test]
    fn to_rules_keep_last_all() {
        let rc = make_config("all");
        let rules = rc.to_rules();
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0], RetentionRule::KeepLast(usize::MAX));
    }

    #[test]
    fn to_rules_keep_last_invalid_ignored() {
        let rc = make_config("abc");
        assert!(rc.to_rules().is_empty());
    }

    #[test]
    fn to_rules_keep_within_invalid_ignored() {
        let rc = make_config_with_field("Keep within:", "notaduration");
        assert!(rc.to_rules().is_empty());
    }

    #[test]
    fn to_rules_multiple_fields() {
        let mut rc = RetentionConfig::new(None);
        for field in rc.form.fields_mut() {
            if let FormFieldType::Text(input) = &mut field.field_type {
                match field.label.as_str() {
                    "Keep last:" => *input = TextInput::with_text("3".into()),
                    "Keep daily:" => *input = TextInput::with_text("7".into()),
                    _ => {}
                }
            }
        }
        let rules = rc.to_rules();
        assert_eq!(rules.len(), 2);
        assert_eq!(rules[0], RetentionRule::KeepLast(3));
        assert_eq!(rules[1], RetentionRule::KeepDaily(7));
    }

    #[test]
    fn to_rules_keep_tags_from_multiselect() {
        let mut rc = RetentionConfig::new(None);
        set_multi_by_label(&mut rc, "Keep tags:", &["important", "archive"]);
        let rules = rc.to_rules();
        assert_eq!(rules.len(), 1);
        match &rules[0] {
            RetentionRule::KeepTags(tags) => {
                assert!(tags.contains("important"));
                assert!(tags.contains("archive"));
            }
            _ => panic!("expected KeepTags rule"),
        }
    }

    #[test]
    fn to_rules_empty_fields_between_filled_ignored() {
        let rc = make_config_with_field("Keep weekly:", "2");
        let rules = rc.to_rules();
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0], RetentionRule::KeepWeekly(2));
    }

    #[test]
    fn invalid_retention_values_block_submit() {
        for (label, value) in [
            ("Keep last:", "0"),
            ("Keep daily:", "abc"),
            ("Keep within:", "notaduration"),
            ("Keep min:", "0"),
        ] {
            let mut config = make_config_with_field(label, value);
            config.form.handle_key(KeyCode::BackTab);
            assert!(matches!(
                config.handle_key(KeyCode::Enter),
                RetentionAction::None
            ));
            assert!(!config.validate());
        }
        let mut config = make_config("all");
        config.form.handle_key(KeyCode::BackTab);
        assert!(matches!(
            config.handle_key(KeyCode::Enter),
            RetentionAction::Apply
        ));
    }

    #[test]
    fn configured_duration_is_valid_and_preserved() {
        let duration = chrono::Duration::days(7);
        let defaults = cmd_forget::CmdArgs {
            keep_within: Some(duration),
            ..Default::default()
        };
        let mut config = RetentionConfig::new(Some(&defaults));
        assert!(config.validate());
        assert_eq!(config.to_rules(), vec![RetentionRule::KeepWithin(duration)]);
    }

    #[test]
    fn discard_reverts_force_but_apply_establishes_new_checkpoint() {
        let mut config = RetentionConfig::new(None);
        config.form.focus_field("Force:");
        config.handle_key(KeyCode::Enter);
        assert!(config.force());
        assert!(matches!(
            config.handle_key(KeyCode::Esc),
            RetentionAction::None
        ));
        assert!(matches!(
            config.handle_key(KeyCode::Esc),
            RetentionAction::Cancel
        ));
        assert!(!config.force());
        config.form.focus_field("Force:");
        config.handle_key(KeyCode::Enter);
        config.handle_key(KeyCode::Tab);
        assert!(matches!(
            config.handle_key(KeyCode::Enter),
            RetentionAction::Apply
        ));
        assert!(matches!(
            config.handle_key(KeyCode::Esc),
            RetentionAction::Cancel
        ));
        assert!(config.force());
    }

    #[test]
    fn retention_defaults_preserve_filters_keep_min_and_force() {
        let defaults = cmd_forget::CmdArgs {
            keep_min: Some(3),
            force: true,
            hosts: vec!["server".to_string()],
            tags_str: Some("daily,important".to_string()),
            keep_tags: Some("archive".to_string()),
            ..Default::default()
        };
        let mut config = RetentionConfig::new(Some(&defaults));
        assert!(config.validate());
        assert_eq!(config.keep_min(), Some(3));
        assert!(config.force());
        assert_eq!(config.hosts(), defaults.hosts);
        assert_eq!(config.tags_filter(), vec!["daily", "important"]);
        assert!(
            matches!(&config.to_rules()[0], RetentionRule::KeepTags(tags) if tags.contains("archive"))
        );
        assert_eq!(
            make_config_with_field("Keep min:", "all").keep_min(),
            Some(usize::MAX)
        );
    }
}
