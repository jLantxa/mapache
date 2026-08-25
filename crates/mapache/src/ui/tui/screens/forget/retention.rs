use crossterm::event::KeyCode;
use ratatui::{Frame, layout::Rect};

use crate::{
    commands::cmd_forget,
    repository::retention::RetentionRule,
    ui::tui::widgets::{Form, FormAction, FormField, FormFieldType, TextInput},
    utils,
};

pub struct RetentionConfig {
    pub form: Form,
}

impl RetentionConfig {
    pub fn new(config: Option<&cmd_forget::CmdArgs>) -> Self {
        let fields = vec![
            FormField {
                label: "Keep last:".to_string(),
                field_type: FormFieldType::Text(TextInput::with_text(
                    config
                        .and_then(|c| c.keep_last.map(|n| n.to_string()))
                        .unwrap_or_default(),
                )),
            },
            FormField {
                label: "Keep within:".to_string(),
                field_type: FormFieldType::Text(TextInput::with_text(
                    config
                        .and_then(|c| c.keep_within.map(|d| d.to_string()))
                        .unwrap_or_default(),
                )),
            },
            FormField {
                label: "Keep hourly:".to_string(),
                field_type: FormFieldType::Text(TextInput::with_text(
                    config
                        .and_then(|c| c.keep_hourly.map(|n| n.to_string()))
                        .unwrap_or_default(),
                )),
            },
            FormField {
                label: "Keep daily:".to_string(),
                field_type: FormFieldType::Text(TextInput::with_text(
                    config
                        .and_then(|c| c.keep_daily.map(|n| n.to_string()))
                        .unwrap_or_default(),
                )),
            },
            FormField {
                label: "Keep weekly:".to_string(),
                field_type: FormFieldType::Text(TextInput::with_text(
                    config
                        .and_then(|c| c.keep_weekly.map(|n| n.to_string()))
                        .unwrap_or_default(),
                )),
            },
            FormField {
                label: "Keep monthly:".to_string(),
                field_type: FormFieldType::Text(TextInput::with_text(
                    config
                        .and_then(|c| c.keep_monthly.map(|n| n.to_string()))
                        .unwrap_or_default(),
                )),
            },
            FormField {
                label: "Keep yearly:".to_string(),
                field_type: FormFieldType::Text(TextInput::with_text(
                    config
                        .and_then(|c| c.keep_yearly.map(|n| n.to_string()))
                        .unwrap_or_default(),
                )),
            },
            FormField {
                label: "".to_string(),
                field_type: FormFieldType::Action("Apply Rules".to_string()),
            },
        ];

        Self {
            form: Form::new(fields, 15),
        }
    }

    pub fn handle_key(&mut self, key: KeyCode) -> RetentionAction {
        match self.form.handle_key(key) {
            FormAction::Submit => RetentionAction::Apply,
            FormAction::Cancel => RetentionAction::Cancel,
            _ => RetentionAction::None,
        }
    }

    pub fn render(&self, frame: &mut Frame, area: Rect) {
        self.form.render(frame, area, "Retention Rules");
    }

    pub fn to_rules(&self) -> Vec<RetentionRule> {
        let mut rules = Vec::new();

        if let Some(s) = self.form.get_text(0)
            && !s.is_empty()
        {
            if let Ok(n) = s.parse::<usize>() {
                rules.push(RetentionRule::KeepLast(n));
            } else if s == "all" {
                rules.push(RetentionRule::KeepLast(usize::MAX));
            }
        }

        if let Some(s) = self.form.get_text(1)
            && !s.is_empty()
            && let Ok(dur) = utils::parse_duration_string(s)
        {
            rules.push(RetentionRule::KeepWithin(dur));
        }

        if let Some(s) = self.form.get_text(2)
            && !s.is_empty()
        {
            if let Ok(n) = s.parse::<usize>() {
                rules.push(RetentionRule::KeepHourly(n));
            } else if s == "all" {
                rules.push(RetentionRule::KeepHourly(usize::MAX));
            }
        }

        if let Some(s) = self.form.get_text(3)
            && !s.is_empty()
        {
            if let Ok(n) = s.parse::<usize>() {
                rules.push(RetentionRule::KeepDaily(n));
            } else if s == "all" {
                rules.push(RetentionRule::KeepDaily(usize::MAX));
            }
        }

        if let Some(s) = self.form.get_text(4)
            && !s.is_empty()
        {
            if let Ok(n) = s.parse::<usize>() {
                rules.push(RetentionRule::KeepWeekly(n));
            } else if s == "all" {
                rules.push(RetentionRule::KeepWeekly(usize::MAX));
            }
        }

        if let Some(s) = self.form.get_text(5)
            && !s.is_empty()
        {
            if let Ok(n) = s.parse::<usize>() {
                rules.push(RetentionRule::KeepMonthly(n));
            } else if s == "all" {
                rules.push(RetentionRule::KeepMonthly(usize::MAX));
            }
        }

        if let Some(s) = self.form.get_text(6)
            && !s.is_empty()
        {
            if let Ok(n) = s.parse::<usize>() {
                rules.push(RetentionRule::KeepYearly(n));
            } else if s == "all" {
                rules.push(RetentionRule::KeepYearly(usize::MAX));
            }
        }

        rules
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

    fn make_config(keep_last: &str) -> RetentionConfig {
        let mut rc = RetentionConfig::new(None);
        // Set the "Keep last" field (index 0) directly
        let field = &mut rc.form.fields_mut()[0];
        if let FormFieldType::Text(ref mut input) = field.field_type {
            *input = TextInput::with_text(keep_last.into());
        }
        rc
    }

    fn make_config_with_field(index: usize, value: &str) -> RetentionConfig {
        let mut rc = RetentionConfig::new(None);
        let field = &mut rc.form.fields_mut()[index];
        if let FormFieldType::Text(ref mut input) = field.field_type {
            *input = TextInput::with_text(value.into());
        }
        rc
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
        let rc = make_config_with_field(1, "notaduration");
        assert!(rc.to_rules().is_empty());
    }

    #[test]
    fn to_rules_multiple_fields() {
        let mut rc = RetentionConfig::new(None);
        // Set keep_last = 3 and keep_daily = 7
        let fields = rc.form.fields_mut();
        if let FormFieldType::Text(ref mut input) = fields[0].field_type {
            *input = TextInput::with_text("3".into());
        }
        if let FormFieldType::Text(ref mut input) = fields[3].field_type {
            *input = TextInput::with_text("7".into());
        }
        let rules = rc.to_rules();
        assert_eq!(rules.len(), 2);
        assert_eq!(rules[0], RetentionRule::KeepLast(3));
        assert_eq!(rules[1], RetentionRule::KeepDaily(7));
    }

    #[test]
    fn to_rules_empty_fields_between_filled_ignored() {
        let mut rc = RetentionConfig::new(None);
        // Set only keep_weekly (index 4), leave others empty
        let field = &mut rc.form.fields_mut()[4];
        if let FormFieldType::Text(ref mut input) = field.field_type {
            *input = TextInput::with_text("2".into());
        }
        let rules = rc.to_rules();
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0], RetentionRule::KeepWeekly(2));
    }
}
