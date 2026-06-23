use crossterm::event::KeyCode;
use ratatui::{Frame, layout::Rect};

use crate::{
    repository::retention::RetentionRule,
    ui::tui::widgets::{Form, FormAction, FormField, FormFieldType, TextInput},
    utils,
};

pub struct RetentionConfig {
    pub form: Form,
}

impl RetentionConfig {
    pub fn new(config: Option<&crate::commands::cmd_forget::CmdArgs>) -> Self {
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
