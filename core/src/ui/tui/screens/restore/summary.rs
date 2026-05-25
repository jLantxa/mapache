use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::Paragraph,
};

use crate::{
    ui::tui::{theme, widgets::TaskProgressState},
    utils,
};

pub fn render_summary(
    frame: &mut Frame,
    area: Rect,
    state: &TaskProgressState,
    result: &Option<Option<String>>,
) {
    let inner = area.inner(ratatui::layout::Margin::new(2, 1));
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(10), Constraint::Length(1)])
        .split(inner);

    let mut lines = Vec::with_capacity(12);
    let title_style = Style::default().bold().add_modifier(Modifier::UNDERLINED);

    if let Some(res) = result {
        match res {
            None => {
                lines.push(Line::from(Span::styled(
                    "RESTORE SUCCESSFUL",
                    Style::default().bold().fg(Color::Green),
                )));
            }
            Some(e) => {
                lines.push(Line::from(Span::styled(
                    "RESTORE FAILED",
                    Style::default().bold().fg(Color::Red),
                )));
                lines.push(Line::from(format!("Error: {}", e)));
            }
        }
    }
    lines.push(Line::from(""));

    lines.push(Line::from(Span::styled("Statistics:", title_style)));
    lines.push(Line::from(format!(
        "  Items restored: {}",
        state.processed_items
    )));
    lines.push(Line::from(format!(
        "  Bytes restored: {}",
        utils::format_size_binary(state.processed_bytes, 2)
    )));

    if let Some(finish) = state.finish_time {
        let duration = finish.duration_since(state.start_time);
        let duration_str = utils::pretty_print_duration(duration);
        lines.push(Line::from(format!("  Duration:       {}", duration_str)));

        let rate = if duration.as_secs_f64() > 0.0 {
            state.processed_bytes as f64 / duration.as_secs_f64()
        } else {
            0.0
        };
        lines.push(Line::from(format!(
            "  Average rate:   {}/s",
            utils::format_size_binary(rate as u64, 2)
        )));
    }

    lines.push(Line::from(""));
    lines.push(Line::from(vec![
        Span::styled("Errors:   ", Style::default().bold()),
        Span::styled(
            state.errors.len().to_string(),
            if state.errors.is_empty() {
                Style::default()
            } else {
                Style::default().fg(Color::Red)
            },
        ),
        Span::raw("    "),
        Span::styled("Warnings: ", Style::default().bold()),
        Span::styled(
            state.warnings.len().to_string(),
            if state.warnings.is_empty() {
                Style::default()
            } else {
                Style::default().fg(Color::Yellow)
            },
        ),
    ]));

    let summary = Paragraph::new(Text::from(lines)).block(theme::themed_block("Summary"));
    frame.render_widget(summary, chunks[0]);

    let footer = Line::from(vec![
        Span::styled("[Enter/Esc]", theme::THEME.style_menu_key),
        Span::raw(" back to dashboard"),
    ]);
    frame.render_widget(Paragraph::new(footer), chunks[1]);
}
