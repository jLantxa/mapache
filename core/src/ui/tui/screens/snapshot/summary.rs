use crossterm::event::KeyCode;
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Margin, Rect},
    style::Style,
    text::{Line, Span, Text},
    widgets::{Cell, Paragraph, Row, Table},
};

use crate::{
    mapache::{ID, defaults::SHORT_SNAPSHOT_ID_LEN},
    repository::snapshot::SnapshotSummary,
    ui::tui::{screens::snapshot::progress::SummaryResult, theme, widgets::Toast},
    utils,
};

#[derive(Debug)]
pub enum SummaryAction {
    None,
    Quit,
    Done,
}

pub fn render_summary(frame: &mut Frame, summary: &Option<SummaryResult>) {
    let area = frame.area();
    let inner = area.inner(Margin::new(2, 1));

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(10), Constraint::Length(1)])
        .split(inner);

    match summary {
        Some(SummaryResult::Success {
            summary,
            snapshot_id,
            duration,
        }) => {
            render_success(frame, chunks[0], summary.as_ref(), snapshot_id, *duration);
        }
        Some(SummaryResult::Cancelled) => {
            Toast::with_text(
                "Cancelled",
                theme::THEME.yellow,
                Text::from(vec![
                    Line::from("Snapshot was cancelled."),
                    Line::from(""),
                    Line::from("Some data may have been written to the repository."),
                    Line::from("You may want to run 'clean' to clean up."),
                ]),
            )
            .render(area, frame);
        }
        Some(SummaryResult::Error(msg)) => {
            Toast::new("Error", theme::THEME.red, msg).render(area, frame);
        }
        Some(SummaryResult::NoChanges) => {
            Toast::with_text(
                "No Changes",
                theme::THEME.blue,
                Text::from(vec![
                    Line::from("No changes detected since parent."),
                    Line::from(""),
                    Line::from("Snapshot was skipped."),
                ]),
            )
            .render(area, frame);
        }
        None => {
            Toast::new(
                "Info",
                theme::THEME.blue,
                "Waiting for snapshot to complete...",
            )
            .render(area, frame);
        }
    }

    render_footer(frame, chunks[1], summary);
}

fn render_success(
    frame: &mut Frame,
    area: Rect,
    summary: &SnapshotSummary,
    snapshot_id: &ID,
    duration: std::time::Duration,
) {
    let inner = area.inner(Margin::new(1, 1));
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(5),
            Constraint::Length(6),
            Constraint::Length(1),
            Constraint::Length(1),
        ])
        .split(inner);

    let col_label: u16 = 10;
    let avail = inner.width.saturating_sub(col_label + 4);
    let col_w = (avail / 4).max(6);

    let header = Row::new([
        Cell::from("").style(Style::default()),
        Cell::from("new").style(Style::default().fg(theme::THEME.green).bold()),
        Cell::from("changed").style(Style::default().fg(theme::THEME.yellow).bold()),
        Cell::from("deleted").style(Style::default().fg(theme::THEME.red).bold()),
        Cell::from("unchanged").style(Style::default().bold()),
    ]);

    let files_row = Row::new([
        Cell::from("Files").style(Style::default().bold()),
        Cell::from(summary.diff_counts.new_files.to_string())
            .style(Style::default().fg(theme::THEME.green)),
        Cell::from(summary.diff_counts.changed_files.to_string())
            .style(Style::default().fg(theme::THEME.yellow)),
        Cell::from(summary.diff_counts.deleted_files.to_string())
            .style(Style::default().fg(theme::THEME.red)),
        Cell::from(summary.diff_counts.unchanged_files.to_string()),
    ]);

    let dirs_row = Row::new([
        Cell::from("Dirs").style(Style::default().bold()),
        Cell::from(summary.diff_counts.new_dirs.to_string())
            .style(Style::default().fg(theme::THEME.green)),
        Cell::from(summary.diff_counts.changed_dirs.to_string())
            .style(Style::default().fg(theme::THEME.yellow)),
        Cell::from(summary.diff_counts.deleted_dirs.to_string())
            .style(Style::default().fg(theme::THEME.red)),
        Cell::from(summary.diff_counts.unchanged_dirs.to_string()),
    ]);

    let table = Table::new(
        [header, files_row, dirs_row],
        [
            Constraint::Length(col_label),
            Constraint::Length(col_w),
            Constraint::Length(col_w),
            Constraint::Length(col_w),
            Constraint::Length(col_w),
        ],
    )
    .block(theme::block("Changes since parent snapshot"));
    frame.render_widget(table, chunks[0]);

    let raw_data_str = utils::format_size_binary(summary.raw_bytes, 3);
    let enc_data_str = utils::format_size_binary(summary.encoded_bytes, 3);
    let raw_meta_str = utils::format_size_binary(summary.meta_raw_bytes, 3);
    let enc_meta_str = utils::format_size_binary(summary.meta_encoded_bytes, 3);
    let raw_total_str = utils::format_size_binary(summary.total_raw_bytes, 3);
    let enc_total_str = utils::format_size_binary(summary.total_encoded_bytes, 3);

    let max_val_len = raw_data_str
        .len()
        .max(enc_data_str.len())
        .max(raw_meta_str.len())
        .max(enc_meta_str.len())
        .max(raw_total_str.len())
        .max(enc_total_str.len());
    let data_col_w: u16 = (max_val_len + 2) as u16;
    let label_w: u16 = 10;

    let data_header = Row::new([
        Cell::from(""),
        Cell::from("Raw").style(Style::default().fg(theme::THEME.yellow).bold()),
        Cell::from("Compressed").style(Style::default().fg(theme::THEME.green).bold()),
    ]);

    let data_rows = [
        Row::new([
            Cell::from("Data").style(Style::default().bold()),
            Cell::from(raw_data_str).style(Style::default().fg(theme::THEME.yellow)),
            Cell::from(enc_data_str).style(Style::default().fg(theme::THEME.green)),
        ]),
        Row::new([
            Cell::from("Metadata").style(Style::default().bold()),
            Cell::from(raw_meta_str).style(Style::default().fg(theme::THEME.yellow)),
            Cell::from(enc_meta_str).style(Style::default().fg(theme::THEME.green)),
        ]),
        Row::new([
            Cell::from("Total").style(Style::default().bold()),
            Cell::from(raw_total_str).style(Style::default().fg(theme::THEME.yellow).bold()),
            Cell::from(enc_total_str).style(Style::default().fg(theme::THEME.green).bold()),
        ]),
    ];

    let data_table = Table::new(
        std::iter::once(data_header).chain(data_rows),
        [
            Constraint::Length(label_w),
            Constraint::Length(data_col_w),
            Constraint::Length(data_col_w),
        ],
    )
    .block(theme::block("This snapshot added"));
    frame.render_widget(data_table, chunks[1]);

    let stats_line = Line::from(vec![Span::raw(format!(
        "Processed {} and {} items in {}",
        utils::format_size_binary(summary.processed_bytes, 3),
        summary.processed_items_count,
        utils::pretty_print_duration(duration),
    ))]);
    frame.render_widget(Paragraph::new(stats_line), chunks[2]);

    let id_line = Line::from(vec![
        Span::styled("Snapshot ID: ", Style::default().bold()),
        Span::styled(
            snapshot_id.to_short_hex(SHORT_SNAPSHOT_ID_LEN),
            theme::THEME.snap_id,
        ),
    ]);
    frame.render_widget(Paragraph::new(id_line), chunks[3]);
}

fn render_footer(frame: &mut Frame, area: Rect, summary: &Option<SummaryResult>) {
    let footer = match summary {
        Some(SummaryResult::Success { .. })
        | Some(SummaryResult::Cancelled)
        | Some(SummaryResult::Error(_))
        | Some(SummaryResult::NoChanges) => {
            theme::key_hint_footer(&[("Enter", "done"), ("Esc", "done"), ("q", "quit")])
        }
        _ => theme::key_hint_footer(&[("q", "quit")]),
    };
    frame.render_widget(Paragraph::new(footer), area);
}

pub fn handle_summary_key(key: KeyCode) -> SummaryAction {
    match key {
        KeyCode::Char('q') => SummaryAction::Quit,
        KeyCode::Enter | KeyCode::Esc => SummaryAction::Done,
        _ => SummaryAction::None,
    }
}
