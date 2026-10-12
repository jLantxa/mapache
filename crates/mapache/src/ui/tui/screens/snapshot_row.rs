use ratatui::text::Span;

use crate::{
    common::defaults::SHORT_SNAPSHOT_ID_LEN, repository::snapshot::SnapshotEntry, ui::tui::theme,
    utils,
};

/// The short-ID, date and host spans shared by every snapshot table, so those
/// columns look identical wherever snapshots are listed. Screens prepend their
/// own leading marker (active dot, diff source, selection checkbox) and append
/// size or tags as space allows.
pub(crate) fn snapshot_cells(entry: &SnapshotEntry) -> [Span<'static>; 3] {
    [
        Span::styled(
            entry.id.to_short_hex(SHORT_SNAPSHOT_ID_LEN),
            theme::THEME.snap_id,
        ),
        Span::styled(
            utils::pretty_print_timestamp(&entry.snapshot.timestamp, None),
            theme::THEME.snap_date,
        ),
        Span::styled(
            entry
                .snapshot
                .hostname
                .as_deref()
                .unwrap_or_default()
                .to_owned(),
            theme::THEME.snap_host,
        ),
    ]
}
