use colored::Colorize;
use indicatif::ProgressDrawTarget;

use crate::{
    mapache::{self, global::GlobalOpts},
    repository::snapshot::SnapshotEntryList,
    ui::{
        self,
        table::{Alignment, Table},
    },
    utils,
};

pub(crate) mod bundle;
pub mod cli;
pub(crate) mod debug;
pub(crate) mod json_reporter;
pub(crate) mod restore;
pub(crate) mod snapshot;
pub mod table;

pub(crate) const SPINNER_TICK_CHARS: &str = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏";

/// Returns the default draw target for progress bars, with a preconfigured refresh rate
/// and verbosity.
pub(crate) fn default_bar_draw_target() -> ProgressDrawTarget {
    let verbosity = GlobalOpts::verbosity();
    let refresh_interval = GlobalOpts::progress_refresh_interval();

    if verbosity > 0 {
        ProgressDrawTarget::stderr_with_hz((1.0 / refresh_interval.as_secs_f64()) as u8)
    } else {
        ProgressDrawTarget::hidden()
    }
}

/// Logs a list of snapshots in the form of a compact table.
pub fn log_snapshots_compact(snapshots: &SnapshotEntryList) {
    let mut table = Table::new_with_alignments(vec![
        Alignment::Left,
        Alignment::Left,
        Alignment::Left,
        Alignment::Right,
        Alignment::Left,
    ]);

    table.set_headers(vec![
        "ID".bold().to_string(),
        "Date ▼".bold().to_string(),
        "Host".bold().to_string(),
        "Size".bold().to_string(),
        "Tags".bold().to_string(),
    ]);

    for entry in snapshots {
        let id_str = entry
            .id
            .to_short_hex(mapache::defaults::SHORT_SNAPSHOT_ID_LEN);
        let id_str = if entry.active {
            id_str.bold().yellow().to_string()
        } else {
            (id_str + " (dropped)").bold().dimmed().to_string()
        };

        table.add_row(vec![
            id_str,
            utils::pretty_print_timestamp(&entry.snapshot.timestamp, None),
            entry.snapshot.hostname.clone().unwrap_or_default(),
            utils::format_size_binary(entry.snapshot.size(), 3),
            entry
                .snapshot
                .tags
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join(", "),
        ]);
    }

    ui::cli::log!("{}", table.render());
}
