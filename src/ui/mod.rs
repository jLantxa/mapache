use std::{path::PathBuf, sync::LazyLock};

use colored::Colorize;
use indicatif::ProgressDrawTarget;

use crate::{
    mapache::{self, ID, global::GlobalOpts},
    repository::snapshot::Snapshot,
    ui::{
        self,
        table::{Alignment, Table},
    },
    utils,
};

pub mod cli;
pub mod restore_progress;
pub mod snapshot_progress;
pub mod table;

pub(crate) const SPINNER_TICK_CHARS: &str = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏";
pub(crate) static EMPTY_PATHBUF: LazyLock<PathBuf> = LazyLock::new(PathBuf::new);

pub(crate) fn default_bar_draw_target() -> ProgressDrawTarget {
    let verbosity = GlobalOpts::verbosity();
    let refresh_interval = GlobalOpts::progress_refresh_interval();

    if verbosity > 0 {
        ProgressDrawTarget::stderr_with_hz((1.0 / refresh_interval.as_secs_f64()) as u8)
    } else {
        ProgressDrawTarget::hidden()
    }
}

pub fn log_snapshots_compact(snapshots: &Vec<(ID, Snapshot, bool)>) {
    let mut table = Table::new_with_alignments(vec![
        Alignment::Left,
        Alignment::Right,
        Alignment::Right,
        Alignment::Right,
        Alignment::Right,
    ]);

    table.set_headers(vec![
        "ID".bold().to_string(),
        "Date ▼".bold().to_string(),
        "Host".bold().to_string(),
        "Size".bold().to_string(),
        "Tags".bold().to_string(),
    ]);

    for (id, snapshot, active) in snapshots {
        let id_str = id.to_short_hex(mapache::defaults::SHORT_SNAPSHOT_ID_LEN);
        let id_str = if *active {
            id_str.bold().yellow().to_string()
        } else {
            (id_str + " (dropped)").bold().dimmed().to_string()
        };

        table.add_row(vec![
            id_str,
            utils::pretty_print_timestamp(&snapshot.timestamp),
            snapshot.hostname.clone().unwrap_or_default(),
            utils::format_size(snapshot.size(), 3),
            snapshot
                .tags
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join(", "),
        ]);
    }

    ui::cli::log!("{}", table.render());
}
