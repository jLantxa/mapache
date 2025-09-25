// mapache is a secure, de-duplicating, incremental backup tool.
// Copyright (C) 2025  Javier Lancha Vázquez <javier.lancha@gmail.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use std::{path::PathBuf, sync::LazyLock};

use colored::Colorize;
use indicatif::ProgressDrawTarget;

use crate::{
    global::{self, ID, global_opts},
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

// Progress UI parameters
pub(crate) const PROGRESS_REFRESH_RATE_HZ: u8 = 30;
pub(crate) const SPINNER_TICK_CHARS: &str = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏";

pub(crate) const MAX_PATH_DISPLAY_LEN: usize = 100;
pub(crate) static EMPTY_PATHBUF: LazyLock<PathBuf> = LazyLock::new(PathBuf::new);

pub(crate) fn default_bar_draw_target() -> ProgressDrawTarget {
    let verbosity = global_opts().as_ref().unwrap().verbosity;
    if verbosity > 0 {
        ProgressDrawTarget::stderr_with_hz(PROGRESS_REFRESH_RATE_HZ)
    } else {
        ProgressDrawTarget::hidden()
    }
}

pub fn log_snapshots_compact(snapshots: &Vec<(ID, Snapshot)>) {
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

    for (id, snapshot) in snapshots {
        table.add_row(vec![
            id.to_short_hex(global::defaults::SHORT_SNAPSHOT_ID_LEN)
                .bold()
                .yellow()
                .to_string(),
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
