// mapache is an incremental backup tool
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

use indicatif::ProgressDrawTarget;

use crate::global::global_opts;

pub mod cli;
pub mod restore_progress;
pub mod snapshot_progress;
pub mod table;

// Progress UI parameters
pub(crate) const PROGRESS_REFRESH_RATE_HZ: u8 = 30;
pub(crate) const SPINNER_TICK_CHARS: &str = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏";

pub(crate) fn default_bar_draw_target() -> ProgressDrawTarget {
    let verbosity = global_opts().as_ref().unwrap().verbosity;
    if verbosity > 0 {
        ProgressDrawTarget::stderr_with_hz(PROGRESS_REFRESH_RATE_HZ)
    } else {
        ProgressDrawTarget::hidden()
    }
}
