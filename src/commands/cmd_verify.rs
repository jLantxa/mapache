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

use std::collections::BTreeSet;

use anyhow::Result;
use clap::Args;
use colored::Colorize;

use crate::{
    backend::new_backend_with_prompt,
    commands::GlobalArgs,
    global::defaults::SHORT_SNAPSHOT_ID_LEN,
    repository::{self, snapshot::SnapshotStreamer, verify::verify_snapshot},
    ui, utils,
};

#[derive(Args, Debug)]
#[clap(
    about = "Verify the integrity of the data stored in the repository",
    long_about = "Verify the integrity of the data stored in the repository, ensuring that all data\
                  associated to a any active snapshots are valid and reachable. This guarantees\
                  that any active snapshot can be restored."
)]
pub struct CmdArgs {}

pub fn run(global_args: &GlobalArgs, _args: &CmdArgs) -> Result<()> {
    let pass = utils::get_password_from_file(&global_args.password_file)?;
    let backend = new_backend_with_prompt(global_args)?;
    let (repo, _) = repository::try_open(pass, global_args.key.as_ref(), backend)?;

    let snapshot_streamer = SnapshotStreamer::new(repo.clone())?;
    let mut visited_blobs = BTreeSet::new();

    let mut snapshot_counter = 0;
    let mut ok_counter = 0;
    let mut error_counter = 0;
    for (snapshot_id, _snapshot) in snapshot_streamer {
        ui::cli::log!(
            "Verifying snapshot {}",
            snapshot_id
                .to_short_hex(SHORT_SNAPSHOT_ID_LEN)
                .bold()
                .yellow()
        );

        match verify_snapshot(repo.clone(), &snapshot_id, &mut visited_blobs) {
            Ok(_) => {
                ui::cli::log!("{}\n", "[OK]".bold().green());
                ok_counter += 1;
            }
            Err(e) => {
                ui::cli::log!("{} {}\n", "[ERROR]".bold().red(), e.to_string());
                error_counter += 1
            }
        }
        snapshot_counter += 1;
    }

    ui::cli::log!();
    ui::cli::log!("{} snapshots verified", snapshot_counter);
    if ok_counter > 0 {
        ui::cli::log!("{} {}", ok_counter, "[OK]".bold().green());
    }
    if error_counter > 0 {
        ui::cli::log!("{} {}", error_counter, "[ERROR]".bold().red());
    }

    Ok(())
}
