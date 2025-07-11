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

use std::{sync::Arc, time::Instant};

use anyhow::Result;
use clap::Args;
use colored::Colorize;

use crate::{
    backend::{make_dry_backend, new_backend_with_prompt},
    commands::GlobalArgs,
    global::defaults::DEFAULT_GC_TOLERANCE,
    repository::{
        self, RepositoryBackend,
        gc::{self},
    },
    ui::{
        self,
        table::{Alignment, Table},
    },
    utils::{self},
};

#[derive(Args, Debug)]
#[clap(
    about = "Clean up the repository",
    long_about = "Clean up the repository removing obsolete objects and merging pack and index files."
)]
pub struct CmdArgs {
    /// Garbage tolerance. The percentage [0-100] of garbage to tolerate in a
    /// pack file before repacking.
    #[clap(short, long, default_value_t = 100.0 * DEFAULT_GC_TOLERANCE)]
    pub tolerance: f32,

    /// Dry run. Displays what this command would do without
    /// making changes to the repository.
    #[clap(long, default_value_t = false)]
    pub dry_run: bool,
}

pub fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let pass = utils::get_password_from_file(&global_args.password_file)?;
    let backend = new_backend_with_prompt(global_args)?;

    // If dry-run, wrap the backend inside the DryBackend
    let backend = make_dry_backend(backend, args.dry_run);

    let repo: Arc<dyn RepositoryBackend> =
        repository::try_open(pass, global_args.key.as_ref(), backend)?;

    run_with_repo(global_args, args, repo)
}

pub fn run_with_repo(
    _global_args: &GlobalArgs,
    args: &CmdArgs,
    repo: Arc<dyn RepositoryBackend>,
) -> Result<()> {
    let tolerance = args.tolerance.clamp(0.0, 100.0) / 100.0;

    let start = Instant::now();
    ui::cli::log!();

    let plan = gc::scan(repo.clone(), tolerance)?;

    let mut plan_table = Table::new_with_alignments(vec![Alignment::Left, Alignment::Right]);
    plan_table.add_row(vec![
        "Number of packs".bold().to_string(),
        plan.total_packs.to_string(),
    ]);
    plan_table.add_row(vec![
        "Blobs to keep".bold().to_string(),
        plan.referenced_blobs.len().to_string(),
    ]);
    plan_table.add_row(vec![
        "Packs to keep".bold().to_string(),
        plan.referenced_packs.len().to_string(),
    ]);
    plan_table.add_row(vec![
        "Packs to repack".bold().to_string(),
        plan.obsolete_packs.len().to_string(),
    ]);
    plan_table.add_row(vec![
        "Tolerated packs".bold().to_string(),
        plan.tolerated_packs.len().to_string(),
    ]);
    plan_table.add_row(vec![
        "Unused packs".bold().to_string(),
        plan.unused_packs.len().to_string(),
    ]);

    ui::cli::log!();
    ui::cli::log!("{}", "Plan summary:".bold());
    ui::cli::log!("{}", plan_table.render());

    if args.dry_run {
        ui::cli::log!("{} GC not executed", "[DRY RUN]".bold().purple());
    } else {
        plan.execute()?;

        ui::cli::log!();
        ui::cli::log!(
            "Finished in {}",
            utils::pretty_print_duration(start.elapsed())
        );
    }

    Ok(())
}
