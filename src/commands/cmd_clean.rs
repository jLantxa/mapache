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

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Result;
use clap::Args;
use colored::Colorize;
use indicatif::{ProgressBar, ProgressStyle};

use crate::{
    backend::new_backend_with_prompt,
    commands::GlobalArgs,
    global::defaults::{DEFAULT_GC_TOLERANCE, SHORT_REPO_ID_LEN},
    repository::{
        self, RepositoryBackend,
        gc::{self},
        verify::verify_snapshot_links,
    },
    ui::{
        self, PROGRESS_REFRESH_RATE_HZ, SPINNER_TICK_CHARS,
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

    /// Verify that all referenced IDs are stored in the index without reading the data.
    #[clap(long, default_value_t = false)]
    pub verify: bool,

    /// Dry run. Displays what this command would do without
    /// making changes to the repository.
    #[clap(long, default_value_t = false)]
    pub dry_run: bool,
}

pub fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let pass = utils::get_password_from_file(&global_args.password_file)?;
    let backend = new_backend_with_prompt(global_args, args.dry_run)?;
    let (repo, _) = repository::try_open(pass, global_args.key.as_ref(), backend)?;

    run_with_repo(global_args, args, repo)
}

/// Run the command with an initialized repository object.
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
        "Total packs".bold().to_string(),
        plan.total_packs.to_string(),
    ]);
    plan_table.add_row(vec![
        "Referenced blobs".bold().to_string(),
        plan.referenced_blobs.len().to_string(),
    ]);
    plan_table.add_row(vec![
        "Referenced packs".bold().to_string(),
        plan.referenced_packs.len().to_string(),
    ]);
    plan_table.add_row(vec![
        "Unused packs".bold().to_string(),
        plan.unused_packs.len().to_string(),
    ]);
    plan_table.add_row(vec![
        "Packs to repack".bold().to_string(),
        plan.obsolete_packs.len().to_string(),
    ]);
    plan_table.add_row(vec![
        "Small packs".bold().to_string(),
        plan.small_packs.len().to_string(),
    ]);
    plan_table.add_row(vec![
        "Tolerated packs".bold().to_string(),
        plan.tolerated_packs.len().to_string(),
    ]);

    ui::cli::log!();
    ui::cli::log!("{}", "Plan summary:".bold());
    ui::cli::log!("{}", plan_table.render());

    if args.dry_run {
        ui::cli::log!("{} GC not executed", "[DRY RUN]".bold().purple());
    } else {
        plan.execute()?;

        if args.verify {
            ui::cli::log!();
            verify_snapshots(repo.clone())?;
        }

        ui::cli::log!();
        ui::cli::log!(
            "Finished in {}",
            utils::pretty_print_duration(start.elapsed())
        );
    }

    Ok(())
}

fn verify_snapshots(repo: Arc<dyn RepositoryBackend>) -> Result<()> {
    ui::cli::log!("Verifying snapshots...");

    let spinner = ProgressBar::new_spinner();
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.cyan} {msg}")
            .unwrap()
            .tick_chars(SPINNER_TICK_CHARS),
    );
    spinner.enable_steady_tick(Duration::from_millis(
        (1000.0_f32 / PROGRESS_REFRESH_RATE_HZ as f32) as u64,
    ));

    for id in repo.list_snapshot_ids()? {
        spinner.set_message(format!("{}", id.to_short_hex(SHORT_REPO_ID_LEN).yellow()));
        verify_snapshot_links(repo.clone(), &id)?;
    }

    spinner.finish_and_clear();
    ui::cli::log!("{}\n", "[OK]".bold().green());

    Ok(())
}
