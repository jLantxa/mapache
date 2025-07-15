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
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Result, bail};
use clap::Args;
use colored::Colorize;
use indicatif::{ProgressBar, ProgressStyle};

use crate::{
    backend::new_backend_with_prompt,
    commands::{GlobalArgs, UseSnapshot, find_use_snapshot},
    global::defaults::SHORT_SNAPSHOT_ID_LEN,
    repository::{self, streamers::SerializedNodeStreamer},
    restorer::{self, Resolution, Restorer},
    ui::{
        self, PROGRESS_REFRESH_RATE_HZ, SPINNER_TICK_CHARS, cli, default_bar_draw_target,
        restore_progress::RestoreProgressReporter,
    },
    utils::{self, format_size},
};

impl std::fmt::Display for Resolution {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Resolution::Skip => write!(f, "skip"),
            Resolution::Overwrite => write!(f, "overwrite"),
            Resolution::Fail => write!(f, "fail"),
        }
    }
}

#[derive(Args, Debug)]
#[clap(
    about = "Restore a snapshot in a target path",
    long_about = "Restore a snapshot in a target path. Running this command in\
    --dry-run mode simulates the restoration of a snapshot, and can be used to\
    detect errors before running the actual restore."
)]
pub struct CmdArgs {
    /// The ID of the snapshot to restore, or 'latest' to restore the most recent snapshot saved.
    #[arg(value_parser = clap::value_parser!(UseSnapshot), default_value_t=UseSnapshot::Latest)]
    pub snapshot: UseSnapshot,

    /// A path where the files will be restored.
    #[clap(long, required = true)]
    pub target: PathBuf,

    /// A list of paths to restore: path[,path,...]. Can be used multiple times.
    #[clap(long, value_delimiter = ',')]
    pub include: Option<Vec<PathBuf>>,

    /// A list of paths to exclude: path[,path,...]. Can be used multiple times.
    #[clap(long, value_delimiter = ',')]
    pub exclude: Option<Vec<PathBuf>>,

    /// Strip the longest common prefix from all restored routes.
    #[clap(long, value_parser, default_value_t = false)]
    pub strip_prefix: bool,

    /// Method for conflict resolution in case a file or directory already exists in the target location.
    ///
    /// skip: Skips restoring the conflicting item.
    /// overwrite: Overwrites the item in the target location.
    /// fail: Terminates the command with an error.
    #[clap(long, default_value_t=Resolution::Fail)]
    pub resolution: Resolution,

    /// Skip verification of data
    #[clap(long = "no-verify", value_parser, default_value_t = false)]
    pub no_verify: bool,

    /// Dry run
    #[clap(long, default_value_t = false)]
    pub dry_run: bool,
}

pub fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let pass = utils::get_password_from_file(&global_args.password_file)?;
    let backend = new_backend_with_prompt(global_args, args.dry_run)?;
    let (repo, _) = repository::try_open(pass, global_args.key.as_ref(), backend)?;

    let (snapshot_id, snapshot) = match find_use_snapshot(repo.clone(), &args.snapshot) {
        Ok(Some((id, snap))) => (id, snap),
        Ok(None) | Err(_) => bail!("Snapshot not found"),
    };

    let common_prefix: Option<PathBuf> = if args.strip_prefix {
        args.include
            .as_ref()
            .map(|includes| utils::calculate_lcp(includes, false))
    } else {
        None
    };

    ui::cli::log!(
        "Restoring snapshot {}",
        snapshot_id
            .to_short_hex(SHORT_SNAPSHOT_ID_LEN)
            .bold()
            .yellow()
    );

    // Scan snapshot tree
    let mut total_bytes: u64 = 0;
    let mut num_files = 0;
    let mut num_dirs = 0;
    let scan_node_streamer = SerializedNodeStreamer::new(
        repo.clone(),
        Some(snapshot.tree.clone()),
        PathBuf::new(),
        args.include.clone(),
        args.exclude.clone(),
    )?;
    let spinner = ProgressBar::new_spinner();
    spinner.set_draw_target(default_bar_draw_target());
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.cyan} Scanning snapshot tree ({msg})")
            .unwrap()
            .tick_chars(SPINNER_TICK_CHARS),
    );
    spinner.enable_steady_tick(Duration::from_millis(
        (1000.0_f32 / PROGRESS_REFRESH_RATE_HZ as f32) as u64,
    ));
    for (_path, stream_node) in scan_node_streamer.flatten() {
        let node = stream_node.node;

        if node.is_dir() {
            num_dirs += 1;
        } else if node.is_file() {
            num_files += 1;
            total_bytes += node.metadata.size;
            spinner.set_message(format_size(total_bytes, 3));
        }

        spinner.set_message(format!(
            "{} files, {} dirs, {}",
            num_files,
            num_dirs,
            format_size(total_bytes, 3)
        ));
    }
    spinner.finish_and_clear();
    ui::cli::log!(
        "{} {} files, {} directories, {}\n",
        "To restore:".bold().cyan(),
        num_files,
        num_dirs,
        utils::format_size(total_bytes, 3),
    );

    const NUM_SHOWN_PROCESSING_ITEMS: usize = 1;
    let num_expected_items = snapshot.summary.processed_items_count;
    let progress_reporter = Arc::new(RestoreProgressReporter::new(
        num_expected_items,
        total_bytes,
        NUM_SHOWN_PROCESSING_ITEMS,
    ));

    let start = Instant::now();

    Restorer::restore(
        repo.clone(),
        &snapshot,
        &args.target,
        args.include.clone(),
        args.exclude.clone(),
        restorer::Options {
            dry_run: args.dry_run,
            resolution: args.resolution.clone(),
            strip_prefix: common_prefix,
            verify: !args.no_verify,
        },
        progress_reporter.clone(),
    )?;

    progress_reporter.finalize();

    cli::log!(
        "Finished in {}",
        utils::pretty_print_duration(start.elapsed())
    );

    Ok(())
}
