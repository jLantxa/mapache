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

use std::{
    path::PathBuf,
    sync::{Arc, atomic::Ordering},
    time::Instant,
};

use anyhow::{Result, bail};
use clap::Args;
use colored::Colorize;
use indicatif::{ProgressBar, ProgressStyle};

use crate::{
    backend::{BackendOptions, new_backend_with_prompt},
    commands::{GlobalArgs, UseSnapshot, cleanup::CleanupHandler, find_use_snapshot},
    fs::{get_absolute_normalized_path, tree::SerializedNodeStreamer},
    mapache::{defaults::SHORT_SNAPSHOT_ID_LEN, global::GlobalOpts},
    repository::{
        repo::{RepoConfig, Repository},
        verify::verify_snapshot_links,
    },
    restorer::{self, RestoreOptions, Strategy},
    ui::{
        self, SPINNER_TICK_CHARS, default_bar_draw_target,
        restore_progress::RestoreProgressReporter,
    },
    utils::{self, format_size, size},
};

impl std::fmt::Display for Strategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Strategy::Overwrite => write!(f, "overwrite"),
            Strategy::Skip => write!(f, "skip"),
            Strategy::Newer => write!(f, "newer"),
            Strategy::Fail => write!(f, "fail"),
        }
    }
}

#[derive(Args, Debug)]
#[clap(
    about = "Restore a snapshot in a target path",
    long_about = "Restore a snapshot in a target path. Running this command in \
    --dry-run mode simulates the restoration of a snapshot, and can be used to \
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
    /// fail: Terminates the command with an error.
    /// skip: Skips restoring and keeps the local item.
    /// overwrite: Overwrites the item in the target location with the node from the snapshot.
    /// newer: Keeps the item with the more recent modified time.
    #[clap(long = "strategy", default_value_t=Strategy::Fail)]
    pub strategy: Strategy,

    /// Delete files in the target directory that are not present in the snapshot.
    /// Use with caution.
    #[clap(long, value_parser, default_value_t = false)]
    pub delete: bool,

    /// Quit immediately if a restore error occurs
    #[clap(long, default_value_t = false)]
    pub quit_on_error: bool,

    /// Skip verification of data
    #[clap(long = "no-verify", value_parser, default_value_t = false)]
    pub no_verify: bool,

    /// Dry run
    #[clap(long, default_value_t = false)]
    pub dry_run: bool,
}

pub fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let auth = utils::get_auth_from_file(&global_args.auth_file)?;
    let backend = new_backend_with_prompt(BackendOptions {
        repo_path: global_args.repo.clone(),
        ssh_pubkey: global_args.ssh_pubkey.clone(),
        ssh_privatekey: global_args.ssh_privatekey.clone(),
        dry_backend: args.dry_run,
    })?;

    let config = RepoConfig {
        pack_size: (global_args.pack_size_mib * size::MiB as f32) as u64,
    };
    let (repo, _, lock_handle) = Repository::try_open_with_lock(
        auth.as_ref(),
        global_args.key.as_ref(),
        backend.clone(),
        config,
        false,
    )?;

    let lock_handle_clone = lock_handle.clone();
    let _cleanup_handler = CleanupHandler::new(move || {
        lock_handle_clone.write().unlock();
    })?;

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

    if !args.no_verify {
        ui::cli::log!("Verifying snapshot links...");
        verify_snapshot_links(repo.clone(), &snapshot_id)?;
        ui::cli::log!("{}\n", "[OK]".bold().green());
    }

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
    let mut num_expected_items = 0;
    let scan_node_streamer = SerializedNodeStreamer::new(
        repo.clone(),
        Some(snapshot.tree),
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
    spinner.enable_steady_tick(GlobalOpts::progress_refresh_interval());

    for (_path, stream_node) in scan_node_streamer.flatten() {
        let node = stream_node.node;
        num_expected_items += 1;

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
    let progress_reporter = Arc::new(RestoreProgressReporter::new(
        num_expected_items,
        total_bytes,
        NUM_SHOWN_PROCESSING_ITEMS,
    ));

    let abs_normalized_target = get_absolute_normalized_path(&args.target)?;

    let start = Instant::now();

    restorer::restore(
        repo.clone(),
        &snapshot,
        &abs_normalized_target,
        args.include.clone(),
        args.exclude.clone(),
        RestoreOptions {
            dry_run: args.dry_run,
            strategy: args.strategy.clone(),
            quit_on_error: args.quit_on_error,
            strip_prefix: common_prefix,
        },
        progress_reporter.clone(),
    )?;

    progress_reporter.finalize();
    let error_count = progress_reporter.error_counter.load(Ordering::Relaxed);

    if args.delete {
        // Delete local nodes not present in the snapshot tree
        restorer::sync::delete_nodes(
            repo,
            abs_normalized_target.clone(),
            &snapshot.tree,
            args.include.clone(),
            args.exclude.clone(),
            args.dry_run,
        )?;
        ui::cli::log!();
    }

    ui::cli::log!(
        "Finished in {} with {}",
        utils::pretty_print_duration(start.elapsed(),),
        utils::format_count(error_count, "error", "errors")
    );

    Ok(())
}
