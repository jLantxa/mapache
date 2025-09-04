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

use anyhow::{Context, Result};
use clap::Args;
use colored::Colorize;

use crate::{
    backend::new_backend_with_prompt,
    commands::{cleanup::CleanupHandler, parse_tags},
    global::{FileType, ID},
    repository::{
        repo::{RepoConfig, Repository},
        snapshot::{Snapshot, SnapshotStreamer},
    },
    ui::{self, log_snapshots_compact},
    utils::{self, size},
};

use super::GlobalArgs;

#[derive(Args, Debug)]
#[clap(about = "Show all snapshots present in the repository")]
pub struct CmdArgs {
    /// Show a single snapshot with a given ID
    #[arg(value_parser)]
    pub snapshot: Option<String>,

    /// Show a compact list of snapshots
    #[arg(short, long)]
    pub compact: bool,

    /// Only consider snapshots with tags: tag[,tag,...]
    #[arg(long = "tags", value_parser)]
    pub tags_str: Option<String>,
}

pub fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let auth = utils::get_auth_from_file(&global_args.auth_file)?;
    let backend = new_backend_with_prompt(global_args, false)?;

    let config = RepoConfig {
        pack_size: (global_args.pack_size_mib * size::MiB as f32) as u64,
    };
    let (repo, _, lock_handle) = Repository::try_open_with_lock(
        auth.as_ref(),
        global_args.key.as_ref(),
        backend,
        config,
        false,
    )?;

    let lock_handle_clone = lock_handle.clone();
    let _cleanup_handler = CleanupHandler::new(move || {
        lock_handle_clone.write().unlock();
    })?;

    let mut snapshots_sorted: Vec<(ID, Snapshot)> = match &args.snapshot {
        None => SnapshotStreamer::new(repo.clone())?.collect(),
        Some(prefix) => {
            let (id, _) = repo
                .find(FileType::Snapshot, prefix)
                .with_context(|| format!("Could not find snapshot {prefix}"))?;
            let snapshot = repo.load_snapshot(&id)?;
            vec![(id, snapshot)]
        }
    };

    if let Some(tags_str) = &args.tags_str {
        let tags = parse_tags(Some(tags_str));
        snapshots_sorted.retain(|(_id, sn)| sn.has_tags(&tags));
    }
    snapshots_sorted.sort_unstable_by_key(|(_id, snapshot)| snapshot.timestamp);

    if snapshots_sorted.is_empty() {
        ui::cli::log!("No snapshots found");
        return Ok(());
    }

    ui::cli::log!();
    if args.compact {
        log_snapshots_compact(&snapshots_sorted);
    } else {
        log_snapshots_full(&snapshots_sorted);
    }

    ui::cli::log!("{} snapshots", snapshots_sorted.len());

    Ok(())
}

fn log_snapshots_full(snapshots: &[(ID, Snapshot)]) {
    let mut peekable_snapshots = snapshots.iter().peekable();
    while let Some((id, snapshot)) = peekable_snapshots.next() {
        ui::cli::log!("{}", id.to_hex().bold().yellow());
        ui::cli::log!(
            "{} {}",
            "Date:".bold(),
            utils::pretty_print_timestamp(&snapshot.timestamp)
        );
        ui::cli::log!(
            "{} {}",
            "Size:".bold(),
            utils::format_size(snapshot.summary.processed_bytes, 3)
        );
        ui::cli::log!("{} {}", "Root:".bold(), &snapshot.root.display());

        if let Some(hostname) = &snapshot.hostname {
            ui::cli::log!("{} {}", "Host:".bold(), hostname);
        }

        if let Some(username) = &snapshot.username {
            ui::cli::log!("{} {}", "User:".bold(), username);
        }

        if !snapshot.tags.is_empty() {
            ui::cli::log!(
                "{} {}",
                "Tags:".bold(),
                snapshot
                    .tags
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }

        ui::cli::log!();
        ui::cli::log!("{}", "Paths:".bold());
        for path in &snapshot.paths {
            // This should work, since all paths have the common root
            let relative_path = path
                .strip_prefix(&snapshot.root)
                .expect("Could not strip snapshot root from path");
            ui::cli::log!("  {}", relative_path.display());
        }

        if let Some(description) = &snapshot.description {
            ui::cli::log!();
            ui::cli::log!("{}", description);
        }

        if peekable_snapshots.peek().is_some() {
            ui::cli::log!();
        }
    }

    ui::cli::log!();
}
