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
use std::time::Instant;
use std::{path::PathBuf, sync::Arc};

use anyhow::{Result, bail};
use clap::{ArgGroup, Args};
use colored::Colorize;

use crate::{
    archiver::tree_serializer::{self, init_pending_trees},
    backend::new_backend_with_prompt,
    commands::{
        EMPTY_TAG_MARK, GlobalArgs, UseSnapshot, cleanup::CleanupHandler, find_use_snapshot,
        parse_tags,
    },
    fs::tree::SerializedNodeStreamer,
    global::{FileType, ID, defaults::SHORT_SNAPSHOT_ID_LEN},
    repository::{
        repo::{RepoConfig, Repository},
        snapshot::{Snapshot, SnapshotStreamer},
    },
    ui::{self},
    utils::{self, size},
};

#[derive(Args, Debug)]
#[clap(group = ArgGroup::new("snapshot_group").multiple(false))]
#[clap(group = ArgGroup::new("tags_group").multiple(false))]
#[clap(group = ArgGroup::new("description_group").multiple(false))]
#[clap(about = "Amend an existing snapshot")]
pub struct CmdArgs {
    /// The ID of the snapshot to amend, or 'latest' to amend the most recent snapshot.
    #[arg(value_parser = clap::value_parser!(UseSnapshot), default_value_t=UseSnapshot::Latest, group = "snapshot_group")]
    pub snapshot: UseSnapshot,

    /// Apply changes to all snapshots
    #[arg(short, long, group = "snapshot_group")]
    pub all: bool,

    /// Keep the old snapshot
    #[clap(long = "keep-old", value_parser, default_value_t = false)]
    pub keep_old: bool,

    /// Tags (comma-separated)
    #[clap(long = "tags", value_parser, group = "tags_group")]
    pub tags_str: Option<String>,

    /// Clear tags
    #[clap(long, value_parser, group = "tags_group")]
    pub clear_tags: bool,

    /// Snapshot description
    #[clap(long, value_parser, group = "description_group")]
    pub description: Option<String>,

    /// Clear description
    #[clap(long, value_parser, group = "description_group")]
    pub clear_description: bool,

    /// List of paths to exclude from the backup
    #[clap(long, value_parser, required = false)]
    pub exclude: Option<Vec<PathBuf>>,
}

pub fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let pass = utils::get_password_from_file(&global_args.password_file)?;
    let backend = new_backend_with_prompt(global_args, false)?;

    let config = RepoConfig {
        pack_size: (global_args.pack_size_mib * size::MiB as f32) as u64,
    };
    let (repo, _, lock_handle) =
        Repository::try_open_with_lock(pass, global_args.key.as_ref(), backend, config, true)?;

    let lock_handle_clone = lock_handle.clone();
    let _cleanup_handler = CleanupHandler::new(move || {
        lock_handle_clone.write().unlock();
    })?;

    let start = Instant::now();

    let mut snapshots: Vec<(ID, Snapshot)> = Vec::new();

    if args.all {
        let snapshot_streamer = SnapshotStreamer::new(repo.clone())?;
        let mut all_snapshots: Vec<(ID, Snapshot)> = snapshot_streamer.collect();
        snapshots.append(&mut all_snapshots);
    } else {
        match find_use_snapshot(repo.clone(), &args.snapshot) {
            Ok(Some((id, snap))) => snapshots.push((id, snap)),
            Ok(None) | Err(_) => bail!("Snapshot not found"),
        }
    }

    let num_snapshots = snapshots.len();
    for (i, (id, snapshot)) in snapshots.iter_mut().rev().enumerate() {
        let amend_str = format!(
            "Amending snapshot {}",
            id.to_short_hex(SHORT_SNAPSHOT_ID_LEN).bold().red()
        );
        if args.all {
            ui::cli::log!("{} ({}/{})", amend_str, i + 1, num_snapshots);
        } else {
            ui::cli::log!("{} ", amend_str);
        }

        amend(repo.clone(), id, snapshot, args)?;
        ui::cli::log!();
    }

    ui::cli::log!(
        "Finished in {}",
        utils::pretty_print_duration(start.elapsed())
    );

    Ok(())
}

fn amend(
    repo: Arc<Repository>,
    origin_snapshot_id: &ID,
    snapshot: &mut Snapshot,
    args: &CmdArgs,
) -> Result<()> {
    let (mut raw, mut encoded) = (0, 0);

    snapshot.summary.amends = Some(origin_snapshot_id.clone());

    if args.description.is_some() {
        snapshot.description = args.description.clone();
    } else if args.clear_description {
        snapshot.description = None;
    }

    if let Some(a_tag_str) = &args.tags_str {
        let mut tags: BTreeSet<String> = parse_tags(Some(a_tag_str));
        tags.retain(|tag| tag != EMPTY_TAG_MARK);
        snapshot.tags = tags.clone();
    } else if args.clear_tags {
        snapshot.tags = BTreeSet::new();
    }

    let origin_processed_bytes = snapshot.summary.processed_bytes;

    if args.exclude.is_some() {
        rewrite_snapshot_tree(repo.clone(), snapshot, args.exclude.clone())?;
    }

    // Save the amended snapshot and delete the old snapshot file
    let (new_id, raw_meta, encoded_meta) = repo.save_file(
        FileType::Snapshot,
        serde_json::to_string(&snapshot)?.as_bytes(),
    )?;
    raw += raw_meta;
    encoded += encoded_meta;

    // Delete the old snapshot ID if it changed
    // Note: To protect the repo from interruptions, we delete the snapshot only
    // after the new one is saved.
    if new_id != *origin_snapshot_id {
        if !args.keep_old {
            repo.delete_file(FileType::Snapshot, origin_snapshot_id)?;
        }

        ui::cli::log!(
            "New snapshot ID   {}",
            new_id.to_short_hex(SHORT_SNAPSHOT_ID_LEN).bold().green()
        );
        ui::cli::log!(
            "Added to the repository: {} {}",
            utils::format_size(raw, 3).bold().yellow(),
            format!("({} compressed)", utils::format_size(encoded, 3))
                .bold()
                .green()
        );
        ui::cli::log!(
            "Snapshot size: {} -> {}",
            utils::format_size(origin_processed_bytes, 3)
                .yellow()
                .bold(),
            utils::format_size(snapshot.summary.processed_bytes, 3)
                .green()
                .bold()
        );
    } else {
        ui::cli::log!("No changes");
    }

    Ok(())
}

fn rewrite_snapshot_tree(
    repo: Arc<Repository>,
    snapshot: &mut Snapshot,
    excludes: Option<Vec<PathBuf>>,
) -> Result<(u64, u64)> {
    let (mut raw_bytes, mut encoded_bytes) = (0, 0);

    // Cannonicalize the exclude paths and filter the source paths using the excludes
    // This is a simulated cannonical path, since we don't refer to a path in the host,
    // but rather a relative path in the snapshot tree. We can just append the relative path
    // to the snapshot root.
    let cannonical_excludes: Option<Vec<PathBuf>> = if let Some(exclude_paths) = &excludes {
        let mut canonicalized_vec = Vec::new();
        for path in exclude_paths {
            canonicalized_vec.push(snapshot.root.join(path));
        }
        Some(canonicalized_vec)
    } else {
        None
    };

    let mut paths = snapshot.paths.clone();
    paths.retain(|p| utils::filter_path(p, None, cannonical_excludes.as_ref()));

    let mut final_root_tree_id: Option<ID> = None;
    let mut pending_trees = init_pending_trees(&snapshot.root, &paths);

    let node_streamer = SerializedNodeStreamer::new(
        repo.clone(),
        Some(snapshot.tree.clone()),
        snapshot.root.clone(),
        None,
        cannonical_excludes.clone(),
    )?;

    snapshot.summary.processed_items_count = 0;
    snapshot.summary.processed_bytes = 0;

    repo.init_pack_saver(1);
    for (path, stream_node) in node_streamer.flatten() {
        snapshot.summary.processed_items_count += 1;
        if !stream_node.node.is_dir() {
            snapshot.summary.processed_bytes += stream_node.node.metadata.size;
        }

        // The path is not excluded, so we add the node to the pending trees map.
        let (raw, encoded) = tree_serializer::handle_processed_item(
            (path.clone(), stream_node),
            repo.as_ref(),
            &mut pending_trees,
            &mut final_root_tree_id,
            &snapshot.root,
        )?;

        raw_bytes += raw;
        encoded_bytes += encoded;
    }

    let (raw_meta, encoded_meta) = repo.flush()?;
    raw_bytes += raw_meta;
    encoded_bytes += encoded_meta;

    // Increase meta counters in snapshot summary
    snapshot.summary.meta_raw_bytes += raw_bytes;
    snapshot.summary.meta_encoded_bytes += encoded_bytes;
    snapshot.summary.total_raw_bytes += raw_bytes;
    snapshot.summary.total_encoded_bytes += encoded_bytes;

    repo.finalize_pack_saver();

    match final_root_tree_id {
        Some(amended_tree_id) => snapshot.tree = amended_tree_id,
        None => bail!("Failed to serialize new snapshot tree"),
    }

    Ok((raw_bytes, encoded_bytes))
}
