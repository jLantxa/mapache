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

use std::collections::BTreeMap;
use std::{path::PathBuf, sync::Arc};

use anyhow::{Result, bail};
use clap::{ArgGroup, Args};
use colored::Colorize;

use crate::utils::format_size;
use crate::{
    archiver::tree_serializer,
    backend::new_backend_with_prompt,
    commands::{GlobalArgs, UseSnapshot, find_use_snapshot},
    global::{FileType, ID, SaveID, defaults::SHORT_SNAPSHOT_ID_LEN},
    repository::{self, RepositoryBackend, snapshot::Snapshot, streamers::SerializedNodeStreamer},
    ui, utils,
};

#[derive(Args, Debug)]
#[clap(group = ArgGroup::new("tags_group").multiple(false))]
#[clap(group = ArgGroup::new("description_group").multiple(false))]
pub struct CmdArgs {
    /// The ID of the snapshot to restore, or 'latest' to restore the most recent snapshot saved.
    #[arg(value_parser = clap::value_parser!(UseSnapshot), default_value_t=UseSnapshot::Latest)]
    pub snapshot: UseSnapshot,

    /// Tags (comma-separated)
    #[clap(long, value_delimiter = ',', value_parser, group = "tags_group")]
    pub tags: Option<Vec<String>>,

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
    let backend = new_backend_with_prompt(global_args)?;

    let repo: Arc<dyn RepositoryBackend> =
        repository::try_open(pass, global_args.key.as_ref(), backend)?;

    let (mut raw, mut encoded) = (0, 0);
    repo.init_pack_saver(1);

    let (orig_snapshot_id, mut snapshot) = match find_use_snapshot(repo.clone(), &args.snapshot) {
        Ok(Some((id, snap))) => (id, snap),
        Ok(None) | Err(_) => bail!("Snapshot not found"),
    };

    if args.description.is_some() {
        snapshot.description = args.description.clone();
    } else if args.clear_description {
        snapshot.description = None;
    }

    if let Some(tags) = &args.tags {
        snapshot.tags = tags.clone();
    } else if args.clear_tags {
        snapshot.tags = Vec::new();
    }

    if args.exclude.is_some() {
        rewrite_snapshot_tree(repo.clone(), &mut snapshot, args.exclude.clone())?;
    }

    // Save the amended snapshot and delete the old snapshot file
    let (new_id, raw_meta, encoded_meta) = repo.save_file(
        FileType::Snapshot,
        serde_json::to_string(&snapshot)?.as_bytes(),
        SaveID::CalculateID,
    )?;
    raw += raw_meta;
    encoded += encoded_meta;

    // Delete the old snapshot ID if it changed
    // Note: To protect the repo from interruptions, we delete the snapshot only
    // after the new one is saved.
    if new_id != orig_snapshot_id {
        repo.delete_file(FileType::Snapshot, &orig_snapshot_id)?;
    }

    let (raw_meta, encoded_meta) = repo.flush()?;
    raw += raw_meta;
    encoded += encoded_meta;

    repo.finalize_pack_saver();

    ui::cli::log!(
        "Snapshot {} amended.\nNew snapshot ID {}",
        orig_snapshot_id
            .to_short_hex(SHORT_SNAPSHOT_ID_LEN)
            .bold()
            .red(),
        new_id.to_short_hex(SHORT_SNAPSHOT_ID_LEN).bold().green()
    );
    ui::cli::log!(
        "Added to the repository: {} {}",
        format_size(raw, 3).bold().yellow(),
        format!("({} compressed)", format_size(encoded, 3))
            .bold()
            .green()
    );

    Ok(())
}

fn rewrite_snapshot_tree(
    repo: Arc<dyn RepositoryBackend>,
    snapshot: &mut Snapshot,
    excludes: Option<Vec<PathBuf>>,
) -> Result<(u64, u64)> {
    let (mut raw_bytes, mut encoded_bytes) = (0, 0);

    let node_streamer = SerializedNodeStreamer::new(
        repo.clone(),
        Some(snapshot.tree.clone()),
        PathBuf::new(),
        None,
        excludes.clone(),
    )?;

    let mut final_root_tree_id: Option<ID> = None;
    let mut pending_trees: BTreeMap<PathBuf, tree_serializer::PendingTree> = BTreeMap::new();
    pending_trees.insert(
        PathBuf::new(),
        tree_serializer::PendingTree {
            node: None,
            children: BTreeMap::new(),
            num_expected_children: tree_serializer::ExpectedChildren::Known(snapshot.paths.len()),
        },
    );

    for (path, stream_node) in node_streamer.flatten() {
        // The path is not excluded, so we add the node to the pending trees map.
        let (raw, encoded) = tree_serializer::handle_processed_item(
            (path, stream_node),
            repo.as_ref(),
            &mut pending_trees,
            &mut final_root_tree_id,
            &PathBuf::new(),
        )?;

        raw_bytes += raw;
        encoded_bytes += encoded;
    }

    for ex_path in excludes.unwrap_or_default() {
        // The path must be excluded, so, instead of adding the node to the pending trees map,
        // we decrease the parent's expected children counter by 1.
        let parent_path = utils::extract_parent(&ex_path)
            .expect("Excluded path must have a parent in the snapshot tree");
        pending_trees.entry(parent_path.clone()).and_modify(|p| {
            if let tree_serializer::ExpectedChildren::Known(n) = p.num_expected_children {
                p.num_expected_children = tree_serializer::ExpectedChildren::Known(n - 1);
            }
        });

        let (raw, encoded) = tree_serializer::finalize_if_complete(
            parent_path,
            repo.as_ref(),
            &mut pending_trees,
            &mut final_root_tree_id,
            &PathBuf::new(),
        )?;

        raw_bytes += raw;
        encoded_bytes += encoded;
    }

    match final_root_tree_id {
        Some(amended_tree_id) => snapshot.tree = amended_tree_id,
        None => bail!("Failed to serialize new snapshot tree"),
    }

    Ok((raw_bytes, encoded_bytes))
}
