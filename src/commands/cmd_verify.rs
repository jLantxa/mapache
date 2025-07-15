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

use std::{collections::BTreeSet, path::PathBuf, sync::Arc};

use anyhow::{Result, bail};
use clap::Args;
use colored::Colorize;
use indicatif::{ProgressBar, ProgressState, ProgressStyle};

use crate::{
    backend::new_backend_with_prompt,
    commands::GlobalArgs,
    global::{ID, defaults::SHORT_SNAPSHOT_ID_LEN},
    repository::{
        self, RepositoryBackend,
        snapshot::SnapshotStreamer,
        streamers::SerializedNodeStreamer,
        tree::NodeType,
        verify::{verify_blob, verify_pack},
    },
    ui::{self, default_bar_draw_target},
    utils,
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
    let backend = new_backend_with_prompt(global_args, false)?;
    let (repo, secure_storage) = repository::try_open(pass, global_args.key.as_ref(), backend)?;

    let snapshot_streamer = SnapshotStreamer::new(repo.clone())?;
    let mut visited_blobs = BTreeSet::new();

    let packs = repo.list_objects()?;

    let bar = ProgressBar::new(packs.len() as u64);
    bar.set_draw_target(default_bar_draw_target());
    bar.set_style(
        ProgressStyle::default_bar()
            .template("[{bar:20.cyan/white}] Verifying packs: {pos} / {len}")
            .unwrap()
            .progress_chars("=> ")
            .with_key(
                "custom_elapsed",
                move |state: &ProgressState, w: &mut dyn std::fmt::Write| {
                    let elapsed = state.elapsed();
                    let custom_elapsed = utils::pretty_print_duration(elapsed);
                    let _ = w.write_str(&custom_elapsed);
                },
            ),
    );

    let mut num_dangling_blobs = 0;
    for pack_id in &packs {
        num_dangling_blobs += verify_pack(
            repo.as_ref(),
            secure_storage.as_ref(),
            &pack_id,
            &mut visited_blobs,
        )?;
        bar.inc(1);
    }
    bar.finish_and_clear();
    ui::cli::log!(
        "Verified {} blobs from {} packs",
        visited_blobs.len(),
        packs.len()
    );
    if num_dangling_blobs > 0 {
        ui::cli::log!("Found {} dangling blobs", num_dangling_blobs);
    }
    ui::cli::log!();

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
                ui::cli::log!("{}", "[OK]".bold().green());
                ok_counter += 1;
            }
            Err(e) => {
                ui::cli::log!("{} {}", "[ERROR]".bold().red(), e.to_string());
                error_counter += 1
            }
        }
        snapshot_counter += 1;
    }

    ui::cli::log!();
    ui::cli::log!(
        "{} verified",
        utils::format_count(snapshot_counter, "snapshot", "snapshots"),
    );
    if ok_counter > 0 {
        ui::cli::log!("{} {}", ok_counter, "[OK]".bold().green());
    }
    if error_counter > 0 {
        ui::cli::log!("{} {}", error_counter, "[ERROR]".bold().red());
    }

    Ok(())
}

/// Verify the checksum and contents of a snapshot with a known ID in the repository.
///  This function will verify the checksum of the Snapshot object and all blobs referenced by it.
pub fn verify_snapshot(
    repo: Arc<dyn RepositoryBackend>,
    snapshot_id: &ID,
    visited_blobs: &mut BTreeSet<ID>,
) -> Result<()> {
    let snapshot_data = repo.load_file(crate::global::FileType::Snapshot, snapshot_id)?;
    let checksum = utils::calculate_hash(snapshot_data);
    if checksum != snapshot_id.0[..] {
        bail!("Invalid snapshot checksum");
    }

    let snapshot = repo.load_snapshot(snapshot_id)?;
    let tree_id = snapshot.tree.clone();
    let streamer =
        SerializedNodeStreamer::new(repo.clone(), Some(tree_id), PathBuf::new(), None, None)?;

    let bar = ProgressBar::new(snapshot.size());
    bar.set_draw_target(default_bar_draw_target());
    bar.set_style(
        ProgressStyle::default_bar()
            .template("[{custom_elapsed}] [{bar:20.cyan/white}] {processed_bytes_formated} {msg}")
            .unwrap()
            .progress_chars("=> ")
            .with_key(
                "custom_elapsed",
                move |state: &ProgressState, w: &mut dyn std::fmt::Write| {
                    let elapsed = state.elapsed();
                    let custom_elapsed = utils::pretty_print_duration(elapsed);
                    let _ = w.write_str(&custom_elapsed);
                },
            )
            .with_key(
                "processed_bytes_formated",
                move |state: &ProgressState, w: &mut dyn std::fmt::Write| {
                    let s = format!(
                        "{} / {}",
                        utils::format_size(state.pos(), 3),
                        utils::format_size(state.len().unwrap(), 3)
                    );
                    let _ = w.write_str(&s);
                },
            ),
    );

    let mut error_counter = 0;
    for (_path, stream_node) in streamer.flatten() {
        let node = stream_node.node;
        match node.node_type {
            NodeType::File => {
                if let Some(blobs) = node.blobs {
                    for blob in blobs {
                        if !visited_blobs.contains(&blob) {
                            visited_blobs.insert(blob.clone());
                            match verify_blob(repo.as_ref(), &blob) {
                                Ok(blob_len) => bar.inc(blob_len),
                                Err(_) => {
                                    error_counter += 1;
                                    bar.set_message("{error_counter} errors");
                                }
                            }
                        }
                    }
                }
            }
            NodeType::Symlink
            | NodeType::Directory
            | NodeType::BlockDevice
            | NodeType::CharDevice
            | NodeType::Fifo
            | NodeType::Socket => (),
        }
    }

    bar.finish_and_clear();

    if error_counter > 0 {
        bail!("Snapshot has {} corrupt blobs", error_counter);
    }

    Ok(())
}
