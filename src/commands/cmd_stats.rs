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

use std::{collections::BTreeSet, path::PathBuf, sync::Arc, time::Duration};

use anyhow::Result;
use clap::{Args, ValueEnum};
use indicatif::{ProgressBar, ProgressStyle};

use crate::{
    backend::{BackendOptions, StorageBackend, new_backend_with_prompt},
    commands::{GlobalArgs, cleanup::CleanupHandler},
    fs::{node::NodeType, tree::SerializedNodeStreamer},
    global::FileType,
    repository::{
        repo::{RepoConfig, Repository},
        snapshot::SnapshotStreamer,
    },
    ui::{self, PROGRESS_REFRESH_RATE_HZ, SPINNER_TICK_CHARS, default_bar_draw_target},
    utils::{self, size},
};

#[derive(Debug, Clone, ValueEnum)]
pub enum Mode {
    Repository,
    Snapshots,
}

impl std::fmt::Display for Mode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Mode::Repository => write!(f, "repository"),
            Mode::Snapshots => write!(f, "snapshots"),
        }
    }
}

#[derive(Args, Debug)]
#[clap(about = "Display stats about the repository and its contents")]
pub struct CmdArgs {
    #[clap(long = "mode", value_parser, default_value_t = Mode::Repository)]
    pub mode: Mode,
}

pub fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let auth = utils::get_auth_from_file(&global_args.auth_file)?;
    let backend = new_backend_with_prompt(BackendOptions {
        repo_path: global_args.repo.clone(),
        ssh_pubkey: global_args.ssh_pubkey.clone(),
        ssh_privatekey: global_args.ssh_privatekey.clone(),
        dry_backend: false,
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

    match args.mode {
        Mode::Repository => stats_repository(repo, backend),
        Mode::Snapshots => stats_snapshots(repo),
    }
}

fn stats_repository(repo: Arc<Repository>, backend: Arc<dyn StorageBackend>) -> Result<()> {
    let spinner = ProgressBar::new_spinner();
    spinner.set_draw_target(default_bar_draw_target());
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.cyan} Collecting stats... ({msg})")
            .unwrap()
            .tick_chars(SPINNER_TICK_CHARS),
    );
    spinner.enable_steady_tick(Duration::from_millis(
        (1000.0_f32 / PROGRESS_REFRESH_RATE_HZ as f32) as u64,
    ));

    // Pack info
    let all_pack_files = repo.list_files(FileType::Pack)?;
    let num_packs = all_pack_files.len();
    let mut total_pack_size: u64 = 0;

    for (i, pack_file_path) in all_pack_files.into_iter().enumerate() {
        spinner.set_message(format!("pack {} / {}", 1 + i, num_packs));

        // Add header size (raw + encoded) as meta
        let stat = backend.lstat(&pack_file_path)?;
        total_pack_size += stat.size.unwrap_or(0);
    }

    // Index
    spinner.set_message("index");
    let all_index_files = repo.list_files(FileType::Index)?;
    let num_indices = all_index_files.len();
    let mut total_index_size: u64 = 0;
    for index_file_path in all_index_files {
        total_index_size += backend.lstat(&index_file_path)?.size.unwrap_or(0);
    }

    // Snapshots
    spinner.set_message("snapshots");
    let all_snapshot_files = repo.list_files(FileType::Snapshot)?;
    let num_snapshots = all_snapshot_files.len();
    let mut total_snapshot_size: u64 = 0;
    for snapshot_file_path in all_snapshot_files {
        total_snapshot_size += backend.lstat(&snapshot_file_path)?.size.unwrap_or(0);
    }

    // Keys
    spinner.set_message("keys");
    let all_key_files = repo.list_files(FileType::Key)?;
    let num_keys = all_key_files.len();
    let mut total_key_size: u64 = 0;
    for key_file_path in all_key_files {
        total_key_size += backend.lstat(&key_file_path)?.size.unwrap_or(0);
    }

    // Manifest and Total size
    spinner.set_message("manifest");
    let manifest_file = repo.list_files(FileType::Manifest)?;
    let manifest_file = manifest_file.first().expect("There should be a manifest");
    let manifest_size = backend.lstat(manifest_file)?.size.unwrap_or(0);
    let total_size =
        total_pack_size + total_index_size + total_snapshot_size + total_key_size + manifest_size;

    spinner.finish_and_clear();

    ui::cli::log!("Packs:");
    ui::cli::log!("\t{}", utils::format_count(num_packs, "pack", "packs"));
    ui::cli::log!(
        "\tTotal pack size: {}",
        utils::format_size(total_pack_size, 3)
    );
    ui::cli::log!();
    ui::cli::log!("Index:");
    ui::cli::log!(
        "\t{}",
        utils::format_count(num_indices, "index file", "index files")
    );
    ui::cli::log!(
        "\tTotal index size: {}",
        utils::format_size(total_index_size, 3)
    );
    ui::cli::log!();
    ui::cli::log!("Snapshot:");
    ui::cli::log!(
        "\t{}",
        utils::format_count(num_snapshots, "snapshot", "snapshots")
    );
    ui::cli::log!(
        "\tTotal snapshot file size: {}",
        utils::format_size(total_snapshot_size, 3)
    );
    ui::cli::log!();
    ui::cli::log!("Key:");
    ui::cli::log!("\t{}", utils::format_count(num_keys, "key", "keys"));
    ui::cli::log!(
        "\tTotal key file size: {}",
        utils::format_size(total_key_size, 3)
    );
    ui::cli::log!();
    ui::cli::log!("Manifest size: {}", utils::format_size(manifest_size, 3));
    ui::cli::log!();
    ui::cli::log!(
        "Total repository size: {}",
        utils::format_size(total_size, 3)
    );

    Ok(())
}

fn stats_snapshots(repo: Arc<Repository>) -> Result<()> {
    let index = repo.index();
    let snapshot_streamer = SnapshotStreamer::new(repo.clone())?;
    let num_snapshots = snapshot_streamer.len();

    let mut error_counter = 0;
    let mut total_restore_size: u64 = 0;
    let mut num_referenced_blobs = 0;
    let mut total_raw_data_size: u64 = 0;
    let mut total_encoded_data_size: u64 = 0;
    let mut visited_blobs = BTreeSet::new();

    let spinner = ProgressBar::new_spinner();
    spinner.set_draw_target(default_bar_draw_target());
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.cyan} {msg}")
            .unwrap()
            .tick_chars(SPINNER_TICK_CHARS),
    );
    spinner.enable_steady_tick(Duration::from_millis(
        (1000.0_f32 / PROGRESS_REFRESH_RATE_HZ as f32) as u64,
    ));

    for (_id, snapshot) in snapshot_streamer {
        spinner.inc(1);
        spinner.set_message(format!(
            "Analyzing snapshots: {} / {}",
            spinner.position(),
            num_snapshots
        ));

        total_restore_size += snapshot.size();

        let tree_id = snapshot.tree.clone();
        let streamer =
            SerializedNodeStreamer::new(repo.clone(), Some(tree_id), PathBuf::new(), None, None)?;

        for (_path, stream_node) in streamer.flatten() {
            let node = stream_node.node;
            match node.node_type {
                NodeType::File => {
                    if let Some(blobs) = node.blobs {
                        for blob_id in blobs {
                            if !visited_blobs.contains(&blob_id) {
                                match index.read().get(&blob_id) {
                                    Some((_pack_id, _blob_type, _offset, encoded_len, raw_len)) => {
                                        total_raw_data_size += raw_len as u64;
                                        total_encoded_data_size += encoded_len as u64;
                                        num_referenced_blobs += 1;
                                    }
                                    None => {
                                        error_counter += 1;
                                    }
                                }
                                visited_blobs.insert(blob_id);
                            }
                        }
                    }
                }
                NodeType::Directory
                | NodeType::Symlink
                | NodeType::BlockDevice
                | NodeType::CharDevice
                | NodeType::Fifo
                | NodeType::Socket => (),
            }
        }
    }

    spinner.finish_and_clear();

    ui::cli::log!(
        "{}",
        utils::format_count(num_snapshots, "snapshot", "snapshots")
    );
    ui::cli::log!(
        "\t{}",
        utils::format_count(num_referenced_blobs, "referenced blob", "referenced blobs")
    );
    ui::cli::log!(
        "\tRestore size:       {:>12}",
        utils::format_size(total_restore_size, 3)
    );
    ui::cli::log!(
        "\tTotal raw size:     {:>12}",
        utils::format_size(total_raw_data_size, 3)
    );
    ui::cli::log!(
        "\tTotal encoded size: {:>12}",
        utils::format_size(total_encoded_data_size, 3)
    );
    ui::cli::log!(
        "\tCompression ratio: {:.2}x",
        total_raw_data_size as f32 / total_encoded_data_size as f32
    );

    if error_counter > 0 {
        ui::cli::log!();
        ui::cli::warning!("Found {} blobs not indexed", error_counter);
    }

    Ok(())
}
