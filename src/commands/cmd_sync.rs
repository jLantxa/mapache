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

use std::{path::PathBuf, time::Instant};

use anyhow::Result;
use clap::Args;
use colored::Colorize;
use indicatif::{ProgressBar, ProgressState, ProgressStyle};

use crate::{
    backend::{self, BackendNode, BackendOptions, Handle, StorageBackend},
    commands::cleanup::CleanupHandler,
    mapache::global::GlobalOpts,
    repository::repo::{LOCKS_DIR, RepoConfig, Repository},
    ui::{self, SPINNER_TICK_CHARS, default_bar_draw_target},
    utils,
};

use super::GlobalArgs;

#[derive(Args, Debug)]
#[clap(about = "Synchronize a repository in a different location")]
pub struct CmdArgs {
    /// Destination path
    #[clap(long = "target", value_parser)]
    pub target: String,

    /// Delete unused files
    #[clap(long)]
    pub delete: bool,

    /// SSH public key
    #[clap(long = "dst-ssh-pubkey", value_parser)]
    pub dst_ssh_pubkey: Option<PathBuf>,

    /// SSH private key
    #[clap(long = "dst-ssh-privatekey", value_parser)]
    pub dst_ssh_privatekey: Option<PathBuf>,
}

pub fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let src_auth = utils::get_auth_from_file(&global_args.auth_file)?;
    let src_backend = backend::new_backend_with_prompt(BackendOptions {
        repo_path: global_args.repo.clone(),
        ssh_pubkey: global_args.ssh_pubkey.clone(),
        ssh_privatekey: global_args.ssh_privatekey.clone(),
        dry_backend: false,
    })?;

    let (_repo, _, lock_handle) = Repository::try_open_with_lock(
        src_auth.as_ref(),
        global_args.key.as_ref(),
        src_backend.clone(),
        RepoConfig::default(),
        true,
    )?;

    let dst_backend = backend::new_backend_with_prompt(BackendOptions {
        repo_path: args.target.clone(),
        ssh_pubkey: args.dst_ssh_pubkey.clone(),
        ssh_privatekey: args.dst_ssh_privatekey.clone(),
        dry_backend: false,
    })?;
    dst_backend.create()?; // Create the backend to create the directory if it doesn't exist.

    let lock_handle_clone = lock_handle.clone();
    let _cleanup_handler = CleanupHandler::new(move || {
        lock_handle_clone.write().unlock();
    })?;

    let start = Instant::now();

    sync_backends(src_backend.as_ref(), dst_backend.as_ref(), args.delete)?;

    ui::cli::log!(
        "Finished in {}",
        utils::pretty_print_duration(start.elapsed())
    );

    Ok(())
}

/// Synchronize a repository to a destination backend.
fn sync_backends(
    src_backend: &dyn StorageBackend,
    dst_backend: &dyn StorageBackend,
    delete: bool,
) -> Result<()> {
    let forward_cmp = |n0: &BackendNode, n1: &BackendNode| n0.path().cmp(n1.path());
    let reverse_cmp = |n0: &BackendNode, n1: &BackendNode| n1.path().cmp(n0.path());

    let mut src_nodes = backend::read_backend_dir(src_backend, &PathBuf::new())?;
    let mut dst_nodes = backend::read_backend_dir(dst_backend, &PathBuf::new())?;

    // Ignore 'locks' directory.
    // The sync command will acquire a lock, but the locks must not be synchronized.
    src_nodes.retain(|node| !node.path().starts_with(LOCKS_DIR));
    dst_nodes.retain(|node| !node.path().starts_with(LOCKS_DIR));

    src_nodes.sort_unstable_by(forward_cmp);
    dst_nodes.sort_unstable_by(forward_cmp);

    let mut src_iter = src_nodes.into_iter().peekable();
    let mut dst_iter = dst_nodes.into_iter().peekable();

    let mut to_copy: Vec<BackendNode> = Vec::new();
    let mut to_delete: Vec<BackendNode> = Vec::new();

    let spinner = ProgressBar::new_spinner();
    spinner.set_draw_target(default_bar_draw_target());
    spinner.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.cyan} Calculating differences")
            .unwrap()
            .tick_chars(SPINNER_TICK_CHARS),
    );
    spinner.enable_steady_tick(GlobalOpts::progress_refresh_interval());

    loop {
        match (src_iter.peek(), dst_iter.peek()) {
            (Some(src_node), Some(dst_node)) => {
                let ordering = src_node.path().cmp(dst_node.path());
                match ordering {
                    std::cmp::Ordering::Less => {
                        // src_node is in src but not in dst
                        to_copy.push(src_iter.next().unwrap());
                    }
                    std::cmp::Ordering::Greater => {
                        // dst_node is in dst but not in src
                        to_delete.push(dst_iter.next().unwrap());
                    }
                    std::cmp::Ordering::Equal => {
                        // All objects in the repository are named by hash, so files with identical
                        // paths must have the same content.
                        src_iter.next();
                        dst_iter.next();
                    }
                }
            }
            (Some(_src_node), None) => {
                // Remaining src_nodes are all new
                to_copy.push(src_iter.next().unwrap());
            }
            (None, Some(_dst_node)) => {
                // Remaining dst_nodes are all to be deleted
                to_delete.push(dst_iter.next().unwrap());
            }
            (None, None) => {
                // Both iterators exhausted
                break;
            }
        }
    }

    spinner.finish_and_clear();

    ui::cli::log!(
        "{} {}",
        "To copy:".cyan().bold(),
        utils::format_count(to_copy.len(), "item", "items")
    );
    if delete {
        ui::cli::log!(
            "{} {}",
            "To delete:".cyan().bold(),
            utils::format_count(to_delete.len(), "item", "items")
        );
    }
    ui::cli::log!();

    let copy_progress_bar =
        ProgressBar::with_draw_target(Some(to_copy.len() as u64), default_bar_draw_target())
            .with_style(
                ProgressStyle::default_bar()
                    .template(
                        "[{percent} %] [{bar:20.cyan/white}] Copying files: {pos}/{len} [ETA: {custom_eta}]",
                    )
                    .unwrap()
                    .progress_chars("=> ")
                    .with_key(
                    "custom_eta",
                        move |state: &ProgressState, w: &mut dyn std::fmt::Write| {
                            let eta = state.eta();
                            let custom_eta = utils::pretty_print_duration(eta);
                            let _ = w.write_str(&custom_eta);
                        },
                    )

            );

    // Copy nodes to dst
    for node in to_copy {
        // Copy files from src to dst.

        // TODO: For better performance, we should implement buffered I/O in the
        // backend and transfer the files in small chunks.

        match node {
            BackendNode::Dir(path) => dst_backend.create_dir(&path)?,
            BackendNode::File(path) => {
                let handle = Handle::new(&path);
                let data = src_backend.read(&handle, 0, 0)?;
                dst_backend.write(&handle, &data)?;
            }
        }

        copy_progress_bar.inc(1);
    }

    copy_progress_bar.finish_and_clear();

    // Delete obsolete objects
    if delete {
        let delete_progress_bar =
            ProgressBar::with_draw_target(Some(to_delete.len() as u64), default_bar_draw_target())
                .with_style(
                    ProgressStyle::default_bar()
                        .template("[{percent} %] [{bar:20.cyan/white}] Deleting files: {pos}/{len}")
                        .unwrap()
                        .progress_chars("=> "),
                );

        // It's important to delete files before directories if a directory contains files.
        // Sorting in reverse order ensures files within a directory are processed before the directory itself.
        to_delete.sort_unstable_by(reverse_cmp); // Reverse sort for deletion
        for node in to_delete {
            match node {
                BackendNode::File(path) => dst_backend.remove(&path)?,
                BackendNode::Dir(path) => dst_backend.remove(&path)?,
            }

            delete_progress_bar.inc(1);
        }

        delete_progress_bar.finish_and_clear();
    }

    // Create locks folder (ignored by read_backend_dir)
    dst_backend.create_dir(&PathBuf::from(LOCKS_DIR))?;

    Ok(())
}
