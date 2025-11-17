use std::{path::PathBuf, time::Instant};

use anyhow::Result;
use clap::Args;
use colored::Colorize;
use indicatif::{ProgressBar, ProgressState, ProgressStyle};

use crate::{
    backend::{self, BackendNode, BackendOptions, Handle, StorageBackend},
    commands::cleanup::CleanupHandler,
    mapache::{defaults::DEFAULT_PACK_SIZE, global::GlobalOpts},
    repository::repo::{LOCKS_DIR, RepoConfig, Repository},
    ui::{self, SPINNER_TICK_CHARS, default_bar_draw_target},
    utils::{self},
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

    let (_repo, _secure_storage, lock_handle) = Repository::try_open_with_lock(
        src_auth.as_ref(),
        global_args.key.as_ref(),
        src_backend.clone(),
        RepoConfig {
            pack_size: DEFAULT_PACK_SIZE,
            use_cache: !global_args.no_cache,
        },
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
    // Calculate diferences
    let (to_copy, to_delete) = diff(src_backend, dst_backend)?;

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

    // Copy files from src to dst.
    for node in to_copy {
        // TODO: For better performance, we could implement buffered I/O in the
        // backend and transfer the files in small chunks. This would complicate the
        // StorageBackend trait, so it is probably not worth it.

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

/// Calculate differences between the source backend and the destination backend.
/// The results is a sorted list of nodes to copy and nodes to delete.
fn diff(
    src_backend: &dyn StorageBackend,
    dst_backend: &dyn StorageBackend,
) -> Result<(Vec<BackendNode>, Vec<BackendNode>)> {
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
            .template("{spinner:.cyan} Calculating differences: {msg}")
            .unwrap()
            .tick_chars(SPINNER_TICK_CHARS),
    );
    spinner.enable_steady_tick(GlobalOpts::progress_refresh_interval());

    let mut num_to_copy = 0;
    let mut num_to_delete = 0;
    let update_msg = |num_to_copy: usize, num_to_delete: usize| {
        spinner.set_message(format!("{num_to_copy} to copy, {num_to_delete} to delete"));
    };

    loop {
        match (src_iter.peek(), dst_iter.peek()) {
            (Some(src_node), Some(dst_node)) => {
                let ordering = src_node.path().cmp(dst_node.path());
                match ordering {
                    std::cmp::Ordering::Less => {
                        // src_node is in src but not in dst
                        to_copy.push(src_iter.next().unwrap());

                        num_to_copy += 1;
                        update_msg(num_to_copy, num_to_delete);
                    }
                    std::cmp::Ordering::Greater => {
                        // dst_node is in dst but not in src
                        to_delete.push(dst_iter.next().unwrap());

                        num_to_delete += 1;
                        update_msg(num_to_copy, num_to_delete);
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

                num_to_copy += 1;
                update_msg(num_to_copy, num_to_delete);
            }
            (None, Some(_dst_node)) => {
                // Remaining dst_nodes are all to be deleted
                to_delete.push(dst_iter.next().unwrap());

                num_to_delete += 1;
                update_msg(num_to_copy, num_to_delete);
            }
            (None, None) => {
                // Both iterators exhausted
                break;
            }
        }
    }

    // It's important to delete files before directories if a directory contains files.
    // Sorting in reverse order ensures files within a directory are processed before the directory itself.
    to_delete.sort_unstable_by(reverse_cmp); // Reverse sort for deletion

    spinner.finish_and_clear();

    Ok((to_copy, to_delete))
}
