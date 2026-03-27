use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use anyhow::{Result, bail};
use clap::Args;
use colored::Colorize;
use indicatif::{ProgressBar, ProgressState, ProgressStyle};

use crate::{
    backend::{self, BackendNode, BackendOptions, Handle, StorageBackend},
    commands::cleanup::CleanupHandler,
    mapache::{defaults::DEFAULT_PACK_SIZE, global::GlobalOpts},
    repository::repo::{self, LOCKS_DIR, RepoConfig, Repository},
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

    /// SSH private key
    #[clap(long = "dst-ssh-privatekey", value_parser)]
    pub dst_ssh_privatekey: Option<PathBuf>,

    /// Dry run
    #[clap(long, default_value_t = false)]
    pub dry_run: bool,
}

pub async fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    if global_args.repo == args.target {
        ui::cli::warning!("The repo and target backend URLs are the same");
    }

    let src_backend = backend::new_backend_with_prompt(global_args.backend_options(false)).await?;

    let dst_backend = backend::new_backend_with_prompt(BackendOptions {
        repo_path: args.target.clone(),
        ssh_privatekey: args.dst_ssh_privatekey.clone(),
        dry_backend: args.dry_run,
        limit_upload: global_args.limit_upload,
        limit_download: global_args.limit_download,
    })
    .await?;

    let auth =
        utils::get_auth_from_file(&global_args.auth_file)?.unwrap_or_else(ui::cli::request_auth);

    let repo_config = RepoConfig {
        pack_size: DEFAULT_PACK_SIZE,
        use_cache: !global_args.no_cache,
        compression: global_args.compression_level,
    };

    let (_src_repo, _src_ss, mut src_lock) = Repository::try_open_with_lock(
        Some(&auth),
        global_args.key.as_ref(),
        src_backend.clone(),
        repo_config,
        false, // Source lock
        global_args.retry_lock_duration,
    )
    .await?;

    // Try to open the destination repo with the source auth to acquire a lock.
    let dst_lock = if let Ok((_, _, lock)) = Repository::try_open_with_lock(
        Some(&auth),
        global_args.key.as_ref(),
        dst_backend.clone(),
        repo_config,
        args.delete, // Exclusive lock if we are going to delete
        global_args.retry_lock_duration,
    )
    .await
    {
        Some(lock)
    } else {
        None
    };

    dst_backend.create().await?;

    let cleanup_handler = CleanupHandler::new_with_callback(move || {
        ui::cli::log!(
            "\n{}",
            "Process interrupted. Cleaning up...".bold().yellow()
        );
    })?;
    cleanup_handler.add_lock(src_lock.clone());
    if let Some(lock) = &dst_lock {
        cleanup_handler.add_lock(lock.clone());
    }

    let start = Instant::now();

    sync_backends(
        src_backend.as_ref(),
        dst_backend.as_ref(),
        args.delete,
        cleanup_handler.interrupted.clone(),
    )
    .await?;

    ui::cli::log!(
        "Finished in {}",
        utils::pretty_print_duration(start.elapsed())
    );

    src_lock.unlock().await;
    if let Some(mut lock) = dst_lock {
        lock.unlock().await;
    }

    Ok(())
}

/// Synchronize a repository to a destination backend.
async fn sync_backends(
    src_backend: &dyn StorageBackend,
    dst_backend: &dyn StorageBackend,
    delete: bool,
    shutdown_signal: Arc<AtomicBool>,
) -> Result<()> {
    // Calculate diferences
    let (to_copy, to_delete) = diff(src_backend, dst_backend).await?;

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

    // Delete obsolete objects first
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
            if shutdown_signal.load(Ordering::Relaxed) {
                bail!("Interrupted");
            }

            match node {
                BackendNode::File(path, _) => dst_backend.remove(&path).await?,
                BackendNode::Dir(path) => dst_backend.remove(&path).await?,
            }

            delete_progress_bar.inc(1);
        }

        delete_progress_bar.finish_and_clear();
    }

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
    use futures::stream::{self, StreamExt};

    let stream = stream::iter(to_copy)
        .map(|node| {
            let shutdown_signal = shutdown_signal.clone();
            let bar = &copy_progress_bar;

            async move {
                if shutdown_signal.load(Ordering::Relaxed) {
                    bail!("Interrupted");
                }

                match node {
                    BackendNode::Dir(path) => dst_backend.create_dir(&path).await?,
                    BackendNode::File(path, _) => {
                        let handle = Handle::new(&path);
                        let data = src_backend.read(&handle, 0, 0).await?;
                        dst_backend
                            .write(&handle, backend::WriteContents::Owned(data))
                            .await?;
                    }
                }

                bar.inc(1);
                Ok::<(), anyhow::Error>(())
            }
        })
        .buffer_unordered(4); // Use 4 concurrent copy operations

    let results = stream.collect::<Vec<_>>().await;
    for res in results {
        res?;
    }

    copy_progress_bar.finish_and_clear();

    // Finally, synchronize the manifest file to ensure repo validity at destination
    let manifest_path = std::path::Path::new(repo::MANIFEST_PATH);
    let handle = Handle::new(manifest_path);
    if let Ok(data) = src_backend.read(&handle, 0, 0).await {
        dst_backend
            .write(&handle, backend::WriteContents::Owned(data))
            .await?;
    }

    Ok(())
}

/// Calculate differences between the source backend and the destination backend.
/// The results is a sorted list of nodes to copy and nodes to delete.
async fn diff(
    src_backend: &dyn StorageBackend,
    dst_backend: &dyn StorageBackend,
) -> Result<(Vec<BackendNode>, Vec<BackendNode>)> {
    let forward_cmp = |n0: &BackendNode, n1: &BackendNode| n0.path().cmp(n1.path());
    let reverse_cmp = |n0: &BackendNode, n1: &BackendNode| n1.path().cmp(n0.path());

    let spinner = ProgressBar::new_spinner().with_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.cyan} {msg}")
            .unwrap()
            .tick_chars(SPINNER_TICK_CHARS),
    );
    spinner.set_draw_target(default_bar_draw_target());
    spinner.enable_steady_tick(GlobalOpts::progress_refresh_interval());
    spinner.set_message("Reading remote directories...");

    let mut src_nodes: Vec<BackendNode> = backend::read_backend_dir(src_backend, &PathBuf::new())
        .await?
        .into_iter()
        .filter(|n| {
            let p = n.path();
            !p.starts_with(LOCKS_DIR) && p != std::path::Path::new(repo::MANIFEST_PATH)
        })
        .collect();
    let mut dst_nodes: Vec<BackendNode> = backend::read_backend_dir(dst_backend, &PathBuf::new())
        .await?
        .into_iter()
        .filter(|n| {
            let p = n.path();
            !p.starts_with(LOCKS_DIR) && p != std::path::Path::new(repo::MANIFEST_PATH)
        })
        .collect();

    spinner.set_message("Comparing file trees...");

    src_nodes.sort_unstable_by(forward_cmp);
    dst_nodes.sort_unstable_by(forward_cmp);

    let mut src_iter = src_nodes.into_iter().peekable();
    let mut dst_iter = dst_nodes.into_iter().peekable();

    let mut to_copy = Vec::new();
    let mut to_delete = Vec::new();
    let mut num_to_copy = 0;
    let mut num_to_delete = 0;

    let mut processed_nodes_count: usize = 0;
    loop {
        match (src_iter.peek(), dst_iter.peek()) {
            (Some(src_node), Some(dst_node)) => match src_node.path().cmp(dst_node.path()) {
                std::cmp::Ordering::Less => {
                    to_copy.push(src_iter.next().unwrap());
                    num_to_copy += 1;
                }
                std::cmp::Ordering::Greater => {
                    to_delete.push(dst_iter.next().unwrap());
                    num_to_delete += 1;
                }
                std::cmp::Ordering::Equal => {
                    let src = src_iter.next().unwrap();
                    let dst = dst_iter.next().unwrap();
                    match (&src, &dst) {
                        (BackendNode::File(_, _), BackendNode::File(_, _)) => {
                            if src != dst {
                                to_copy.push(src);
                                num_to_copy += 1;
                            }
                        }
                        (BackendNode::Dir(_), BackendNode::Dir(_)) => {
                            // Already exists as dir, do nothing
                        }
                        _ => {
                            // Type mismatch! Delete then copy.
                            to_delete.push(dst);
                            num_to_delete += 1;
                            to_copy.push(src);
                            num_to_copy += 1;
                        }
                    }
                }
            },
            (Some(_), None) => {
                to_copy.push(src_iter.next().unwrap());
                num_to_copy += 1;
            }
            (None, Some(_)) => {
                to_delete.push(dst_iter.next().unwrap());
                num_to_delete += 1;
            }
            (None, None) => break,
        }

        // Throttle UI updates to once every 100 changes.
        processed_nodes_count += 1;
        if processed_nodes_count.is_multiple_of(100) {
            spinner.set_message(format!(
                "Calculating differences: {} to copy, {} to delete",
                num_to_copy, num_to_delete
            ));
        }
    }

    to_delete.sort_unstable_by(reverse_cmp);
    spinner.finish_and_clear();

    Ok((to_copy, to_delete))
}
