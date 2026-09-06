use std::{
    io,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Instant,
};

use crate::common::error::Result;
use clap::Args;
use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use parking_lot::Mutex;
use serde::Serialize;

use crate::{
    backend::{self, BackendNode, BackendOptions, Handle, StorageBackend},
    commands::{GlobalArgs, ToExitCode, cleanup::CleanupHandler},
    common::{defaults::UI_RATE_ESTIMATOR_WINDOW, error::MapacheError, global::GlobalOpts},
    repository::lock::{Lock, LockHandle},
    repository::repo::{self, LOCKS_DIR, Repository},
    ui::{
        self, SPINNER_TICK_CHARS, cli::color::Colorize, default_bar_draw_target,
        default_progress_style,
    },
    utils::{self, rate_estimator::RateEstimator},
};

#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    #[error("failed to open repository: {0}")]
    RepoOpenFail(String),
    #[error("backend error: {0}")]
    BackendError(String),
    #[error("sync failed: {0}")]
    SyncFailed(String),
    #[error("cannot synchronize repositories with different formats: {0}")]
    FormatMismatch(String),
    #[error("sync interrupted by user")]
    Interrupted,
    #[error(transparent)]
    Repo(#[from] MapacheError),
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl ToExitCode for SyncError {
    fn to_exit_code(&self) -> i32 {
        match self {
            SyncError::RepoOpenFail(_) => 10,
            SyncError::BackendError(_) => 11,
            SyncError::FormatMismatch(_) => 13,
            SyncError::SyncFailed(_) => 20,
            SyncError::Interrupted => 130,
            SyncError::Repo(_) => 3,
            SyncError::Io(_) => 4,
        }
    }
}

#[derive(Args, Debug, Clone)]
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

    /// SSH known_hosts file
    #[clap(long = "dst-ssh-known-hosts", value_parser)]
    pub dst_ssh_known_hosts: Option<PathBuf>,

    /// Dry run
    #[clap(long, default_value_t = false)]
    pub dry_run: bool,
}

pub async fn run(global_args: &GlobalArgs, args: &CmdArgs) -> std::result::Result<(), SyncError> {
    let json_out = global_args.json;

    if json_out {
        #[derive(Serialize)]
        struct SyncStartMsg {
            source: String,
            target: String,
            delete: bool,
            dry_run: bool,
        }
        ui::json::emit_static(
            "sync_start",
            &SyncStartMsg {
                source: global_args.repo.clone(),
                target: args.target.clone(),
                delete: args.delete,
                dry_run: args.dry_run,
            },
        );
    }

    if !json_out && global_args.repo == args.target {
        ui::cli::warning!("The repo and target backend URLs are the same");
    }

    let src_backend = backend::new_backend_with_prompt(global_args.backend_options(false))
        .await
        .map_err(|e| {
            SyncError::BackendError(format!(
                "failed to initialize source backend: {}",
                e.inner()
            ))
        })?;

    let dst_backend = backend::new_backend_with_prompt(BackendOptions {
        repo_path: args.target.clone(),
        ssh_privatekey: args.dst_ssh_privatekey.clone(),
        ssh_known_hosts: args.dst_ssh_known_hosts.clone(),
        dry_backend: args.dry_run,
        limit_upload: global_args.limit_upload,
        limit_download: global_args.limit_download,
    })
    .await
    .map_err(|e| {
        SyncError::BackendError(format!(
            "failed to initialize destination backend: {}",
            e.inner()
        ))
    })?;

    let auth = match utils::get_auth(&global_args.auth_file)
        .map_err(|e| SyncError::BackendError(format!("failed to get auth: {}", e.inner())))?
    {
        Some(a) => a,
        None => ui::cli::request_auth()
            .map_err(|e| SyncError::BackendError(format!("failed to get auth: {}", e.inner())))?,
    };

    let repo_config = global_args.to_repo_config();

    let (_src_repo, _src_ss, src_lock) = if global_args.no_lock {
        let (repo, ss) = Repository::try_open_unlocked(
            &auth,
            global_args.key.as_ref(),
            src_backend.clone(),
            repo_config,
        )
        .await
        .map_err(|e| SyncError::RepoOpenFail(e.to_string()))?;
        let lock = LockHandle::new(
            repo.clone(),
            Arc::new(parking_lot::Mutex::new(Lock::new(false))),
            true,
        );
        (repo, ss, lock)
    } else {
        Repository::try_open_with_lock(
            &auth,
            global_args.key.as_ref(),
            src_backend.clone(),
            repo_config,
            false, // Source lock
            global_args.retry_lock_duration,
        )
        .await
        .map_err(|e| SyncError::RepoOpenFail(e.to_string()))?
    };

    // Sync replicates the raw storage layout byte-for-byte (excluding the
    // manifest), so source and destination must use the exact same format
    // version. A destination that is not yet a repository (bootstrap) is
    // exempt: it simply adopts the source layout.
    let ensure_same_format = |src: u32, dst: u32| -> std::result::Result<(), SyncError> {
        if src != dst {
            return Err(SyncError::FormatMismatch(format!(
                "source repository is v{} while destination is v{} (use `mapache migrate` to upgrade a v1 repository)",
                src, dst
            )));
        }
        Ok(())
    };

    // Try to open the destination repo with the source auth to acquire a lock.
    let dst_lock = if global_args.no_lock {
        match Repository::try_open_unlocked(
            &auth,
            global_args.key.as_ref(),
            dst_backend.clone(),
            repo_config,
        )
        .await
        {
            Ok((repo, _ss)) => {
                ensure_same_format(_src_repo.repo_version(), repo.repo_version())?;
                Some(LockHandle::new(
                    repo.clone(),
                    Arc::new(parking_lot::Mutex::new(Lock::new(false))),
                    true,
                ))
            }
            Err(_) => None,
        }
    } else {
        if let Ok((dst_repo, _, lock)) = Repository::try_open_with_lock(
            &auth,
            global_args.key.as_ref(),
            dst_backend.clone(),
            repo_config,
            args.delete, // Exclusive lock if we are going to delete
            global_args.retry_lock_duration,
        )
        .await
        {
            ensure_same_format(_src_repo.repo_version(), dst_repo.repo_version())?;
            Some(lock)
        } else {
            // Never silently proceed without a lock against a repository that
            // already exists (e.g. `--delete` could interleave with another
            // writer). A destination that is not yet a repository (bootstrap)
            // is the only case where omitting the lock is acceptable.
            let dst_is_repo = dst_backend
                .path_exists(std::path::Path::new(repo::MANIFEST_PATH))
                .await;
            if dst_is_repo {
                return Err(SyncError::RepoOpenFail(format!(
                    "destination repository {} is already a repository but could not be opened with a lock",
                    args.target
                )));
            }
            None
        }
    };

    dst_backend.create().await.map_err(|e| {
        SyncError::BackendError(format!(
            "failed to create destination backend: {}",
            e.inner()
        ))
    })?;

    let cleanup_handler = CleanupHandler::new_with_callback(move || {
        ui::cli::log!(
            "\n{}",
            "Process interrupted. Cleaning up...".bold().yellow()
        );
    });
    cleanup_handler.add_lock(Some(src_lock.clone()));
    if let Some(lock) = &dst_lock {
        cleanup_handler.add_lock(Some(lock.clone()));
    }

    let start = Instant::now();

    let sync_result = sync_backends(
        src_backend.as_ref(),
        dst_backend.as_ref(),
        args.delete,
        cleanup_handler.interrupted.clone(),
        json_out,
    )
    .await;

    if sync_result.is_err() && cleanup_handler.is_interrupted() {
        tracing::info!(target: "sync", "Sync interrupted by user");
        src_lock.unlock().await;
        if let Some(lock) = dst_lock {
            lock.unlock().await;
        }
        return Err(SyncError::Interrupted);
    }
    sync_result.map_err(|e| SyncError::SyncFailed(e.to_string()))?;

    if json_out {
        #[derive(Serialize)]
        struct SyncCompleteMsg {
            duration_seconds: f64,
        }
        ui::json::emit_static(
            "sync_complete",
            &SyncCompleteMsg {
                duration_seconds: start.elapsed().as_secs_f64(),
            },
        );
    } else {
        ui::cli::log!(
            "Finished in {}",
            utils::pretty_print_duration(start.elapsed())
        );
    }
    tracing::info!(target: "sync", "Sync command completed in {:?}", start.elapsed());

    src_lock.unlock().await;
    if let Some(lock) = dst_lock {
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
    json_out: bool,
) -> Result<()> {
    // Calculate differences
    let (to_copy, to_delete) = diff(src_backend, dst_backend).await?;

    if json_out {
        #[derive(Serialize)]
        struct SyncDiffMsg {
            to_copy: usize,
            to_delete: usize,
        }
        ui::json::emit_static(
            "sync_diff",
            &SyncDiffMsg {
                to_copy: to_copy.len(),
                to_delete: to_delete.len(),
            },
        );
    } else {
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
    }

    // Delete obsolete objects first
    if delete && !to_delete.is_empty() {
        tracing::info!(target: "sync", "Deleting {} obsolete items from destination", to_delete.len());

        if json_out {
            let total = to_delete.len();
            for (i, node) in to_delete.iter().enumerate() {
                if shutdown_signal.load(Ordering::Acquire) {
                    tracing::info!(target: "sync", "Sync delete interrupted by user");
                    return Err(MapacheError::Interrupted);
                }

                tracing::debug!(target: "sync", "Deleting {:?}", node.path());
                match node {
                    BackendNode::File(path, _) => dst_backend.remove(path).await?,
                    BackendNode::Dir(path) => dst_backend.remove(path).await?,
                }

                let processed = i + 1;
                #[derive(Serialize)]
                struct SyncStatusMsg {
                    phase: String,
                    processed: usize,
                    total: usize,
                }
                ui::json::emit_static(
                    "sync_status",
                    &SyncStatusMsg {
                        phase: "delete".to_string(),
                        processed,
                        total,
                    },
                );
            }
        } else {
            let delete_progress_bar = ProgressBar::with_draw_target(
                Some(to_delete.len() as u64),
                default_bar_draw_target(),
            )
            .with_style(
                default_progress_style()
                    .template("[{percent} %] [{bar:20.cyan/white}] Deleting files: {pos}/{len}")
                    .expect("invalid progress bar template for sync delete"),
            );

            for node in to_delete {
                if shutdown_signal.load(Ordering::Acquire) {
                    tracing::info!(target: "sync", "Sync delete interrupted by user");
                    return Err(MapacheError::Interrupted);
                }

                tracing::debug!(target: "sync", "Deleting {:?}", node.path());
                match node {
                    BackendNode::File(path, _) => dst_backend.remove(&path).await?,
                    BackendNode::Dir(path) => dst_backend.remove(&path).await?,
                }

                delete_progress_bar.inc(1);
            }

            delete_progress_bar.finish_and_clear();
        }
    }

    let total_copy = to_copy.len();

    if json_out {
        let processed = Arc::new(AtomicU64::new(0));

        use futures::stream::{self, StreamExt};

        let stream = stream::iter(to_copy)
            .map(|node| {
                let shutdown_signal = shutdown_signal.clone();
                let processed = processed.clone();

                async move {
                    if shutdown_signal.load(Ordering::Acquire) {
                        tracing::info!(target: "sync", "Sync copy interrupted by user");
                        return Err(MapacheError::Interrupted);
                    }

                    match node {
                        BackendNode::Dir(path) => {
                            tracing::debug!(target: "sync", "Creating directory {:?}", path);
                            dst_backend.create_dir(&path).await?
                        }
                        BackendNode::File(path, _) => {
                            tracing::debug!(target: "sync", "Copying file {:?}", path);
                            let handle = Handle::new(&path);
                            let data = src_backend.read(&handle, 0, 0).await?;
                            dst_backend
                                .write(&handle, backend::WriteContents::Owned(data))
                                .await?;
                        }
                    }

                    let p = processed.fetch_add(1, Ordering::Relaxed) + 1;
                    #[derive(Serialize)]
                    struct SyncStatusMsg {
                        phase: String,
                        processed: u64,
                        total: usize,
                    }
                    ui::json::emit_static(
                        "sync_status",
                        &SyncStatusMsg {
                            phase: "copy".to_string(),
                            processed: p,
                            total: total_copy,
                        },
                    );
                    Ok::<(), MapacheError>(())
                }
            })
            .buffer_unordered(4);

        let results = stream.collect::<Vec<_>>().await;
        for res in results {
            res?;
        }
    } else {
        let sync_rate = Arc::new(Mutex::new(RateEstimator::new(UI_RATE_ESTIMATOR_WINDOW)));

        let copy_progress_bar = ProgressBar::with_draw_target(
            Some(total_copy as u64),
            default_bar_draw_target(),
        )
        .with_style(
            default_progress_style()
                .template(
                    "[{percent} %] [{bar:20.cyan/white}] Copying files: {pos}/{len} [ETA: {custom_eta}]",
                )
                .expect("invalid progress bar template for sync copy")
                .with_key(
                    "custom_eta",
                    {
                        let re = sync_rate.clone();
                        move |state: &ProgressState, w: &mut dyn std::fmt::Write| {
                            let pos = state.pos() as f64;
                            let total = state.len().map(|l| l as f64);
                            match re.lock().eta(pos, total.unwrap_or(pos)) {
                                Some(d) => {
                                    let _ = w.write_str(&utils::pretty_print_duration(d));
                                }
                                None => {
                                    let _ = w.write_str("--");
                                }
                            }
                        }
                    },
                ),
        );

        use futures::stream::{self, StreamExt};

        let stream = stream::iter(to_copy)
            .map(|node| {
                let shutdown_signal = shutdown_signal.clone();
                let bar = &copy_progress_bar;
                let sync_rate = sync_rate.clone();

                async move {
                    if shutdown_signal.load(Ordering::Acquire) {
                        tracing::info!(target: "sync", "Sync copy interrupted by user");
                        return Err(MapacheError::Interrupted);
                    }

                    match node {
                        BackendNode::Dir(path) => {
                            tracing::debug!(target: "sync", "Creating directory {:?}", path);
                            dst_backend.create_dir(&path).await?
                        }
                        BackendNode::File(path, _) => {
                            tracing::debug!(target: "sync", "Copying file {:?}", path);
                            let handle = Handle::new(&path);
                            let data = src_backend.read(&handle, 0, 0).await?;
                            dst_backend
                                .write(&handle, backend::WriteContents::Owned(data))
                                .await?;
                        }
                    }

                    bar.inc(1);
                    let pos = bar.position() as f64;
                    sync_rate.lock().observe(pos);
                    Ok::<(), MapacheError>(())
                }
            })
            .buffer_unordered(4); // Use 4 concurrent copy operations

        let results = stream.collect::<Vec<_>>().await;
        for res in results {
            res?;
        }

        copy_progress_bar.finish_and_clear();
    }

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
            .expect("invalid progress bar template for sync spinner")
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
                    to_copy.push(
                        src_iter
                            .next()
                            .expect("src_iter has next (peek returned Some)"),
                    );
                    num_to_copy += 1;
                }
                std::cmp::Ordering::Greater => {
                    to_delete.push(
                        dst_iter
                            .next()
                            .expect("dst_iter has next (peek returned Some)"),
                    );
                    num_to_delete += 1;
                }
                std::cmp::Ordering::Equal => {
                    let src = src_iter
                        .next()
                        .expect("src_iter has next (peek returned Some)");
                    let dst = dst_iter
                        .next()
                        .expect("dst_iter has next (peek returned Some)");
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
                to_copy.push(
                    src_iter
                        .next()
                        .expect("src_iter has next (peek returned Some)"),
                );
                num_to_copy += 1;
            }
            (None, Some(_)) => {
                to_delete.push(
                    dst_iter
                        .next()
                        .expect("dst_iter has next (peek returned Some)"),
                );
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
