use std::{
    collections::BTreeSet,
    fmt,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Instant,
};

use anyhow::{Result, bail};
use clap::Args;
use futures::{StreamExt, stream};
use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::{
    backend::{self, BackendOptions, WriteContents},
    commands::{GlobalArgs, ToExitCode, cleanup::CleanupHandler, fail, open_repository},
    common::{
        BlobType, ContentIdType, ID, SaveID,
        defaults::{DEFAULT_PACK_SIZE, UI_RATE_ESTIMATOR_WINDOW},
    },
    fs::tree::Tree,
    repository::{
        repo::{RepoConfig, Repository},
        retention::filter_snapshots_by_hosts,
        snapshot::SnapshotStream,
    },
    ui::{self, cli::color::Colorize, default_bar_draw_target},
    utils::{self, rate_estimator::RateEstimator},
};

#[derive(Debug, Clone, Copy)]
pub enum CopyError {
    RepoOpenFail = 10,
    BackendError = 11,
    CopyFailed = 20,
    Interrupted = 130,
}

impl ToExitCode for CopyError {
    fn to_exit_code(&self) -> i32 {
        *self as i32
    }
}

/// Transfer snapshots from one repository to another
#[derive(Args, Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default, rename_all = "kebab-case")]
pub struct CmdArgs {
    /// Destination repository path
    #[clap(long = "target", value_parser)]
    pub target: String,

    /// SSH private key for destination
    #[clap(long = "dst-ssh-privatekey", value_parser)]
    pub dst_ssh_privatekey: Option<PathBuf>,

    /// SSH known_hosts file for destination
    #[clap(long = "dst-ssh-known-hosts", value_parser)]
    pub dst_ssh_known_hosts: Option<PathBuf>,

    /// Copy only snapshots with the given ID prefix. Can be repeated or comma-separated.
    #[clap(long, value_parser, value_delimiter = ',', num_args = 1..)]
    pub snapshot: Option<Vec<String>>,

    /// Copy only snapshots from the given host. Can be repeated or comma-separated.
    #[clap(long, value_parser, value_delimiter = ',', num_args = 1..)]
    pub host: Option<Vec<String>>,

    /// Copy only snapshots with the given tag. Can be repeated or comma-separated.
    #[clap(long, value_parser, value_delimiter = ',', num_args = 1..)]
    pub tags: Option<Vec<String>>,

    /// Dry run
    #[clap(long, default_value_t = false)]
    pub dry_run: bool,
}

pub async fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let json_out = global_args.json;

    if json_out {
        #[derive(Serialize)]
        struct CopyStartMsg {
            source: String,
            target: String,
            dry_run: bool,
        }
        ui::json::emit_static(
            "copy_start",
            &CopyStartMsg {
                source: global_args.repo.clone(),
                target: args.target.clone(),
                dry_run: args.dry_run,
            },
        );
    }

    // Open source repository

    if !json_out {
        ui::cli::log!("{} {}", "Source:".cyan().bold(), global_args.repo);
    }

    let src_backend = backend::new_backend_with_prompt(global_args.backend_options(false))
        .await
        .map_err(|e| {
            fail(
                format!("Failed to initialize source backend: {e}"),
                CopyError::BackendError,
            )
        })?;

    let repo_config = RepoConfig {
        pack_size: DEFAULT_PACK_SIZE,
        use_cache: !global_args.no_cache,
        compression: global_args.compression_level,
    };

    let (src_repo, _src_ss) = open_repository(
        global_args.auth_file.as_ref(),
        global_args.key.as_ref(),
        src_backend.clone(),
        repo_config,
    )
    .await
    .map_err(|e| {
        fail(
            format!("Failed to open source repository: {e}"),
            CopyError::RepoOpenFail,
        )
    })?;

    // Open destination repository

    if !json_out {
        ui::cli::log!("{} {}", "Destination:".cyan().bold(), args.target);
    }

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
        fail(
            format!("Failed to initialize destination backend: {e}"),
            CopyError::BackendError,
        )
    })?;

    let (dst_repo, _dst_ss) = open_repository(
        global_args.auth_file.as_ref(),
        global_args.key.as_ref(),
        dst_backend.clone(),
        repo_config,
    )
    .await
    .map_err(|e| {
        fail(
            format!("Failed to open destination repository: {e}"),
            CopyError::RepoOpenFail,
        )
    })?;

    // Reload master indices for blob lookup
    src_repo.reload_master_index().await?;
    dst_repo.reload_master_index().await?;

    ui::cli::log!();

    // Select snapshots

    let snapshots = select_snapshots(
        src_repo.clone(),
        args.snapshot.clone(),
        args.host.clone().unwrap_or_default(),
        args.tags.clone().unwrap_or_default(),
    )
    .await?;

    if snapshots.is_empty() {
        if !json_out {
            ui::cli::log!("{}", "No snapshots to copy.".bold().yellow());
        }
        return Ok(());
    }

    if !json_out {
        ui::cli::log!(
            "{} {}",
            "Selected:".cyan().bold(),
            utils::format_count(snapshots.len(), "snapshot", "snapshots")
        );
    }

    // Filter out snapshots already present in destination

    let dst_snapshot_ids: std::collections::BTreeSet<ID> =
        dst_repo.list_snapshot_ids().await?.into_iter().collect();

    let snapshots: Vec<(ID, crate::repository::snapshot::Snapshot)> = snapshots
        .into_iter()
        .filter(|(id, _)| !dst_snapshot_ids.contains(id))
        .collect();

    if snapshots.is_empty() {
        if !json_out {
            ui::cli::log!(
                "{}",
                "All snapshots already present in destination."
                    .bold()
                    .green()
            );
        }
        return Ok(());
    }

    // Collect all blob IDs and calculate total bytes

    let (blob_list, total_bytes) = collect_blobs(src_repo.clone(), &snapshots, json_out).await?;

    if args.dry_run {
        if json_out {
            let ids: Vec<String> = snapshots.iter().map(|(id, _)| id.to_hex()).collect();
            #[derive(Serialize)]
            struct DryRunMsg {
                snapshots: Vec<String>,
                total_bytes: u64,
            }
            ui::json::emit_static(
                "copy_dry_run",
                &DryRunMsg {
                    snapshots: ids,
                    total_bytes,
                },
            );
        } else {
            ui::cli::log!("{}", "Dry-run. Would copy:".bold().yellow());
            for (id, snap) in &snapshots {
                ui::cli::log!(
                    "  {}  {}",
                    id.to_short_hex(SHORT_SNAPSHOT_ID_LEN).bold().yellow(),
                    snap.timestamp.format("%Y-%m-%d %H:%M:%S %:z")
                );
            }
            ui::cli::log!(
                "{} {}",
                "Total bytes to transfer:".bold(),
                utils::format_size_binary(total_bytes, 2)
            );
        }
        return Ok(());
    }

    if !json_out {
        ui::cli::log!("{}", "Snapshots to copy:".cyan().bold());
        for (id, snap) in &snapshots {
            ui::cli::log!(
                "  {}  {}",
                id.to_short_hex(SHORT_SNAPSHOT_ID_LEN).bold().yellow(),
                snap.timestamp.format("%Y-%m-%d %H:%M:%S %:z")
            );
        }
    }

    // Transfer

    let cleanup_handler = CleanupHandler::new_with_callback(move || {
        ui::cli::log!(
            "\n{}",
            "Process interrupted. Cleaning up...".bold().yellow()
        );
    })?;

    let start = Instant::now();

    let result = copy_snapshots(
        src_repo.clone(),
        dst_repo.clone(),
        snapshots,
        blob_list,
        cleanup_handler.interrupted.clone(),
        json_out,
    )
    .await;

    if result.is_err() && cleanup_handler.is_interrupted() {
        tracing::info!(target: "copy", "Copy interrupted by user");
        return Err(fail("Copy interrupted by user.", CopyError::Interrupted));
    }

    result.map_err(|e| fail(format!("Copy failed: {e}"), CopyError::CopyFailed))?;

    if json_out {
        #[derive(Serialize)]
        struct CopyCompleteMsg {
            duration_seconds: f64,
        }
        ui::json::emit_static(
            "copy_complete",
            &CopyCompleteMsg {
                duration_seconds: start.elapsed().as_secs_f64(),
            },
        );
    } else {
        ui::cli::log!(
            "Finished in {}",
            utils::pretty_print_duration(start.elapsed())
        );
    }

    tracing::info!(target: "copy", "Copy command completed in {:?}", start.elapsed());
    Ok(())
}

const SHORT_SNAPSHOT_ID_LEN: usize = 8;

/// Select snapshots from source based on filters.
async fn select_snapshots(
    repo: Arc<Repository>,
    snapshot_filters: Option<Vec<String>>,
    host_filters: Vec<String>,
    tag_filters: Vec<String>,
) -> Result<Vec<(ID, crate::repository::snapshot::Snapshot)>> {
    use crate::repository::snapshot::SnapshotEntry;

    let stream = SnapshotStream::new(repo.clone()).await?;
    let mut entries: Vec<SnapshotEntry> = stream.collect_entries(true).await?;

    if let Some(prefixes) = &snapshot_filters {
        entries.retain(|e| {
            let hex = e.id.to_hex();
            prefixes.iter().any(|p| hex.starts_with(p))
        });
    }

    if !host_filters.is_empty() {
        entries = filter_snapshots_by_hosts(entries.iter(), &host_filters)
            .into_iter()
            .cloned()
            .collect();
    }

    if !tag_filters.is_empty() {
        let tags: BTreeSet<String> = tag_filters.into_iter().collect();
        entries.retain(|e| e.snapshot.has_tags(&tags));
    }

    Ok(entries.into_iter().map(|e| (e.id, e.snapshot)).collect())
}

/// Collect all blob IDs and their total raw size from the selected snapshots.
async fn collect_blobs(
    src_repo: Arc<Repository>,
    snapshots: &[(ID, crate::repository::snapshot::Snapshot)],
    json_out: bool,
) -> Result<(Vec<(ID, BlobType)>, u64)> {
    let mut blob_list: Vec<(ID, BlobType)> = Vec::new();
    let mut tree_stack: Vec<ID> = Vec::new();
    let mut visited_trees = std::collections::HashSet::new();
    let mut total_bytes: u64 = 0;

    for (_snap_id, snap) in snapshots {
        tree_stack.push(snap.tree);
    }

    while let Some(tree_id) = tree_stack.pop() {
        if !visited_trees.insert(tree_id) {
            continue;
        }
        blob_list.push((tree_id, BlobType::Tree));
        if let Some(loc) = src_repo.index().get(&tree_id) {
            total_bytes += loc.raw_length as u64;
        }

        match Tree::load_from_repo(src_repo.as_ref(), &tree_id).await {
            Ok(tree) => {
                for node in tree.nodes {
                    if let Some(subtree_id) = node.tree {
                        tree_stack.push(subtree_id);
                    }
                    if let Some(blob_ids) = node.blobs {
                        for blob_id in blob_ids {
                            blob_list.push((blob_id, BlobType::Data));
                            if let Some(loc) = src_repo.index().get(&blob_id) {
                                total_bytes += loc.raw_length as u64;
                            }
                        }
                    }
                }
            }
            Err(e) => {
                tracing::warn!(target: "copy", "Failed to load tree {tree_id}: {e}");
                if json_out {
                    #[derive(Serialize)]
                    struct TreeErrorMsg {
                        tree_id: String,
                        error: String,
                    }
                    ui::json::emit_static(
                        "copy_tree_error",
                        &TreeErrorMsg {
                            tree_id: tree_id.to_hex(),
                            error: e.to_string(),
                        },
                    );
                }
            }
        }
    }

    Ok((blob_list, total_bytes))
}

/// Copy selected snapshots from source to destination.
async fn copy_snapshots(
    src_repo: Arc<Repository>,
    dst_repo: Arc<Repository>,
    snapshots: Vec<(ID, crate::repository::snapshot::Snapshot)>,
    blob_list: Vec<(ID, BlobType)>,
    shutdown_signal: Arc<AtomicBool>,
    json_out: bool,
) -> Result<()> {
    let total_blobs = blob_list.len();

    // Filter out blobs already present in destination
    let dest_index = dst_repo.index();
    let to_copy: Vec<(ID, BlobType)> = blob_list
        .into_iter()
        .filter(|(id, _)| !dest_index.contains(id))
        .collect();

    if to_copy.is_empty() {
        if !json_out && total_blobs > 0 {
            ui::cli::log!(
                "{}",
                "All blobs already present in destination.".bold().green()
            );
        }
        write_snapshots_to_dest(&src_repo, &dst_repo, &snapshots, json_out).await?;
        return Ok(());
    }

    let transfer_bytes: u64 = to_copy
        .iter()
        .filter_map(|(id, _)| src_repo.index().get(id))
        .map(|loc| loc.raw_length as u64)
        .sum();

    if !json_out {
        ui::cli::log!(
            "{} {} ({})",
            "To transfer:".cyan().bold(),
            utils::format_count(to_copy.len(), "blob", "blobs"),
            utils::format_size_binary(transfer_bytes, 2)
        );
    }

    // Transfer blobs

    dst_repo.init_pack_saver(4)?;
    let bytes_copied = Arc::new(AtomicU64::new(0));

    let copy_rate = Arc::new(Mutex::new(RateEstimator::new(UI_RATE_ESTIMATOR_WINDOW)));
    let bar = if !json_out {
        Some(
            ProgressBar::with_draw_target(Some(transfer_bytes), default_bar_draw_target())
                .with_style(
                    ProgressStyle::default_bar()
                        .template("[{percent}%] [{bar:20.cyan/white}] {bytes_fmt} / {total_fmt} [{binary_bytes_per_sec}] [ETA: {custom_eta}]")
                        .expect("invalid progress bar template for copy")
                        .progress_chars("=> ")
                        .with_key("bytes_fmt", {
                            move |state: &ProgressState, w: &mut dyn fmt::Write| {
                                let _ = write!(w, "{}", utils::format_size_binary(state.pos(), 2));
                            }
                        })
                        .with_key("total_fmt", {
                            move |state: &ProgressState, w: &mut dyn fmt::Write| {
                                let _ = write!(w, "{}", utils::format_size_binary(state.len().unwrap_or(0), 2));
                            }
                        })
                        .with_key("custom_eta", {
                            let re = copy_rate.clone();
                            move |state: &ProgressState, w: &mut dyn fmt::Write| {
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
                        }),
                ),
        )
    } else {
        None
    };

    let mut stream = stream::iter(to_copy)
        .map(|(id, blob_type)| {
            let src_repo = src_repo.clone();
            let dst_repo = dst_repo.clone();
            let shutdown_signal = shutdown_signal.clone();
            let bytes_copied = bytes_copied.clone();

            async move {
                if shutdown_signal.load(Ordering::Acquire) {
                    bail!("Interrupted");
                }

                let data = src_repo.load_blob(&id).await?;
                let size = data.len() as u64;

                tokio::task::spawn_blocking(move || {
                    dst_repo.encode_and_save_blob(
                        blob_type,
                        WriteContents::Owned(data),
                        SaveID::WithID(id),
                    )
                })
                .await
                .map_err(|e| anyhow::anyhow!("Encoding task failed: {e}"))??;

                let copied = bytes_copied.fetch_add(size, Ordering::Relaxed) + size;

                if json_out {
                    #[derive(Serialize)]
                    struct CopyStatusMsg {
                        phase: String,
                        copied: u64,
                        total: u64,
                    }
                    ui::json::emit_static(
                        "copy_status",
                        &CopyStatusMsg {
                            phase: "copy".to_string(),
                            copied,
                            total: transfer_bytes,
                        },
                    );
                }

                Ok::<_, anyhow::Error>(())
            }
        })
        .buffer_unordered(8);

    while let Some(result) = stream.next().await {
        result?;
        if let Some(ref bar) = bar {
            let pos = bytes_copied.load(Ordering::Relaxed);
            bar.set_position(pos);
            copy_rate.lock().observe(pos as f64);
        }
    }

    if let Some(ref bar) = bar {
        bar.finish_and_clear();
    }

    // Flush pack saver (persist all pending packs + index)
    if !json_out {
        ui::cli::log!("{}", "Persisting destination index...".cyan().bold());
    }
    dst_repo.flush_and_finalize_pack_saver().await?;

    // Write snapshots to destination
    write_snapshots_to_dest(&src_repo, &dst_repo, &snapshots, json_out).await?;

    Ok(())
}

/// Write snapshot metadata to the destination repository.
async fn write_snapshots_to_dest(
    src_repo: &Repository,
    dst_repo: &Repository,
    snapshots: &[(ID, crate::repository::snapshot::Snapshot)],
    json_out: bool,
) -> Result<()> {
    let total = snapshots.len();
    if !json_out && total > 0 {
        ui::cli::log!("{}", "Writing snapshots...".cyan().bold());
    }

    for (i, (snap_id, _snap)) in snapshots.iter().enumerate() {
        let raw_data = src_repo
            .load_file(
                snap_id,
                crate::backend::StorageHint {
                    file_type: ContentIdType::Snapshot,
                    is_metadata: true,
                },
                None,
            )
            .await?;

        dst_repo
            .save_file(
                &SaveID::WithID(*snap_id),
                &raw_data,
                crate::backend::StorageHint {
                    file_type: ContentIdType::Snapshot,
                    is_metadata: true,
                },
                None,
            )
            .await?;

        if json_out {
            #[derive(Serialize)]
            struct SnapshotCopiedMsg {
                snapshot_id: String,
                processed: usize,
                total: usize,
            }
            ui::json::emit_static(
                "snapshot_copied",
                &SnapshotCopiedMsg {
                    snapshot_id: snap_id.to_hex(),
                    processed: i + 1,
                    total,
                },
            );
        } else {
            ui::cli::log!(
                "  {} ({}/{})",
                snap_id.to_short_hex(SHORT_SNAPSHOT_ID_LEN).yellow().bold(),
                i + 1,
                total
            );
        }
    }

    Ok(())
}
