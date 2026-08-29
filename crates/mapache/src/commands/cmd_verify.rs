use std::{
    io,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Instant,
};

use clap::Args;
use futures::StreamExt;
use indicatif::{ProgressBar, ProgressState};
use parking_lot::Mutex;
use serde::Serialize;

use crate::{
    backend::new_backend_with_prompt,
    commands::{GlobalArgs, HookArgs, ToExitCode, cleanup::CleanupHandler, with_repository_lock},
    common::{
        ContentIdType, ID, config::CommandHooks, defaults::UI_RATE_ESTIMATOR_WINDOW,
        error::MapacheError, global::GlobalOpts, hooks,
    },
    fs::tree::SerializedNodeStream,
    repository::{
        lock::LockHandle,
        repo::Repository,
        snapshot::SnapshotStream,
        storage::SecureStorage,
        verify::{verify_metadata_file, verify_pack, verify_snapshot_refs},
    },
    ui::{self, cli::color::Colorize, default_bar_draw_target, default_progress_style},
    utils::{self, collections::IdSet, rate_estimator::RateEstimator},
};

#[derive(Serialize)]
struct VerifyStartMsg {
    read_packs: bool,
    sample: Option<f64>,
}

#[derive(Serialize, Default)]
struct VerifyProgressMsg {
    phase: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    missing_packs: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    corrupt_blobs: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pack_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    packs_total: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    packs_processed: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    packs_corrupt: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    blobs_verified: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    blobs_dangling: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    failed_early: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    snapshot_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    snapshots_total: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    snapshots_processed: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    snapshots_corrupt: Option<usize>,
}

#[derive(Serialize)]
struct VerifyErrorMsg {
    #[serde(skip_serializing_if = "Option::is_none")]
    file_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    file_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pack_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    snapshot: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    blob_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<String>,
    error: String,
}

impl VerifyErrorMsg {
    fn metadata(file_type: impl std::fmt::Display, file_id: &ID, error: impl Into<String>) -> Self {
        Self {
            file_type: Some(file_type.to_string()),
            file_id: Some(file_id.to_hex()),
            pack_id: None,
            snapshot: None,
            blob_id: None,
            path: None,
            error: error.into(),
        }
    }

    fn pack(pack_id: &ID, error: impl Into<String>) -> Self {
        Self {
            file_type: None,
            file_id: None,
            pack_id: Some(pack_id.to_hex()),
            snapshot: None,
            blob_id: None,
            path: None,
            error: error.into(),
        }
    }

    fn snapshot(snapshot_id: &ID, error: impl Into<String>) -> Self {
        Self {
            file_type: None,
            file_id: None,
            pack_id: None,
            snapshot: Some(snapshot_id.to_short_hex(12)),
            blob_id: None,
            path: None,
            error: error.into(),
        }
    }

    fn blob(blob_id: &ID, path: &Path, snapshot_id: &ID) -> Self {
        Self {
            file_type: None,
            file_id: None,
            pack_id: None,
            snapshot: Some(snapshot_id.to_short_hex(12)),
            blob_id: Some(blob_id.to_short_hex(8)),
            path: Some(path.display().to_string()),
            error: "corrupt blob affects this file".to_string(),
        }
    }

    fn simple(error: impl Into<String>) -> Self {
        Self {
            file_type: None,
            file_id: None,
            pack_id: None,
            snapshot: None,
            blob_id: None,
            path: None,
            error: error.into(),
        }
    }
}

#[derive(Serialize)]
struct VerifyCompleteMsg {
    duration_seconds: f64,
    packs_processed: usize,
    packs_corrupt: usize,
    packs_repaired: usize,
    blobs_verified: usize,
    blobs_dangling: usize,
    snapshots_verified: usize,
    snapshots_corrupt: usize,
    metadata_files_corrupt: usize,
    passed: bool,
    failed_early: bool,
    read_packs: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum VerifyError {
    #[error("failed to open repository: {0}")]
    RepoOpenFail(String),
    #[error("corrupt packs detected: {0}")]
    CorruptPacks(String),
    #[error("corrupt snapshots detected: {0}")]
    CorruptSnapshots(String),
    #[error("corrupt metadata files detected: {0}")]
    CorruptMetadata(String),
    #[error("verification failed: {0}")]
    VerifyFailed(String),
    #[error("verify interrupted by user")]
    Interrupted,
    #[error(transparent)]
    Repo(#[from] MapacheError),
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl ToExitCode for VerifyError {
    fn to_exit_code(&self) -> i32 {
        match self {
            VerifyError::RepoOpenFail(_) => 10,
            VerifyError::CorruptPacks(_) => 20,
            VerifyError::CorruptSnapshots(_) => 21,
            VerifyError::CorruptMetadata(_) => 23,
            VerifyError::VerifyFailed(_) => 22,
            VerifyError::Interrupted => 130,
            VerifyError::Repo(_) => 1,
            VerifyError::Io(_) => 1,
        }
    }
}

#[derive(Args, Debug, Clone)]
#[clap(
    about = "Verify the integrity of the data stored in the repository",
    long_about = "Verify the integrity of the data stored in the repository. \
                  By default, it checks logical consistency (snapshots point to known index entries). \
                  Use --read-packs to enforce a full physical verification (decryption + checksums)."
)]
pub struct CmdArgs {
    /// Read, decrypt, and hash ALL data in the repository (Slow but thorough)
    #[clap(long, default_value_t = false)]
    pub read_packs: bool,

    /// Number of packs to process in parallel. N must be greater than 0.
    #[clap(
        short,
        long,
        default_value_t = 4,
        requires = "read_packs",
        value_parser = parse_parallel
    )]
    pub parallel: usize,

    /// Use local cache
    #[clap(long, default_value_t = false)]
    pub with_cache: bool,

    /// Fail early on first error encountered, but still show the final report
    #[clap(long, default_value_t = false)]
    pub fail_early: bool,

    /// Verify only a random percentage of packs (e.g. 10.5%)
    #[clap(long, value_parser = parse_sample_percentage, requires = "read_packs")]
    pub sample: Option<f64>,

    /// Attempt to repair corrupt files using ECC sidecars.
    /// Requires the repository to have ECC enabled (`--ecc` at init time).
    #[clap(long, default_value_t = false)]
    pub repair: bool,

    #[clap(flatten)]
    pub hook_args: HookArgs,
}

fn parse_sample_percentage(s: &str) -> Result<f64, String> {
    if !s.ends_with('%') {
        return Err("sample percentage must end with '%' (e.g. 10.5%)".to_string());
    }
    let num_str = &s[..s.len() - 1];
    let val = num_str
        .parse::<f64>()
        .map_err(|_| format!("'{}' is not a valid number", num_str))?;

    if !(0.0..=100.0).contains(&val) {
        return Err("sample percentage must be between 0 and 100".to_string());
    }
    Ok(val)
}

fn parse_parallel(s: &str) -> Result<usize, String> {
    let n = s
        .parse::<usize>()
        .map_err(|_| format!("'{s}' is not a valid number"))?;
    if n == 0 {
        return Err("parallel must be greater than 0".to_string());
    }
    Ok(n)
}

struct VerifyStats {
    packs_processed: AtomicUsize,
    packs_corrupt: AtomicUsize,
    packs_repaired: AtomicUsize,
    blobs_verified: AtomicUsize,
    blobs_dangling: AtomicUsize,
    metadata_files_processed: AtomicUsize,
    metadata_files_corrupt: AtomicUsize,
    metadata_files_repaired: AtomicUsize,
}

impl VerifyStats {
    fn new() -> Self {
        Self {
            packs_processed: AtomicUsize::new(0),
            packs_corrupt: AtomicUsize::new(0),
            packs_repaired: AtomicUsize::new(0),
            blobs_verified: AtomicUsize::new(0),
            blobs_dangling: AtomicUsize::new(0),
            metadata_files_processed: AtomicUsize::new(0),
            metadata_files_corrupt: AtomicUsize::new(0),
            metadata_files_repaired: AtomicUsize::new(0),
        }
    }
}

struct VerifyCtx<'a> {
    repo: Arc<Repository>,
    secure_storage: Arc<SecureStorage>,
    stats: &'a VerifyStats,
    corrupt_blobs: &'a Arc<parking_lot::Mutex<IdSet<ID>>>,
    cleanup_handler: &'a CleanupHandler,
    json_out: bool,
    parallel: usize,
    fail_early: bool,
    is_sampled: bool,
    repair: bool,
}

struct VerifyReport<'a> {
    start: Instant,
    stats: &'a VerifyStats,
    snapshots_corrupt: usize,
    num_snapshots_total: usize,
    physical_failed_early: bool,
    logical_failed_early: bool,
    read_packs: bool,
    json_out: bool,
}

pub async fn run(
    global_args: &GlobalArgs,
    args: &CmdArgs,
    cmd_hooks: Option<&CommandHooks>,
) -> Result<(), VerifyError> {
    let json_out = global_args.json;
    tracing::info!(target: "verify", "Starting verify command");
    if !json_out && global_args.no_cache {
        ui::cli::warning!(
            "--no-cache has no effect on this command. \
             The local cache is disabled by default. \
             Use --with-cache to enable it."
        );
    }

    let repo_result = with_repository_lock(
        global_args.auth_file.as_ref(),
        global_args.key.as_ref(),
        new_backend_with_prompt(global_args.backend_options(false))
            .await
            .map_err(|e| {
                VerifyError::VerifyFailed(format!("failed to initialize backend: {}", e.inner()))
            })?,
        {
            let mut config = global_args.to_repo_config();
            config.use_cache = args.with_cache;
            config
        },
        false,
        global_args.retry_lock_duration,
        global_args.no_lock,
        |repo, secure_storage, lock_handle| async move {
            hooks::run_command_pre(
                cmd_hooks,
                "verify",
                &global_args.repo,
                args.hook_args.pre_hook.as_deref(),
                false,
            )
            .await?;

            run_with_repo(repo, secure_storage, lock_handle, args, json_out).await
        },
    )
    .await;

    hooks::run_command_post(
        cmd_hooks,
        "verify",
        &global_args.repo,
        &repo_result,
        args.hook_args.post_hook.as_deref(),
        false,
    )
    .await;

    repo_result.map_err(|e| match e {
        VerifyError::Repo(err) => VerifyError::RepoOpenFail(err.inner()),
        other => other,
    })
}

pub async fn run_with_repo(
    repo: Arc<Repository>,
    secure_storage: Arc<SecureStorage>,
    lock_handle: Option<LockHandle>,
    args: &CmdArgs,
    json_out: bool,
) -> Result<(), VerifyError> {
    let cleanup_handler = CleanupHandler::new_with_callback(move || {
        ui::cli::log!(
            "\n{}",
            "Process interrupted. Cleaning up...".bold().yellow()
        );
    });
    cleanup_handler.add_lock(lock_handle);

    if args.repair && repo.manifest().ecc().is_none() {
        return Err(VerifyError::VerifyFailed(
            "ECC is not enabled on this repository. \
             Use --ecc <PERCENT> when initializing to enable error correction."
                .to_string(),
        ));
    }

    let start = Instant::now();

    if json_out {
        ui::json::emit_static(
            "verify_start",
            &VerifyStartMsg {
                read_packs: args.read_packs,
                sample: args.sample,
            },
        );
    }

    repo.reload_master_index().await?;

    let stats = VerifyStats::new();
    let packs_all = repo.list_packs().await?;

    // Sampling (Optional)
    let mut packs_to_verify = packs_all.iter().cloned().collect::<Vec<_>>();
    if let Some(sample_pct) = args.sample {
        use rand::seq::SliceRandom;
        let mut rng = rand::rng();
        let target_count = sample_pack_count(packs_to_verify.len(), sample_pct);

        ui::cli::log!(
            "{} verifying {} out of {} packs ({:.2}% of the packs).\n",
            "Sampling:".bold().cyan(),
            target_count.to_string().bold(),
            packs_to_verify.len(),
            sample_pct
        );
        packs_to_verify.shuffle(&mut rng);
        packs_to_verify.truncate(target_count);
    }

    let corrupt_blobs = Arc::new(parking_lot::Mutex::new(IdSet::default()));

    check_index_consistency(&repo, &packs_all, args, json_out).await?;

    let physical_failed_early = if args.read_packs {
        let verify_ctx = VerifyCtx {
            repo: repo.clone(),
            secure_storage: secure_storage.clone(),
            stats: &stats,
            corrupt_blobs: &corrupt_blobs,
            cleanup_handler: &cleanup_handler,
            json_out,
            parallel: args.parallel,
            fail_early: args.fail_early,
            is_sampled: args.sample.is_some(),
            repair: args.repair,
        };
        let failed_early = verify_packs_physically(&verify_ctx, &packs_to_verify).await?;
        if cleanup_handler.is_interrupted() {
            tracing::info!(target: "verify", "Verify interrupted by user");
            return Err(VerifyError::Interrupted);
        }
        failed_early
    } else {
        false
    };
    // Verify metadata files (index, snapshot, manifest).
    // Always runs; ECC repair only when --repair is set and ECC is enabled.
    if !cleanup_handler.is_interrupted() {
        verify_metadata_files(
            repo.clone(),
            secure_storage.clone(),
            &stats,
            args.repair && repo.manifest().ecc().is_some(),
            json_out,
            &cleanup_handler,
        )
        .await?;
    }
    let mut logical_failed_early = false;
    let mut snapshots_corrupt = 0;
    let mut num_snapshots_total = 0;
    let verified_trees = Arc::new(utils::collections::ShardedIdSet::new());

    if (!physical_failed_early || !args.fail_early) && !cleanup_handler.is_interrupted() {
        (logical_failed_early, snapshots_corrupt, num_snapshots_total) =
            verify_snapshots_logically(
                repo.clone(),
                &packs_all,
                &verified_trees,
                args,
                json_out,
                &cleanup_handler,
            )
            .await?;
    } else if cleanup_handler.is_interrupted() {
        tracing::info!(target: "verify", "Verify interrupted by user");
        return Err(VerifyError::Interrupted);
    }

    // Back-referencing Corruption
    if !corrupt_blobs.lock().is_empty() {
        ui::cli::log!();
        ui::cli::log!("{}", "Analyzing impact of corruption...".bold().red());

        if json_out {
            ui::json::emit_static(
                "verify_progress",
                &VerifyProgressMsg {
                    phase: "corruption",
                    corrupt_blobs: Some(corrupt_blobs.lock().len()),
                    ..VerifyProgressMsg::default()
                },
            );
        }

        let snapshot_ids = repo.list_snapshot_ids().await?;

        futures::stream::iter(snapshot_ids)
            .map(|snapshot_id| {
                let repo = repo.clone();
                let corrupt_blobs = corrupt_blobs.clone();
                async move {
                    let mut stream = SerializedNodeStream::new(
                        repo.clone(),
                        Some(repo.load_snapshot(&snapshot_id, None).await?.tree),
                        PathBuf::new(),
                        None,
                        None,
                    )
                    .await?;

                    while let Some(res) = stream.next().await {
                        let (path, sn_node_res) = res?;
                        let sn_node = sn_node_res?;
                        let node = sn_node.node;
                        let blobs = match node.blobs {
                            Some(b) => b,
                            None => continue,
                        };

                        let corrupt_ids = corrupt_blobs.lock();
                        for blob_id in blobs {
                            if !corrupt_ids.contains(&blob_id) {
                                continue;
                            }

                            ui::cli::error!(
                                "Corrupt blob {} affects file \"{}\" in snapshot {}",
                                blob_id.to_short_hex(8).red(),
                                path.display().to_string().bold(),
                                snapshot_id.to_short_hex(12).yellow()
                            );

                            if json_out {
                                emit_blob_corruption_json(&blob_id, &path, &snapshot_id);
                            }
                        }
                    }
                    Ok::<(), MapacheError>(())
                }
            })
            .buffer_unordered(4)
            .collect::<Vec<_>>()
            .await;
    }

    let final_report = VerifyReport {
        start,
        stats: &stats,
        snapshots_corrupt,
        num_snapshots_total,
        physical_failed_early,
        logical_failed_early,
        read_packs: args.read_packs,
        json_out,
    };
    emit_final_report(&final_report)?;

    Ok(())
}

async fn check_index_consistency(
    repo: &Repository,
    packs_all: &IdSet<ID>,
    args: &CmdArgs,
    json_out: bool,
) -> Result<IdSet<ID>, VerifyError> {
    ui::cli::log!("{}", "Verifying Index Consistency...".bold());
    tracing::info!(target: "verify", "Verifying index consistency");
    let mut missing_packs = IdSet::default();
    repo.index().for_each_pack_id(|pack_id| {
        if !packs_all.contains(pack_id) {
            missing_packs.insert(*pack_id);
        }
    });

    if !missing_packs.is_empty() {
        tracing::error!(
            target: "verify",
            "Index refers to {} missing packs",
            missing_packs.len()
        );
        ui::cli::error!(
            "Index refers to {} missing packs!",
            missing_packs.len().to_string().bold().red()
        );
        for p in &missing_packs {
            ui::cli::log!("  - Missing Pack: {}", p);
        }
        if args.fail_early {
            return Err(VerifyError::VerifyFailed(
                "index consistency check failed.".to_string(),
            ));
        }
    } else {
        tracing::info!(target: "verify", "Index consistency check passed");
        ui::cli::log!(
            "{} {}",
            "Index consistency check passed.".bold().green(),
            "All indexed blobs point to existing packs."
        );
    }

    if json_out {
        ui::json::emit_static(
            "verify_progress",
            &VerifyProgressMsg {
                phase: "index",
                missing_packs: Some(missing_packs.len()),
                ..VerifyProgressMsg::default()
            },
        );
    }

    ui::cli::log!();

    Ok(missing_packs)
}

async fn verify_metadata_files(
    repo: Arc<Repository>,
    secure_storage: Arc<SecureStorage>,
    stats: &VerifyStats,
    repair: bool,
    json_out: bool,
    cleanup_handler: &CleanupHandler,
) -> Result<(), VerifyError> {
    if repair {
        ui::cli::log!("{}", "Verifying Metadata Files (ECC)...".bold());
    } else {
        ui::cli::log!("{}", "Verifying Metadata Files...".bold());
    }

    let mut files_to_verify: Vec<(ContentIdType, Option<ID>, std::path::PathBuf)> = Vec::new();

    if let Ok(index_ids) = repo.list_index_ids().await {
        for id in index_ids {
            let path = repo.get_path(ContentIdType::Index, &id);
            files_to_verify.push((ContentIdType::Index, Some(id), path));
        }
    }

    if let Ok(snapshot_ids) = repo.list_snapshot_ids().await {
        for id in snapshot_ids {
            let path = repo.get_path(ContentIdType::Snapshot, &id);
            files_to_verify.push((ContentIdType::Snapshot, Some(id), path));
        }
    }

    let total = files_to_verify.len();
    if total == 0 {
        ui::cli::log!("No metadata files to verify.");
        return Ok(());
    }

    let style = default_progress_style()
        .template("[{elapsed}] [{bar:25.cyan/white}] {pos}/{len} files ({msg})")
        .expect("invalid progress bar template for verify metadata");

    let bar = ProgressBar::new(total as u64);
    bar.set_draw_target(default_bar_draw_target());
    bar.enable_steady_tick(GlobalOpts::progress_refresh_interval());
    bar.set_style(style);
    bar.set_message("OK");

    let interrupted_flag = cleanup_handler.interrupted.clone();

    for (file_type, file_id, file_path) in &files_to_verify {
        if interrupted_flag.load(Ordering::Relaxed) {
            break;
        }

        match verify_metadata_file(
            repo.backend(),
            secure_storage.clone(),
            repo.repo_version(),
            *file_type,
            *file_id,
            file_path.clone(),
            repair,
        )
        .await
        {
            Ok(file_stats) => {
                stats
                    .metadata_files_processed
                    .fetch_add(1, Ordering::Relaxed);

                if file_stats.repaired {
                    stats
                        .metadata_files_repaired
                        .fetch_add(1, Ordering::Relaxed);
                    let label = file_stats
                        .file_id
                        .map(|id| id.to_short_hex(8))
                        .unwrap_or_else(|| file_path.display().to_string());
                    bar.suspend(|| {
                        ui::cli::log!(
                            "{} {} {} REPAIRED via ECC.",
                            "[REPAIRED]".green().bold(),
                            file_stats.file_type,
                            label
                        );
                    });
                } else if file_stats.bit_rot {
                    stats.metadata_files_corrupt.fetch_add(1, Ordering::Relaxed);
                    let label = file_stats
                        .file_id
                        .map(|id| id.to_short_hex(8))
                        .unwrap_or_else(|| file_path.display().to_string());
                    bar.suspend(|| {
                        ui::cli::error!(
                            "{} {} {} CORRUPT: ECC detected bit-rot.",
                            "[ERROR]".red().bold(),
                            file_stats.file_type,
                            label
                        );
                    });

                    if json_out {
                        ui::json::emit_static(
                            "verify_error",
                            &VerifyErrorMsg::metadata(
                                file_stats.file_type,
                                &file_stats.file_id.unwrap_or_default(),
                                "ECC detected bit-rot",
                            ),
                        );
                    }
                } else if !file_stats.readable {
                    stats.metadata_files_corrupt.fetch_add(1, Ordering::Relaxed);
                    let label = file_stats
                        .file_id
                        .map(|id| id.to_short_hex(8))
                        .unwrap_or_else(|| file_path.display().to_string());
                    let reason = if repair {
                        "file is unreadable and no ECC sidecar available"
                    } else {
                        "file is unreadable or corrupt"
                    };
                    bar.suspend(|| {
                        ui::cli::error!(
                            "{} {} {} CORRUPT: {}.",
                            "[ERROR]".red().bold(),
                            file_stats.file_type,
                            label,
                            reason
                        );
                    });

                    if json_out {
                        ui::json::emit_static(
                            "verify_error",
                            &VerifyErrorMsg::metadata(
                                file_stats.file_type,
                                &file_stats.file_id.unwrap_or_default(),
                                reason,
                            ),
                        );
                    }
                }
            }
            Err(e) => {
                stats.metadata_files_corrupt.fetch_add(1, Ordering::Relaxed);
                bar.suspend(|| {
                    ui::cli::error!("Failed to verify {}: {}", file_path.display(), e);
                });
            }
        }

        let corrupt = stats.metadata_files_corrupt.load(Ordering::Relaxed);
        if corrupt > 0 {
            bar.set_message(
                utils::format_count(corrupt, "ERROR", "ERRORS")
                    .red()
                    .to_string(),
            );
        } else {
            bar.set_message("OK".to_string());
        }
        bar.inc(1);
    }

    if cleanup_handler.is_interrupted() {
        bar.abandon();
        return Ok(());
    }

    bar.finish();

    let corrupt = stats.metadata_files_corrupt.load(Ordering::Relaxed);
    let repaired = stats.metadata_files_repaired.load(Ordering::Relaxed);
    if corrupt > 0 {
        ui::cli::error!("Metadata verification failed. {} corrupt file(s).", corrupt);
    } else {
        ui::cli::log!(
            "{} {} metadata files verified.",
            "Metadata verification passed.".bold().green(),
            total
        );
    }
    if repaired > 0 {
        ui::cli::log!(
            "{} {} repaired via ECC.",
            "[INFO]".green(),
            utils::format_count(repaired, "file was", "files were")
        );
    }
    ui::cli::log!();

    Ok(())
}

async fn verify_packs_physically(
    ctx: &VerifyCtx<'_>,
    packs_to_verify: &[ID],
) -> Result<bool, VerifyError> {
    let suffix = if ctx.is_sampled { " (sampled)" } else { "" };
    ui::cli::log!("{}", format!("Verifying Pack Integrity{suffix}...").bold());

    let verify_rate = Arc::new(Mutex::new(RateEstimator::new(UI_RATE_ESTIMATOR_WINDOW)));

    let style = default_progress_style()
        .template(
            "[{custom_elapsed}] [{bar:25.cyan/white}] [ETA: {custom_eta}] {pos}/{len} packs ({msg})",
        )
        .expect("invalid progress bar template for verify pack integrity")
        .with_key(
            "custom_elapsed",
            |state: &ProgressState, w: &mut dyn std::fmt::Write| {
                let _ = write!(w, "{}", utils::pretty_print_duration(state.elapsed()));
            },
        )
        .with_key(
            "custom_eta",
            {
                let re = verify_rate.clone();
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
        );

    let bar = ProgressBar::new(packs_to_verify.len() as u64);
    bar.set_draw_target(default_bar_draw_target());
    bar.enable_steady_tick(GlobalOpts::progress_refresh_interval());
    bar.set_style(style);
    bar.set_message("OK");

    let stop_flag = AtomicBool::new(false);
    let interrupted_flag = ctx.cleanup_handler.interrupted.clone();
    let total_packs = packs_to_verify.len();
    let json_out = ctx.json_out;

    futures::stream::iter(packs_to_verify.iter())
        .take_while(|_| {
            futures::future::ready(
                !stop_flag.load(Ordering::Relaxed) && !interrupted_flag.load(Ordering::Relaxed),
            )
        })
        .map(|pack_id| {
            let repo = ctx.repo.clone();
            let backend = repo.backend();
            let secure = ctx.secure_storage.clone();
            let bar = bar.clone();
            let verify_rate = verify_rate.clone();
            let stats = ctx.stats;
            let stop_flag = &stop_flag;
            let corrupt_blobs = ctx.corrupt_blobs.clone();

            async move {
                match verify_pack(
                    repo.clone(),
                    backend.clone(),
                    secure.clone(),
                    *pack_id,
                    ctx.repair,
                )
                .await
                {
                    Ok(pack_stats) => {
                        stats.packs_processed.fetch_add(1, Ordering::Relaxed);
                        stats
                            .blobs_verified
                            .fetch_add(pack_stats.verified_blobs, Ordering::Relaxed);
                        stats
                            .blobs_dangling
                            .fetch_add(pack_stats.dangling, Ordering::Relaxed);

                        if pack_stats.repaired {
                            stats.packs_repaired.fetch_add(1, Ordering::Relaxed);
                            bar.suspend(|| {
                                ui::cli::log!(
                                    "{} Pack {} REPAIRED via ECC.",
                                    "[REPAIRED]".green().bold(),
                                    pack_id
                                );
                            });
                        } else if pack_stats.bit_rot || !pack_stats.corrupt_blobs.is_empty() {
                            bar.suspend(|| {
                                if pack_stats.bit_rot {
                                    ui::cli::error!(
                                        "Pack {} CORRUPT: Bit-rot detected (file hash mismatch).",
                                        pack_id
                                    );
                                }
                                if !pack_stats.corrupt_blobs.is_empty() {
                                    ui::cli::error!(
                                        "Pack {} CORRUPT: {} found.",
                                        pack_id,
                                        utils::format_count(
                                            pack_stats.corrupt_blobs.len(),
                                            "damaged blob",
                                            "damaged blobs",
                                        )
                                    );
                                }
                            });

                            if json_out {
                                let mut parts = Vec::new();
                                if pack_stats.bit_rot {
                                    parts.push("bit-rot detected".to_string());
                                }
                                if !pack_stats.corrupt_blobs.is_empty() {
                                    parts.push(format!(
                                        "{} damaged blob(s)",
                                        pack_stats.corrupt_blobs.len()
                                    ));
                                }
                                ui::json::emit_static(
                                    "verify_error",
                                    &VerifyErrorMsg::pack(pack_id, parts.join("; ")),
                                );
                            }

                            stats.packs_corrupt.fetch_add(1, Ordering::Relaxed);

                            if !pack_stats.corrupt_blobs.is_empty() {
                                let mut corrupt_set = corrupt_blobs.lock();
                                for id in pack_stats.corrupt_blobs {
                                    corrupt_set.insert(id);
                                }
                            }

                            if ctx.fail_early {
                                stop_flag.store(true, Ordering::Relaxed);
                            }
                        }
                    }
                    Err(e) => {
                        bar.suspend(|| {
                            ui::cli::error!("Failed to process pack {}: {}", pack_id, e);
                        });

                        if json_out {
                            ui::json::emit_static(
                                "verify_error",
                                &VerifyErrorMsg::pack(pack_id, e.to_string()),
                            );
                        }

                        stats.packs_corrupt.fetch_add(1, Ordering::Relaxed);

                        if ctx.fail_early {
                            stop_flag.store(true, Ordering::Relaxed);
                        }
                    }
                }

                let corrupt = stats.packs_corrupt.load(Ordering::Relaxed);
                if corrupt > 0 {
                    bar.set_message(
                        utils::format_count(corrupt, "ERROR", "ERRORS")
                            .red()
                            .to_string(),
                    );
                } else {
                    bar.set_message("OK".to_string());
                }
                bar.inc(1);
                let pos = bar.position() as f64;
                verify_rate.lock().observe(pos);

                if json_out {
                    ui::json::emit_static(
                        "verify_progress",
                        &VerifyProgressMsg {
                            phase: "physical",
                            pack_id: Some(pack_id.to_hex()),
                            packs_total: Some(total_packs),
                            packs_processed: Some(stats.packs_processed.load(Ordering::Relaxed)),
                            packs_corrupt: Some(stats.packs_corrupt.load(Ordering::Relaxed)),
                            blobs_verified: Some(stats.blobs_verified.load(Ordering::Relaxed)),
                            blobs_dangling: Some(stats.blobs_dangling.load(Ordering::Relaxed)),
                            failed_early: Some(stop_flag.load(Ordering::Relaxed)),
                            ..VerifyProgressMsg::default()
                        },
                    );
                }
            }
        })
        .buffer_unordered(ctx.parallel)
        .collect::<()>()
        .await;

    if ctx.cleanup_handler.is_interrupted() {
        bar.abandon();
        return Ok(true);
    }

    bar.finish();
    let failed_early = stop_flag.load(Ordering::Relaxed);

    if ctx.stats.packs_corrupt.load(Ordering::Relaxed) > 0 {
        ui::cli::log!();
        if failed_early {
            ui::cli::warning!("Physical verification halted early due to errors.");
        } else {
            ui::cli::error!("Physical verification failed. The repository data is corrupt.");
        }
    } else {
        ui::cli::log!(
            "{} {} blobs verified.",
            "Physical verification passed.".bold().green(),
            ctx.stats.blobs_verified.load(Ordering::Relaxed)
        );
    }
    ui::cli::log!();

    Ok(failed_early)
}

async fn verify_snapshots_logically(
    repo: Arc<Repository>,
    packs_all: &IdSet<ID>,
    verified_trees: &Arc<utils::collections::ShardedIdSet>,
    args: &CmdArgs,
    json_out: bool,
    cleanup_handler: &CleanupHandler,
) -> Result<(bool, usize, usize), VerifyError> {
    ui::cli::log!("{}", "Verifying Snapshot References...".bold());

    let snapshots_corrupt = AtomicUsize::new(0);
    let snapshot_stream = SnapshotStream::new(repo.clone()).await?;
    let mut snapshots: Vec<(ID, chrono::DateTime<chrono::Local>)> = Vec::new();
    let mut stream = snapshot_stream;
    while let Some(res) = stream.next().await {
        match res {
            Ok((id, snapshot)) => snapshots.push((id, snapshot.timestamp)),
            Err(e) => {
                ui::cli::error!("Failed to load snapshot: {}", e);

                if json_out {
                    ui::json::emit_static(
                        "verify_error",
                        &VerifyErrorMsg::simple("failed to load snapshot"),
                    );
                }

                snapshots_corrupt.fetch_add(1, Ordering::Relaxed);
                if args.fail_early {
                    return Err(VerifyError::CorruptSnapshots(
                        "verification halted due to corrupted snapshot.".to_string(),
                    ));
                }
            }
        }
    }

    snapshots.sort_by_key(|(_, timestamp)| *timestamp);
    let num_snapshots_total = snapshots.len();

    let stop_flag = AtomicBool::new(false);
    let interrupted_flag = cleanup_handler.interrupted.clone();

    let mut stream = futures::stream::iter(snapshots.into_iter().enumerate())
        .take_while(|_| {
            futures::future::ready(
                !stop_flag.load(Ordering::Relaxed) && !interrupted_flag.load(Ordering::Relaxed),
            )
        })
        .map(|(i, (snapshot_id, _))| {
            let repo = repo.clone();
            let packs = packs_all;
            let verified_trees = verified_trees.clone();

            async move {
                let res =
                    verify_snapshot_refs(repo.clone(), &snapshot_id, packs, verified_trees).await;
                (i, snapshot_id, res, json_out)
            }
        })
        .buffered(4);

    while let Some((i, snapshot_id, res, json_out)) = stream.next().await {
        let msg = format!(
            "{} {}",
            snapshot_id.to_short_hex(12).bold().yellow(),
            format!("({}/{})", i + 1, num_snapshots_total).dimmed()
        );

        match res {
            Ok(_) => {
                ui::cli::log!("{} {}", msg, "[OK]".bold().green());
            }
            Err(e) => {
                ui::cli::log!("{} {}", msg, "[ERROR]".bold().red());
                ui::cli::error!("{}", e);

                if json_out {
                    ui::json::emit_static(
                        "verify_error",
                        &VerifyErrorMsg::snapshot(&snapshot_id, format!("{}", e)),
                    );
                }

                snapshots_corrupt.fetch_add(1, Ordering::Relaxed);

                if args.fail_early {
                    stop_flag.store(true, Ordering::Relaxed);
                }
            }
        }

        if json_out {
            ui::json::emit_static(
                "verify_progress",
                &VerifyProgressMsg {
                    phase: "logical",
                    snapshot_id: Some(snapshot_id.to_short_hex(12)),
                    snapshots_total: Some(num_snapshots_total),
                    snapshots_processed: Some(i + 1),
                    snapshots_corrupt: Some(snapshots_corrupt.load(Ordering::Relaxed)),
                    ..VerifyProgressMsg::default()
                },
            );
        }
    }

    let failed_early = stop_flag.load(Ordering::Relaxed);

    Ok((
        failed_early,
        snapshots_corrupt.load(Ordering::Relaxed),
        num_snapshots_total,
    ))
}

fn emit_final_report(report: &VerifyReport<'_>) -> Result<(), VerifyError> {
    let metadata_corrupt_count = report.stats.metadata_files_corrupt.load(Ordering::Relaxed);
    if report.json_out {
        let packs_corrupt_count = report.stats.packs_corrupt.load(Ordering::Relaxed);
        let packs_repaired_count = report.stats.packs_repaired.load(Ordering::Relaxed);
        let dangling_count = report.stats.blobs_dangling.load(Ordering::Relaxed);
        let passed = packs_corrupt_count == 0
            && report.snapshots_corrupt == 0
            && metadata_corrupt_count == 0;

        ui::json::emit_static(
            "verify_complete",
            &VerifyCompleteMsg {
                duration_seconds: report.start.elapsed().as_secs_f64(),
                packs_processed: report.stats.packs_processed.load(Ordering::Relaxed),
                packs_corrupt: packs_corrupt_count,
                packs_repaired: packs_repaired_count,
                blobs_verified: report.stats.blobs_verified.load(Ordering::Relaxed),
                blobs_dangling: dangling_count,
                snapshots_verified: report.num_snapshots_total,
                snapshots_corrupt: report.snapshots_corrupt,
                metadata_files_corrupt: metadata_corrupt_count,
                passed,
                failed_early: report.physical_failed_early || report.logical_failed_early,
                read_packs: report.read_packs,
            },
        );
    }

    let packs_corrupt_count = report.stats.packs_corrupt.load(Ordering::Relaxed);
    let packs_repaired_count = report.stats.packs_repaired.load(Ordering::Relaxed);
    let dangling_count = report.stats.blobs_dangling.load(Ordering::Relaxed);

    if packs_corrupt_count > 0 || report.snapshots_corrupt > 0 || metadata_corrupt_count > 0 {
        ui::cli::log!("{}", "VERIFICATION FAILED".bold().on_red());

        if packs_corrupt_count > 0 {
            ui::cli::log!(
                "- {} corrupt/unreadable.",
                utils::format_count(packs_corrupt_count, "pack", "packs")
            );
        }
        if metadata_corrupt_count > 0 {
            ui::cli::log!(
                "- {} with corrupted metadata.",
                utils::format_count(metadata_corrupt_count, "metadata file", "metadata files")
            );
        }
        if report.snapshots_corrupt > 0 {
            ui::cli::log!(
                "- {} with broken references.",
                utils::format_count(report.snapshots_corrupt, "snapshot", "snapshots")
            );
        }
        if report.physical_failed_early || report.logical_failed_early {
            ui::cli::log!(
                "{}",
                "Note: Verification was partial due to --fail-early.".dimmed()
            );
        }

        return Err(if packs_corrupt_count > 0 {
            VerifyError::CorruptPacks("repository integrity check failed".to_string())
        } else if metadata_corrupt_count > 0 {
            VerifyError::CorruptMetadata("metadata integrity check failed".to_string())
        } else {
            VerifyError::CorruptSnapshots("repository integrity check failed".to_string())
        });
    }

    if packs_repaired_count > 0 {
        ui::cli::log!(
            "{} {} (ECC repair successful).",
            "[INFO]".green(),
            utils::format_count(packs_repaired_count, "pack was", "packs were")
        );
    }

    if dangling_count > 0 {
        ui::cli::log!(
            "{} Found {} (run 'prune' to clean up).",
            "[INFO]".yellow(),
            utils::format_count(dangling_count, "unreferenced blob", "unreferenced blobs")
        );
    }

    if !report.read_packs {
        ui::cli::log!(
            "{} {} {}.\n",
            "Note:".bold().dimmed(),
            "Only references were checked. To verify data integrity, run this command with"
                .dimmed(),
            "--read-packs".dimmed().bold()
        );
    }

    ui::cli::log!(
        "{} Verified {} and {} in {}",
        "[SUCCESS]".bold().green(),
        utils::format_count(report.num_snapshots_total, "snapshot", "snapshots"),
        utils::format_count(
            report.stats.packs_processed.load(Ordering::Relaxed),
            "pack",
            "packs"
        ),
        utils::pretty_print_duration(report.start.elapsed())
    );
    tracing::info!(
        target: "verify",
        "Verify command completed successfully in {:?}",
        report.start.elapsed()
    );

    Ok(())
}

fn emit_blob_corruption_json(blob_id: &ID, path: &Path, snapshot_id: &ID) {
    ui::json::emit_static(
        "verify_error",
        &VerifyErrorMsg::blob(blob_id, path, snapshot_id),
    );
}

/// How many packs `--sample PCT%` should verify. `0%` and an empty pack list both yield 0.
fn sample_pack_count(pack_count: usize, sample_pct: f64) -> usize {
    if pack_count == 0 || sample_pct <= 0.0 {
        return 0;
    }
    let rounded = ((pack_count as f64) * (sample_pct / 100.0)).round() as usize;
    rounded.clamp(1, pack_count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser, Debug)]
    #[command(no_binary_name = true)]
    struct VerifyArgsParse {
        #[command(flatten)]
        args: CmdArgs,
    }

    #[test]
    fn parallel_rejects_zero() {
        let err = VerifyArgsParse::try_parse_from(["--read-packs", "--parallel", "0"])
            .expect_err("--parallel 0 must be rejected");
        assert!(
            err.to_string().contains("greater than 0"),
            "unexpected error message: {err}"
        );
    }

    #[test]
    fn parallel_accepts_positive() {
        let parsed = VerifyArgsParse::try_parse_from(["--read-packs", "--parallel", "8"])
            .expect("--parallel 8 must parse");
        assert_eq!(parsed.args.parallel, 8);
    }

    #[test]
    fn sample_zero_percent_verifies_no_packs() {
        assert_eq!(sample_pack_count(2, 0.0), 0);
        assert_eq!(sample_pack_count(10, 0.0), 0);
        assert_eq!(sample_pack_count(1, 0.0), 0);
    }

    #[test]
    fn sample_empty_pack_list_is_zero() {
        assert_eq!(sample_pack_count(0, 10.0), 0);
        assert_eq!(sample_pack_count(0, 0.0), 0);
        assert_eq!(sample_pack_count(0, 100.0), 0);
    }

    #[test]
    fn sample_positive_percent_keeps_at_least_one_when_packs_exist() {
        assert_eq!(sample_pack_count(10, 50.0), 5);
        assert_eq!(sample_pack_count(10, 100.0), 10);
        assert_eq!(sample_pack_count(10, 0.01), 1);
    }
}
