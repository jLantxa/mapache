use std::{
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Instant,
};

use clap::Args;
use futures::{StreamExt, TryStreamExt};
use serde::Serialize;

use crate::{
    backend::new_backend_with_prompt,
    commands::{GlobalArgs, HookArgs, ToExitCode, cleanup::CleanupHandler, with_repository_lock},
    common::{ContentIdType, ID, config::CommandHooks, error::MapacheError, hooks},
    fs::tree::SerializedNodeStream,
    repository::{
        lock::LockHandle,
        packer::Packer,
        repo::Repository,
        snapshot::SnapshotStream,
        storage::SecureStorage,
        verify::{verify_metadata_file, verify_pack, verify_snapshot_refs},
    },
    ui::{
        self,
        reporter::{
            CliVerifyReporter, PhaseStyle,
            VerifyMessage::{
                Error, Failure, FinalSuccess, Heading, HeadingError, Info, InfoSoft, Log, Note,
                Repaired, Success, Warning,
            },
            VerifyReporter,
        },
    },
    utils::{self, collections::IdSet},
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
    packs_missing: usize,
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
            VerifyError::Io(_) => 4,
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

    /// Dump every blob descriptor found in the pack footers to a plain-text
    /// file. Each line: `<blob_id> <type> <pack_id>`. Useful to cross-check
    /// packs against the index (e.g. locate phantom descriptors).
    #[clap(long, value_name = "FILE")]
    pub dump_pack_blobs: Option<PathBuf>,

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
    reporter: &'a dyn VerifyReporter,
    json_out: bool,
    parallel: usize,
    fail_early: bool,
    is_sampled: bool,
    repair: bool,
}

/// Result of a verification run, suitable for both CLI reporting and TUI
/// summaries.
#[derive(Debug, Clone)]
pub struct VerifySummary {
    pub duration: std::time::Duration,
    pub packs_processed: usize,
    pub packs_corrupt: usize,
    /// Packs the index still references that are absent from storage.
    pub packs_missing: usize,
    pub packs_repaired: usize,
    pub blobs_verified: usize,
    pub blobs_dangling: usize,
    pub snapshots_verified: usize,
    pub snapshots_corrupt: usize,
    pub metadata_files_corrupt: usize,
    pub passed: bool,
    pub failed_early: bool,
    pub read_packs: bool,
}

impl VerifySummary {
    /// The error a CLI caller should report for a failed verification, mapping
    /// to the historical exit codes.
    pub fn failure(&self) -> Option<VerifyError> {
        if self.packs_corrupt > 0 {
            Some(VerifyError::CorruptPacks(
                "repository integrity check failed".to_string(),
            ))
        } else if self.packs_missing > 0 {
            Some(VerifyError::CorruptPacks(
                "index references missing packs".to_string(),
            ))
        } else if self.metadata_files_corrupt > 0 {
            Some(VerifyError::CorruptMetadata(
                "metadata integrity check failed".to_string(),
            ))
        } else if self.snapshots_corrupt > 0 {
            Some(VerifyError::CorruptSnapshots(
                "repository integrity check failed".to_string(),
            ))
        } else {
            None
        }
    }
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

            let reporter: Arc<dyn VerifyReporter> = Arc::new(CliVerifyReporter::new());
            let summary = run_with_repo(
                repo,
                secure_storage,
                lock_handle,
                args,
                json_out,
                reporter,
                None,
            )
            .await?;
            match summary.failure() {
                Some(err) => Err(err),
                None => Ok(()),
            }
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
    reporter: Arc<dyn VerifyReporter>,
    interrupt: Option<Arc<AtomicBool>>,
) -> Result<VerifySummary, VerifyError> {
    let callback = {
        let reporter = reporter.clone();
        move || {
            reporter.message(Warning, "Process interrupted. Cleaning up...");
        }
    };
    let cleanup_handler = match interrupt {
        Some(flag) => CleanupHandler::new_with_interrupt_and_callback(flag, callback),
        None => CleanupHandler::new_with_callback(callback),
    };
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

    if let Some(dump_path) = args.dump_pack_blobs.as_ref() {
        dump_pack_blobs(
            repo.clone(),
            secure_storage.clone(),
            &packs_all,
            dump_path,
            reporter.as_ref(),
        )
        .await?;
    }

    // Sampling (Optional)
    let mut packs_to_verify = packs_all.iter().cloned().collect::<Vec<_>>();
    if let Some(sample_pct) = args.sample {
        use rand::seq::SliceRandom;
        let mut rng = rand::rng();
        let target_count = sample_pack_count(packs_to_verify.len(), sample_pct);

        reporter.message(
            Log,
            &format!(
                "Sampling: verifying {} out of {} packs ({:.2}% of the packs).",
                target_count,
                packs_to_verify.len(),
                sample_pct
            ),
        );
        reporter.message(Log, "");
        packs_to_verify.shuffle(&mut rng);
        packs_to_verify.truncate(target_count);
    }

    let corrupt_blobs = Arc::new(parking_lot::Mutex::new(IdSet::default()));

    let packs_missing =
        check_index_consistency(&repo, &packs_all, args, json_out, reporter.as_ref()).await?;

    let physical_failed_early = if args.read_packs {
        let verify_ctx = VerifyCtx {
            repo: repo.clone(),
            secure_storage: secure_storage.clone(),
            stats: &stats,
            corrupt_blobs: &corrupt_blobs,
            cleanup_handler: &cleanup_handler,
            reporter: reporter.as_ref(),
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
            reporter.as_ref(),
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
                reporter.as_ref(),
            )
            .await?;
    } else if cleanup_handler.is_interrupted() {
        tracing::info!(target: "verify", "Verify interrupted by user");
        return Err(VerifyError::Interrupted);
    }

    // Back-referencing Corruption
    if !corrupt_blobs.lock().is_empty() {
        reporter.message(Log, "");
        reporter.message(HeadingError, "Analyzing impact of corruption...");

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

        let traverse_results: Vec<Result<(), MapacheError>> = futures::stream::iter(snapshot_ids)
            .map(|snapshot_id| {
                let repo = repo.clone();
                let corrupt_blobs = corrupt_blobs.clone();
                let reporter = reporter.clone();
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

                            reporter.message(
                                Error,
                                &format!(
                                    "Corrupt blob {} affects file \"{}\" in snapshot {}",
                                    blob_id.to_short_hex(8),
                                    path.display(),
                                    snapshot_id.to_short_hex(12)
                                ),
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
            .collect()
            .await;

        // A failed traversal means we could not fully analyze a snapshot's
        // corruption impact; report it instead of silently discarding the result.
        for res in traverse_results {
            if let Err(e) = res {
                tracing::error!(
                    target: "verify",
                    "Failed to traverse snapshot for corruption analysis: {e}"
                );
                reporter.message(
                    Error,
                    &format!("Failed to analyze corruption impact: {e:#}"),
                );
                if args.fail_early {
                    return Err(VerifyError::VerifyFailed(format!(
                        "failed to analyze corruption impact: {e}"
                    )));
                }
            }
        }
    }

    let packs_corrupt = stats.packs_corrupt.load(Ordering::Relaxed);
    let metadata_files_corrupt = stats.metadata_files_corrupt.load(Ordering::Relaxed);
    let summary = VerifySummary {
        duration: start.elapsed(),
        packs_processed: stats.packs_processed.load(Ordering::Relaxed),
        packs_corrupt,
        packs_missing,
        packs_repaired: stats.packs_repaired.load(Ordering::Relaxed),
        blobs_verified: stats.blobs_verified.load(Ordering::Relaxed),
        blobs_dangling: stats.blobs_dangling.load(Ordering::Relaxed),
        snapshots_verified: num_snapshots_total,
        snapshots_corrupt,
        metadata_files_corrupt,
        passed: packs_corrupt == 0
            && packs_missing == 0
            && snapshots_corrupt == 0
            && metadata_files_corrupt == 0,
        failed_early: physical_failed_early || logical_failed_early,
        read_packs: args.read_packs,
    };
    emit_final_report(&summary, json_out, reporter.as_ref());

    Ok(summary)
}

async fn check_index_consistency(
    repo: &Repository,
    packs_all: &IdSet<ID>,
    args: &CmdArgs,
    json_out: bool,
    reporter: &dyn VerifyReporter,
) -> Result<usize, VerifyError> {
    reporter.message(Heading, "Verifying Index Consistency...");
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
        reporter.message(
            Error,
            &format!("Index refers to {} missing packs!", missing_packs.len()),
        );
        for p in &missing_packs {
            reporter.message(Log, &format!("  - Missing Pack: {p}"));
        }
        if args.fail_early {
            return Err(VerifyError::VerifyFailed(
                "index consistency check failed.".to_string(),
            ));
        }
    } else {
        tracing::info!(target: "verify", "Index consistency check passed");
        reporter.message(
            Success,
            "Index consistency check passed. All indexed blobs point to existing packs.",
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

    reporter.message(Log, "");

    Ok(missing_packs.len())
}

async fn verify_metadata_files(
    repo: Arc<Repository>,
    secure_storage: Arc<SecureStorage>,
    stats: &VerifyStats,
    repair: bool,
    json_out: bool,
    cleanup_handler: &CleanupHandler,
    reporter: &dyn VerifyReporter,
) -> Result<(), VerifyError> {
    if repair {
        reporter.message(Heading, "Verifying Metadata Files (ECC)...");
    } else {
        reporter.message(Heading, "Verifying Metadata Files...");
    }

    let mut files_to_verify: Vec<(ContentIdType, Option<ID>, std::path::PathBuf)> = Vec::new();

    let index_ids = repo.list_index_ids().await?;
    for id in index_ids {
        let path = repo.get_path(ContentIdType::Index, &id);
        files_to_verify.push((ContentIdType::Index, Some(id), path));
    }

    let snapshot_ids = repo.list_snapshot_ids().await?;
    for id in snapshot_ids {
        let path = repo.get_path(ContentIdType::Snapshot, &id);
        files_to_verify.push((ContentIdType::Snapshot, Some(id), path));
    }

    let total = files_to_verify.len();
    if total == 0 {
        reporter.message(Log, "No metadata files to verify.");
        return Ok(());
    }

    reporter.begin_phase("Metadata files", total as u64, PhaseStyle::Metadata);
    reporter.phase_progress(0, Some("OK"));

    let interrupted_flag = cleanup_handler.interrupted.clone();

    let mut processed: u64 = 0;
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
                    reporter.message(
                        Repaired,
                        &format!("{} {} REPAIRED via ECC.", file_stats.file_type, label),
                    );
                } else if file_stats.bit_rot {
                    stats.metadata_files_corrupt.fetch_add(1, Ordering::Relaxed);
                    let label = file_stats
                        .file_id
                        .map(|id| id.to_short_hex(8))
                        .unwrap_or_else(|| file_path.display().to_string());
                    reporter.message(
                        Error,
                        &format!(
                            "{} {} CORRUPT: ECC detected bit-rot.",
                            file_stats.file_type, label
                        ),
                    );

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
                    reporter.message(
                        Error,
                        &format!("{} {} CORRUPT: {}.", file_stats.file_type, label, reason),
                    );

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
                reporter.message(
                    Error,
                    &format!("Failed to verify {}: {e}", file_path.display()),
                );
            }
        }

        processed += 1;
        let corrupt = stats.metadata_files_corrupt.load(Ordering::Relaxed);
        let message = if corrupt > 0 {
            utils::format_count(corrupt, "ERROR", "ERRORS")
        } else {
            "OK".to_string()
        };
        reporter.phase_progress(processed, Some(&message));
    }

    if cleanup_handler.is_interrupted() {
        reporter.end_phase(true);
        return Ok(());
    }

    reporter.end_phase(false);

    let corrupt = stats.metadata_files_corrupt.load(Ordering::Relaxed);
    let repaired = stats.metadata_files_repaired.load(Ordering::Relaxed);
    if corrupt > 0 {
        reporter.message(
            Error,
            &format!("Metadata verification failed. {corrupt} corrupt file(s)."),
        );
    } else {
        reporter.message(
            Success,
            &format!("Metadata verification passed. {total} metadata files verified."),
        );
    }
    if repaired > 0 {
        reporter.message(
            Info,
            &format!(
                "{} repaired via ECC.",
                utils::format_count(repaired, "file was", "files were")
            ),
        );
    }
    reporter.message(Log, "");

    Ok(())
}

/// Concurrency for the pack footer dump.
const PACK_FOOTER_DUMP_CONCURRENCY: usize = 8;

/// Dumps every blob descriptor found in the repository's pack footers to a
/// plain-text file, one line per descriptor:
///
/// ```text
/// <blob_id_hex> <type> <pack_id_hex>
/// ```
///
/// Padding descriptors are skipped (already filtered out by the footer parser).
/// Lines are written as soon as each pack footer is parsed, so the memory
/// footprint stays bounded to a single pack regardless of repository size; the
/// output is not sorted (packs complete out of order). Duplicates are kept
/// intact on purpose, so the file can be cross-checked against the index to
/// spot phantom descriptor entries that the index-based scan cannot see.
async fn dump_pack_blobs(
    repo: Arc<Repository>,
    secure_storage: Arc<SecureStorage>,
    pack_ids: &IdSet<ID>,
    path: &Path,
    reporter: &dyn VerifyReporter,
) -> Result<(), VerifyError> {
    let pack_ids: Vec<ID> = pack_ids.iter().copied().collect();
    let total = pack_ids.len();

    reporter.message(
        Log,
        &format!(
            "Pack blobs: dumping blob descriptors from {total} packs to {}",
            path.display()
        ),
    );

    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent).map_err(VerifyError::Io)?;
    }
    let file = std::fs::File::create(path).map_err(VerifyError::Io)?;
    let writer = Arc::new(parking_lot::Mutex::new(io::BufWriter::new(file)));

    reporter.begin_phase("Pack blobs", total as u64, PhaseStyle::Generic);
    reporter.phase_progress(0, Some("dumping"));
    let done = AtomicUsize::new(0);

    futures::stream::iter(pack_ids)
        .map(|pack_id| {
            let repo = repo.clone();
            let backend = repo.backend();
            let secure_storage = secure_storage.clone();
            let writer = writer.clone();
            let done = &done;
            async move {
                let descriptors = Packer::parse_pack_footer(
                    repo.as_ref(),
                    backend.as_ref(),
                    secure_storage.as_ref(),
                    &pack_id,
                    secure_storage.nonce_at_end(),
                )
                .await
                .map_err(|e| {
                    VerifyError::VerifyFailed(format!(
                        "failed to parse footer for pack {}: {}",
                        pack_id.to_hex(),
                        e.inner()
                    ))
                })?;

                let mut lines = descriptors
                    .iter()
                    .map(|d| format!("{} {:?} {}", d.id.to_hex(), d.blob_type, pack_id.to_hex()))
                    .collect::<Vec<_>>();
                if lines.is_empty() {
                    let n = done.fetch_add(1, Ordering::Relaxed) + 1;
                    reporter.phase_progress(n as u64, Some(&format!("{n}/{total} packs")));
                    return Ok::<_, VerifyError>(());
                }
                lines.push(String::new());

                // Serialize writes through the lock: a single contiguous write is
                // atomic, so concurrent tasks never interleave lines mid-line.
                let mut writer = writer.lock();
                writer
                    .write_all(lines.join("\n").as_bytes())
                    .map_err(VerifyError::Io)?;

                let n = done.fetch_add(1, Ordering::Relaxed) + 1;
                reporter.phase_progress(n as u64, Some(&format!("{n}/{total} packs")));
                Ok::<_, VerifyError>(())
            }
        })
        .buffer_unordered(PACK_FOOTER_DUMP_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;

    reporter.end_phase(false);
    let mut writer = writer.lock();
    writer.flush().map_err(VerifyError::Io)?;
    drop(writer);

    Ok(())
}

async fn verify_packs_physically(
    ctx: &VerifyCtx<'_>,
    packs_to_verify: &[ID],
) -> Result<bool, VerifyError> {
    let suffix = if ctx.is_sampled { " (sampled)" } else { "" };
    ctx.reporter
        .message(Heading, &format!("Verifying Pack Integrity{suffix}..."));

    let total_packs = packs_to_verify.len();
    ctx.reporter
        .begin_phase("Pack integrity", total_packs as u64, PhaseStyle::Packs);
    ctx.reporter.phase_progress(0, Some("OK"));

    let stop_flag = AtomicBool::new(false);
    let interrupted_flag = ctx.cleanup_handler.interrupted.clone();
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
            let reporter = ctx.reporter;
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
                            reporter
                                .message(Repaired, &format!("Pack {pack_id} REPAIRED via ECC."));
                        } else if pack_stats.bit_rot || !pack_stats.corrupt_blobs.is_empty() {
                            if pack_stats.bit_rot {
                                reporter.message(
                                    Error,
                                    &format!(
                                        "Pack {} CORRUPT: Bit-rot detected (file hash mismatch).",
                                        pack_id
                                    ),
                                );
                            }
                            if !pack_stats.corrupt_blobs.is_empty() {
                                reporter.message(
                                    Error,
                                    &format!(
                                        "Pack {} CORRUPT: {} found.",
                                        pack_id,
                                        utils::format_count(
                                            pack_stats.corrupt_blobs.len(),
                                            "damaged blob",
                                            "damaged blobs",
                                        )
                                    ),
                                );
                            }

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
                        reporter.message(Error, &format!("Failed to process pack {pack_id}: {e}"));

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
                let done = stats.packs_processed.load(Ordering::Relaxed)
                    + stats.packs_corrupt.load(Ordering::Relaxed);
                let message = if corrupt > 0 {
                    utils::format_count(corrupt, "ERROR", "ERRORS")
                } else {
                    "OK".to_string()
                };
                reporter.phase_progress(done as u64, Some(&message));

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
        ctx.reporter.end_phase(true);
        return Ok(true);
    }

    ctx.reporter.end_phase(false);
    let failed_early = stop_flag.load(Ordering::Relaxed);

    if ctx.stats.packs_corrupt.load(Ordering::Relaxed) > 0 {
        ctx.reporter.message(Log, "");
        if failed_early {
            ctx.reporter
                .message(Warning, "Physical verification halted early due to errors.");
        } else {
            ctx.reporter.message(
                Error,
                "Physical verification failed. The repository data is corrupt.",
            );
        }
    } else {
        ctx.reporter.message(
            Success,
            &format!(
                "Physical verification passed. {} blobs verified.",
                ctx.stats.blobs_verified.load(Ordering::Relaxed)
            ),
        );
    }
    ctx.reporter.message(Log, "");

    Ok(failed_early)
}

async fn verify_snapshots_logically(
    repo: Arc<Repository>,
    packs_all: &IdSet<ID>,
    verified_trees: &Arc<utils::collections::ShardedIdSet>,
    args: &CmdArgs,
    json_out: bool,
    cleanup_handler: &CleanupHandler,
    reporter: &dyn VerifyReporter,
) -> Result<(bool, usize, usize), VerifyError> {
    reporter.message(Heading, "Verifying Snapshot References...");

    let snapshots_corrupt = AtomicUsize::new(0);
    let snapshot_stream = SnapshotStream::new(repo.clone()).await?;
    let mut snapshots: Vec<(ID, chrono::DateTime<chrono::Local>)> = Vec::new();
    let mut stream = snapshot_stream;
    while let Some(res) = stream.next().await {
        match res {
            Ok((id, snapshot)) => snapshots.push((id, snapshot.timestamp)),
            Err(e) => {
                reporter.message(Error, &format!("Failed to load snapshot: {e}"));

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
        let id = snapshot_id.to_short_hex(12);

        match res {
            Ok(_) => {
                reporter.check(&id, i + 1, num_snapshots_total, true);
            }
            Err(e) => {
                reporter.check(&id, i + 1, num_snapshots_total, false);
                reporter.message(Error, &format!("{e}"));

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

fn emit_final_report(summary: &VerifySummary, json_out: bool, reporter: &dyn VerifyReporter) {
    if json_out {
        ui::json::emit_static(
            "verify_complete",
            &VerifyCompleteMsg {
                duration_seconds: summary.duration.as_secs_f64(),
                packs_processed: summary.packs_processed,
                packs_corrupt: summary.packs_corrupt,
                packs_missing: summary.packs_missing,
                packs_repaired: summary.packs_repaired,
                blobs_verified: summary.blobs_verified,
                blobs_dangling: summary.blobs_dangling,
                snapshots_verified: summary.snapshots_verified,
                snapshots_corrupt: summary.snapshots_corrupt,
                metadata_files_corrupt: summary.metadata_files_corrupt,
                passed: summary.passed,
                failed_early: summary.failed_early,
                read_packs: summary.read_packs,
            },
        );
    }

    if !summary.passed {
        reporter.message(Failure, "VERIFICATION FAILED");

        if summary.packs_corrupt > 0 {
            reporter.message(
                Log,
                &format!(
                    "- {} corrupt/unreadable.",
                    utils::format_count(summary.packs_corrupt, "pack", "packs")
                ),
            );
        }
        if summary.packs_missing > 0 {
            reporter.message(
                Log,
                &format!(
                    "- {} referenced by the index but missing.",
                    utils::format_count(summary.packs_missing, "pack", "packs")
                ),
            );
        }
        if summary.metadata_files_corrupt > 0 {
            reporter.message(
                Log,
                &format!(
                    "- {} with corrupted metadata.",
                    utils::format_count(
                        summary.metadata_files_corrupt,
                        "metadata file",
                        "metadata files"
                    )
                ),
            );
        }
        if summary.snapshots_corrupt > 0 {
            reporter.message(
                Log,
                &format!(
                    "- {} with broken references.",
                    utils::format_count(summary.snapshots_corrupt, "snapshot", "snapshots")
                ),
            );
        }
        if summary.failed_early {
            reporter.message(Note, "Note: Verification was partial due to --fail-early.");
        }
        return;
    }

    if summary.packs_repaired > 0 {
        reporter.message(
            Info,
            &format!(
                "{} (ECC repair successful).",
                utils::format_count(summary.packs_repaired, "pack was", "packs were")
            ),
        );
    }

    if summary.blobs_dangling > 0 {
        reporter.message(
            InfoSoft,
            &format!(
                "Found {} (run 'prune' to clean up).",
                utils::format_count(
                    summary.blobs_dangling,
                    "unreferenced blob",
                    "unreferenced blobs"
                )
            ),
        );
    }

    if !summary.read_packs {
        reporter.message(Note,
            "Note: Only references were checked. To verify data integrity, run this command with --read-packs.",
        );
    }

    reporter.message(
        FinalSuccess,
        &format!(
            "Verified {} and {} in {}",
            utils::format_count(summary.snapshots_verified, "snapshot", "snapshots"),
            utils::format_count(summary.packs_processed, "pack", "packs"),
            utils::pretty_print_duration(summary.duration)
        ),
    );
    tracing::info!(
        target: "verify",
        "Verify command completed successfully in {:?}",
        summary.duration
    );
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
        assert_eq!(
            err.kind(),
            clap::error::ErrorKind::ValueValidation,
            "unexpected error kind: {err}"
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
