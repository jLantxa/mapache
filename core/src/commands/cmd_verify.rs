use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Instant,
};

use anyhow::{Result, bail};
use clap::Args;
use futures::StreamExt;
use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use serde::Serialize;

use crate::{
    backend::new_backend_with_prompt,
    commands::{GlobalArgs, ToExitCode, cleanup::CleanupHandler, fail, with_repository_lock},
    fs::tree::SerializedNodeStream,
    mapache::{ID, global::GlobalOpts},
    repository::{
        repo::Repository,
        snapshot::SnapshotStream,
        verify::{verify_pack, verify_snapshot_refs},
    },
    ui::{self, cli::color::Colorize, default_bar_draw_target},
    utils::{self, collections::IdSet},
};

#[derive(Debug, Clone, Copy)]
pub enum VerifyError {
    RepoOpenFail = 10,
    CorruptPacks = 20,
    CorruptSnapshots = 21,
    VerifyFailed = 22,
    Interrupted = 130,
}

impl ToExitCode for VerifyError {
    fn to_exit_code(&self) -> i32 {
        *self as i32
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

    /// Number of packs to process in parallel
    #[clap(short, long, default_value_t = 4, requires = "read_packs")]
    pub parallel: usize,

    /// Use local cache
    #[clap(long, default_value_t = false)]
    pub with_cache: bool,

    /// Fail early on first error encountered, but still show the final report
    #[clap(long, default_value_t = false)]
    pub fail_early: bool,

    /// Verify only a random percentage of packs (e.g. 10.5%)
    #[clap(long, value_parser = parse_sample_percentage)]
    pub sample: Option<f64>,
}

fn parse_sample_percentage(s: &str) -> Result<f64, String> {
    if !s.ends_with('%') {
        return Err("Sample percentage must end with '%' (e.g. 10.5%)".to_string());
    }
    let num_str = &s[..s.len() - 1];
    let val = num_str
        .parse::<f64>()
        .map_err(|_| format!("'{}' is not a valid number", num_str))?;

    if !(0.0..=100.0).contains(&val) {
        return Err("Sample percentage must be between 0 and 100".to_string());
    }
    Ok(val)
}

struct VerifyStats {
    packs_processed: AtomicUsize,
    packs_corrupt: AtomicUsize,
    blobs_verified: AtomicUsize,
    blobs_dangling: AtomicUsize,
}

impl VerifyStats {
    fn new() -> Self {
        Self {
            packs_processed: AtomicUsize::new(0),
            packs_corrupt: AtomicUsize::new(0),
            blobs_verified: AtomicUsize::new(0),
            blobs_dangling: AtomicUsize::new(0),
        }
    }
}

struct VerifyCtx<'a> {
    repo: Arc<Repository>,
    secure_storage: Arc<crate::repository::storage::SecureStorage>,
    stats: &'a VerifyStats,
    corrupt_blobs: &'a Arc<parking_lot::Mutex<IdSet<ID>>>,
    cleanup_handler: &'a CleanupHandler,
    json_out: bool,
    parallel: usize,
    fail_early: bool,
    is_sampled: bool,
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

pub async fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let json_out = global_args.json;
    tracing::info!(target: "verify", "Starting verify command");
    if !json_out && global_args.no_cache {
        ui::cli::warning!(
            "--no-cache has no effect on this command. \
             The local cache is disabled by default. \
             Use --with-cache to enable it."
        );
    }

    with_repository_lock(
        global_args.auth_file.as_ref(),
        global_args.key.as_ref(),
        new_backend_with_prompt(global_args.backend_options(false))
            .await
            .map_err(|e| {
                fail(
                    format!("Failed to initialize backend: {}", e),
                    VerifyError::VerifyFailed,
                )
            })?,
        {
            let mut config = global_args.to_repo_config();
            config.use_cache = args.with_cache;
            config
        },
        false,
        global_args.retry_lock_duration,
        |repo, secure_storage, lock_handle| async move {
            run_with_repo(repo, secure_storage, lock_handle, args, json_out).await
        },
    )
    .await
    .map_err(|e| {
        if e.is::<crate::commands::error::MapacheError>() {
            e
        } else {
            fail(
                format!("Failed to open repository: {}", e),
                VerifyError::RepoOpenFail,
            )
        }
    })
}

pub async fn run_with_repo(
    repo: Arc<Repository>,
    secure_storage: Arc<crate::repository::storage::SecureStorage>,
    lock_handle: crate::repository::lock::LockHandle,
    args: &CmdArgs,
    json_out: bool,
) -> Result<()> {
    let cleanup_handler = CleanupHandler::new_with_callback(move || {
        ui::cli::log!(
            "\n{}",
            "Process interrupted. Cleaning up...".bold().yellow()
        );
    })?;
    cleanup_handler.add_lock(lock_handle.clone());

    let start = Instant::now();

    if json_out {
        #[derive(Serialize)]
        struct VerifyStartMsg {
            read_packs: bool,
            sample: Option<f64>,
        }
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
        let target_count = ((packs_to_verify.len() as f64) * (sample_pct / 100.0)).round() as usize;
        let target_count = target_count.clamp(1, packs_to_verify.len());

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
        };
        let failed_early = verify_packs_physically(&verify_ctx, &packs_to_verify).await?;
        if cleanup_handler.is_interrupted() {
            return Err(fail(
                "Verify interrupted by user.",
                VerifyError::Interrupted,
            ));
        }
        failed_early
    } else {
        false
    };

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
        return Err(fail(
            "Verify interrupted by user.",
            VerifyError::Interrupted,
        ));
    }

    // Back-referencing Corruption
    if !corrupt_blobs.lock().is_empty() {
        ui::cli::log!();
        ui::cli::log!("{}", "Analyzing impact of corruption...".bold().red());

        if json_out {
            #[derive(Serialize)]
            struct VerifyProgressMsg {
                phase: &'static str,
                corrupt_blobs: usize,
            }
            ui::json::emit_static(
                "verify_progress",
                &VerifyProgressMsg {
                    phase: "corruption",
                    corrupt_blobs: corrupt_blobs.lock().len(),
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
                    Ok::<(), anyhow::Error>(())
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
) -> Result<IdSet<ID>> {
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
            bail!("Index consistency check failed.");
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
        #[derive(Serialize)]
        struct VerifyProgressMsg {
            phase: &'static str,
            missing_packs: usize,
        }
        ui::json::emit_static(
            "verify_progress",
            &VerifyProgressMsg {
                phase: "index",
                missing_packs: missing_packs.len(),
            },
        );
    }

    ui::cli::log!();

    Ok(missing_packs)
}

async fn verify_packs_physically(ctx: &VerifyCtx<'_>, packs_to_verify: &[ID]) -> Result<bool> {
    let suffix = if ctx.is_sampled { " (sampled)" } else { "" };
    ui::cli::log!("{}", format!("Verifying Pack Integrity{suffix}...").bold());

    let style = ProgressStyle::default_bar()
        .template(
            "[{custom_elapsed}] [{bar:25.cyan/white}] [ETA: {custom_eta}] {pos}/{len} packs ({msg})",
        )
        .expect("invalid progress bar template for verify pack integrity")
        .progress_chars("=> ")
        .with_key(
            "custom_elapsed",
            |state: &ProgressState, w: &mut dyn std::fmt::Write| {
                let _ = write!(w, "{}", utils::pretty_print_duration(state.elapsed()));
            },
        )
        .with_key(
            "custom_eta",
            move |state: &ProgressState, w: &mut dyn std::fmt::Write| {
                let eta = state.eta();
                let custom_eta = utils::pretty_print_duration(eta);
                let _ = w.write_str(&custom_eta);
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
            let stats = ctx.stats;
            let stop_flag = &stop_flag;
            let corrupt_blobs = ctx.corrupt_blobs.clone();

            async move {
                match verify_pack(repo.clone(), backend.clone(), secure.clone(), *pack_id).await {
                    Ok(pack_stats) => {
                        stats.packs_processed.fetch_add(1, Ordering::Relaxed);
                        stats
                            .blobs_verified
                            .fetch_add(pack_stats.verified_blobs, Ordering::Relaxed);
                        stats
                            .blobs_dangling
                            .fetch_add(pack_stats.dangling, Ordering::Relaxed);

                        if pack_stats.bit_rot || !pack_stats.corrupt_blobs.is_empty() {
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
                                    parts.push("Bit-rot detected".to_string());
                                }
                                if !pack_stats.corrupt_blobs.is_empty() {
                                    parts.push(format!(
                                        "{} damaged blob(s)",
                                        pack_stats.corrupt_blobs.len()
                                    ));
                                }
                                #[derive(Serialize)]
                                struct VerifyErrorMsg {
                                    pack_id: String,
                                    error: String,
                                }
                                ui::json::emit_static(
                                    "verify_error",
                                    &VerifyErrorMsg {
                                        pack_id: pack_id.to_hex(),
                                        error: parts.join("; "),
                                    },
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
                            #[derive(Serialize)]
                            struct VerifyErrorMsg {
                                pack_id: String,
                                error: String,
                            }
                            ui::json::emit_static(
                                "verify_error",
                                &VerifyErrorMsg {
                                    pack_id: pack_id.to_hex(),
                                    error: e.to_string(),
                                },
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

                if json_out {
                    #[derive(Serialize)]
                    struct VerifyProgressMsg {
                        phase: &'static str,
                        pack_id: String,
                        packs_total: usize,
                        packs_processed: usize,
                        packs_corrupt: usize,
                        blobs_verified: usize,
                        blobs_dangling: usize,
                        failed_early: bool,
                    }
                    ui::json::emit_static(
                        "verify_progress",
                        &VerifyProgressMsg {
                            phase: "physical",
                            pack_id: pack_id.to_hex(),
                            packs_total: total_packs,
                            packs_processed: stats.packs_processed.load(Ordering::Relaxed),
                            packs_corrupt: stats.packs_corrupt.load(Ordering::Relaxed),
                            blobs_verified: stats.blobs_verified.load(Ordering::Relaxed),
                            blobs_dangling: stats.blobs_dangling.load(Ordering::Relaxed),
                            failed_early: stop_flag.load(Ordering::Relaxed),
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
) -> Result<(bool, usize, usize)> {
    ui::cli::log!("{}", "Verifying Snapshot References...".bold());

    let snapshots_corrupt = AtomicUsize::new(0);
    let snapshot_stream = SnapshotStream::new(repo.clone()).await?;
    let mut snapshots: Vec<(ID, chrono::DateTime<chrono::Local>)> = Vec::new();
    let mut stream = snapshot_stream;
    while let Some(res) = stream.next().await {
        match res {
            Ok((id, snapshot)) => snapshots.push((id, snapshot.timestamp)),
            Err(e) => {
                ui::cli::error!("Failed to load snapshot: {:?}", e);

                if json_out {
                    #[derive(Serialize)]
                    struct VerifyErrorMsg {
                        error: String,
                    }
                    ui::json::emit_static(
                        "verify_error",
                        &VerifyErrorMsg {
                            error: format!("Failed to load snapshot: {:?}", e),
                        },
                    );
                }

                snapshots_corrupt.fetch_add(1, Ordering::Relaxed);
                if args.fail_early {
                    bail!("Verification halted due to corrupted snapshot.");
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
                ui::cli::error!("{:?}", e);

                if json_out {
                    #[derive(Serialize)]
                    struct VerifyErrorMsg {
                        snapshot: String,
                        error: String,
                    }
                    ui::json::emit_static(
                        "verify_error",
                        &VerifyErrorMsg {
                            snapshot: snapshot_id.to_short_hex(12),
                            error: format!("{:?}", e),
                        },
                    );
                }

                snapshots_corrupt.fetch_add(1, Ordering::Relaxed);

                if args.fail_early {
                    stop_flag.store(true, Ordering::Relaxed);
                }
            }
        }

        if json_out {
            #[derive(Serialize)]
            struct VerifyProgressMsg {
                phase: &'static str,
                snapshot_id: String,
                snapshots_total: usize,
                snapshots_processed: usize,
                snapshots_corrupt: usize,
            }
            ui::json::emit_static(
                "verify_progress",
                &VerifyProgressMsg {
                    phase: "logical",
                    snapshot_id: snapshot_id.to_short_hex(12),
                    snapshots_total: num_snapshots_total,
                    snapshots_processed: i + 1,
                    snapshots_corrupt: snapshots_corrupt.load(Ordering::Relaxed),
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

fn emit_final_report(report: &VerifyReport<'_>) -> Result<()> {
    if report.json_out {
        let packs_corrupt_count = report.stats.packs_corrupt.load(Ordering::Relaxed);
        let dangling_count = report.stats.blobs_dangling.load(Ordering::Relaxed);
        let passed = packs_corrupt_count == 0 && report.snapshots_corrupt == 0;

        #[derive(Serialize)]
        struct VerifyCompleteMsg {
            duration_seconds: f64,
            packs_processed: usize,
            packs_corrupt: usize,
            blobs_verified: usize,
            blobs_dangling: usize,
            snapshots_verified: usize,
            snapshots_corrupt: usize,
            passed: bool,
            failed_early: bool,
            read_packs: bool,
        }
        ui::json::emit_static(
            "verify_complete",
            &VerifyCompleteMsg {
                duration_seconds: report.start.elapsed().as_secs_f64(),
                packs_processed: report.stats.packs_processed.load(Ordering::Relaxed),
                packs_corrupt: packs_corrupt_count,
                blobs_verified: report.stats.blobs_verified.load(Ordering::Relaxed),
                blobs_dangling: dangling_count,
                snapshots_verified: report.num_snapshots_total,
                snapshots_corrupt: report.snapshots_corrupt,
                passed,
                failed_early: report.physical_failed_early || report.logical_failed_early,
                read_packs: report.read_packs,
            },
        );
    }

    let packs_corrupt_count = report.stats.packs_corrupt.load(Ordering::Relaxed);
    let dangling_count = report.stats.blobs_dangling.load(Ordering::Relaxed);

    if packs_corrupt_count > 0 || report.snapshots_corrupt > 0 {
        ui::cli::log!("{}", "VERIFICATION FAILED".bold().on_red());

        if packs_corrupt_count > 0 {
            ui::cli::log!(
                "- {} corrupt/unreadable.",
                utils::format_count(packs_corrupt_count, "pack", "packs")
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

        let code = if packs_corrupt_count > 0 {
            VerifyError::CorruptPacks
        } else {
            VerifyError::CorruptSnapshots
        };

        return Err(fail("Repository integrity check failed.", code));
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
    #[derive(Serialize)]
    struct VerifyErrorMsg {
        blob_id: String,
        path: String,
        snapshot: String,
        error: String,
    }

    ui::json::emit_static(
        "verify_error",
        &VerifyErrorMsg {
            blob_id: blob_id.to_short_hex(8),
            path: path.display().to_string(),
            snapshot: snapshot_id.to_short_hex(12),
            error: "Corrupt blob affects this file".to_string(),
        },
    );
}
