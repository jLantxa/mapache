use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Instant;

use anyhow::{Result, bail};
use clap::Args;
use colored::Colorize;
use futures::StreamExt;
use indicatif::{ProgressBar, ProgressState, ProgressStyle};

use crate::fs::tree::SerializedNodeStream;
use crate::mapache::global::GlobalOpts;
use crate::{
    backend::new_backend_with_prompt,
    commands::{GlobalArgs, cleanup::CleanupHandler},
    mapache::ID,
    repository::{
        repo::{RepoConfig, Repository},
        snapshot::{Snapshot, SnapshotStream},
        verify::{verify_pack, verify_snapshot_refs},
    },
    ui::{self, default_bar_draw_target},
    utils::{self, collections::IdSet, size},
};

#[derive(Args, Debug)]
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
    #[clap(short, long, default_value_t = 8, requires = "read_packs")]
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

pub async fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let auth = utils::get_auth_from_file(&global_args.auth_file)?;
    let backend_options = global_args.backend_options(false);
    let backend_arc = new_backend_with_prompt(backend_options).await?;

    let config = RepoConfig {
        pack_size: (global_args.pack_size_mib * size::MiB as f32) as u64,
        use_cache: args.with_cache,
        compression: global_args.compression_level,
    };

    if global_args.no_cache {
        ui::cli::warning!(
            "--no-cache has no effect on this command. \
             The local cache is disabled by default. \
             Use --with-cache to enable it."
        );
    }

    // Open repository
    let (repo, secure_storage, _lock_handle) = Repository::try_open_with_lock(
        auth.as_ref(),
        global_args.key.as_ref(),
        backend_arc.clone(),
        config,
        false,
        global_args.retry_lock_duration,
    )
    .await?;

    // Init cleanup handler
    let cleanup_handler = CleanupHandler::new_with_callback(move || {
        ui::cli::log!(
            "\n{}",
            "Process interrupted. Cleaning up...".bold().yellow()
        );
    })?;

    let start = Instant::now();

    repo.reload_master_index().await?;

    let stats = VerifyStats::new();
    let packs_all = repo.list_packs().await?;

    // ------------------------------------------
    // Sampling (Optional)
    // ------------------------------------------
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

    let mut physical_failed_early = false;
    let corrupt_blobs = Arc::new(parking_lot::Mutex::new(IdSet::default()));

    // ------------------------------------------
    // Index Consistency (Blobs -> Packs)
    // ------------------------------------------
    ui::cli::log!("{}", "Verifying Index Consistency...".bold());
    let mut missing_packs = IdSet::default();
    repo.index().for_each_id(|_id, locator| {
        if !packs_all.contains(&locator.pack_id) {
            missing_packs.insert(locator.pack_id);
        }
    });

    if !missing_packs.is_empty() {
        ui::cli::error!(
            "Index refers to {} missing packs!",
            missing_packs.len().to_string().bold().red()
        );
        for p in missing_packs {
            ui::cli::log!("  - Missing Pack: {}", p);
        }
        if args.fail_early {
            bail!("Index consistency check failed.");
        }
    } else {
        ui::cli::log!(
            "{} {}",
            "Index consistency check passed.".bold().green(),
            "All indexed blobs point to existing packs."
        );
    }
    ui::cli::log!();

    // --------------------------------
    // Physical Verification (Optional)
    // --------------------------------
    if args.read_packs {
        let suffix = if args.sample.is_none() {
            ""
        } else {
            " (sampled)"
        };
        ui::cli::log!("{}", format!("Verifying Pack Integrity{suffix}...").bold());

        let style = ProgressStyle::default_bar()
            .template("[{custom_elapsed}] [{bar:25.cyan/white}] {pos}/{len} packs ({msg})")
            .unwrap()
            .progress_chars("=> ")
            .with_key(
                "custom_elapsed",
                |state: &ProgressState, w: &mut dyn std::fmt::Write| {
                    write!(w, "{}", utils::pretty_print_duration(state.elapsed())).unwrap()
                },
            );

        let bar = ProgressBar::new(packs_to_verify.len() as u64);
        bar.set_draw_target(default_bar_draw_target());
        bar.enable_steady_tick(GlobalOpts::progress_refresh_interval());
        bar.set_style(style);
        bar.set_message("OK");

        // Atomic flag to stop scheduling new work if fail_early is set
        let stop_flag = AtomicBool::new(false);
        let interrupted_flag = cleanup_handler.interrupted.clone();

        // I/O & CPU Pipelining:
        // buffer_unordered(N) effectively pipelines up to N packs.
        // Some will be in the 'await backend.read()' stage (I/O bound),
        // while others will be in the 'Rayon par_iter' stage (CPU bound).
        futures::stream::iter(packs_to_verify.iter())
            .take_while(|_| {
                futures::future::ready(
                    !stop_flag.load(Ordering::Relaxed) && !interrupted_flag.load(Ordering::Relaxed),
                )
            })
            .map(|pack_id| {
                let repo = repo.clone();
                let backend = backend_arc.clone();
                let secure = secure_storage.clone();
                let bar = bar.clone();
                let stats = &stats;
                let stop_flag = &stop_flag;
                let corrupt_blobs = corrupt_blobs.clone();

                                async move {
                                    match verify_pack(repo.as_ref(), backend.as_ref(), secure.as_ref(), pack_id).await {
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
                                                            utils::format_count(pack_stats.corrupt_blobs.len(), "damaged blob", "damaged blobs")
                                                        );
                                                    }
                                                });
                                                stats.packs_corrupt.fetch_add(1, Ordering::Relaxed);

                                                if !pack_stats.corrupt_blobs.is_empty() {
                                                    let mut corrupt_set = corrupt_blobs.lock();
                                                    for id in pack_stats.corrupt_blobs {
                                                        corrupt_set.insert(id);
                                                    }
                                                }

                                                if args.fail_early {
                                                    stop_flag.store(true, Ordering::Relaxed);
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            bar.suspend(|| {
                                                ui::cli::error!("Failed to process pack {}: {}", pack_id, e);
                                            });
                                            stats.packs_corrupt.fetch_add(1, Ordering::Relaxed);

                                            if args.fail_early {
                                                stop_flag.store(true, Ordering::Relaxed);
                                            }
                                        }
                                    }

                    // Update bar message
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
                }
            })
            .buffer_unordered(args.parallel)
            .collect::<()>()
            .await;

        if cleanup_handler.is_interrupted() {
            bar.abandon();
            return Ok(());
        }

        bar.finish();
        physical_failed_early = stop_flag.load(Ordering::Relaxed);

        if stats.packs_corrupt.load(Ordering::Relaxed) > 0 {
            ui::cli::log!();
            if physical_failed_early {
                ui::cli::warning!("Physical verification halted early due to errors.");
            } else {
                ui::cli::error!("Physical verification failed. The repository data is corrupt.");
            }
        } else {
            ui::cli::log!(
                "{} {} blobs verified.",
                "Physical verification passed.".bold().green(),
                stats.blobs_verified.load(Ordering::Relaxed)
            );
        }
        ui::cli::log!();
    }

    // ------------------------------------------
    // Logical Verification (Snapshot References)
    // ------------------------------------------
    let mut logical_failed_early = false;
    let snapshots_corrupt = AtomicUsize::new(0);
    let mut num_snapshots_total = 0;
    let verified_trees = Arc::new(parking_lot::Mutex::new(IdSet::default()));

    if (!physical_failed_early || !args.fail_early) && !cleanup_handler.is_interrupted() {
        ui::cli::log!("{}", "Verifying Snapshot References...".bold());

        let snapshot_stream = SnapshotStream::new(repo.clone()).await?;
        let mut snapshots: Vec<(ID, Snapshot)> = snapshot_stream.collect().await;
        // Sort snapshots by timestamp (oldest first)
        snapshots.sort_by_key(|(_, s)| s.timestamp);
        num_snapshots_total = snapshots.len();

        let stop_flag = AtomicBool::new(false);
        let interrupted_flag = cleanup_handler.interrupted.clone();

        let mut stream = futures::stream::iter(snapshots.into_iter().enumerate())
            .take_while(|_| {
                futures::future::ready(
                    !stop_flag.load(Ordering::Relaxed) && !interrupted_flag.load(Ordering::Relaxed),
                )
            })
            .map(|(i, (snapshot_id, _snapshot))| {
                let repo = repo.clone();
                let packs = &packs_all;
                let verified_trees = verified_trees.clone();

                async move {
                    let res =
                        verify_snapshot_refs(repo.clone(), &snapshot_id, packs, verified_trees)
                            .await;
                    (i, snapshot_id, res)
                }
            })
            .buffered(4);

        while let Some((i, snapshot_id, res)) = stream.next().await {
            let msg = format!(
                "{} {}",
                snapshot_id.to_short_hex(12).bold().yellow(),
                format!("({}/{})", i + 1, num_snapshots_total).dimmed()
            );

            match res {
                Ok(_) => ui::cli::log!("{} {}", msg, "[OK]".bold().green()),
                Err(e) => {
                    ui::cli::log!("{} {}", msg, "[ERROR]".bold().red());
                    ui::cli::error!("{e}");
                    snapshots_corrupt.fetch_add(1, Ordering::Relaxed);

                    if args.fail_early {
                        stop_flag.store(true, Ordering::Relaxed);
                    }
                }
            }
        }

        logical_failed_early = stop_flag.load(Ordering::Relaxed);
    }

    // ------------------------------------------
    // Back-referencing Corruption
    // ------------------------------------------
    if !corrupt_blobs.lock().is_empty() {
        ui::cli::log!();
        ui::cli::log!("{}", "Analyzing impact of corruption...".bold().red());

        let mut snapshot_stream = SnapshotStream::new(repo.clone()).await?;
        while let Some((snapshot_id, _)) = snapshot_stream.next().await {
            let mut stream = SerializedNodeStream::new(
                repo.clone(),
                Some(repo.load_snapshot(&snapshot_id, None).await?.tree),
                PathBuf::new(),
                None,
                None,
            )
            .await?;

            while let Some(res) = stream.next().await {
                let (path, sn_node) = res?;
                let node = sn_node.node;
                if let Some(blobs) = node.blobs {
                    let corrupt_ids = corrupt_blobs.lock();
                    for blob_id in blobs {
                        if corrupt_ids.contains(&blob_id) {
                            ui::cli::error!(
                                "Corrupt blob {} affects file \"{}\" in snapshot {}",
                                blob_id.to_short_hex(8).red(),
                                path.display().to_string().bold(),
                                snapshot_id.to_short_hex(12).yellow()
                            );
                        }
                    }
                }
            }
        }
    }

    // -------------
    // FINAL REPORT
    // -------------
    if cleanup_handler.is_interrupted() {
        return Ok(());
    }

    ui::cli::log!();

    let packs_corrupt_count = stats.packs_corrupt.load(Ordering::Relaxed);
    let dangling_count = stats.blobs_dangling.load(Ordering::Relaxed);
    let snapshots_corrupt_count = snapshots_corrupt.load(Ordering::Relaxed);

    if packs_corrupt_count > 0 || snapshots_corrupt_count > 0 {
        ui::cli::log!("{}", "VERIFICATION FAILED".bold().on_red());

        if packs_corrupt_count > 0 {
            ui::cli::log!(
                "- {} corrupt/unreadable.",
                utils::format_count(packs_corrupt_count, "pack", "packs")
            );
        }
        if snapshots_corrupt_count > 0 {
            ui::cli::log!(
                "- {} with broken references.",
                utils::format_count(snapshots_corrupt_count, "snapshot", "snapshots")
            );
        }
        if physical_failed_early || logical_failed_early {
            ui::cli::log!(
                "{}",
                "Note: Verification was partial due to --fail-early.".dimmed()
            );
        }

        bail!("Repository integrity check failed.");
    }

    if dangling_count > 0 {
        ui::cli::log!(
            "{} Found {} (run 'prune' to clean up).",
            "[INFO]".yellow(),
            utils::format_count(dangling_count, "unreferenced blob", "unreferenced blobs")
        );
    }

    if !args.read_packs {
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
        utils::format_count(num_snapshots_total, "snapshot", "snapshots"),
        utils::format_count(
            stats.packs_processed.load(Ordering::Relaxed),
            "pack",
            "packs"
        ),
        utils::pretty_print_duration(start.elapsed())
    );

    Ok(())
}
