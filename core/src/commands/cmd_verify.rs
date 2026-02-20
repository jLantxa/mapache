use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Instant;

use anyhow::{Result, bail};
use clap::Args;
use colored::Colorize;
use futures::StreamExt;
use indicatif::{ProgressBar, ProgressState, ProgressStyle};

use crate::{
    backend::new_backend_with_prompt,
    commands::{GlobalArgs, cleanup::CleanupHandler},
    mapache::ID,
    repository::{
        repo::{RepoConfig, Repository},
        snapshot::SnapshotStream,
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

    /// Use local cache
    #[clap(long, default_value_t = false)]
    pub with_cache: bool,

    /// Fail early on first error encountered, but still show the final report
    #[clap(long, default_value_t = false)]
    pub fail_early: bool,
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

    let _cleanup_handler = CleanupHandler::new()?;

    let start = Instant::now();

    repo.reload_master_index().await?;

    let stats = VerifyStats::new();
    let packs = repo.list_packs().await?;
    let mut physical_failed_early = false;

    // ------------------------------------------
    // Index Consistency (Blobs -> Packs)
    // ------------------------------------------
    ui::cli::log!("{}", "Verifying Index Consistency...".bold());
    let mut missing_packs = IdSet::default();
    repo.index().for_each_id(|_id, locator| {
        if !packs.contains(&locator.pack_id) {
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
        ui::cli::log!("{}", "Verifying Pack Integrity...".bold());

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

        let bar = ProgressBar::new(packs.len() as u64);
        bar.set_draw_target(default_bar_draw_target());
        bar.set_style(style);

        // Atomic flag to stop scheduling new work if fail_early is set
        let stop_flag = AtomicBool::new(false);

        // Async stream replaces Rayon par_iter to allow .await inside verification
        futures::stream::iter(packs.iter())
            .take_while(|_| futures::future::ready(!stop_flag.load(Ordering::Relaxed)))
            .map(|pack_id| {
                let repo = repo.clone();
                let backend = backend_arc.clone();
                let secure = secure_storage.clone();
                let bar = bar.clone();
                let stats = &stats;
                let stop_flag = &stop_flag;

                async move {
                    match verify_pack(repo.as_ref(), backend.as_ref(), secure.as_ref(), pack_id)
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
                        }
                        Err(e) => {
                            bar.suspend(|| {
                                ui::cli::error!("Pack {} CORRUPT: {}", pack_id, e);
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
            .buffer_unordered(8)
            .collect::<()>()
            .await;

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

    if !physical_failed_early || !args.fail_early {
        ui::cli::log!("{}", "Verifying Snapshot References...".bold());

        let snapshot_stream = SnapshotStream::new(repo.clone()).await?;
        let snapshots: Vec<ID> = snapshot_stream.map(|(id, _)| id).collect::<Vec<_>>().await;
        num_snapshots_total = snapshots.len();

        let stop_flag = AtomicBool::new(false);

        futures::stream::iter(snapshots.into_iter().enumerate())
            .take_while(|_| futures::future::ready(!stop_flag.load(Ordering::Relaxed)))
            .map(|(i, snapshot_id): (usize, ID)| {
                let repo = repo.clone();
                let packs = &packs;
                let snapshots_corrupt = &snapshots_corrupt;
                let stop_flag = &stop_flag;
                let num_total = num_snapshots_total;

                async move {
                    let msg = format!(
                        "{} {}",
                        snapshot_id.to_short_hex(12).bold().yellow(),
                        format!("({}/{})", i + 1, num_total).dimmed()
                    );

                    match verify_snapshot_refs(repo.clone(), &snapshot_id, packs).await {
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
            })
            .buffer_unordered(4) // Parallelize logical check too
            .collect::<()>()
            .await;

        logical_failed_early = stop_flag.load(Ordering::Relaxed);
    }

    // -------------
    // FINAL REPORT
    // -------------
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
