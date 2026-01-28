use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use anyhow::{Result, bail};
use clap::Args;
use colored::Colorize;
use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

use crate::{
    backend::new_backend_with_prompt,
    commands::{GlobalArgs, cleanup::CleanupHandler},
    mapache::defaults::SHORT_SNAPSHOT_ID_LEN,
    repository::{
        repo::{RepoConfig, Repository},
        snapshot::SnapshotStream,
        verify::{verify_pack, verify_snapshot_refs},
    },
    ui::{self, default_bar_draw_target},
    utils::{self, size},
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
    #[clap(long = "read-packs", default_value_t = false)]
    pub read_packs: bool,
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

pub fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let auth = utils::get_auth_from_file(&global_args.auth_file)?;
    let backend_options = global_args.backend_options(false);
    let backend_arc = new_backend_with_prompt(backend_options)?;

    let config = RepoConfig {
        pack_size: (global_args.pack_size_mib * size::MiB as f32) as u64,
        use_cache: !global_args.no_cache,
        compression: global_args.compression_level,
    };

    // Open repository
    let (repo_arc, secure_storage, lock_handle) = Repository::try_open_with_lock(
        auth.as_ref(),
        global_args.key.as_ref(),
        backend_arc.clone(),
        config,
        false,
        global_args.retry_lock_duration,
    )?;

    // Ensure unlock on panic/drop
    let lock_handle_clone = lock_handle.clone();
    let _cleanup_handler = CleanupHandler::new(move || {
        lock_handle_clone.write().unlock();
    })?;

    let start = Instant::now();
    let stats = VerifyStats::new();

    // --------------------------------
    // Physical Verification (Optional)
    // --------------------------------
    if args.read_packs {
        ui::cli::log!("{}", "Verifying Pack Integrity (Physical)...".bold());
        let packs = repo_arc.list_packs()?;

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

        // References for the closure
        let repo_ref = repo_arc.as_ref();
        let backend_ref = backend_arc.as_ref();
        let secure_ref = secure_storage.as_ref();

        packs.par_iter().for_each(|pack_id| {
            match verify_pack(repo_ref, backend_ref, secure_ref, pack_id) {
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
                    // Log IMMEDIATELY so user sees which pack failed
                    bar.suspend(|| {
                        ui::cli::error!("Pack {} CORRUPT: {}", pack_id, e);
                    });
                    stats.packs_corrupt.fetch_add(1, Ordering::Relaxed);
                }
            }

            // Update bar message with dynamic stats
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
        });

        bar.finish();

        if stats.packs_corrupt.load(Ordering::Relaxed) > 0 {
            ui::cli::log!();
            ui::cli::error!("Physical verification failed. The repository data is corrupt.");
        } else {
            ui::cli::log!(
                "Physical verification passed. {} blobs verified.",
                stats.blobs_verified.load(Ordering::Relaxed)
            );
        }
        ui::cli::log!();
    }

    // ------------------------------------------
    // Logical Verification (Snapshot References)
    // ------------------------------------------
    ui::cli::log!("{}", "Verifying Snapshots (Logical)...".bold());

    let snapshot_stream = SnapshotStream::new(repo_arc.clone())?;
    let snapshots: Vec<_> = snapshot_stream.collect(); // Collect to know total count
    let num_snapshots = snapshots.len();

    let mut snapshots_corrupt = 0;

    for (i, (snapshot_id, _)) in snapshots.iter().enumerate() {
        let short_id = snapshot_id.to_short_hex(SHORT_SNAPSHOT_ID_LEN);

        // Print progress
        let msg = format!(
            "Snapshot {} ({}/{})",
            short_id.bold().yellow(),
            i + 1,
            num_snapshots
        );

        // Check refs
        match verify_snapshot_refs(repo_arc.clone(), snapshot_id) {
            Ok(_) => {
                ui::cli::log!("{} {}", msg, "[OK]".bold().green());
            }
            Err(e) => {
                ui::cli::log!("{} {}", msg, "[ERROR]".bold().red());
                ui::cli::error!("{e}");
                snapshots_corrupt += 1;
            }
        }
    }

    // -------------
    // FINAL REPORT
    // -------------
    ui::cli::log!();

    let packs_corrupt_count = stats.packs_corrupt.load(Ordering::Relaxed);
    let dangling_count = stats.blobs_dangling.load(Ordering::Relaxed);

    if packs_corrupt_count > 0 || snapshots_corrupt > 0 {
        ui::cli::log!("{}", "VERIFICATION FAILED".bold().on_red());

        if packs_corrupt_count > 0 {
            ui::cli::log!(
                "- {} corrupt/unreadable.",
                utils::format_count(packs_corrupt_count, "pack", "packs")
            );
        }
        if snapshots_corrupt > 0 {
            ui::cli::log!(
                "- {} with broken references.",
                utils::format_count(snapshots_corrupt, "snapshot", "snapshots")
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

    ui::cli::log!(
        "{} Verified {} and {} in {}",
        "[SUCCESS]".bold().green(),
        utils::format_count(num_snapshots, "snapshot", "snapshots"),
        utils::format_count(
            stats.packs_processed.load(Ordering::Relaxed),
            "pack",
            "packs"
        ),
        utils::pretty_print_duration(start.elapsed())
    );

    Ok(())
}
