use std::{sync::Arc, time::Instant};

use anyhow::Result;
use clap::Args;
use serde::Serialize;

use crate::{
    backend::new_backend_with_prompt,
    commands::{self, GlobalArgs, ToExitCode, cleanup::CleanupHandler, fail, with_repository_lock},
    common::{defaults::DEFAULT_GC_TOLERANCE, hooks},
    repository::{gc, lock::LockHandle, repo::Repository},
    ui::{
        self,
        cli::color::Colorize,
        cli::gc as cli_gc,
        events::{Event, GcEvent},
    },
    utils::{self},
};

#[derive(Debug, Clone, Copy)]
pub enum CleanError {
    RepoOpenFail = 10,
    ScanFailed = 20,
    ExecuteFailed = 30,
    Interrupted = 130,
}

impl ToExitCode for CleanError {
    fn to_exit_code(&self) -> i32 {
        *self as i32
    }
}

#[derive(Args, Debug, Clone)]
#[clap(
    about = "Clean up the repository",
    long_about = "Clean up the repository removing obsolete objects and merging pack and index files."
)]
pub struct CmdArgs {
    /// Garbage tolerance. The percentage [0-100] of garbage to tolerate in a
    /// pack file before repacking.
    #[clap(short, long, default_value_t = 100.0 * DEFAULT_GC_TOLERANCE, conflicts_with = "no_repack")]
    pub tolerance: f32,

    /// Don't repack
    #[clap(long, default_value_t = false)]
    pub no_repack: bool,

    /// Dry run. Displays what this command would do without
    /// making changes to the repository.
    #[clap(long, default_value_t = false)]
    pub dry_run: bool,
}

pub async fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    tracing::info!(target: "clean", "Starting clean command");
    let repo_result = with_repository_lock(
        global_args.auth_file.as_ref(),
        global_args.key.as_ref(),
        new_backend_with_prompt(global_args.backend_options(args.dry_run))
            .await
            .map_err(|e| {
                fail(
                    format!("Failed to initialize backend: {}", e),
                    CleanError::ExecuteFailed,
                )
            })?,
        global_args.to_repo_config(),
        true,
        global_args.retry_lock_duration,
        global_args.no_lock,
        |repo, _, lock_handle| async move {
            if !args.dry_run {
                // Run pre-hook: abort command if it fails
                hooks::run_pre(hooks::clean(), "clean", &global_args.repo).await?;
            }

            run_with_repo(global_args.json, args, repo, lock_handle).await
        },
    )
    .await;

    let result_str = match &repo_result {
        Ok(_) => "success".to_string(),
        Err(e) => format!("{e}"),
    };
    if !args.dry_run {
        // Run post-hook: warning on failure, always continues
        hooks::run_post(hooks::clean(), "clean", &global_args.repo, &result_str).await;
    }

    repo_result.map_err(|e| {
        if e.is::<commands::error::MapacheError>() {
            e
        } else {
            fail(
                format!("Failed to open repository: {}", e),
                CleanError::RepoOpenFail,
            )
        }
    })
}

/// Run the garbage collector against an already-locked repository.
///
/// Sets up its own `CleanupHandler` and progress reporter, so callers
/// (including `forget --run-gc`) should not pass a signal or reporter in.
/// Callers must drop any `CleanupHandler` they own before invoking this
/// function.
pub async fn run_with_repo(
    json_output: bool,
    args: &CmdArgs,
    repo: Arc<Repository>, // The repository must have its master index loaded
    lock_handle: Option<LockHandle>,
) -> Result<()> {
    tracing::info!(target: "clean", "Reloading master index");
    repo.reload_master_index().await.map_err(|e| {
        fail(
            format!("Failed to reload master index: {}", e),
            CleanError::ExecuteFailed,
        )
    })?;

    let event_sender = cli_gc::make_event_sender();
    let sender_for_cleanup = event_sender.clone();
    let cleanup_handler = CleanupHandler::new_with_callback(move || {
        // On interrupt, emit Finished to trigger cleanup in the handler
        sender_for_cleanup(crate::ui::events::Event::Gc(GcEvent::Finished {
            added_bytes: 0,
            deleted_bytes: 0,
        }));
    })
    .map_err(|e| {
        fail(
            format!("Failed to initialize cleanup handler: {}", e),
            CleanError::ExecuteFailed,
        )
    })?;
    cleanup_handler.add_lock(lock_handle);
    let shutdown_signal = cleanup_handler.interrupted.clone();

    tracing::info!(target: "clean", "Starting garbage collection scan");
    let tolerance = if args.no_repack {
        100.0
    } else {
        args.tolerance.clamp(0.0, 100.0) / 100.0
    };

    let start = Instant::now();
    if !json_output {
        ui::cli::log!();
    }

    let plan = gc::scan(
        repo.clone(),
        tolerance,
        event_sender.clone(),
        shutdown_signal.clone(),
    )
    .await
    .map_err(|e| {
        if shutdown_signal.load(std::sync::atomic::Ordering::Acquire) {
            tracing::info!(target: "clean", "GC scan interrupted by user");
            if !json_output {
                ui::cli::log!("Clean interrupted by user.");
            }
            return fail("Clean interrupted by user.", CleanError::Interrupted);
        }
        fail(
            format!("Failed to scan repository: {}", e),
            CleanError::ScanFailed,
        )
    })?;
    tracing::info!(target: "clean", "GC scan finished. Plan: {} packs to remove, {} to repack", plan.unused_packs.len() + plan.obsolete_packs.len(), plan.small_packs.len());

    let total_packs = plan.total_packs;
    let referenced_blobs = plan.referenced_blobs.len();
    let referenced_packs = plan.referenced_packs.len();
    let unused_packs = plan.unused_packs.len();
    let obsolete_packs = plan.obsolete_packs.len();
    let small_packs = plan.small_packs.len();
    let tolerated_packs = plan.tolerated_packs.len();

    if !json_output {
        ui::cli::log!();
        ui::cli::log!("Total packs: {}", total_packs.to_string());
        ui::cli::log!("Referenced blobs: {}", referenced_blobs.to_string());
        ui::cli::log!("Referenced packs: {}", referenced_packs.to_string());
        ui::cli::log!("Unused packs: {}", unused_packs.to_string());
        ui::cli::log!("Obsolete packs: {}", obsolete_packs.to_string());
        ui::cli::log!("Small packs: {}", small_packs.to_string());
        ui::cli::log!("Tolerated packs: {}", tolerated_packs.to_string());
        ui::cli::log!();
    }

    let (added_bytes, deleted_bytes) = if args.dry_run {
        if !json_output {
            ui::cli::log!("{} GC not executed", "[DRY RUN]".bold().purple());
        }
        tracing::info!(target: "clean", "Dry run enabled. GC not executed.");
        (0, 0)
    } else {
        tracing::info!(target: "clean", "Executing GC plan");
        let gc_sizes = plan.execute(event_sender.clone()).await.map_err(|e| {
            if shutdown_signal.load(std::sync::atomic::Ordering::Acquire) {
                tracing::info!(target: "clean", "GC execution interrupted by user");
                if !json_output {
                    ui::cli::log!("Clean interrupted by user.");
                }
                return fail("Clean interrupted by user.", CleanError::Interrupted);
            }
            fail(
                format!("Failed to execute GC plan: {}", e),
                CleanError::ExecuteFailed,
            )
        })?;
        tracing::info!(target: "clean", "GC execution finished. Added: {}, Deleted: {}", utils::format_size_binary(gc_sizes.added_bytes, 1), utils::format_size_binary(gc_sizes.deleted_bytes, 1));
        (gc_sizes.added_bytes, gc_sizes.deleted_bytes)
    };

    event_sender(Event::Gc(GcEvent::Finished {
        added_bytes,
        deleted_bytes,
    }));

    let duration = start.elapsed();

    if json_output {
        let net_freed = deleted_bytes as i64 - added_bytes as i64;
        ui::json::emit_static(
            "clean",
            &CleanOutput {
                total_packs,
                referenced_blobs,
                referenced_packs,
                unused_packs,
                obsolete_packs,
                small_packs,
                tolerated_packs,
                added_bytes,
                deleted_bytes,
                net_freed_bytes: net_freed,
                duration_secs: duration.as_secs_f64(),
            },
        );
    } else {
        let net_deleted_bytes = deleted_bytes as i64 - added_bytes as i64;

        ui::cli::log!(
            "Written new bytes: {}",
            utils::format_size_binary(added_bytes, 3)
        );
        ui::cli::log!(
            "Deleted bytes: {}",
            utils::format_size_binary(deleted_bytes, 3)
        );

        if net_deleted_bytes >= 0 {
            ui::cli::log!(
                "Net freed space: {}",
                utils::format_size_binary(net_deleted_bytes.unsigned_abs(), 3)
                    .bold()
                    .green()
            );
        } else {
            ui::cli::log!(
                "Net added space: {}",
                utils::format_size_binary(net_deleted_bytes.unsigned_abs(), 3)
                    .bold()
                    .yellow()
            );
        }
        ui::cli::log!();
        ui::cli::log!("Finished in {}", utils::pretty_print_duration(duration));
    }

    tracing::info!(target: "clean", "Clean command completed in {:?}", duration);

    Ok(())
}

#[derive(Serialize)]
struct CleanOutput {
    total_packs: usize,
    referenced_blobs: usize,
    referenced_packs: usize,
    unused_packs: usize,
    obsolete_packs: usize,
    small_packs: usize,
    tolerated_packs: usize,
    added_bytes: u64,
    deleted_bytes: u64,
    net_freed_bytes: i64,
    duration_secs: f64,
}
