use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use clap::Args;
use serde::Serialize;

use crate::{
    backend::new_backend_with_prompt,
    commands::{GlobalArgs, HookArgs, ToExitCode, cleanup::CleanupHandler, with_repository_lock},
    common::{config::CommandHooks, defaults::DEFAULT_GC_TOLERANCE, error::MapacheError, hooks},
    repository::{gc, lock::LockHandle, repo::Repository},
    ui::{
        self,
        cli::{color::Colorize, gc as cli_gc},
        events::{Event, EventSender, GcEvent},
    },
    utils::{self},
};

#[derive(Debug, thiserror::Error)]
pub enum CleanError {
    #[error("failed to open repository: {0}")]
    RepoOpenFail(String),
    #[error("scan failed: {0}")]
    ScanFailed(String),
    #[error("cleanup execution failed: {0}")]
    ExecuteFailed(String),
    #[error("cleanup interrupted by user")]
    Interrupted,
    #[error(transparent)]
    Repo(#[from] MapacheError),
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl ToExitCode for CleanError {
    fn to_exit_code(&self) -> i32 {
        match self {
            CleanError::RepoOpenFail(_) => 10,
            CleanError::ScanFailed(_) => 20,
            CleanError::ExecuteFailed(_) => 30,
            CleanError::Interrupted => 130,
            CleanError::Repo(_) => 1,
            CleanError::Io(_) => 4,
        }
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

    #[clap(flatten)]
    pub hook_args: HookArgs,
}

pub async fn run(
    global_args: &GlobalArgs,
    args: &CmdArgs,
    cmd_hooks: Option<&CommandHooks>,
) -> Result<(), CleanError> {
    tracing::info!(target: "clean", "Starting clean command");
    let repo_result = with_repository_lock(
        global_args.auth_file.as_ref(),
        global_args.key.as_ref(),
        new_backend_with_prompt(global_args.backend_options(args.dry_run))
            .await
            .map_err(|e| {
                CleanError::ExecuteFailed(format!("failed to initialize backend: {}", e.inner()))
            })?,
        global_args.to_repo_config(),
        true,
        global_args.retry_lock_duration,
        global_args.no_lock,
        |repo, _, lock_handle| async move {
            hooks::run_command_pre(
                cmd_hooks,
                "clean",
                &global_args.repo,
                args.hook_args.pre_hook.as_deref(),
                args.dry_run,
            )
            .await?;

            run_with_repo(global_args.json, args, repo, lock_handle).await
        },
    )
    .await;

    hooks::run_command_post(
        cmd_hooks,
        "clean",
        &global_args.repo,
        &repo_result,
        args.hook_args.post_hook.as_deref(),
        args.dry_run,
    )
    .await;

    repo_result.map_err(|e| match e {
        CleanError::Repo(err) => CleanError::RepoOpenFail(err.inner()),
        other => other,
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
) -> Result<(), CleanError> {
    let event_sender = cli_gc::make_event_sender();
    let sender_for_cleanup = event_sender.clone();
    let cleanup_handler = CleanupHandler::new_with_callback(move || {
        // On interrupt, emit Finished to trigger cleanup in the handler
        sender_for_cleanup(Event::Gc(GcEvent::Finished {
            added_bytes: 0,
            deleted_bytes: 0,
        }));
    });
    cleanup_handler.add_lock(lock_handle);

    tracing::info!(target: "clean", "Starting garbage collection scan");
    let tolerance = effective_tolerance(args.no_repack, args.tolerance);
    let dry_run = args.dry_run;

    if !json_output {
        ui::cli::log!();
    }

    let report = run_scan_and_execute(
        repo,
        tolerance,
        dry_run,
        event_sender,
        cleanup_handler.interrupted.clone(),
        |plan| {
            if !json_output {
                ui::cli::log!();
                ui::cli::log!("{}", "Repository scan:".bold());
                ui::cli::log!(
                    "  {} packs total ({} referenced blobs)",
                    plan.total_packs,
                    plan.referenced_blobs.len()
                );
                ui::cli::log!(
                    "  Packs: {} referenced, {} unused, {} obsolete, {} small, {} tolerated",
                    plan.referenced_packs.len(),
                    plan.unused_packs.len(),
                    plan.obsolete_packs.len(),
                    plan.actionable_small_packs(),
                    plan.tolerated_packs
                );
                let actionable = plan.unused_packs.len()
                    + plan.obsolete_packs.len()
                    + plan.actionable_small_packs();
                if actionable > 0 {
                    ui::cli::log!(
                        "  Action: removing {} unused, repacking {} obsolete + {} small",
                        plan.unused_packs.len(),
                        plan.obsolete_packs.len(),
                        plan.actionable_small_packs()
                    );
                } else {
                    ui::cli::log!("  Action: repository is already clean");
                }
            }

            ui::cli::log!();

            if dry_run {
                if !json_output {
                    ui::cli::log!("{}GC not executed", super::dry_run_prefix(true));
                }
                tracing::info!(target: "clean", "Dry run enabled. GC not executed.");
            }
        },
    )
    .await?;

    let duration = report.duration;

    if json_output {
        ui::json::emit_static(
            "clean",
            &CleanOutput {
                total_packs: report.total_packs,
                referenced_blobs: report.referenced_blobs,
                referenced_packs: report.referenced_packs,
                unused_packs: report.unused_packs,
                obsolete_packs: report.obsolete_packs,
                small_packs: report.small_packs,
                tolerated_packs: report.tolerated_packs,
                added_bytes: report.added_bytes,
                deleted_bytes: report.deleted_bytes,
                net_freed_bytes: report.net_freed(),
                duration_secs: duration.as_secs_f64(),
            },
        );
    } else {
        let net_deleted_bytes = report.net_freed();

        ui::cli::log!();

        if report.is_noop() {
            ui::cli::log!(
                "{} Repository is already clean — no action needed",
                "[SUCCESS]".bold().green()
            );
        } else if net_deleted_bytes >= 0 {
            ui::cli::log!(
                "{} Cleaned repository in {} — freed {}",
                "[SUCCESS]".bold().green(),
                utils::pretty_print_duration(duration),
                utils::format_size_binary(net_deleted_bytes as u64, 3)
                    .bold()
                    .green()
            );
        } else {
            ui::cli::log!(
                "{} Cleaned repository in {} — net added {}",
                "[SUCCESS]".bold().green(),
                utils::pretty_print_duration(duration),
                utils::format_size_binary(net_deleted_bytes.unsigned_abs(), 3)
                    .bold()
                    .yellow()
            );
        }
    }

    tracing::info!(target: "clean", "Clean command completed in {:?}", duration);

    Ok(())
}

/// Result of a garbage-collection run, independent of how it is presented.
#[derive(Debug, Clone, Copy, Default)]
pub struct CleanReport {
    pub total_packs: usize,
    pub referenced_blobs: usize,
    pub referenced_packs: usize,
    pub unused_packs: usize,
    pub obsolete_packs: usize,
    pub small_packs: usize,
    pub tolerated_packs: usize,
    pub added_bytes: u64,
    pub deleted_bytes: u64,
    pub dry_run: bool,
    pub duration: Duration,
}

impl CleanReport {
    /// Bytes reclaimed by the run (negative when the run added more than it
    /// deleted).
    pub fn net_freed(&self) -> i64 {
        self.deleted_bytes as i64 - self.added_bytes as i64
    }

    /// Whether the run neither added nor removed any bytes.
    pub fn is_noop(&self) -> bool {
        self.added_bytes == 0 && self.deleted_bytes == 0
    }
}

/// Scans the repository and, unless `dry_run`, executes the resulting plan.
///
/// Shared by the CLI and the TUI clean screen. Progress is emitted on
/// `event_sender`; `shutdown_signal` aborts the run at the next safe
/// checkpoint. `on_scan` runs once the plan is ready and before it is executed,
/// so callers can report the scan summary. Interruptions are reported as
/// [`CleanError::Interrupted`].
pub async fn run_scan_and_execute<F>(
    repo: Arc<Repository>,
    tolerance: f32,
    dry_run: bool,
    event_sender: EventSender,
    shutdown_signal: Arc<AtomicBool>,
    on_scan: F,
) -> Result<CleanReport, CleanError>
where
    F: FnOnce(&gc::Plan),
{
    // `reload_master_index` uses the configured mode, so `--index-mode lazy`
    // (or `index_mode = "lazy"` in the config) applies to the GC too. `cleanup`
    // streams cold indices into the rewritten index, so nothing is dropped.
    tracing::info!(target: "clean", "Reloading master index");
    repo.reload_master_index().await.map_err(|e| {
        CleanError::ExecuteFailed(format!("failed to reload master index: {}", e.inner()))
    })?;

    let start = Instant::now();

    let plan = gc::scan(
        repo.clone(),
        tolerance,
        event_sender.clone(),
        shutdown_signal.clone(),
    )
    .await
    .map_err(|e| {
        if shutdown_signal.load(Ordering::Acquire) {
            tracing::info!(target: "clean", "GC scan interrupted by user");
            return CleanError::Interrupted;
        }
        CleanError::ScanFailed(e.to_string())
    })?;
    tracing::info!(target: "clean", "GC scan finished. Plan: {} packs to remove, {} to repack", plan.unused_packs.len() + plan.obsolete_packs.len(), plan.small_data_packs.len() + plan.small_tree_packs.len());

    let mut report = CleanReport {
        total_packs: plan.total_packs,
        referenced_blobs: plan.referenced_blobs.len(),
        referenced_packs: plan.referenced_packs.len(),
        unused_packs: plan.unused_packs.len(),
        obsolete_packs: plan.obsolete_packs.len(),
        small_packs: plan.actionable_small_packs(),
        tolerated_packs: plan.tolerated_packs,
        dry_run,
        ..Default::default()
    };

    on_scan(&plan);

    if !dry_run {
        tracing::info!(target: "clean", "Executing GC plan");
        let gc_sizes = plan.execute(event_sender.clone()).await.map_err(|e| {
            if shutdown_signal.load(Ordering::Acquire) {
                tracing::info!(target: "clean", "GC execution interrupted by user");
                return CleanError::Interrupted;
            }
            CleanError::ExecuteFailed(e.to_string())
        })?;
        tracing::info!(target: "clean", "GC execution finished. Added: {}, Deleted: {}", utils::format_size_binary(gc_sizes.added_bytes, 1), utils::format_size_binary(gc_sizes.deleted_bytes, 1));
        report.added_bytes = gc_sizes.added_bytes;
        report.deleted_bytes = gc_sizes.deleted_bytes;
    }

    event_sender(Event::Gc(GcEvent::Finished {
        added_bytes: report.added_bytes,
        deleted_bytes: report.deleted_bytes,
    }));

    report.duration = start.elapsed();
    Ok(report)
}

/// Effective garbage tolerance as a fraction in `[0.0, 1.0]`.
///
/// `--no-repack` tolerates all garbage (nothing is repacked), regardless of the
/// configured percentage, which matches the CLI's `conflicts_with` semantics.
pub fn effective_tolerance(no_repack: bool, tolerance: f32) -> f32 {
    if no_repack {
        1.0
    } else {
        tolerance.clamp(0.0, 100.0) / 100.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn effective_tolerance_ignores_percentage_when_no_repack() {
        assert_eq!(effective_tolerance(true, 0.0), 1.0);
        assert_eq!(effective_tolerance(true, 50.0), 1.0);
        assert_eq!(effective_tolerance(true, 100.0), 1.0);
    }

    #[test]
    fn effective_tolerance_scales_and_clamps() {
        assert_eq!(effective_tolerance(false, 0.0), 0.0);
        assert_eq!(effective_tolerance(false, 50.0), 0.5);
        assert_eq!(effective_tolerance(false, 100.0), 1.0);
        assert_eq!(effective_tolerance(false, 150.0), 1.0);
        assert_eq!(effective_tolerance(false, -10.0), 0.0);
    }
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
