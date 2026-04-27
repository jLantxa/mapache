use std::{sync::Arc, time::Instant};

use anyhow::Result;
use clap::Args;
use colored::Colorize;

use crate::{
    backend::new_backend_with_prompt,
    commands::{GlobalArgs, ToExitCode, cleanup::CleanupHandler, fail, with_repository_lock},
    mapache::defaults::DEFAULT_GC_TOLERANCE,
    repository::{
        gc::{self},
        repo::Repository,
    },
    ui::{self},
    utils::{self},
};

#[derive(Debug, Clone, Copy)]
pub enum CleanError {
    RepoOpenFail = 10,
    ScanFailed = 20,
    ExecuteFailed = 30,
}

impl ToExitCode for CleanError {
    fn to_exit_code(&self) -> i32 {
        *self as i32
    }
}

#[derive(Args, Debug)]
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
    with_repository_lock(
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
        |repo, _, lock_handle| async move {
            let cleanup_handler = CleanupHandler::new().map_err(|e| {
                fail(
                    format!("Failed to initialize cleanup handler: {}", e),
                    CleanError::ExecuteFailed,
                )
            })?;
            cleanup_handler.add_lock(lock_handle.clone());
            repo.reload_master_index().await.map_err(|e| {
                fail(
                    format!("Failed to reload master index: {}", e),
                    CleanError::ExecuteFailed,
                )
            })?;

            run_with_repo(global_args, args, repo).await
        },
    )
    .await
    .map_err(|e| {
        if e.is::<crate::commands::error::MapacheError>() {
            e
        } else {
            fail(
                format!("Failed to open repository: {}", e),
                CleanError::RepoOpenFail,
            )
        }
    })
}

/// Run the command with an initialized repository object.
pub async fn run_with_repo(
    _global_args: &GlobalArgs,
    args: &CmdArgs,
    repo: Arc<Repository>, // The repository must have its master index loaded
) -> Result<()> {
    let tolerance = if args.no_repack {
        // No repack means a tolerance of 100 %.
        100.0
    } else {
        args.tolerance.clamp(0.0, 100.0) / 100.0
    };

    let start = Instant::now();
    ui::cli::log!();

    let plan = gc::scan(repo.clone(), tolerance).await.map_err(|e| {
        fail(
            format!("Failed to scan repository: {}", e),
            CleanError::ScanFailed,
        )
    })?;

    ui::cli::log!();
    ui::cli::log!("Total packs: {}", plan.total_packs.to_string(),);
    ui::cli::log!(
        "Referenced blobs: {}",
        plan.referenced_blobs.len().to_string(),
    );
    ui::cli::log!(
        "Referenced packs: {}",
        plan.referenced_packs.len().to_string(),
    );
    ui::cli::log!("Unused packs: {}", plan.unused_packs.len().to_string(),);
    ui::cli::log!("Obsolete packs: {}", plan.obsolete_packs.len().to_string(),);
    ui::cli::log!("Small packs: {}", plan.small_packs.len().to_string(),);
    ui::cli::log!(
        "Tolerated packs: {}",
        plan.tolerated_packs.len().to_string(),
    );

    ui::cli::log!();
    if args.dry_run {
        ui::cli::log!("{} GC not executed", "[DRY RUN]".bold().purple());
    } else {
        let gc_sizes = plan.execute().await.map_err(|e| {
            fail(
                format!("Failed to execute GC plan: {}", e),
                CleanError::ExecuteFailed,
            )
        })?;
        let net_deleted_bytes = gc_sizes.deleted_bytes as i64 - gc_sizes.added_bytes as i64;

        // Report the total written and deleted bytes.
        // The net balance can be 0, but the user might like to know how much
        // we are writing/deleting in the backend.
        ui::cli::log!(
            "Written new bytes: {}",
            utils::format_size_binary(gc_sizes.added_bytes, 3)
        );
        ui::cli::log!(
            "Deleted bytes: {}",
            utils::format_size_binary(gc_sizes.deleted_bytes, 3)
        );

        // Report net freed/added space
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
        ui::cli::log!(
            "Finished in {}",
            utils::pretty_print_duration(start.elapsed())
        );
    }

    Ok(())
}
