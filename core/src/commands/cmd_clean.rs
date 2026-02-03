use std::{sync::Arc, time::Instant};

use anyhow::Result;
use clap::Args;
use colored::Colorize;

use crate::{
    backend::new_backend_with_prompt,
    commands::{GlobalArgs, cleanup::CleanupHandler},
    mapache::defaults::DEFAULT_GC_TOLERANCE,
    repository::{
        gc::{self},
        repo::{RepoConfig, Repository},
    },
    ui::{self},
    utils::{self, size},
};

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

pub fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let auth = utils::get_auth_from_file(&global_args.auth_file)?;
    let backend = new_backend_with_prompt(global_args.backend_options(args.dry_run))?;

    let config = RepoConfig {
        pack_size: (global_args.pack_size_mib * size::MiB as f32) as u64,
        use_cache: !global_args.no_cache,
        compression: global_args.compression_level,
    };
    let (repo, _, lock_handle) = Repository::try_open_with_lock(
        auth.as_ref(),
        global_args.key.as_ref(),
        backend,
        config,
        true,
        global_args.retry_lock_duration,
    )?;

    let lock_handle_clone = lock_handle.clone();
    let _cleanup_handler = CleanupHandler::new(move || {
        lock_handle_clone.write().unlock();
    })?;

    repo.reload_master_index()?;

    run_with_repo(global_args, args, repo)
}

/// Run the command with an initialized repository object.
pub fn run_with_repo(
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

    let plan = gc::scan(repo.clone(), tolerance)?;

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
        let deleted_size = plan.execute()?;

        // Report freed up space
        if deleted_size >= 0 {
            ui::cli::log!(
                "Freed space: {}",
                utils::format_size_binary(deleted_size.unsigned_abs(), 3)
                    .bold()
                    .green()
            );
        } else {
            ui::cli::log!(
                "Added space: {}",
                utils::format_size_binary(deleted_size.unsigned_abs(), 3)
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
