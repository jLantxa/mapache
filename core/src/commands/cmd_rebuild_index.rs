use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

use anyhow::Result;
use clap::Args;
use colored::Colorize;
use indicatif::{ProgressBar, ProgressStyle};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

use crate::{
    backend::new_backend_with_prompt,
    commands::{GlobalArgs, cleanup::CleanupHandler},
    mapache::ContentIdType,
    repository::{
        index::MasterIndex,
        packer::Packer,
        repo::{RepoConfig, Repository},
    },
    ui::{self, default_bar_draw_target},
    utils::{self, size},
};

#[derive(Args, Debug)]
#[clap(about = "Rebuild the index by scanning all existing packs")]
pub struct CmdArgs {
    /// Dry run
    #[clap(long, default_value_t = false)]
    pub dry_run: bool,
}

pub fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let auth = utils::get_auth_from_file(&global_args.auth_file)?;

    let backend_options = global_args.backend_options(args.dry_run);
    let backend = new_backend_with_prompt(backend_options)?;

    let config = RepoConfig {
        pack_size: (global_args.pack_size_mib * size::MiB as f32) as u64,
        use_cache: !global_args.no_cache,
    };
    let (repo, secure_storage, lock_handle) = Repository::try_open_with_lock(
        auth.as_ref(),
        global_args.key.as_ref(),
        backend.clone(),
        config,
        false,
        global_args.retry_lock_duration,
    )?;

    let lock_handle_clone = lock_handle.clone();
    let _cleanup_handler = CleanupHandler::new(move || {
        lock_handle_clone.write().unlock();
    })?;

    let start = Instant::now();

    if args.dry_run {
        ui::cli::log!("{}", "[DRY RUN]".bold().purple());
    }

    // Discover packs and blobs
    ui::cli::log!("Discovering packs...");

    let all_pack_ids = repo.list_packs()?;
    let old_index_ids = repo.index().read().ids();
    let mut new_master_index = MasterIndex::new();
    new_master_index.set_autosave(false);
    ui::cli::log!("Found {} packs", all_pack_ids.len());

    let scan_bar =
        ProgressBar::with_draw_target(Some(all_pack_ids.len() as u64), default_bar_draw_target())
            .with_style(
                ProgressStyle::default_bar()
                    .template("[{bar:20.cyan/white}] Scanning packs: {pos}/{len}")
                    .unwrap()
                    .progress_chars("=> "),
            );

    let results: Vec<_> = all_pack_ids
        .par_iter()
        .map(|pack_id| {
            let res = Packer::parse_pack_footer(
                repo.as_ref(),
                backend.as_ref(),
                secure_storage.as_ref(),
                pack_id,
            );
            scan_bar.inc(1);
            (pack_id, res)
        })
        .collect();

    scan_bar.finish_and_clear();

    // Populate the new index
    let mut blob_count = 0;
    let mut error_count = 0;

    for (pack_id, res) in results {
        match res {
            Ok(descriptors) => {
                blob_count += descriptors.len();
                new_master_index.add_pack(repo.as_ref(), pack_id, descriptors)?;
            }
            Err(e) => {
                error_count += 1;
                ui::cli::error!("Error reading pack {pack_id}: {e}");
            }
        }
    }

    ui::cli::log!("Found {} blobs", blob_count);
    if error_count > 0 {
        ui::cli::warning!("Skipped {} packs due to errors", error_count);
    }

    // Save the new index
    let (_new_raw, new_enc) = new_master_index.persist(repo.as_ref())?;
    ui::cli::log!("Persisted {} new indices", new_master_index.ids().len());

    // Delete the old index
    let index_delete_bar =
        ProgressBar::with_draw_target(Some(old_index_ids.len() as u64), default_bar_draw_target())
            .with_style(
                ProgressStyle::default_bar()
                    .template(
                        "[{percent} %] [{bar:20.cyan/white}] Deleting old index files: {pos}/{len}",
                    )
                    .unwrap()
                    .progress_chars("=> "),
            );

    let deleted_size = AtomicU64::new(0);
    old_index_ids.par_iter().for_each(|id| {
        let size_res = repo.delete_file(ContentIdType::Index, id, None);
        deleted_size.fetch_add(size_res.unwrap_or(0), Ordering::AcqRel);
        index_delete_bar.inc(1);
    });
    index_delete_bar.finish_and_clear();
    ui::cli::log!(
        "Deleted {} obsolete index files",
        index_delete_bar.position()
    );

    // Report added space
    let added_size: i64 = new_enc as i64 - deleted_size.load(Ordering::Relaxed) as i64;
    if added_size >= 0 {
        ui::cli::log!(
            "Added space: {}",
            utils::format_size_binary(added_size.unsigned_abs(), 3)
                .bold()
                .yellow()
        );
    } else {
        ui::cli::log!(
            "Freed space: {}",
            utils::format_size_binary(added_size.unsigned_abs(), 3)
                .bold()
                .green()
        );
    }

    let prefix = if args.dry_run {
        format!("{} ", "[DRY RUN]".bold().purple())
    } else {
        String::new()
    };

    ui::cli::log!(
        "\n{}Finished in {}",
        prefix,
        utils::pretty_print_duration(start.elapsed(),),
    );

    Ok(())
}
