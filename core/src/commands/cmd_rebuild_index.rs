use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

use anyhow::{Result, bail};
use clap::Args;
use colored::Colorize;
use futures::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};

use crate::{
    backend::new_backend_with_prompt,
    commands::{GlobalArgs, cleanup::CleanupHandler, open_repository_with_lock},
    mapache::{ContentIdType, ID},
    repository::{index::MasterIndex, packer::Packer, repo::RepoConfig},
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

pub async fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let backend_options = global_args.backend_options(args.dry_run);
    let backend = new_backend_with_prompt(backend_options).await?;

    let config = RepoConfig {
        pack_size: (global_args.pack_size_mib * size::MiB as f32) as u64,
        use_cache: !global_args.no_cache,
        compression: global_args.compression_level,
    };
    let (repo, secure_storage, mut lock_handle) = open_repository_with_lock(
        global_args.auth_file.as_ref(),
        global_args.key.as_ref(),
        backend.clone(),
        config,
        false,
        global_args.retry_lock_duration,
    )
    .await?;

    let cleanup_handler = CleanupHandler::new_with_callback(move || {
        ui::cli::log!(
            "\n{}",
            "Process interrupted. Cleaning up...".bold().yellow()
        );
    })?;
    cleanup_handler.add_lock(lock_handle.clone());

    let start = Instant::now();

    if args.dry_run {
        ui::cli::log!("{}", "[DRY RUN]".bold().purple());
    }

    // Discover packs and blobs
    ui::cli::log!("Discovering packs...");

    let all_pack_ids = repo.list_packs().await?;
    let old_index_ids = repo.list_index_ids().await?;
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

    let results: Vec<_> = futures::stream::iter(all_pack_ids.iter())
        .map(|pack_id| {
            let repo = repo.clone();
            let backend = backend.clone();
            let secure_storage = secure_storage.clone();
            let scan_bar = scan_bar.clone();

            async move {
                let res = Packer::parse_pack_footer(
                    repo.as_ref(),
                    backend.as_ref(),
                    secure_storage.as_ref(),
                    pack_id,
                )
                .await;

                scan_bar.inc(1);
                (pack_id, res)
            }
        })
        .buffered(8)
        .collect()
        .await;

    scan_bar.finish_and_clear();

    // Populate the new index
    let mut blob_count = 0;
    let mut error_count = 0;

    for (pack_id, res) in results {
        if cleanup_handler.is_interrupted() {
            bail!("Interrupted");
        }

        match res {
            Ok(descriptors) => {
                blob_count += descriptors.len();
                new_master_index
                    .add_pack(repo.as_ref(), pack_id, descriptors)
                    .await?;
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
    let new_index_size = new_master_index.persist(repo.as_ref()).await?;
    let new_index_ids = new_master_index.ids();
    ui::cli::log!("Persisted {} new indices", new_index_ids.len());

    // Delete the old index
    // We must ensure we don't delete any of the NEW indices if they happen to have
    // the same ID as an old one.
    let old_index_ids: Vec<ID> = old_index_ids
        .into_iter()
        .filter(|id| !new_index_ids.contains(id))
        .collect();

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

    futures::stream::iter(old_index_ids.iter())
        .map(|id| {
            let repo = repo.clone();
            let index_delete_bar = index_delete_bar.clone();
            let deleted_size_ptr = &deleted_size;

            async move {
                match repo.delete_file(ContentIdType::Index, id, None).await {
                    Ok(size) => {
                        deleted_size_ptr.fetch_add(size, Ordering::AcqRel);
                    }
                    Err(e) => {
                        ui::cli::error!("Failed to delete index {}: {}", id, e);
                    }
                }
                index_delete_bar.inc(1);
            }
        })
        .buffer_unordered(5)
        .collect::<()>()
        .await;

    index_delete_bar.finish_and_clear();

    ui::cli::log!(
        "Deleted {} obsolete index files",
        index_delete_bar.position()
    );

    // Report added space
    let added_size: i64 =
        new_index_size.encoded as i64 - deleted_size.load(Ordering::Relaxed) as i64;
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

    lock_handle.unlock().await;

    Ok(())
}
