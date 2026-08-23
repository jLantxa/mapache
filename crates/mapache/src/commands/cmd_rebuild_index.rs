use std::{
    io,
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

use clap::Args;
use futures::StreamExt;
use indicatif::ProgressBar;

use crate::{
    backend::new_backend_with_prompt,
    commands::{GlobalArgs, ToExitCode, cleanup::CleanupHandler, with_repository_lock},
    common::{ContentIdType, ID, error::MapacheError},
    repository::{index::MasterIndex, packer::Packer},
    ui::{self, cli::color::Colorize, default_bar_draw_target, default_progress_style},
    utils::{self},
};

#[derive(Debug, thiserror::Error)]
pub enum RebuildIndexError {
    #[error("index rebuild interrupted by user")]
    Interrupted,
    #[error(transparent)]
    Repo(#[from] MapacheError),
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl ToExitCode for RebuildIndexError {
    fn to_exit_code(&self) -> i32 {
        match self {
            RebuildIndexError::Interrupted => 130,
            RebuildIndexError::Repo(_) => 1,
            RebuildIndexError::Io(_) => 1,
        }
    }
}

#[derive(Args, Debug, Clone)]
#[clap(about = "Rebuild the index by scanning all existing packs")]
pub struct CmdArgs {
    /// Dry run
    #[clap(long, default_value_t = false)]
    pub dry_run: bool,
}

pub async fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<(), RebuildIndexError> {
    tracing::info!(target: "rebuild-index", "Starting rebuild-index command");
    with_repository_lock(
        global_args.auth_file.as_ref(),
        global_args.key.as_ref(),
        new_backend_with_prompt(global_args.backend_options(args.dry_run)).await?,
        global_args.to_repo_config(),
        false,
        global_args.retry_lock_duration,
        global_args.no_lock,
        |repo, secure_storage, lock_handle| async move {
            let backend = repo.backend();
            let cleanup_handler = CleanupHandler::new_with_callback(move || {
                ui::cli::log!(
                    "\n{}",
                    "Process interrupted. Cleaning up...".bold().yellow()
                );
            });
            cleanup_handler.add_lock(lock_handle);

            let start = Instant::now();

            if args.dry_run {
                ui::cli::log!("{}", "[DRY RUN]".bold().purple());
                tracing::info!(target: "rebuild-index", "Dry run enabled");
            }

            // Discover packs and blobs
            ui::cli::log!("Discovering packs...");
            tracing::info!(target: "rebuild-index", "Listing packs and existing indices");

            let all_pack_ids = repo.list_packs().await?;
            let old_index_ids = repo.list_index_ids().await?;
            let mut new_master_index = MasterIndex::new(global_args.index_mode);
            new_master_index.set_autosave(false);
            ui::cli::log!("Found {} packs", all_pack_ids.len());
            tracing::info!(target: "rebuild-index", "Found {} packs and {} indices", all_pack_ids.len(), old_index_ids.len());

            let scan_bar = ProgressBar::with_draw_target(
                Some(all_pack_ids.len() as u64),
                default_bar_draw_target(),
            )
            .with_style(
                default_progress_style()
                    .template("[{bar:20.cyan/white}] Scanning packs: {pos}/{len}")
                    .expect("invalid progress bar template for rebuild scanning"),
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
                            secure_storage.nonce_at_end(),
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
            tracing::info!(target: "rebuild-index", "Pack scanning finished");

            // Populate the new index
            let populate_bar = ProgressBar::with_draw_target(
                Some(all_pack_ids.len() as u64),
                default_bar_draw_target(),
            )
            .with_style(
                default_progress_style()
                    .template("[{bar:20.cyan/white}] Building index: {pos}/{len}")
                    .expect("invalid progress bar template for rebuild building"),
            );

            let mut blob_count = 0;
            let mut error_count = 0;

            for (pack_id, res) in results {
                if cleanup_handler.is_interrupted() {
                    tracing::info!(target: "rebuild-index", "Rebuild index interrupted by user");
                    return Err(RebuildIndexError::Interrupted);
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
                        ui::cli::error!("error reading pack {pack_id}: {e}");
                    }
                }
                populate_bar.inc(1);
            }

            populate_bar.finish_and_clear();

            ui::cli::log!("Found {} blobs", blob_count);
            tracing::info!(target: "rebuild-index", "Rebuild summary: {} blobs found, {} packs skipped due to errors", blob_count, error_count);
            if error_count > 0 {
                ui::cli::warning!("Skipped {} packs due to errors", error_count);
            }

            // Save the new index
            tracing::info!(target: "rebuild-index", "Persisting new index");
            let new_index_size = new_master_index.persist(repo.as_ref()).await?;
            let new_index_ids = new_master_index.ids();
            ui::cli::log!("Persisted {} new indices", new_index_ids.len());
            tracing::info!(target: "rebuild-index", "Persisted {} new index files", new_index_ids.len());

            // Delete the old index
            // We must ensure we don't delete any of the NEW indices if they happen to have
            // the same ID as an old one.
            let old_index_ids: Vec<ID> = old_index_ids
                .into_iter()
                .filter(|id| !new_index_ids.contains(id))
                .collect();

            let index_delete_bar = ProgressBar::with_draw_target(
                Some(old_index_ids.len() as u64),
                default_bar_draw_target(),
            )
            .with_style(
                default_progress_style()
                    .template(
                        "[{percent} %] [{bar:20.cyan/white}] Deleting old index files: {pos}/{len}",
                    )
                    .expect("invalid progress bar template for rebuild delete"),
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
                                ui::cli::error!("failed to delete index {}: {}", id, e);
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
            tracing::info!(target: "rebuild-index", "Deleted {} obsolete index files", index_delete_bar.position());

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

            let prefix = super::dry_run_prefix(args.dry_run);

            ui::cli::log!(
                "\n{}Finished in {}",
                prefix,
                utils::pretty_print_duration(start.elapsed()),
            );
            tracing::info!(target: "rebuild-index", "Rebuild-index command completed in {:?}", start.elapsed());

            Ok(())
        },
    )
    .await
}
