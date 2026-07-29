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
    common::{ContentIdType, error::MapacheError},
    repository::{
        index::{IndexMode, MasterIndex},
        manifest::Manifest,
        packer::Packer,
        repo::THIS_REPOSITORY_VERSION,
    },
    ui::{self, cli::color::Colorize, default_bar_draw_target, default_progress_style},
    utils,
};

#[derive(Debug, thiserror::Error)]
pub enum MigrateError {
    #[error("migration interrupted by user")]
    Interrupted,
    #[error(transparent)]
    Repo(#[from] MapacheError),
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl ToExitCode for MigrateError {
    fn to_exit_code(&self) -> i32 {
        match self {
            MigrateError::Interrupted => 130,
            MigrateError::Repo(_) => 1,
            MigrateError::Io(_) => 1,
        }
    }
}

#[derive(Args, Debug, Clone)]
#[clap(about = "Migrate repository format from v1 to v2 (binary index)")]
pub struct CmdArgs {
    /// Dry run
    #[clap(long, default_value_t = false)]
    pub dry_run: bool,
}

pub async fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<(), MigrateError> {
    tracing::info!(target: "migrate", "Starting migrate command");
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
            let current_version = repo.repo_version();

            if args.dry_run {
                ui::cli::log!("{}", "[DRY RUN]".bold().purple());
                tracing::info!(target: "migrate", "Dry run enabled");
            }

            // Check if migration is needed
            if current_version >= THIS_REPOSITORY_VERSION {
                ui::cli::log!(
                    "Repository is already at version {} (current: {}). No migration needed.",
                    THIS_REPOSITORY_VERSION,
                    current_version,
                );
                return Ok(());
            }

            ui::cli::log!(
                "Migrating repository from v{} to v{}...",
                current_version,
                THIS_REPOSITORY_VERSION
            );
            tracing::info!(target: "migrate", "Migrating from v{} to v{}", current_version, THIS_REPOSITORY_VERSION);

            // Step 1: Rebuild index (converts JSON index files to binary format)
            ui::cli::log!("\nStep 1/2: Rebuilding index...");
            tracing::info!(target: "migrate", "Step 1: Rebuilding index");

            let all_pack_ids = repo.list_packs().await?;
            let old_index_ids = repo.list_index_ids().await?;
            let mut new_master_index = MasterIndex::new(IndexMode::Eager);
            new_master_index.set_autosave(false);
            ui::cli::log!("Found {} packs", all_pack_ids.len());

            let scan_bar = ProgressBar::with_draw_target(
                Some(all_pack_ids.len() as u64),
                default_bar_draw_target(),
            )
            .with_style(
                default_progress_style()
                    .template("[{bar:20.cyan/white}] Scanning packs: {pos}/{len}")
                    .expect("invalid progress bar template"),
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

            let populate_bar = ProgressBar::with_draw_target(
                Some(all_pack_ids.len() as u64),
                default_bar_draw_target(),
            )
            .with_style(
                default_progress_style()
                    .template("[{bar:20.cyan/white}] Building index: {pos}/{len}")
                    .expect("invalid progress bar template"),
            );

            let mut blob_count = 0;
            let mut error_count = 0;

            for (pack_id, res) in results {
                if cleanup_handler.is_interrupted() {
                    tracing::info!(target: "migrate", "Migration interrupted by user");
                    return Err(MigrateError::Interrupted);
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
            if error_count > 0 {
                ui::cli::warning!("Skipped {} packs due to errors", error_count);
            }

            if !args.dry_run {
                // Persist new binary index
                let new_index_size = new_master_index.persist(repo.as_ref()).await?;
                let new_index_ids = new_master_index.ids();
                ui::cli::log!("Persisted {} new binary index files", new_index_ids.len());

                // Delete old JSON index files
                let old_index_ids: Vec<_> = old_index_ids
                    .into_iter()
                    .filter(|id| !new_index_ids.contains(id))
                    .collect();

                let deleted_size = AtomicU64::new(0);
                for id in &old_index_ids {
                    match repo.delete_file(ContentIdType::Index, id, None).await {
                        Ok(size) => {
                            deleted_size.fetch_add(size, Ordering::AcqRel);
                        }
                        Err(e) => {
                            ui::cli::error!("failed to delete index {}: {}", id, e);
                        }
                    }
                }

                ui::cli::log!("Deleted {} old index files", old_index_ids.len());

                // Step 2: Update manifest version
                ui::cli::log!("\nStep 2/2: Updating manifest version...");
                tracing::info!(target: "migrate", "Step 2: Updating manifest version to {}", THIS_REPOSITORY_VERSION);

                let mut manifest = Manifest::new(THIS_REPOSITORY_VERSION);
                // Preserve original ID and creation time
                manifest.set_version(THIS_REPOSITORY_VERSION);

                // We need to reconstruct the manifest with the original ID and time.
                // Since we can't mutate the existing one easily (it's behind Arc in the repo),
                // we'll load it, modify, and save.
                let manifest_data = backend
                    .read(
                        &crate::backend::Handle::new(std::path::Path::new(
                            crate::repository::repo::MANIFEST_PATH,
                        )),
                        0,
                        0,
                    )
                    .await
                    .map_err(|e| MapacheError::Repo(format!("failed to read manifest: {e}")))?;
                let decoded = secure_storage.decode(&manifest_data)?;
                let mut manifest: Manifest =
                    serde_json::from_slice(&decoded).map_err(MapacheError::Serialization)?;
                manifest.set_version(THIS_REPOSITORY_VERSION);

                repo.save_manifest(&manifest).await?;
                ui::cli::log!(
                    "Manifest updated to v{}",
                    THIS_REPOSITORY_VERSION
                );

                let added_size: i64 = new_index_size.encoded as i64
                    - deleted_size.load(Ordering::Relaxed) as i64;
                if added_size >= 0 {
                    ui::cli::log!(
                        "Index space change: +{}",
                        utils::format_size_binary(added_size.unsigned_abs(), 3)
                            .bold()
                            .yellow()
                    );
                } else {
                    ui::cli::log!(
                        "Index space freed: {}",
                        utils::format_size_binary(added_size.unsigned_abs(), 3)
                            .bold()
                            .green()
                    );
                }
            } else {
                ui::cli::log!("\nStep 2/2: Would update manifest version to {}", THIS_REPOSITORY_VERSION);
            }

            let prefix = super::dry_run_prefix(args.dry_run);
            ui::cli::log!(
                "\n{}Migration completed in {}",
                prefix,
                utils::pretty_print_duration(start.elapsed()),
            );
            tracing::info!(target: "migrate", "Migration completed in {:?}", start.elapsed());

            Ok(())
        },
    )
    .await
}
