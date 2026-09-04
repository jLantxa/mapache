// TODO(v1-removal): Remove this entire command.
use std::{
    collections::{HashMap, HashSet},
    io,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use clap::Args;
use futures::StreamExt;
use indicatif::ProgressBar;
use parking_lot::Mutex;

use crate::{
    backend::new_backend_with_prompt,
    commands::{GlobalArgs, ToExitCode, cleanup::CleanupHandler, with_repository_lock},
    common::{ContentIdType, ID, defaults::UI_RATE_ESTIMATOR_WINDOW, error::MapacheError},
    repository::{
        index::MasterIndex,
        migration::{self, ReEncryptParams},
        repo::{REPO_DROPPED_EXTENSION, THIS_REPOSITORY_VERSION},
    },
    ui::{
        self, cli::color::Colorize, default_bar_draw_target, default_progress_style,
        with_custom_elapsed, with_custom_eta,
    },
    utils::{self, rate_estimator::RateEstimator},
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
#[clap(about = "Migrate repository format from v1 to v2 (binary index, nonce at end)")]
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

            let old_nonce_at_end = false;
            let new_nonce_at_end = true;

            let mut old_snapshot_ids: Vec<(ID, Option<&str>)> = Vec::new();
            let mut new_snapshot_ids = HashSet::new();

            // ── Step 1: Re-encrypt packs ──────────────────────────────
            let all_pack_ids = repo.list_packs().await?;
            ui::cli::log!("\nStep 1/4: Re-encrypting {} packs...", all_pack_ids.len());
            tracing::info!(target: "migrate", "Step 1: Re-encrypting {} packs", all_pack_ids.len());

            let reenc_label = if args.dry_run {
                "Scanning packs"
            } else {
                "Re-encrypting packs"
            };
            let reenc_rate = Arc::new(Mutex::new(RateEstimator::new(UI_RATE_ESTIMATOR_WINDOW)));
            let reenc_bar = ProgressBar::with_draw_target(
                Some(all_pack_ids.len() as u64),
                default_bar_draw_target(),
            )
            .with_style(
                with_custom_eta(
                    with_custom_elapsed(
                        default_progress_style()
                            .template(&format!(
                                "[{{percent}} %] [{{bar:20.cyan/white}}] [{{custom_elapsed}}] {reenc_label}: {{pos}}/{{len}} [ETA: {{custom_eta}}]"
                            ))
                            .expect("invalid progress bar template"),
                    ),
                    Arc::clone(&reenc_rate),
                ),
            );

            let mut pack_map: Vec<(ID, ID, Vec<_>)> = Vec::new();

            let dry = args.dry_run;
            let interrupted = cleanup_handler.interrupted.clone();
            let results: Vec<_> = futures::stream::iter(all_pack_ids.iter())
                .map(|pack_id| {
                    let repo = repo.clone();
                    let backend = backend.clone();
                    let secure_storage = secure_storage.clone();
                    let reenc_bar = reenc_bar.clone();
                    let reenc_rate = reenc_rate.clone();
                    let interrupted = interrupted.clone();

                    async move {
                        if interrupted.load(Ordering::SeqCst) {
                            reenc_bar.inc(1);
                            reenc_rate.lock().observe(reenc_bar.position() as f64);
                            return (pack_id, None);
                        }
                        let res = if !dry {
                            Some(
                                migration::re_encrypt_pack(
                                    repo.as_ref(),
                                    backend.as_ref(),
                                    secure_storage.as_ref(),
                                    pack_id,
                                    old_nonce_at_end,
                                    new_nonce_at_end,
                                )
                                .await,
                            )
                        } else {
                            Some(
                                migration::validate_pack(
                                    repo.as_ref(),
                                    backend.as_ref(),
                                    secure_storage.as_ref(),
                                    pack_id,
                                    old_nonce_at_end,
                                )
                                .await
                                .map(|_blob_count| (*pack_id, Vec::new(), HashMap::new())),
                            )
                        };

                        reenc_bar.inc(1);
                        reenc_rate.lock().observe(reenc_bar.position() as f64);
                        (pack_id, res)
                    }
                })
                .buffered(8)
                .collect()
                .await;

            reenc_bar.finish_and_clear();

            if cleanup_handler.is_interrupted() {
                return Err(MigrateError::Interrupted);
            }

            let mut error_count = 0;
            for (old_id, res) in results {
                match res {
                    None => {}
                    Some(Ok((new_id, descriptors, _tree_plaintexts))) => {
                        pack_map.push((*old_id, new_id, descriptors));
                    }
                    Some(Err(e)) => {
                        error_count += 1;
                        ui::cli::error!("error re-encrypting pack {old_id}: {e}");
                    }
                }
            }

            if dry {
                ui::cli::log!(
                    "[DRY RUN] Would re-encrypt {} packs",
                    all_pack_ids.len()
                );
                if error_count > 0 {
                    ui::cli::warning!(
                        "[DRY RUN] {error_count} pack(s) failed validation — migration would fail"
                    );
                }
            } else {
                ui::cli::log!("Re-encrypted {} packs successfully", pack_map.len());
                if error_count > 0 {
                    return Err(MigrateError::Repo(MapacheError::Format(format!(
                        "aborting migration: {error_count} pack(s) failed to re-encrypt"
                    ))));
                }
            }

            // ── Step 2: Re-encrypt snapshots ──────────────────────
            let snapshot_ids = repo.list_snapshot_ids().await?;
            {
                let dropped_ids = repo.list_dropped_snapshot_ids().await?;
                old_snapshot_ids.extend(snapshot_ids.iter().map(|id| (*id, None)));
                old_snapshot_ids.extend(
                    dropped_ids
                        .iter()
                        .map(|id| (*id, Some(REPO_DROPPED_EXTENSION))),
                );
                let total_files = snapshot_ids.len() + dropped_ids.len();

                if total_files > 0 {
                    ui::cli::log!("\nStep 2/4: Re-encrypting {} snapshots...", total_files);
                    tracing::info!(target: "migrate", "Step 2: Re-encrypting {} snapshots", total_files);

                    let file_label = if dry { "Scanning snapshots" } else { "Re-encrypting snapshots" };
                    let file_rate = Arc::new(Mutex::new(RateEstimator::new(UI_RATE_ESTIMATOR_WINDOW)));
                    let file_bar = ProgressBar::with_draw_target(
                        Some(total_files as u64),
                        default_bar_draw_target(),
                    )
                    .with_style(
                        with_custom_eta(
                            with_custom_elapsed(
                                default_progress_style()
                                    .template(&format!(
                                        "[{{percent}} %] [{{bar:20.cyan/white}}] [{{custom_elapsed}}] {file_label}: {{pos}}/{{len}} [ETA: {{custom_eta}}]"
                                    ))
                                    .expect("invalid progress bar template"),
                            ),
                            Arc::clone(&file_rate),
                        ),
                    );

                    // Merge active + dropped into a single iterator: (id, tag)
                    let all_snapshots = snapshot_ids
                        .iter().map(|id| (*id, None))
                        .chain(
                            dropped_ids
                                .iter()
                                .map(|id| (*id, Some(REPO_DROPPED_EXTENSION))),
                        );
                    let re_encrypt_params = ReEncryptParams {
                        repo: repo.as_ref(),
                        backend: backend.as_ref(),
                        secure_storage: secure_storage.as_ref(),
                        old_nonce_at_end,
                        new_nonce_at_end,
                    };

                    let mut snapshot_error_count = 0;
                    for (old_id, tag) in all_snapshots {
                        if cleanup_handler.is_interrupted() {
                            return Err(MigrateError::Interrupted);
                        }
                        if dry {
                            file_bar.inc(1);
                            file_rate.lock().observe(file_bar.position() as f64);
                            continue;
                        }

                        let result = migration::re_encrypt_file(
                            &re_encrypt_params,
                            ContentIdType::Snapshot,
                            &old_id,
                            tag,
                        )
                        .await;

                        match result {
                            Ok(new_id) => {
                                new_snapshot_ids.insert(new_id);
                            }
                            Err(e) => {
                                snapshot_error_count += 1;
                                ui::cli::error!("error re-encrypting snapshot {old_id}: {e}");
                            }
                        }
                        file_bar.inc(1);
                        file_rate.lock().observe(file_bar.position() as f64);
                    }

                    file_bar.finish_and_clear();

                    if !dry && snapshot_error_count > 0 {
                        return Err(MigrateError::Repo(MapacheError::Format(format!(
                            "aborting migration: {snapshot_error_count} snapshot(s) failed to re-encrypt"
                        ))));
                    }

                    ui::cli::log!("Re-encrypted {} snapshot files", total_files);
                } else {
                    ui::cli::log!("\nStep 2/4: No snapshots to re-encrypt");
                }
            }

            // ── Step 3: Rebuild index and persist ──────────────────────
            ui::cli::log!("\nStep 3/4: Rebuilding index...");
            tracing::info!(target: "migrate", "Step 3: Rebuilding index");

            let mut new_master_index = MasterIndex::new(global_args.index_mode);
            new_master_index.set_autosave(false);

            let scan_rate = Arc::new(Mutex::new(RateEstimator::new(UI_RATE_ESTIMATOR_WINDOW)));
            let scan_bar = ProgressBar::with_draw_target(
                Some(pack_map.len() as u64),
                default_bar_draw_target(),
            )
            .with_style(
                with_custom_eta(
                    with_custom_elapsed(
                        default_progress_style()
                            .template("[{percent} %] [{bar:20.cyan/white}] [{custom_elapsed}] Building index: {pos}/{len} [ETA: {custom_eta}]")
                            .expect("invalid progress bar template"),
                    ),
                    Arc::clone(&scan_rate),
                ),
            );

            let mut blob_count = 0;
            let mut zero_blob_count = 0;
            for (_old_id, new_id, descriptors) in &pack_map {
                if cleanup_handler.is_interrupted() {
                    return Err(MigrateError::Interrupted);
                }

                zero_blob_count += descriptors
                    .iter()
                    .filter(|d| matches!(d.blob_type, crate::common::BlobType::Zero))
                    .count();
                blob_count += descriptors.len();
                new_master_index
                    .add_pack(repo.as_ref(), new_id, descriptors.clone())
                    .await?;
                scan_bar.inc(1);
                scan_rate.lock().observe(scan_bar.position() as f64);
            }

            scan_bar.finish_and_clear();

            let total_packs = pack_map.len();
            if zero_blob_count > 0 {
                ui::cli::log!("Index covers {} blobs ({} zero) across {} packs", blob_count, zero_blob_count, total_packs);
            } else {
                ui::cli::log!("Index covers {} blobs across {} packs", blob_count, total_packs);
            }

            let old_index_ids = repo.list_index_ids().await?;

            // ── Step 4: Update manifest (atomic commit point) ──────────
            ui::cli::log!("\nStep 4/4: Updating manifest and cleaning up...");
            tracing::info!(target: "migrate", "Step 4: Updating manifest and cleaning up");

            if !args.dry_run {
                secure_storage.set_nonce_at_end(new_nonce_at_end);

                let mut manifest = repo.manifest().clone();
                manifest.set_version(THIS_REPOSITORY_VERSION);

                let new_index_size = new_master_index
                    .persist_with_version(repo.as_ref(), Some(THIS_REPOSITORY_VERSION))
                    .await?;
                let new_index_ids = new_master_index.ids();
                ui::cli::log!("Persisted {} new binary index files", new_index_ids.len());

                repo.save_manifest(&manifest).await?;
                ui::cli::log!("Manifest updated to v{}", THIS_REPOSITORY_VERSION);

                // Cleanup: drop old files to .dropped (atomic), then clean up
                let mut deleted_pack_count = 0u64;
                let mut deletion_failures = 0u32;
                for (old_id, new_id, _) in &pack_map {
                    if old_id == new_id {
                        continue;
                    }
                    match repo.drop_file(ContentIdType::Pack, old_id).await {
                        Ok(_) => { deleted_pack_count += 1; }
                        Err(e) => { ui::cli::warning!("failed to drop old pack {}: {}", old_id, e); deletion_failures += 1; }
                    }
                }
                ui::cli::log!("Dropped {} old pack files", deleted_pack_count);

                let mut deleted_snap_count = 0u64;
                for (old_id, extension) in &old_snapshot_ids {
                    if new_snapshot_ids.contains(old_id) {
                        continue;
                    }

                    let cleanup_result = repo.delete_file(ContentIdType::Snapshot, old_id, *extension).await;

                    match cleanup_result {
                        Ok(_) => deleted_snap_count += 1,
                        Err(e) => {
                            ui::cli::warning!(
                                "failed to remove old snapshot {}: {}",
                                old_id,
                                e
                            );
                            deletion_failures += 1;
                        }
                    }
                }
                ui::cli::log!("Removed {} old snapshot files", deleted_snap_count);

                let deleted_index_size = AtomicU64::new(0);
                let old_index_ids: Vec<_> = old_index_ids
                    .into_iter()
                    .filter(|id| !new_index_ids.contains(id))
                    .collect();
                for id in &old_index_ids {
                    match repo.drop_file(ContentIdType::Index, id).await {
                        Ok(size) => { deleted_index_size.fetch_add(size, Ordering::AcqRel); }
                        Err(e) => { ui::cli::error!("failed to drop index {}: {}", id, e); deletion_failures += 1; }
                    }
                }
                ui::cli::log!("Dropped {} old index files", old_index_ids.len());

                if deletion_failures > 0 {
                    return Err(MigrateError::Repo(MapacheError::Repo(format!(
                        "migration completed but failed to drop {deletion_failures} old files"
                    ))));
                }

                // Clean up dropped pack and index files after successful migration.
                for ct in [ContentIdType::Pack, ContentIdType::Index] {
                    if let Err(e) = repo.clean_dropped(ct).await {
                        ui::cli::warning!("failed to clean {ct} dropped files after migration: {e}");
                    }
                }

                let added_size: i64 = new_index_size.encoded as i64
                    - deleted_index_size.load(Ordering::Relaxed) as i64;
                if added_size >= 0 {
                    ui::cli::log!(
                        "Index space change: +{}",
                        utils::format_size_binary(added_size.unsigned_abs(), 3).bold().yellow()
                    );
                } else {
                    ui::cli::log!(
                        "Index space freed: {}",
                        utils::format_size_binary(added_size.unsigned_abs(), 3).bold().green()
                    );
                }
            } else {
                ui::cli::log!("[DRY RUN] Would update manifest to v{}", THIS_REPOSITORY_VERSION);
                ui::cli::log!("[DRY RUN] Would rebuild index with {} packs", pack_map.len());
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
