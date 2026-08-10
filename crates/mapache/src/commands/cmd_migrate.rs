// TODO(v1-removal): Remove this entire command.
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
    common::{BlobType, ContentIdType, ID, error::MapacheError},
    repository::{
        index::{IndexMode, MasterIndex},
        migration,
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

            // v1 → v2: nonce moves from start to end
            let old_nonce_at_end = false;
            let new_nonce_at_end = true;

            // Collect old IDs for cleanup AFTER manifest update.
            let mut old_pack_ids: Vec<ID> = Vec::new();
            let mut old_snapshot_ids: Vec<ID> = Vec::new();
            let mut new_snapshot_ids: Vec<ID> = Vec::new();

            // ── Step 1: Re-encrypt packs ──────────────────────────────
            let all_pack_ids = repo.list_packs().await?;
            old_pack_ids.extend(all_pack_ids.iter().copied());
            ui::cli::log!("\nStep 1/4: Re-encrypting {} packs...", all_pack_ids.len());
            tracing::info!(target: "migrate", "Step 1: Re-encrypting {} packs", all_pack_ids.len());

            let reenc_label = if args.dry_run {
                "Scanning packs"
            } else {
                "Re-encrypting packs"
            };
            let reenc_bar = ProgressBar::with_draw_target(
                Some(all_pack_ids.len() as u64),
                default_bar_draw_target(),
            )
            .with_style(
                default_progress_style()
                    .template(&format!(
                        "[{{bar:20.cyan/white}}] {reenc_label}: {{pos}}/{{len}}"
                    ))
                    .expect("invalid progress bar template"),
            );

            // Mapping from old pack_id → (new_pack_id, descriptors)
            let mut pack_map: Vec<(ID, ID, Vec<_>)> = Vec::new();

            let dry = args.dry_run;
            let results: Vec<_> = futures::stream::iter(all_pack_ids.iter())
                .map(|pack_id| {
                    let repo = repo.clone();
                    let backend = backend.clone();
                    let secure_storage = secure_storage.clone();
                    let reenc_bar = reenc_bar.clone();

                    async move {
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
                            // Dry run: validate pack can be read and decrypted.
                            Some(
                                migration::validate_pack(
                                    repo.as_ref(),
                                    backend.as_ref(),
                                    secure_storage.as_ref(),
                                    pack_id,
                                    old_nonce_at_end,
                                )
                                .await
                                .map(|_blob_count| (*pack_id, Vec::new())),
                            )
                        };

                        reenc_bar.inc(1);
                        (pack_id, res)
                    }
                })
                .buffered(4)
                .collect()
                .await;

            reenc_bar.finish_and_clear();

            let mut error_count = 0;
            for (old_id, res) in results {
                match res {
                    None => {
                        // Cannot happen: both branches return Some.
                    }
                    Some(Ok((new_id, descriptors))) => {
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

            // ── Step 2: Re-encrypt snapshots ──────────────────────────
            let snapshot_ids = repo.list_snapshot_ids().await?;
            let dropped_ids = repo.list_dropped_snapshot_ids().await?;
            old_snapshot_ids.extend(snapshot_ids.iter().copied());
            old_snapshot_ids.extend(dropped_ids.iter().copied());
            let total_files = snapshot_ids.len() + dropped_ids.len();

            if total_files > 0 {
                ui::cli::log!("\nStep 2/4: Re-encrypting {} snapshots...", total_files);
                tracing::info!(target: "migrate", "Step 2: Re-encrypting {} snapshots", total_files);

                let file_label = if args.dry_run {
                    "Scanning snapshots"
                } else {
                    "Re-encrypting snapshots"
                };
                let file_bar = ProgressBar::with_draw_target(
                    Some(total_files as u64),
                    default_bar_draw_target(),
                )
                .with_style(
                    default_progress_style()
                        .template(&format!(
                            "[{{bar:20.cyan/white}}] {file_label}: {{pos}}/{{len}}"
                        ))
                        .expect("invalid progress bar template"),
                );

                // Re-encrypt active snapshots
                let mut snapshot_error_count = 0;
                for old_id in &snapshot_ids {
                    if cleanup_handler.is_interrupted() {
                        return Err(MigrateError::Interrupted);
                    }
                    if !args.dry_run {
                        match migration::re_encrypt_file(
                            repo.as_ref(),
                            backend.as_ref(),
                            secure_storage.as_ref(),
                            ContentIdType::Snapshot,
                            old_id,
                            old_nonce_at_end,
                            new_nonce_at_end,
                        )
                        .await
                        {
                            Ok(new_id) => {
                            new_snapshot_ids.push(new_id);
                        }
                            Err(e) => {
                                snapshot_error_count += 1;
                                ui::cli::error!("error re-encrypting snapshot {old_id}: {e}");
                            }
                        }
                    }
                    file_bar.inc(1);
                }

                // Re-encrypt dropped snapshots
                for old_id in &dropped_ids {
                    if cleanup_handler.is_interrupted() {
                        return Err(MigrateError::Interrupted);
                    }
                    if !args.dry_run {
                        match migration::re_encrypt_file(
                            repo.as_ref(),
                            backend.as_ref(),
                            secure_storage.as_ref(),
                            ContentIdType::Snapshot,
                            old_id,
                            old_nonce_at_end,
                            new_nonce_at_end,
                        )
                        .await
                        {
                            Ok(new_id) => {
                            new_snapshot_ids.push(new_id);
                        }
                            Err(e) => {
                                snapshot_error_count += 1;
                                ui::cli::error!("error re-encrypting dropped snapshot {old_id}: {e}");
                            }
                        }
                    }
                    file_bar.inc(1);
                }

                file_bar.finish_and_clear();

                if !args.dry_run && snapshot_error_count > 0 {
                    return Err(MigrateError::Repo(MapacheError::Format(format!(
                        "aborting migration: {snapshot_error_count} snapshot(s) failed to re-encrypt"
                    ))));
                }

                ui::cli::log!("Re-encrypted {} snapshot files", total_files);
            } else {
                ui::cli::log!("\nStep 2/4: No snapshots to re-encrypt");
            }

            // ── Step 3: Rebuild index and persist ──────────────────────
            // This step writes NEW index files without touching old ones.
            ui::cli::log!("\nStep 3/4: Rebuilding index...");
            tracing::info!(target: "migrate", "Step 3: Rebuilding index");

            let mut new_master_index = MasterIndex::new(IndexMode::Eager);
            new_master_index.set_autosave(false);

            let scan_bar = ProgressBar::with_draw_target(
                Some(pack_map.len() as u64),
                default_bar_draw_target(),
            )
            .with_style(
                default_progress_style()
                    .template("[{bar:20.cyan/white}] Building index: {pos}/{len}")
                    .expect("invalid progress bar template"),
            );

            let mut blob_count = 0;
            let mut zero_blob_count = 0;
            for (_old_id, new_id, descriptors) in &pack_map {
                if cleanup_handler.is_interrupted() {
                    return Err(MigrateError::Interrupted);
                }

                // Zero blobs are registered in pack footers as BlobType::Zero with length=0.
                // They go through add_pack like data/tree blobs.
                // parse_footer already filters out Padding entries.
                zero_blob_count += descriptors
                    .iter()
                    .filter(|d| matches!(d.blob_type, BlobType::Zero))
                    .count();
                blob_count += descriptors.len();
                new_master_index
                    .add_pack(repo.as_ref(), new_id, descriptors.clone())
                    .await?;
                scan_bar.inc(1);
            }
            scan_bar.finish_and_clear();

            if zero_blob_count > 0 {
                ui::cli::log!("Index covers {} blobs ({} zero) across {} packs", blob_count, zero_blob_count, pack_map.len());
            } else {
                ui::cli::log!("Index covers {} blobs across {} packs", blob_count, pack_map.len());
            }

            let old_index_ids = repo.list_index_ids().await?;



            // ── Step 4: Update manifest (atomic commit point) ──────────
            // Everything before this point writes NEW data without touching OLD data.
            // After this point, the repo is v2 and consistent.
            ui::cli::log!("\nStep 4/4: Updating manifest and cleaning up...");
            tracing::info!(target: "migrate", "Step 4: Updating manifest and cleaning up");

            if !args.dry_run {
                // Flip to v2 nonce position (at end) before writing new data.
                secure_storage.set_nonce_at_end(new_nonce_at_end);

                // Re-save manifest with new nonce position (nonce at end).
                let mut manifest = repo.manifest().clone();
                manifest.set_version(THIS_REPOSITORY_VERSION);

                // Persist new binary index BEFORE updating manifest.
                let new_index_size = new_master_index
                    .persist_with_version(repo.as_ref(), Some(THIS_REPOSITORY_VERSION))
                    .await?;
                let new_index_ids = new_master_index.ids();
                ui::cli::log!("Persisted {} new binary index files", new_index_ids.len());

                // NOW update the manifest — this is the atomic commit point.
                repo.save_manifest(&manifest).await?;
                ui::cli::log!("Manifest updated to v{}", THIS_REPOSITORY_VERSION);

                // ── Cleanup: delete old data ────────────────────────────
                // From here on, failure is safe: the repo is consistent (v2),
                // orphaned files are just wasted space.

                // Delete ALL old packs — every pack was re-encrypted into a new file.
                // The new pack has a different ID (different nonce → different hash).
                let mut deleted_pack_count = 0u64;
                for (old_id, new_id, _) in &pack_map {
                    if old_id == new_id {
                        continue;
                    }

                    match repo.delete_file(ContentIdType::Pack, old_id, None).await {
                        Ok(_) => {
                            deleted_pack_count += 1;
                        }
                        Err(e) => {
                            ui::cli::warning!("failed to delete old pack {}: {}", old_id, e);
                        }
                    }
                }
                ui::cli::log!("Deleted {} old pack files", deleted_pack_count);

                // Delete ALL old snapshots — every snapshot was re-encrypted into a new file.
                let new_snapshot_set: std::collections::HashSet<ID> =
                    new_snapshot_ids.iter().copied().collect();
                let mut deleted_snap_count = 0u64;
                for old_id in &old_snapshot_ids {
                    if new_snapshot_set.contains(old_id) {
                        continue;
                    }

                    match repo.delete_file(ContentIdType::Snapshot, old_id, None).await {
                        Ok(_) => {
                            deleted_snap_count += 1;
                        }
                        Err(e) => {
                            ui::cli::warning!("failed to delete old snapshot {}: {}", old_id, e);
                        }
                    }
                }
                ui::cli::log!("Deleted {} old snapshot files", deleted_snap_count);

                // Delete old index files
                let deleted_index_size = AtomicU64::new(0);
                let old_index_ids: Vec<_> = old_index_ids
                    .into_iter()
                    .filter(|id| !new_index_ids.contains(id))
                    .collect();

                for id in &old_index_ids {
                    match repo.delete_file(ContentIdType::Index, id, None).await {
                        Ok(size) => {
                            deleted_index_size.fetch_add(size, Ordering::AcqRel);
                        }
                        Err(e) => {
                            ui::cli::error!("failed to delete index {}: {}", id, e);
                        }
                    }
                }
                ui::cli::log!("Deleted {} old index files", old_index_ids.len());

                let added_size: i64 = new_index_size.encoded as i64
                    - deleted_index_size.load(Ordering::Relaxed) as i64;
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
