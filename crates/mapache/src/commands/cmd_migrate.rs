// TODO(v1-removal): Remove this entire command.
use std::{
    collections::HashMap,
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
    repository::{index::MasterIndex, migration, repo::THIS_REPOSITORY_VERSION},
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

            let mut old_pack_ids: Vec<ID> = Vec::new();
            let mut old_snapshot_ids: Vec<ID> = Vec::new();
            let mut new_snapshot_ids: Vec<ID> = Vec::new();

            // ── Step 1: Re-encrypt packs ──────────────────────────────
            let all_pack_ids = repo.list_packs().await?;
            old_pack_ids.extend(all_pack_ids.iter().copied());
            ui::cli::log!("\nStep 1/5: Re-encrypting {} packs...", all_pack_ids.len());
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
                        "[{{bar:20.cyan/white}}] {reenc_label}: {{pos}}/{{len}} {{elapsed}} {{eta}}"
                    ))
                    .expect("invalid progress bar template"),
            );

            let mut pack_map: Vec<(ID, ID, Vec<_>)> = Vec::new();
            let mut all_tree_plaintexts: HashMap<ID, Vec<u8>> = HashMap::new();

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
                        (pack_id, res)
                    }
                })
                .buffered(8)
                .collect()
                .await;

            reenc_bar.finish_and_clear();

            let mut error_count = 0;
            for (old_id, res) in results {
                match res {
                    None => {}
                    Some(Ok((new_id, descriptors, tree_plaintexts))) => {
                        all_tree_plaintexts.extend(tree_plaintexts);
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

            // ── Step 2: Re-serialize trees (JSON → binary) + pack them ─
            let snapshot_ids = repo.list_snapshot_ids().await?;
            let mut root_tree_ids: Vec<ID> = Vec::new();
            for snap_id in &snapshot_ids {
                let snap = repo.load_snapshot(snap_id, None).await?;
                root_tree_ids.push(snap.tree);
            }

            let mut root_map: HashMap<ID, ID> = HashMap::new();
            let mut tree_pack_descriptor: Option<(ID, Vec<_>)> = None;

            if !all_tree_plaintexts.is_empty() && !root_tree_ids.is_empty() {
                let tree_count = all_tree_plaintexts.len();
                ui::cli::log!("\nStep 2/5: Re-serializing {tree_count} tree blobs (JSON → binary)...");
                tracing::info!(target: "migrate", "Step 2: Re-serializing {tree_count} tree blobs");

                let tree_bar = ProgressBar::with_draw_target(
                    Some(tree_count as u64),
                    default_bar_draw_target(),
                )
                .with_style(
                    default_progress_style()
                        .template("[{bar:20.cyan/white}] Re-serializing trees: {pos}/{len} {elapsed} {eta}")
                        .expect("invalid progress bar template"),
                );

                if !dry {
                    let (rm, serialized_trees) =
                        migration::update_tree_hierarchy(&all_tree_plaintexts, &root_tree_ids)?;

                    tree_bar.finish_and_clear();
                    ui::cli::log!(
                        "Re-serialized {} tree blobs, {} root trees updated",
                        serialized_trees.len(),
                        rm.len()
                    );

                    root_map = rm;

                    // Pack all re-serialized trees into a single pack.
                    if !serialized_trees.is_empty() {
                        secure_storage.set_nonce_at_end(new_nonce_at_end);
                        let mut blobs = Vec::with_capacity(serialized_trees.len());
                        for (tree_id, binary) in &serialized_trees {
                            let encoded = secure_storage.encode(binary)?;
                            blobs.push((*tree_id, crate::common::BlobType::Tree, encoded, binary.len() as u64));
                        }
                        if let Some((pack_id, descriptors)) = migration::create_pack_from_blobs(
                            repo.as_ref(), backend.as_ref(), &secure_storage, &blobs,
                        ).await? {
                            tree_pack_descriptor = Some((pack_id, descriptors));
                        }
                    }

                    drop(all_tree_plaintexts);
                } else {
                    tree_bar.finish_and_clear();
                    ui::cli::log!("[DRY RUN] Would re-serialize {tree_count} tree blobs");
                    drop(all_tree_plaintexts);
                }
            } else {
                ui::cli::log!("\nStep 2/5: No tree blobs to re-serialize");
                drop(all_tree_plaintexts);
            }

            // ── Step 3: Re-encrypt snapshots ──────────────────────
            {
                let dropped_ids = repo.list_dropped_snapshot_ids().await?;
                old_snapshot_ids.extend(snapshot_ids.iter().copied());
                old_snapshot_ids.extend(dropped_ids.iter().copied());
                let total_files = snapshot_ids.len() + dropped_ids.len();

                if total_files > 0 {
                    let has_tree_changes = !root_map.is_empty();
                    ui::cli::log!("\nStep 3/5: Re-encrypting {} snapshots{}...", total_files,
                        if has_tree_changes { " (with tree updates)" } else { "" });
                    tracing::info!(target: "migrate", "Step 3: Re-encrypting {} snapshots", total_files);

                    let file_label = if dry { "Scanning snapshots" } else { "Re-encrypting snapshots" };
                    let file_bar = ProgressBar::with_draw_target(
                        Some(total_files as u64),
                        default_bar_draw_target(),
                    )
                    .with_style(
                        default_progress_style()
                            .template(&format!(
                                "[{{bar:20.cyan/white}}] {file_label}: {{pos}}/{{len}} {{elapsed}} {{eta}}"
                            ))
                            .expect("invalid progress bar template"),
                    );

                    // Merge active + dropped into a single iterator: (id, tag)
                    let all_snapshots = snapshot_ids
                        .iter().map(|id| (*id, None))
                        .chain(dropped_ids.iter().map(|id| (*id, Some("dropped"))));

                    let mut snapshot_error_count = 0;
                    for (old_id, tag) in all_snapshots {
                        if cleanup_handler.is_interrupted() {
                            return Err(MigrateError::Interrupted);
                        }
                        if dry {
                            file_bar.inc(1);
                            continue;
                        }

                        let result = if has_tree_changes {
                            let snap = repo.load_snapshot(&old_id, tag).await?;
                            let new_root = root_map.get(&snap.tree).copied().unwrap_or(snap.tree);
                            let params = migration::ReEncryptParams {
                                repo: repo.as_ref(),
                                backend: backend.as_ref(),
                                secure_storage: secure_storage.as_ref(),
                                old_nonce_at_end,
                                new_nonce_at_end,
                            };
                            migration::re_encrypt_snapshot(
                                &params,
                                &old_id, new_root,
                            ).await
                        } else {
                            migration::re_encrypt_file(
                                repo.as_ref(), backend.as_ref(), secure_storage.as_ref(),
                                ContentIdType::Snapshot, &old_id, old_nonce_at_end, new_nonce_at_end,
                            ).await
                        };

                        match result {
                            Ok(new_id) => { new_snapshot_ids.push(new_id); }
                            Err(e) => {
                                snapshot_error_count += 1;
                                ui::cli::error!("error re-encrypting snapshot {old_id}: {e}");
                            }
                        }
                        file_bar.inc(1);
                    }

                    file_bar.finish_and_clear();

                    if !dry && snapshot_error_count > 0 {
                        return Err(MigrateError::Repo(MapacheError::Format(format!(
                            "aborting migration: {snapshot_error_count} snapshot(s) failed to re-encrypt"
                        ))));
                    }

                    ui::cli::log!("Re-encrypted {} snapshot files", total_files);
                } else {
                    ui::cli::log!("\nStep 3/5: No snapshots to re-encrypt");
                }
            }

            // ── Step 4: Rebuild index and persist ──────────────────────
            ui::cli::log!("\nStep 4/5: Rebuilding index...");
            tracing::info!(target: "migrate", "Step 4: Rebuilding index");

            let mut new_master_index = MasterIndex::new(global_args.index_mode);
            new_master_index.set_autosave(false);

            let scan_bar = ProgressBar::with_draw_target(
                Some(pack_map.len() as u64),
                default_bar_draw_target(),
            )
            .with_style(
                default_progress_style()
                    .template("[{bar:20.cyan/white}] Building index: {pos}/{len} {elapsed} {eta}")
                    .expect("invalid progress bar template"),
            );

            let mut blob_count = 0;
            let mut zero_blob_count = 0;
            let total_packs = pack_map.len();
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
            }

            // Register the tree pack (re-serialized binary trees) if any.
            let mut extra_pack_count = 0usize;
            if let Some((tree_pack_id, tree_descriptors)) = tree_pack_descriptor {
                blob_count += tree_descriptors.len();
                new_master_index
                    .add_pack(repo.as_ref(), &tree_pack_id, tree_descriptors)
                    .await?;
                extra_pack_count += 1;
            }

            scan_bar.finish_and_clear();

            let total_indexed_packs = total_packs + extra_pack_count;
            if zero_blob_count > 0 {
                ui::cli::log!("Index covers {} blobs ({} zero) across {} packs", blob_count, zero_blob_count, total_indexed_packs);
            } else {
                ui::cli::log!("Index covers {} blobs across {} packs", blob_count, total_indexed_packs);
            }

            let old_index_ids = repo.list_index_ids().await?;

            // ── Step 5: Update manifest (atomic commit point) ──────────
            ui::cli::log!("\nStep 5/5: Updating manifest and cleaning up...");
            tracing::info!(target: "migrate", "Step 5: Updating manifest and cleaning up");

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

                // Cleanup: move old files to .dropped (atomic), then clean up
                let mut deleted_pack_count = 0u64;
                let mut deletion_failures = 0u32;
                for (old_id, new_id, _) in &pack_map {
                    if old_id == new_id {
                        continue;
                    }
                    match repo.move_to_trash(ContentIdType::Pack, old_id).await {
                        Ok(_) => { deleted_pack_count += 1; }
                        Err(e) => { ui::cli::warning!("failed to move old pack {} to trash: {}", old_id, e); deletion_failures += 1; }
                    }
                }
                ui::cli::log!("Moved {} old pack files to trash", deleted_pack_count);

                let new_snapshot_set: std::collections::HashSet<ID> =
                    new_snapshot_ids.iter().copied().collect();
                let mut deleted_snap_count = 0u64;
                for old_id in &old_snapshot_ids {
                    if new_snapshot_set.contains(old_id) {
                        continue;
                    }
                    match repo.move_to_trash(ContentIdType::Snapshot, old_id).await {
                        Ok(_) => { deleted_snap_count += 1; }
                        Err(e) => { ui::cli::warning!("failed to move old snapshot {} to trash: {}", old_id, e); deletion_failures += 1; }
                    }
                }
                ui::cli::log!("Moved {} old snapshot files to trash", deleted_snap_count);

                let deleted_index_size = AtomicU64::new(0);
                let old_index_ids: Vec<_> = old_index_ids
                    .into_iter()
                    .filter(|id| !new_index_ids.contains(id))
                    .collect();
                for id in &old_index_ids {
                    match repo.move_to_trash(ContentIdType::Index, id).await {
                        Ok(size) => { deleted_index_size.fetch_add(size, Ordering::AcqRel); }
                        Err(e) => { ui::cli::error!("failed to move index {} to trash: {}", id, e); deletion_failures += 1; }
                    }
                }
                ui::cli::log!("Moved {} old index files to trash", old_index_ids.len());

                if deletion_failures > 0 {
                    return Err(MigrateError::Repo(MapacheError::Repo(format!(
                        "migration completed but failed to move {deletion_failures} old files to trash"
                    ))));
                }

                // Clean up trash files after successful migration
                for ct in [ContentIdType::Pack, ContentIdType::Snapshot, ContentIdType::Index] {
                    let _ = repo.clean_trash(ct).await;
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
