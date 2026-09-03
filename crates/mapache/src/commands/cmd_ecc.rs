use std::{io, path::PathBuf, sync::Arc, time::Instant};

use clap::{Args, Subcommand};
use indicatif::ProgressBar;

use crate::{
    backend::{Handle, StorageBackend, WriteContents, new_backend_with_prompt},
    commands::{GlobalArgs, ToExitCode, cleanup::CleanupHandler, with_repository_lock},
    common::{ContentIdType, error::MapacheError},
    ecc,
    repository::{
        manifest::EccConfig,
        repo::{REPO_ECC_EXTENSION, Repository},
        storage::SecureStorage,
    },
    ui::{self, default_bar_draw_target, default_progress_style},
    utils,
};

#[derive(Debug, thiserror::Error)]
pub enum EccError {
    #[error("ecc operation interrupted by user")]
    Interrupted,
    #[error(transparent)]
    Repo(#[from] MapacheError),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("{0}")]
    InvalidArg(String),
}

impl ToExitCode for EccError {
    fn to_exit_code(&self) -> i32 {
        match self {
            EccError::Interrupted => 130,
            EccError::Repo(_) => 1,
            EccError::Io(_) => 1,
            EccError::InvalidArg(_) => 1,
        }
    }
}

#[derive(Subcommand, Debug, Clone)]
pub enum SubCmd {
    #[clap(about = "Enable ECC and generate all sidecars")]
    Enable {
        /// ECC overhead percentage (1–100)
        percent: u32,
    },
    #[clap(about = "Disable ECC and remove all sidecars")]
    Disable,
    #[clap(about = "Change ECC percentage and regenerate all sidecars")]
    SetPercent {
        /// New ECC overhead percentage (1–100)
        percent: u32,
    },
    #[clap(about = "Regenerate all ECC sidecars using current config")]
    Regenerate,
}

#[derive(Args, Debug, Clone)]
#[clap(
    about = "Manage ECC sidecars",
    long_about = "Manage Reed-Solomon ECC sidecars for the repository.\n\n\
        ECC (Error Correction Code) sidecars protect packs, indices, and snapshots\n\
        against bit-rot. This command enables, disables, or regenerates those sidecars."
)]
pub struct CmdArgs {
    #[clap(subcommand)]
    pub subcmd: SubCmd,
}

fn parse_percent(percent: u32) -> Result<EccConfig, EccError> {
    if percent == 0 || percent > 100 {
        return Err(EccError::InvalidArg(format!(
            "ECC overhead percentage must be between 1 and 100, got {percent}"
        )));
    }
    Ok(EccConfig::from_overhead(percent).expect("percent is in 1..=100"))
}

pub async fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<(), EccError> {
    with_repository_lock(
        global_args.auth_file.as_ref(),
        global_args.key.as_ref(),
        new_backend_with_prompt(global_args.backend_options(false)).await?,
        global_args.to_repo_config(),
        true,
        global_args.retry_lock_duration,
        global_args.no_lock,
        |repo, secure_storage, lock_handle| async move {
            let backend = repo.backend();
            let cleanup_handler = CleanupHandler::new();
            cleanup_handler.add_lock(lock_handle);

            // TODO(v1-removal): Remove this check after v1 support is dropped.
            if repo.manifest().version() < 2 {
                return Err(EccError::InvalidArg(
                    "ECC is not supported in repository format v1; \
                     migrate to v2 first with 'mapache migrate'"
                        .into(),
                ));
            }

            match &args.subcmd {
                SubCmd::Enable { percent } => {
                    cmd_enable(&repo, &backend, &secure_storage, &cleanup_handler, *percent).await
                }
                SubCmd::Disable => cmd_disable(&repo, &backend, &cleanup_handler).await,
                SubCmd::SetPercent { percent } => {
                    cmd_set_percent(&repo, &backend, &secure_storage, &cleanup_handler, *percent)
                        .await
                }
                SubCmd::Regenerate => {
                    cmd_regenerate(&repo, &backend, &secure_storage, &cleanup_handler).await
                }
            }
        },
    )
    .await
}

async fn cmd_enable(
    repo: &Arc<Repository>,
    backend: &Arc<dyn StorageBackend>,
    secure_storage: &SecureStorage,
    cleanup_handler: &CleanupHandler,
    percent: u32,
) -> Result<(), EccError> {
    let config = parse_percent(percent)?;
    let start = Instant::now();

    if repo.manifest().ecc().is_some() {
        return Err(EccError::InvalidArg(
            "ECC is already enabled (use 'ecc set-percent' to change the percentage)".into(),
        ));
    }

    ui::cli::log!("Enabling ECC with {}% overhead...", percent);
    tracing::info!(target: "ecc", "Enabling ECC with {}% overhead", percent);

    let count =
        regenerate_sidecars(repo, backend, secure_storage, &config, cleanup_handler).await?;

    let mut manifest = repo.manifest().clone();
    manifest.set_ecc(Some(config));
    repo.save_manifest(&manifest).await?;

    ui::cli::log!(
        "Done. Generated {} sidecars in {}",
        count,
        utils::pretty_print_duration(start.elapsed()),
    );
    tracing::info!(target: "ecc", "Enable completed: {} sidecars in {:?}", count, start.elapsed());

    Ok(())
}

async fn cmd_disable(
    repo: &Arc<Repository>,
    backend: &Arc<dyn StorageBackend>,
    cleanup_handler: &CleanupHandler,
) -> Result<(), EccError> {
    let start = Instant::now();

    if repo.manifest().ecc().is_none() {
        return Err(EccError::InvalidArg("ECC is not enabled".into()));
    }

    ui::cli::log!("Disabling ECC...");
    tracing::info!(target: "ecc", "Disabling ECC");

    let count = delete_all_sidecars(backend, cleanup_handler).await?;

    let mut manifest = repo.manifest().clone();
    manifest.set_ecc(None);
    repo.save_manifest(&manifest).await?;

    ui::cli::log!(
        "Done. Removed {} sidecars in {}",
        count,
        utils::pretty_print_duration(start.elapsed()),
    );
    tracing::info!(target: "ecc", "Disable completed: {} sidecars removed in {:?}", count, start.elapsed());

    Ok(())
}

async fn cmd_set_percent(
    repo: &Arc<Repository>,
    backend: &Arc<dyn StorageBackend>,
    secure_storage: &SecureStorage,
    cleanup_handler: &CleanupHandler,
    percent: u32,
) -> Result<(), EccError> {
    let new_config = parse_percent(percent)?;
    let start = Instant::now();

    ui::cli::log!(
        "Changing ECC overhead to {}% (regenerating all sidecars)...",
        percent
    );
    tracing::info!(target: "ecc", "Setting ECC to {}% overhead", percent);

    // Step 1: Disable ECC in the manifest so interrupted runs are safe.
    {
        let mut manifest = repo.manifest().clone();
        manifest.set_ecc(None);
        repo.save_manifest(&manifest).await?;
    }

    // Step 2: Delete all existing sidecars.
    let _deleted = delete_all_sidecars(backend, cleanup_handler).await?;

    // Step 3: Regenerate all sidecars with the new config.
    let count =
        regenerate_sidecars(repo, backend, secure_storage, &new_config, cleanup_handler).await?;

    // Step 4: Update manifest with the new ECC config.
    {
        let mut manifest = repo.manifest().clone();
        manifest.set_ecc(Some(new_config));
        repo.save_manifest(&manifest).await?;
    }

    ui::cli::log!(
        "Done. Generated {} sidecars in {}",
        count,
        utils::pretty_print_duration(start.elapsed()),
    );
    tracing::info!(target: "ecc", "Set-percent completed: {} sidecars in {:?}", count, start.elapsed());

    Ok(())
}

async fn cmd_regenerate(
    repo: &Arc<Repository>,
    backend: &Arc<dyn StorageBackend>,
    secure_storage: &SecureStorage,
    cleanup_handler: &CleanupHandler,
) -> Result<(), EccError> {
    let config = repo
        .manifest()
        .ecc()
        .cloned()
        .ok_or_else(|| EccError::InvalidArg("ECC is not enabled in the manifest".into()))?;

    let start = Instant::now();

    ui::cli::log!(
        "Regenerating all ECC sidecars (K={}, P={})...",
        config.data_shards,
        config.parity_shards,
    );
    tracing::info!(
        target: "ecc",
        "Regenerating sidecars with K={}, P={}",
        config.data_shards,
        config.parity_shards,
    );

    // Regenerate sidecars. We don't need to delete the sidecars first because:
    // they will be rewritten or discarded by the GC the next time we run clean.
    let count =
        regenerate_sidecars(repo, backend, secure_storage, &config, cleanup_handler).await?;

    ui::cli::log!(
        "Done. Generated {} sidecars in {}",
        count,
        utils::pretty_print_duration(start.elapsed()),
    );
    tracing::info!(target: "ecc", "Regenerate completed: {} sidecars in {:?}", count, start.elapsed());

    Ok(())
}

/// Delete all existing `.ecc` sidecar files across packs, indices, and snapshots.
async fn delete_all_sidecars(
    backend: &Arc<dyn StorageBackend>,
    cleanup_handler: &CleanupHandler,
) -> Result<usize, EccError> {
    let mut count = 0usize;

    for file_type in &[
        ContentIdType::Pack,
        ContentIdType::Index,
        ContentIdType::Snapshot,
    ] {
        let all_files = list_all_sidecar_paths(backend, *file_type).await?;

        for path in &all_files {
            if cleanup_handler.is_interrupted() {
                return Err(EccError::Interrupted);
            }
            // Best-effort delete — ignore errors for missing files.
            let _ = backend.remove(path).await;
            count += 1;
        }
    }

    Ok(count)
}

/// List all `.ecc` sidecar paths for a given file type.
async fn list_all_sidecar_paths(
    backend: &Arc<dyn StorageBackend>,
    file_type: ContentIdType,
) -> Result<Vec<PathBuf>, EccError> {
    use crate::repository::repo::SNAPSHOTS_DIR;

    let dir = match file_type {
        ContentIdType::Pack => crate::repository::repo::OBJECTS_DIR,
        ContentIdType::Index => crate::repository::repo::INDEX_DIR,
        ContentIdType::Snapshot => SNAPSHOTS_DIR,
        _ => return Ok(Vec::new()),
    };

    let mut stack = vec![PathBuf::from(dir)];
    let mut ecc_paths = Vec::new();

    while let Some(current_dir) = stack.pop() {
        let entries = crate::backend::read_backend_dir(backend.as_ref(), &current_dir)
            .await
            .map_err(|e| {
                MapacheError::Backend(format!(
                    "failed to list {}: {}",
                    current_dir.display(),
                    e.inner()
                ))
            })?;

        for entry in entries {
            match entry {
                crate::backend::BackendNode::File(path, _) => {
                    if path
                        .extension()
                        .is_some_and(|ext| ext == REPO_ECC_EXTENSION)
                    {
                        ecc_paths.push(path);
                    }
                }
                crate::backend::BackendNode::Dir(subdir) => {
                    stack.push(subdir);
                }
            }
        }
    }

    Ok(ecc_paths)
}

/// Generate fresh ECC sidecars for every file that supports ECC
/// (packs, indices, snapshots) using the given config.
///
/// Existing sidecars must be deleted by the caller if needed.
async fn regenerate_sidecars(
    repo: &Arc<Repository>,
    backend: &Arc<dyn StorageBackend>,
    secure_storage: &SecureStorage,
    config: &EccConfig,
    cleanup_handler: &CleanupHandler,
) -> Result<usize, EccError> {
    let k = config.data_shards as usize;
    let p = config.parity_shards as usize;

    let mut total = 0usize;

    for file_type in &[
        ContentIdType::Pack,
        ContentIdType::Index,
        ContentIdType::Snapshot,
    ] {
        let files = repo.list_files(*file_type).await?;
        if files.is_empty() {
            continue;
        }

        let label = match file_type {
            ContentIdType::Pack => "packs",
            ContentIdType::Index => "indices",
            ContentIdType::Snapshot => "snapshots",
            _ => "other",
        };

        let bar =
            ProgressBar::with_draw_target(Some(files.len() as u64), default_bar_draw_target())
                .with_style(
                default_progress_style()
                    .template(&format!(
                        "[{{bar:20.cyan/white}}] Generating ECC sidecars ({label}): {{pos}}/{{len}}"
                    ))
                    .expect("invalid progress bar template for ecc"),
            );

        for path in &files {
            if cleanup_handler.is_interrupted() {
                bar.finish_and_clear();
                return Err(EccError::Interrupted);
            }

            let handle = Handle::new(path);
            let raw_data = backend.read(&handle, 0, 0).await.map_err(|e| {
                MapacheError::Backend(format!("failed to read {}: {}", path.display(), e.inner()))
            })?;

            if raw_data.is_empty() {
                bar.inc(1);
                continue;
            }

            let raw_ecc = tokio::task::spawn_blocking({
                // `raw_data` is moved into the closure; the reference is valid
                // because spawn_blocking takes ownership of the closure and
                // awaits the JoinHandle before raw_data goes out of scope.
                move || ecc::ecc_encode(&raw_data, k, p)
            })
            .await
            .map_err(|e| MapacheError::Internal(format!("ECC encode task failed: {e}")))?
            .map_err(|e| {
                MapacheError::Internal(format!("ECC encode failed for {}: {e}", path.display()))
            })?;

            if raw_ecc.is_empty() {
                bar.inc(1);
                continue;
            }

            let encoded_ecc = secure_storage.encode(&raw_ecc)?;
            let ecc_path = path.with_extension(REPO_ECC_EXTENSION);
            let tmp_path = path.with_extension("ecc.tmp");
            let tmp_handle = Handle::new(&tmp_path);

            backend
                .write(&tmp_handle, WriteContents::Owned(encoded_ecc))
                .await
                .map_err(|e| {
                    MapacheError::Backend(format!(
                        "failed to write ECC sidecar {}: {}",
                        ecc_path.display(),
                        e.inner()
                    ))
                })?;
            backend.rename(&tmp_path, &ecc_path).await.map_err(|e| {
                MapacheError::Backend(format!(
                    "failed to rename ECC sidecar {}: {}",
                    ecc_path.display(),
                    e.inner()
                ))
            })?;

            total += 1;
            bar.inc(1);
        }

        bar.finish_and_clear();
    }

    Ok(total)
}
