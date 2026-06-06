use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result};
use clap::Args;
use colored::Colorize;

pub use crate::fuse::fs::MapacheFS;
use crate::{
    backend::new_backend_with_prompt,
    bundle::reader::BundleReader,
    commands::{GlobalArgs, ToExitCode, cleanup::CleanupHandler, fail, with_repository_lock},
    fs,
    fuse::fs::MountOptions,
    mapache::{defaults::DEFAULT_FUSE_STASH_CACHE_SIZE_MIB, traits::BlobLoader},
    ui,
    utils::size,
};

#[derive(Debug, Clone, Copy)]
pub enum MountError {
    RepoOpenFail = 10,
    MountFailed = 20,
    Interrupted = 130,
}

impl ToExitCode for MountError {
    fn to_exit_code(&self) -> i32 {
        *self as i32
    }
}

#[derive(Args, Debug)]
#[clap(about = "Mount the repository or a .mapache bundle as a file system")]
pub struct CmdArgs {
    /// Mount point
    #[arg(value_parser)]
    pub mountpoint: PathBuf,

    /// Force mounting as a .mapache bundle
    #[arg(short, long, default_value_t = false)]
    pub bundle: bool,

    /// Mount point
    #[arg(long, value_parser, default_value_t = false)]
    pub allow_other: bool,

    /// Create the mountpoint if it does not exist
    #[arg(short, long, value_parser, default_value_t = false)]
    pub create_mountpoint: bool,

    /// Display files, but do not load its contents
    #[arg(long, value_parser, default_value_t = false)]
    pub metadata_only: bool,

    /// Max size of the internal data cache.
    #[arg(long = "cache-size-mib", value_parser, default_value_t = DEFAULT_FUSE_STASH_CACHE_SIZE_MIB)]
    pub data_cache_size_mib: f32,

    #[arg(skip)]
    pub internal_password: Option<String>,
}

pub async fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    tracing::info!(target: "mount", "Starting mount command (mountpoint={:?})", args.mountpoint);
    let actual_mountpoint = args.mountpoint.clone();
    let mut created_mountpoint = false;

    if !fs::path_exists(&actual_mountpoint).await {
        if args.create_mountpoint {
            std::fs::create_dir_all(&actual_mountpoint).context("Could not create mount point")?;
            created_mountpoint = true;
        } else {
            return Err(fail("Mountpoint doesn't exist", MountError::MountFailed));
        }
    } else if !actual_mountpoint.is_dir() {
        return Err(fail(
            "Mountpoint must be a directory",
            MountError::MountFailed,
        ));
    }

    let canonical_mountpoint = fs::get_absolute_normalized_path(&actual_mountpoint)?;

    // Check if we are mounting a standalone bundle file
    let is_bundle = args.bundle || {
        let repo_path = PathBuf::from(&global_args.repo);
        repo_path.is_file() && repo_path.extension().is_some_and(|ext| ext == "mapache")
    };

    if is_bundle {
        return mount_bundle(global_args, args, &canonical_mountpoint, created_mountpoint).await;
    }

    // Standard repository mounting
    with_repository_lock(
        global_args.auth_file.as_ref(),
        global_args.key.as_ref(),
        new_backend_with_prompt(global_args.backend_options(false))
            .await
            .map_err(|e| {
                fail(
                    format!("Failed to initialize backend: {}", e),
                    MountError::MountFailed,
                )
            })?,
        global_args.to_repo_config(),
        false,
        global_args.retry_lock_duration,
        |repo, _, lock_handle| async move {
            let cleanup_handler = CleanupHandler::new().map_err(|e| {
                fail(
                    format!("Failed to initialize cleanup handler: {}", e),
                    MountError::MountFailed,
                )
            })?;
            cleanup_handler.add_lock(lock_handle.clone());
            repo.reload_master_index().await.map_err(|e| {
                fail(
                    format!("Failed to reload master index: {}", e),
                    MountError::MountFailed,
                )
            })?;

            let allow_other = args.allow_other;
            let metadata_only = args.metadata_only;
            let data_cache_size = (args.data_cache_size_mib * size::MiB as f32) as u64;

            ui::cli::log!("Mounting repository in {}", canonical_mountpoint.display());
            tracing::info!(target: "mount", "Mounting repository at {:?}", canonical_mountpoint);
            let created_time = repo.manifest().created_time();
            super::cmd_bundle::run_mount_loop(&canonical_mountpoint, cleanup_handler, move |mp| {
                MapacheFS::mount(
                    repo.clone() as Arc<dyn BlobLoader>,
                    Some(repo),
                    None,
                    mp,
                    MountOptions {
                        allow_other,
                        metadata_only,
                        data_cache_size,
                        created_time,
                    },
                )
            })
            .await
            .map_err(|e| fail(format!("Mount interrupted: {}", e), MountError::Interrupted))?;

            if created_mountpoint {
                let _ = std::fs::remove_dir_all(&canonical_mountpoint);
            }
            Ok(())
        },
    )
    .await
    .map_err(|e| {
        if e.is::<crate::commands::error::MapacheError>() {
            e
        } else {
            fail(
                format!("Failed to open repository: {}", e),
                MountError::RepoOpenFail,
            )
        }
    })
}

async fn mount_bundle(
    global_args: &GlobalArgs,
    args: &CmdArgs,
    mountpoint: &std::path::Path,
    created_mountpoint: bool,
) -> Result<()> {
    tracing::info!(target: "mount", "Mounting bundle at {:?}", mountpoint);
    let password = match &args.internal_password {
        Some(p) => zeroize::Zeroizing::new(p.clone()),
        None => crate::ui::cli::request_password("Enter bundle password")?,
    };

    let reader = BundleReader::open(&global_args.repo, &password).map_err(|e| {
        fail(
            format!("Failed to open bundle: {}", e),
            MountError::MountFailed,
        )
    })?;
    let root_tree_id = reader.trailer.root_tree;
    let loader: Arc<dyn BlobLoader> = Arc::new(reader);

    let cleanup_handler = CleanupHandler::new().map_err(|e| {
        fail(
            format!("Failed to initialize cleanup handler: {}", e),
            MountError::MountFailed,
        )
    })?;
    ui::cli::log!(
        "Mounting bundle {} in {}",
        global_args.repo.bold(),
        mountpoint.display()
    );

    let data_cache_size = (args.data_cache_size_mib * size::MiB as f32) as u64;
    let allow_other = args.allow_other;
    let metadata_only = args.metadata_only;
    let mp_clone = mountpoint.to_path_buf();

    super::cmd_bundle::run_mount_loop(mountpoint, cleanup_handler, move |mp| {
        MapacheFS::mount(
            loader,
            None,
            Some(root_tree_id),
            mp,
            MountOptions {
                allow_other,
                metadata_only,
                data_cache_size,
                created_time: chrono::Local::now(),
            },
        )
    })
    .await
    .map_err(|e| fail(format!("Mount interrupted: {}", e), MountError::Interrupted))?;

    if created_mountpoint {
        let _ = std::fs::remove_dir_all(&mp_clone);
    }

    Ok(())
}
