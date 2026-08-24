pub use crate::mount::fuse::fs::MapacheFS;

use std::{io, path::PathBuf, sync::Arc};

use clap::Args;

use crate::{
    backend::new_backend_with_prompt,
    bundle::reader::BundleReader,
    commands::{GlobalArgs, ToExitCode, cleanup::CleanupHandler, with_repository_lock},
    common::{
        defaults::DEFAULT_FUSE_STASH_CACHE_SIZE_MIB, error::MapacheError, traits::BlobLoader,
    },
    fs,
    mount::fuse::fs::MountOptions,
    ui::{self, cli::color::Colorize},
    utils::size,
};

#[derive(Debug, thiserror::Error)]
pub enum MountError {
    #[error("failed to open repository: {0}")]
    RepoOpenFail(String),
    #[error("mount failed: {0}")]
    MountFailed(String),
    #[error("mount interrupted by user")]
    Interrupted,
    #[error(transparent)]
    Repo(#[from] MapacheError),
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl ToExitCode for MountError {
    fn to_exit_code(&self) -> i32 {
        match self {
            MountError::RepoOpenFail(_) => 10,
            MountError::MountFailed(_) => 20,
            MountError::Interrupted => 130,
            MountError::Repo(_) => 1,
            MountError::Io(_) => 1,
        }
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

    /// Allow other users to access the mount
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

pub async fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<(), MountError> {
    tracing::info!(target: "mount", "Starting mount command (mountpoint={:?})", args.mountpoint);
    let actual_mountpoint = args.mountpoint.clone();
    let mut created_mountpoint = false;

    if !fs::path_exists(&actual_mountpoint).await {
        if args.create_mountpoint {
            std::fs::create_dir_all(&actual_mountpoint).map_err(|e| {
                MountError::MountFailed(format!("could not create mount point: {e}"))
            })?;
            created_mountpoint = true;
        } else {
            return Err(MountError::MountFailed(
                "mountpoint doesn't exist".to_string(),
            ));
        }
    } else if !actual_mountpoint.is_dir() {
        return Err(MountError::MountFailed(
            "mountpoint must be a directory".to_string(),
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
        new_backend_with_prompt(global_args.backend_options(false)).await?,
        global_args.to_repo_config(),
        false,
        global_args.retry_lock_duration,
        global_args.no_lock,
        |repo, _, lock_handle| async move {
            let cleanup_handler = CleanupHandler::new();
            cleanup_handler.add_lock(lock_handle);
            repo.reload_master_index().await?;

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
            .map_err(|e| MountError::MountFailed(e.inner()))?;

            if created_mountpoint {
                let _ = std::fs::remove_dir_all(&canonical_mountpoint);
            }
            Ok(())
        },
    )
    .await
}

async fn mount_bundle(
    global_args: &GlobalArgs,
    args: &CmdArgs,
    mountpoint: &std::path::Path,
    created_mountpoint: bool,
) -> Result<(), MountError> {
    tracing::info!(target: "mount", "Mounting bundle at {:?}", mountpoint);
    let password = match &args.internal_password {
        Some(p) => zeroize::Zeroizing::new(p.clone()),
        None => ui::cli::request_password("Enter bundle password").map_err(|e| {
            MountError::MountFailed(format!("password prompt failed: {}", e.inner()))
        })?,
    };

    let reader = BundleReader::open(&global_args.repo, &password)
        .map_err(|e| MountError::MountFailed(format!("failed to open bundle: {}", e.inner())))?;
    let root_tree_id = reader.trailer.root_tree;
    let loader: Arc<dyn BlobLoader> = Arc::new(reader);

    let cleanup_handler = CleanupHandler::new();
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
    .map_err(|e| MountError::MountFailed(e.to_string()))?;

    if created_mountpoint {
        let _ = std::fs::remove_dir_all(&mp_clone);
    }

    Ok(())
}
