use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use clap::Args;
use colored::Colorize;

use crate::{
    backend::new_backend_with_prompt,
    bundle::reader::BundleReader,
    commands::{GlobalArgs, cleanup::CleanupHandler, with_repository_lock},
    fs,
    mapache::defaults::DEFAULT_FUSE_STASH_CACHE_SIZE_MIB,
    mapache::traits::BlobLoader,
    ui,
    utils::size,
};

pub use crate::fuse::fs::MapacheFS;

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
            bail!("Mountpoint doesn't exist");
        }
    } else if !actual_mountpoint.is_dir() {
        bail!("Mountpoint must be a directory");
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
        |repo, _, lock_handle| async move {
            let cleanup_handler = CleanupHandler::new()?;
            cleanup_handler.add_lock(lock_handle.clone());
            repo.reload_master_index().await?;

            let allow_other = args.allow_other;
            let metadata_only = args.metadata_only;
            let data_cache_size = (args.data_cache_size_mib * size::MiB as f32) as u64;

            ui::cli::log!("Mounting repository in {}", canonical_mountpoint.display());
            tracing::info!(target: "mount", "Mounting repository at {:?}", canonical_mountpoint);
            run_mount_loop(&canonical_mountpoint, cleanup_handler, move |mp| {
                MapacheFS::<dyn BlobLoader>::mount(
                    repo,
                    mp,
                    allow_other,
                    metadata_only,
                    data_cache_size,
                )
            })
            .await?;

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
) -> Result<()> {
    tracing::info!(target: "mount", "Mounting bundle at {:?}", mountpoint);
    let password = match &args.internal_password {
        Some(p) => zeroize::Zeroizing::new(p.clone()),
        None => crate::ui::cli::request_password("Enter bundle password")?,
    };

    let reader = BundleReader::open(&global_args.repo, &password)?;
    let root_tree_id = reader.trailer.root_tree;
    let loader: Arc<dyn BlobLoader> = Arc::new(reader);

    let cleanup_handler = CleanupHandler::new()?;
    ui::cli::log!(
        "Mounting bundle {} in {}",
        global_args.repo.bold(),
        mountpoint.display()
    );

    let data_cache_size = (args.data_cache_size_mib * size::MiB as f32) as u64;
    let allow_other = args.allow_other;
    let metadata_only = args.metadata_only;
    let mp_clone = mountpoint.to_path_buf();

    run_mount_loop(mountpoint, cleanup_handler, move |mp| {
        MapacheFS::mount_loader(
            loader,
            None,
            Some(root_tree_id),
            mp,
            crate::fuse::fs::MountOptions {
                allow_other,
                metadata_only,
                data_cache_size,
                created_time: chrono::Local::now(),
            },
        )
    })
    .await?;

    if created_mountpoint {
        let _ = std::fs::remove_dir_all(&mp_clone);
    }

    Ok(())
}

async fn run_mount_loop<F>(
    mountpoint: &std::path::Path,
    cleanup_handler: CleanupHandler,
    mount_fn: F,
) -> Result<()>
where
    F: FnOnce(&std::path::Path) -> Result<()> + Send + 'static,
{
    ui::cli::log!(
        "Press {} to finish or unmount the filesystem manually.",
        "Ctrl+C".bold()
    );

    let mp_clone = mountpoint.to_path_buf();
    let mount_res = tokio::task::spawn_blocking(move || mount_fn(&mp_clone));

    tokio::select! {
        res = mount_res => {
            res.context("Mount task panicked")??;
        }
        _ = async {
            loop {
                if cleanup_handler.is_interrupted() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        } => {
            ui::cli::log!("Interrupt received. Unmounting...");
            tracing::info!(target: "mount", "Interrupt received. Unmounting {:?}", mountpoint);
            let _ = MapacheFS::<dyn BlobLoader>::unmount(mountpoint);
        }
    }
    tracing::info!(target: "mount", "Mount loop finished");
    Ok(())
}
