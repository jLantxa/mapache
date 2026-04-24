use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::Args;
use colored::Colorize;

use crate::{
    backend::{BackendUrl, new_backend_with_prompt},
    commands::{GlobalArgs, cleanup::CleanupHandler, with_repository_lock},
    fs,
    fuse::fs::MapacheFS,
    mapache::defaults::DEFAULT_FUSE_STASH_CACHE_SIZE_MIB,
    ui,
    utils::size,
};

#[derive(Args, Debug)]
#[clap(about = "Mount the repository as a file system")]
pub struct CmdArgs {
    /// Mount point
    #[arg(value_parser)]
    pub mountpoint: PathBuf,

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
}

pub async fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    // Check that mountpoint exists and is a directory, or create it if requested
    let actual_mountpoint = args.mountpoint.clone();

    // The mountpoint was created by us and should be deleted when we finish.
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

    let cannonical_mountpoint = fs::get_absolute_normalized_path(&actual_mountpoint)?;

    // Don't allow mounting on the repo path
    if let BackendUrl::Local(repo_path) = BackendUrl::from(&global_args.repo)?
        && cannonical_mountpoint == fs::get_absolute_normalized_path(&repo_path)?
    {
        bail!("Cannot mount the repository on itself");
    }

    with_repository_lock(
        global_args.auth_file.as_ref(),
        global_args.key.as_ref(),
        new_backend_with_prompt(global_args.backend_options(false)).await?,
        global_args.to_repo_config(),
        false,
        global_args.retry_lock_duration,
        |repo, _, lock_handle| async move {
            // Listen for CTRL + C to unmount.
            let cleanup_handler = CleanupHandler::new()?;
            cleanup_handler.add_lock(lock_handle.clone());

            repo.reload_master_index().await?;

            ui::cli::log!("Mounting repository in {}", cannonical_mountpoint.display());
            ui::cli::log!(
                "Press {} to finish or unmount the filesystem manually.",
                "Ctrl+C".bold()
            );

            let mount_path = cannonical_mountpoint.clone();
            let allow_other = args.allow_other;
            let metadata_only = args.metadata_only;
            let data_cache_size = (args.data_cache_size_mib * size::MiB as f32) as u64;

            let mount_res = tokio::task::spawn_blocking(move || {
                MapacheFS::mount(
                    repo,
                    &mount_path,
                    allow_other,
                    metadata_only,
                    data_cache_size,
                )
            });

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
                    let _ = MapacheFS::unmount(&cannonical_mountpoint);
                }
            }

            if created_mountpoint {
                ui::cli::verbose_1!(
                    "Removing created mountpoint {}",
                    cannonical_mountpoint.display()
                );

                let _ = std::fs::remove_dir_all(&cannonical_mountpoint);
            }

            Ok(())
        },
    )
    .await
}
