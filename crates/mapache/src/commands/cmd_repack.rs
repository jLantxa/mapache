use std::{io, time::Instant};

use clap::Args;

use crate::{
    backend::new_backend_with_prompt,
    commands::{GlobalArgs, ToExitCode, cleanup::CleanupHandler, with_repository_lock},
    common::error::MapacheError,
    repository::gc,
    ui::{self, cli::gc as cli_gc},
    utils,
};

#[derive(Debug, thiserror::Error)]
pub enum RepackError {
    #[error("repack interrupted by user")]
    Interrupted,
    #[error(transparent)]
    Repo(#[from] MapacheError),
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl ToExitCode for RepackError {
    fn to_exit_code(&self) -> i32 {
        match self {
            RepackError::Interrupted => 130,
            RepackError::Repo(_) => 1,
            RepackError::Io(_) => 4,
        }
    }
}

#[derive(Args, Debug, Clone)]
#[clap(about = "Repack all blobs with the current settings")]
#[clap(long_about = "Re-encode all reachable blobs with the current settings.")]
pub struct CmdArgs {}

pub async fn run(global_args: &GlobalArgs, _args: &CmdArgs) -> Result<(), RepackError> {
    with_repository_lock(
        global_args.auth_file.as_ref(),
        global_args.key.as_ref(),
        new_backend_with_prompt(global_args.backend_options(false)).await?,
        global_args.to_repo_config(),
        true,
        global_args.retry_lock_duration,
        global_args.no_lock,
        |repo, _secure_storage, lock_handle| async move {
            let cleanup_handler = CleanupHandler::new();
            cleanup_handler.add_lock(lock_handle);
            let shutdown_signal = cleanup_handler.interrupted.clone();

            repo.reload_master_index().await?;

            let start = Instant::now();
            let event_sender = cli_gc::make_event_sender();

            gc::repack_all(repo, event_sender, shutdown_signal.clone()).await?;

            if shutdown_signal.load(std::sync::atomic::Ordering::Acquire) {
                return Err(RepackError::Interrupted);
            }

            ui::cli::log!(
                "\nFinished in {}",
                utils::pretty_print_duration(start.elapsed())
            );
            Ok(())
        },
    )
    .await
}
