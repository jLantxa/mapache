use std::{io, time::Instant};

use clap::Args;

use crate::{
    backend::new_backend_with_prompt,
    commands::{GlobalArgs, ToExitCode, cleanup::CleanupHandler, with_repository_lock},
    common::error::MapacheError,
    repository::gc,
    ui::{
        self,
        cli::gc as cli_gc,
        events::{Event, GcEvent},
    },
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
        global_args.password_command.as_deref(),
        global_args.key.as_ref(),
        new_backend_with_prompt(global_args.backend_options(false)).await?,
        global_args.to_repo_config(),
        true,
        global_args.retry_lock_duration,
        global_args.no_lock,
        |repo, _secure_storage, lock_handle| async move {
            let start = Instant::now();
            let event_sender = cli_gc::make_event_sender();
            let sender_for_cleanup = event_sender.clone();
            let sender_for_finish = event_sender.clone();
            let cleanup_handler = CleanupHandler::new_with_callback(move || {
                // Clear the progress bar before the command unwinds so the
                // interrupt message is not printed over a live bar.
                sender_for_cleanup(Event::Gc(GcEvent::Finished {
                    added_bytes: 0,
                    deleted_bytes: 0,
                }));
            });
            cleanup_handler.add_lock(lock_handle);
            let shutdown_signal = cleanup_handler.interrupted.clone();

            // Uses the configured mode; `cleanup` streams cold indices into the
            // rewritten index, so a lazy pool loses nothing.
            repo.reload_master_index().await?;

            let result = gc::repack_all(repo, event_sender, shutdown_signal.clone()).await;
            if result.is_err() && shutdown_signal.load(std::sync::atomic::Ordering::Acquire) {
                tracing::info!(target: "repack", "Repack interrupted by user");
                return Err(RepackError::Interrupted);
            }
            result?;

            sender_for_finish(Event::Gc(GcEvent::Finished {
                added_bytes: 0,
                deleted_bytes: 0,
            }));
            ui::cli::log!(
                "\nFinished in {}",
                utils::pretty_print_duration(start.elapsed())
            );
            Ok(())
        },
    )
    .await
}
