use std::{collections::HashMap, io, sync::Arc, time::Instant};

use clap::Args;
use futures::StreamExt;

use crate::{
    archiver::{
        progress::SnapshotProgress,
        rewrite::{RewriteCtx, rewrite_snapshot_tree},
    },
    backend::{StorageHint, new_backend_with_prompt},
    commands::{GlobalArgs, ToExitCode, cleanup::CleanupHandler, with_repository_lock},
    common::{ContentIdType, SaveID, defaults::SHORT_SNAPSHOT_ID_LEN, error::MapacheError},
    repository::snapshot::SnapshotStream,
    ui::{
        self,
        cli::{color::Colorize, snapshot},
    },
    utils::{self},
};

#[derive(Debug, thiserror::Error)]
pub enum RechunkError {
    #[error("rechunk interrupted by user")]
    Interrupted,
    #[error(transparent)]
    Repo(#[from] MapacheError),
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl ToExitCode for RechunkError {
    fn to_exit_code(&self) -> i32 {
        match self {
            RechunkError::Interrupted => 130,
            RechunkError::Repo(_) => 1,
            RechunkError::Io(_) => 1,
        }
    }
}

#[derive(Args, Debug, Clone)]
#[clap(about = "Rechunk all snapshots")]
#[clap(long_about = "Rechunk all snapshots using the current chunker and parameters.")]
pub struct CmdArgs {}

pub async fn run(global_args: &GlobalArgs, _args: &CmdArgs) -> Result<(), RechunkError> {
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

            repo.reload_master_index().await?;
            repo.init_pack_saver(1)?;

            let start = Instant::now();

            let mut snapshot_stream = SnapshotStream::new(repo.clone()).await?;
            let num_snapshots = snapshot_stream.len();
            let mut rechunked_blob_list_map = HashMap::new();

            let mut i = 0;
            while let Some(res) = snapshot_stream.next().await {
                if cleanup_handler.is_interrupted() {
                    tracing::info!(target: "rechunk", "Rechunk interrupted by user");
                    return Err(RechunkError::Interrupted);
                }
                let (snapshot_id, mut snapshot) = res?;
                ui::cli::log!(
                    "Rechunking snapshot {} ({}/{})",
                    snapshot_id
                        .to_short_hex(SHORT_SNAPSHOT_ID_LEN)
                        .bold()
                        .yellow(),
                    i + 1,
                    num_snapshots
                );
                i += 1;

                let progress = Arc::new(SnapshotProgress::new());

                let event_sender = snapshot::make_event_sender(
                    Some(snapshot.summary.processed_items_count),
                    Some(snapshot.summary.processed_bytes),
                    1,
                );

                let rewrite_ctx = RewriteCtx {
                    progress: progress.clone(),
                    event_sender,
                    shutdown_signal: cleanup_handler.interrupted.clone(),
                };
                rewrite_snapshot_tree(
                    repo.clone(),
                    &mut snapshot,
                    None,
                    true,
                    Some(&mut rechunked_blob_list_map),
                    rewrite_ctx,
                )
                .await?;

                // Save the amended snapshot and delete the old snapshot file
                repo.save_file(
                    &SaveID::CalculateID,
                    serde_json::to_string(&snapshot)
                        .map_err(|e| RechunkError::Repo(MapacheError::Serialization(e)))?
                        .as_bytes(),
                    StorageHint {
                        file_type: ContentIdType::Snapshot,
                        is_metadata: true,
                    },
                    None,
                )
                .await?;

                repo.delete_file(ContentIdType::Snapshot, &snapshot_id, None)
                    .await?;
            }

            repo.flush_and_finalize_pack_saver().await?;

            ui::cli::log!(
                "Finished in {}",
                utils::pretty_print_duration(start.elapsed())
            );
            tracing::info!(target: "rechunk", "Rechunk command completed in {:?}", start.elapsed());

            Ok(())
        },
    )
    .await
}
