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
    common::{
        ContentIdType, SaveID,
        defaults::{self, SHORT_SNAPSHOT_ID_LEN},
        error::MapacheError,
    },
    repository::{repo::Repository, snapshot::SnapshotStream},
    ui::{
        self,
        cli::{color::Colorize, snapshot},
    },
    utils,
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
            RechunkError::Io(_) => 4,
        }
    }
}

#[derive(Args, Debug, Clone)]
#[clap(about = "Rechunk all snapshot files")]
#[clap(
    long_about = "Recalculate chunk boundaries for every snapshot file using the current chunker and settings."
)]
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

            let start = Instant::now();
            rechunk_with_repo(repo, cleanup_handler.interrupted.clone()).await?;

            ui::cli::log!(
                "\nFinished in {}",
                utils::pretty_print_duration(start.elapsed())
            );
            Ok(())
        },
    )
    .await
}

async fn rechunk_with_repo(
    repo: Arc<Repository>,
    shutdown_signal: Arc<std::sync::atomic::AtomicBool>,
) -> Result<(), RechunkError> {
    let mut snapshot_stream = SnapshotStream::new(repo.clone()).await?;
    let num_snapshots = snapshot_stream.len();
    let mut rechunked_blob_list_map = HashMap::new();

    repo.init_pack_saver(defaults::DEFAULT_SNAPSHOT_PACKERS)?;

    let mut index = 0;
    while let Some(res) = snapshot_stream.next().await {
        if shutdown_signal.load(std::sync::atomic::Ordering::Acquire) {
            return Err(RechunkError::Interrupted);
        }
        let (snapshot_id, mut snapshot_data) = res?;
        ui::cli::log!(
            "Rechunking snapshot {} ({}/{})",
            snapshot_id
                .to_short_hex(SHORT_SNAPSHOT_ID_LEN)
                .bold()
                .yellow(),
            index + 1,
            num_snapshots
        );
        index += 1;

        let progress = Arc::new(SnapshotProgress::new());
        let event_sender = snapshot::make_event_sender(
            Some(snapshot_data.summary.processed_items_count),
            Some(snapshot_data.summary.processed_bytes),
            1,
        );
        let rewrite_ctx = RewriteCtx {
            progress: progress.clone(),
            event_sender,
            shutdown_signal: shutdown_signal.clone(),
        };

        rewrite_snapshot_tree(
            repo.clone(),
            &mut snapshot_data,
            None,
            true,
            Some(&mut rechunked_blob_list_map),
            rewrite_ctx,
        )
        .await?;

        let (new_snapshot_id, _) = repo
            .save_file(
                &SaveID::CalculateID,
                serde_json::to_string(&snapshot_data)
                    .map_err(MapacheError::Serialization)?
                    .as_bytes(),
                StorageHint {
                    file_type: ContentIdType::Snapshot,
                    is_metadata: true,
                },
                None,
            )
            .await?;
        // A rechunk that produces no changes rewrites the snapshot to identical
        // bytes, yielding the same content-addressed ID. Deleting the old file
        // in that case would remove the snapshot we just saved.
        if new_snapshot_id != snapshot_id {
            repo.delete_file(ContentIdType::Snapshot, &snapshot_id, None)
                .await?;
        }
    }

    repo.flush_and_finalize_pack_saver().await?;
    Ok(())
}
