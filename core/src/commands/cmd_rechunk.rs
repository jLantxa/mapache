use std::{collections::HashMap, sync::Arc, time::Instant};

use anyhow::Result;
use clap::Args;
use colored::Colorize;
use futures::StreamExt;

use crate::{
    archiver::progress::SnapshotProgress,
    backend::{StorageHint, new_backend_with_prompt},
    commands::{GlobalArgs, cleanup::CleanupHandler, with_repository_lock},
    mapache::{ContentIdType, SaveID, defaults::SHORT_SNAPSHOT_ID_LEN, rewrite_snapshot_tree},
    repository::snapshot::SnapshotStream,
    ui::{
        self,
        snapshot::{SnapshotProgressReporter, cli::CliSnapshotProgressReporter},
    },
    utils::{self},
};

#[derive(Args, Debug, Clone)]
#[clap(about = "Rechunk all snapshots")]
#[clap(long_about = "Rechunk all snapshots using the current chunker and parameters.")]
pub struct CmdArgs {}

pub async fn run(global_args: &GlobalArgs, _args: &CmdArgs) -> Result<()> {
    with_repository_lock(
        global_args.auth_file.as_ref(),
        global_args.key.as_ref(),
        new_backend_with_prompt(global_args.backend_options(false)).await?,
        global_args.to_repo_config(),
        true,
        global_args.retry_lock_duration,
        |repo, _secure_storage, lock_handle| async move {
            let cleanup_handler = CleanupHandler::new()?;
            cleanup_handler.add_lock(lock_handle.clone());

            repo.reload_master_index().await?;
            repo.init_pack_saver(1)?;

            let start = Instant::now();

            let mut snapshot_stream = SnapshotStream::new(repo.clone()).await?;
            let num_snapshots = snapshot_stream.len();
            let mut rechunked_blob_list_map = HashMap::new();

            let mut i = 0;
            while let Some(res) = snapshot_stream.next().await {
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

                let progress_reporter: Arc<dyn SnapshotProgressReporter> =
                    Arc::new(CliSnapshotProgressReporter::new(
                        Some(snapshot.summary.processed_items_count),
                        Some(snapshot.summary.processed_bytes),
                        1,
                    ));

                rewrite_snapshot_tree(
                    repo.clone(),
                    &mut snapshot,
                    None,
                    true,
                    Some(&mut rechunked_blob_list_map),
                    progress.clone(),
                    progress_reporter.clone(),
                    cleanup_handler.interrupted.clone(),
                )
                .await?;

                // Save the amended snapshot and delete the old snapshot file
                repo.save_file(
                    &SaveID::CalculateID,
                    serde_json::to_string(&snapshot)?.as_bytes(),
                    StorageHint {
                        file_type: ContentIdType::Snapshot,
                        is_metadata: true,
                    },
                    None,
                )
                .await?;

                repo.delete_file(ContentIdType::Snapshot, &snapshot_id, None)
                    .await?;

                progress_reporter.finalize();
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
