use std::{collections::HashMap, sync::Arc, time::Instant};

use anyhow::Result;
use clap::Args;
use colored::Colorize;

use crate::{
    backend::{StorageHint, new_backend_with_prompt},
    commands::{GlobalArgs, cleanup::CleanupHandler},
    mapache::{ContentIdType, SaveID, defaults::SHORT_SNAPSHOT_ID_LEN, rewrite_snapshot_tree},
    repository::{
        repo::{RepoConfig, Repository},
        snapshot::SnapshotStream,
    },
    ui::{self, snapshot_progress::SnapshotProgressReporter},
    utils::{self, size},
};

#[derive(Args, Debug)]
#[clap(about = "Rechunk all snapshots")]
#[clap(long_about = "Rechunk all snapshots using the current chunker and parameters.")]
pub struct CmdArgs {}

pub fn run(global_args: &GlobalArgs, _args: &CmdArgs) -> Result<()> {
    let auth = utils::get_auth_from_file(&global_args.auth_file)?;
    let backend = new_backend_with_prompt(global_args.backend_options(false))?;

    let config = RepoConfig {
        pack_size: (global_args.pack_size_mib * size::MiB as f32) as u64,
        use_cache: !global_args.no_cache,
        compression: global_args.compression_level,
    };
    let (repo, _, lock_handle) = Repository::try_open_with_lock(
        auth.as_ref(),
        global_args.key.as_ref(),
        backend,
        config,
        true,
        global_args.retry_lock_duration,
    )?;

    let lock_handle_clone = lock_handle.clone();
    let _cleanup_handler = CleanupHandler::new(move || {
        lock_handle_clone.write().unlock();
    })?;

    repo.init_pack_saver(1);

    let start = Instant::now();

    let snapshot_stream = SnapshotStream::new(repo.clone())?;
    let num_snapshots = snapshot_stream.len();
    let mut rechunked_blob_list_map = HashMap::new();

    for (i, (snapshot_id, mut snapshot)) in snapshot_stream.enumerate() {
        ui::cli::log!(
            "Rechunking snapshot {} ({}/{})",
            snapshot_id
                .to_short_hex(SHORT_SNAPSHOT_ID_LEN)
                .bold()
                .yellow(),
            i + 1,
            num_snapshots
        );

        let progress_reporter = Arc::new(SnapshotProgressReporter::new(
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
            progress_reporter.clone(),
        )?;

        // Save the amended snapshot and delete the old snapshot file
        repo.save_file(
            &SaveID::CalculateID,
            serde_json::to_string(&snapshot)?.as_bytes(),
            StorageHint {
                file_type: ContentIdType::Snapshot,
                is_metadata: true,
            },
            None,
        )?;

        repo.delete_file(ContentIdType::Snapshot, &snapshot_id, None)?;

        progress_reporter.finalize();
    }

    repo.finalize_pack_saver()?;

    ui::cli::log!(
        "Finished in {}",
        utils::pretty_print_duration(start.elapsed())
    );

    Ok(())
}
