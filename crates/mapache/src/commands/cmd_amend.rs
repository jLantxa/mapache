use std::{
    collections::BTreeSet,
    io,
    path::PathBuf,
    sync::{Arc, atomic::AtomicBool},
    time::Instant,
};

use clap::{ArgGroup, Args};
use futures::StreamExt;

use crate::{
    archiver::{
        progress::SnapshotProgress,
        rewrite::{RewriteCtx, rewrite_snapshot_tree},
    },
    backend::{StorageHint, new_backend_with_prompt},
    commands::{
        EMPTY_TAG_MARK, GlobalArgs, ToExitCode, UseSnapshot, cleanup::CleanupHandler,
        find_use_snapshot, parse_tags, with_repository_lock,
    },
    common::{ContentIdType, ID, SaveID, defaults::SHORT_SNAPSHOT_ID_LEN, error::MapacheError},
    fs::filter::{
        merge_filtered_paths, parse_relative_filter_paths, read_filtered_paths_from_file,
    },
    repository::{
        repo::Repository,
        snapshot::{Snapshot, SnapshotStream},
    },
    ui::{self, cli::color::Colorize, cli::snapshot},
    utils,
};

#[derive(Debug, thiserror::Error)]
pub enum AmendError {
    #[error("amend interrupted by user")]
    Interrupted,
    #[error("snapshot not found: {0}")]
    NotFound(String),
    #[error(transparent)]
    Repo(#[from] MapacheError),
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl ToExitCode for AmendError {
    fn to_exit_code(&self) -> i32 {
        match self {
            AmendError::Interrupted => 130,
            AmendError::NotFound(_) => 1,
            AmendError::Repo(_) => 1,
            AmendError::Io(_) => 1,
        }
    }
}

#[derive(Args, Debug, Clone)]
#[clap(group = ArgGroup::new("snapshot_group").multiple(false))]
#[clap(group = ArgGroup::new("tags_group").multiple(false))]
#[clap(group = ArgGroup::new("description_group").multiple(false))]
#[clap(about = "Amend an existing snapshot")]
pub struct CmdArgs {
    /// The ID of the snapshot to amend, or 'latest' to amend the most recent snapshot.
    #[arg(value_parser = clap::value_parser!(UseSnapshot), default_value_t=UseSnapshot::Latest, group = "snapshot_group")]
    pub snapshot: UseSnapshot,

    /// Apply changes to all snapshots
    #[arg(short, long, group = "snapshot_group")]
    pub all: bool,

    /// Keep the old snapshot
    #[clap(long = "keep-old", value_parser, default_value_t = false)]
    pub keep_old: bool,

    /// Tags (comma-separated)
    #[clap(long = "tags", value_parser, group = "tags_group")]
    pub tags_str: Option<String>,

    /// Clear tags
    #[clap(long, value_parser, group = "tags_group")]
    pub clear_tags: bool,

    /// Snapshot description
    #[clap(long, value_parser, group = "description_group")]
    pub description: Option<String>,

    /// Clear description
    #[clap(long, value_parser, group = "description_group")]
    pub clear_description: bool,

    /// List of paths to exclude from the backup
    #[clap(long, value_parser, required = false, value_delimiter = ',', num_args = 1..)]
    pub exclude: Option<Vec<String>>,

    /// A file containing a list of paths to exclude, one per line.
    #[clap(long, value_parser)]
    pub exclude_file: Option<PathBuf>,
}

pub async fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<(), AmendError> {
    tracing::info!(target: "amend", "Starting amend command");
    with_repository_lock(
        global_args.auth_file.as_ref(),
        global_args.key.as_ref(),
        new_backend_with_prompt(global_args.backend_options(false)).await?,
        global_args.to_repo_config(),
        true,
        global_args.retry_lock_duration,
        global_args.no_lock,
        |repo, _, lock_handle| async move {
            let cleanup_handler = CleanupHandler::new();
            cleanup_handler.add_lock(lock_handle);

            let start = Instant::now();

            repo.reload_master_index().await?;

            let mut snapshots: Vec<(ID, Snapshot)> = Vec::new();

            if args.all {
                let mut snapshot_stream = SnapshotStream::new(repo.clone()).await?;
                while let Some(res) = snapshot_stream.next().await {
                    snapshots.push(res?);
                }
            } else {
                match find_use_snapshot(repo.clone(), &args.snapshot).await {
                    Ok(Some((id, snap))) => snapshots.push((id, snap)),
                    Ok(None) => {
                        return Err(AmendError::NotFound(format!(
                            "no snapshot found for {} (only snapshots created by this host are considered)",
                            args.snapshot
                        )));
                    }
                    Err(e) => return Err(AmendError::Repo(e)),
                }
            }

            let num_snapshots = snapshots.len();
            for (i, (id, snapshot)) in snapshots.iter_mut().rev().enumerate() {
                if cleanup_handler.is_interrupted() {
                    tracing::info!(target: "amend", "Amend interrupted by user");
                    return Err(AmendError::Interrupted);
                }

                let amend_str = format!(
                    "Amending snapshot {}",
                    id.to_short_hex(SHORT_SNAPSHOT_ID_LEN).bold().red()
                );
                if args.all {
                    ui::cli::log!("{} ({}/{})", amend_str, i + 1, num_snapshots);
                } else {
                    ui::cli::log!("{} ", amend_str);
                }

                amend(
                    repo.clone(),
                    id,
                    snapshot,
                    args,
                    cleanup_handler.interrupted.clone(),
                )
                .await?;
                ui::cli::log!();
            }

            ui::cli::log!(
                "Finished in {}",
                utils::pretty_print_duration(start.elapsed())
            );
            tracing::info!(target: "amend", "Amend command completed in {:?}", start.elapsed());

            Ok(())
        },
    )
    .await
}

async fn amend(
    repo: Arc<Repository>,
    origin_snapshot_id: &ID,
    snapshot: &mut Snapshot,
    args: &CmdArgs,
    shutdown_signal: Arc<AtomicBool>,
) -> Result<(), AmendError> {
    tracing::info!(target: "amend", "Amending snapshot {}", origin_snapshot_id.to_short_hex(8));
    snapshot.summary.amends = Some(*origin_snapshot_id);

    if args.description.is_some() {
        snapshot.description = args.description.clone();
    } else if args.clear_description {
        snapshot.description = None;
    }

    if let Some(a_tag_str) = &args.tags_str {
        let mut tags: BTreeSet<String> = parse_tags(Some(a_tag_str));
        tags.retain(|tag| tag != EMPTY_TAG_MARK);
        snapshot.tags = tags.clone();
    } else if args.clear_tags {
        snapshot.tags = BTreeSet::new();
    }

    let origin_processed_bytes = snapshot.summary.processed_bytes;

    // Read exclude paths from file if provided.
    let excludes_from_file = match &args.exclude_file {
        Some(path) => Some(read_filtered_paths_from_file(path)?),
        None => None,
    };
    let all_excludes = merge_filtered_paths(args.exclude.as_ref(), excludes_from_file.as_ref());

    let parsed_excludes = parse_relative_filter_paths(all_excludes.as_ref());

    if parsed_excludes.is_some() {
        repo.init_pack_saver(1)?;
        let progress = Arc::new(SnapshotProgress::new());
        let event_sender = snapshot::make_event_sender(None, None, 1);
        let rewrite_ctx = RewriteCtx {
            progress: progress.clone(),
            event_sender,
            shutdown_signal,
        };
        rewrite_snapshot_tree(
            repo.clone(),
            snapshot,
            parsed_excludes.as_ref(),
            false,
            None,
            rewrite_ctx,
        )
        .await?;

        repo.flush_and_finalize_pack_saver().await?;
    }

    // Save the amended snapshot and delete the old snapshot file
    let (new_id, _meta_size) = repo
        .save_file(
            &SaveID::CalculateID,
            serde_json::to_string(&snapshot)
                .map_err(|e| AmendError::Repo(MapacheError::Serialization(e)))?
                .as_bytes(),
            StorageHint {
                file_type: ContentIdType::Snapshot,
                is_metadata: true,
            },
            None,
        )
        .await?;

    // Delete the old snapshot ID if it changed
    // Note: To protect the repo from interruptions, we delete the snapshot only
    // after the new one is saved.
    if new_id != *origin_snapshot_id {
        tracing::info!(target: "amend", "Amended snapshot saved as {}", new_id.to_short_hex(8));
        if !args.keep_old {
            tracing::info!(target: "amend", "Deleting old snapshot {}", origin_snapshot_id.to_short_hex(8));
            repo.delete_file(ContentIdType::Snapshot, origin_snapshot_id, None)
                .await?;
        }

        ui::cli::log!(
            "New snapshot ID   {}",
            new_id.to_short_hex(SHORT_SNAPSHOT_ID_LEN).bold().green()
        );
        ui::cli::log!(
            "Snapshot size: {} -> {}",
            utils::format_size_binary(origin_processed_bytes, 3)
                .yellow()
                .bold(),
            utils::format_size_binary(snapshot.summary.processed_bytes, 3)
                .green()
                .bold()
        );
    } else {
        tracing::info!(target: "amend", "No changes detected for snapshot {}", origin_snapshot_id.to_short_hex(8));
        ui::cli::log!("No changes");
    }

    Ok(())
}
