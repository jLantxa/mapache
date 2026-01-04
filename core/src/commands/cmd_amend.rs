use std::{collections::BTreeSet, path::PathBuf, sync::Arc, time::Instant};

use anyhow::{Result, bail};
use clap::{ArgGroup, Args};
use colored::Colorize;

use crate::{
    backend::{StorageHint, new_backend_with_prompt},
    commands::{
        EMPTY_TAG_MARK, GlobalArgs, UseSnapshot, cleanup::CleanupHandler, find_use_snapshot,
        parse_tags,
    },
    mapache::{ContentIdType, ID, SaveID, defaults::SHORT_SNAPSHOT_ID_LEN, rewrite_snapshot_tree},
    repository::{
        repo::{RepoConfig, Repository},
        snapshot::{Snapshot, SnapshotStream},
    },
    ui::{self, snapshot_progress::SnapshotProgressReporter},
    utils::{self, size},
};

#[derive(Args, Debug)]
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
    #[clap(long, value_parser, required = false)]
    pub exclude: Option<Vec<PathBuf>>,
}

pub fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let auth = utils::get_auth_from_file(&global_args.auth_file)?;
    let backend = new_backend_with_prompt(global_args.backend_options(false))?;

    let config = RepoConfig {
        pack_size: (global_args.pack_size_mib * size::MiB as f32) as u64,
        use_cache: !global_args.no_cache,
    };
    let (repo, _, lock_handle) = Repository::try_open_with_lock(
        auth.as_ref(),
        global_args.key.as_ref(),
        backend,
        config,
        true,
    )?;

    let lock_handle_clone = lock_handle.clone();
    let _cleanup_handler = CleanupHandler::new(move || {
        lock_handle_clone.write().unlock();
    })?;

    let start = Instant::now();

    let mut snapshots: Vec<(ID, Snapshot)> = Vec::new();

    if args.all {
        let snapshot_stream = SnapshotStream::new(repo.clone())?;
        let mut all_snapshots: Vec<(ID, Snapshot)> = snapshot_stream.collect();
        snapshots.append(&mut all_snapshots);
    } else {
        match find_use_snapshot(repo.clone(), &args.snapshot) {
            Ok(Some((id, snap))) => snapshots.push((id, snap)),
            Ok(None) | Err(_) => bail!("Snapshot not found"),
        }
    }

    let num_snapshots = snapshots.len();
    for (i, (id, snapshot)) in snapshots.iter_mut().rev().enumerate() {
        let amend_str = format!(
            "Amending snapshot {}",
            id.to_short_hex(SHORT_SNAPSHOT_ID_LEN).bold().red()
        );
        if args.all {
            ui::cli::log!("{} ({}/{})", amend_str, i + 1, num_snapshots);
        } else {
            ui::cli::log!("{} ", amend_str);
        }

        amend(repo.clone(), id, snapshot, args)?;
        ui::cli::log!();
    }

    ui::cli::log!(
        "Finished in {}",
        utils::pretty_print_duration(start.elapsed())
    );

    Ok(())
}

fn amend(
    repo: Arc<Repository>,
    origin_snapshot_id: &ID,
    snapshot: &mut Snapshot,
    args: &CmdArgs,
) -> Result<()> {
    let (mut raw, mut encoded) = (0, 0);

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

    if args.exclude.is_some() {
        repo.init_pack_saver(1);
        let progress_reporter = Arc::new(SnapshotProgressReporter::new(
            Some(snapshot.summary.processed_items_count),
            Some(snapshot.summary.processed_bytes),
            1,
        ));
        rewrite_snapshot_tree(
            repo.clone(),
            snapshot,
            args.exclude.clone(),
            false,
            None,
            progress_reporter.clone(),
        )?;
        progress_reporter.finalize();
        repo.finalize_pack_saver();
    }

    // Save the amended snapshot and delete the old snapshot file
    let (new_id, raw_meta, encoded_meta) = repo.save_file(
        &SaveID::CalculateID,
        serde_json::to_string(&snapshot)?.as_bytes(),
        StorageHint {
            file_type: ContentIdType::Snapshot,
            is_metadata: true,
        },
        None,
    )?;
    raw += raw_meta;
    encoded += encoded_meta;

    // Delete the old snapshot ID if it changed
    // Note: To protect the repo from interruptions, we delete the snapshot only
    // after the new one is saved.
    if new_id != *origin_snapshot_id {
        if !args.keep_old {
            repo.delete_file(ContentIdType::Snapshot, origin_snapshot_id, None)?;
        }

        ui::cli::log!(
            "New snapshot ID   {}",
            new_id.to_short_hex(SHORT_SNAPSHOT_ID_LEN).bold().green()
        );
        ui::cli::log!(
            "Added to the repository: {} {}",
            utils::format_size_binary(raw, 3).bold().yellow(),
            format!("({} compressed)", utils::format_size_binary(encoded, 3))
                .bold()
                .green()
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
        ui::cli::log!("No changes");
    }

    Ok(())
}
