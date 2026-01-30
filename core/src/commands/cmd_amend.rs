use std::{collections::BTreeSet, sync::Arc, time::Instant};

use anyhow::{Result, bail};
use clap::{ArgGroup, Args};
use colored::Colorize;

use crate::{
    backend::{StorageHint, new_backend_with_prompt},
    commands::{
        EMPTY_TAG_MARK, GlobalArgs, UseSnapshot, cleanup::CleanupHandler, find_use_snapshot,
        parse_tags,
    },
    fs::filter::parse_relative_filter_paths,
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
    #[clap(long, value_parser, required = false, value_delimiter = ',', num_args = 1..)]
    pub exclude: Option<Vec<String>>,
}

pub fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
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
    let parsed_excludes = parse_relative_filter_paths(args.exclude.as_ref());

    if parsed_excludes.is_some() {
        repo.init_pack_saver(1)?;
        let progress_reporter = Arc::new(SnapshotProgressReporter::new(None, None, 1));
        rewrite_snapshot_tree(
            repo.clone(),
            snapshot,
            parsed_excludes.as_ref(),
            false,
            None,
            progress_reporter.clone(),
        )?;

        repo.flush_and_finalize_pack_saver()?;
        progress_reporter.finalize();
    }

    // Save the amended snapshot and delete the old snapshot file
    let (new_id, _meta_size) = repo.save_file(
        &SaveID::CalculateID,
        serde_json::to_string(&snapshot)?.as_bytes(),
        StorageHint {
            file_type: ContentIdType::Snapshot,
            is_metadata: true,
        },
        None,
    )?;

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
