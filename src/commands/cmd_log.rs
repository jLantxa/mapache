use anyhow::{Context, Result, bail};
use clap::{ArgGroup, Args};
use colored::Colorize;

use crate::{
    backend::{BackendOptions, new_backend_with_prompt},
    commands::{cleanup::CleanupHandler, parse_tags},
    mapache::{ContentIdType, ID, defaults::SHORT_SNAPSHOT_ID_LEN},
    repository::{
        repo::{REPO_DROPPED_EXTENSION, RepoConfig, Repository},
        snapshot::{Snapshot, SnapshotStreamer},
    },
    ui::{self, log_snapshots_compact},
    utils::{self, size},
};

use super::GlobalArgs;

#[derive(Args, Debug)]
#[clap(about = "Show all snapshots present in the repository")]
#[clap(group = ArgGroup::new("filter").multiple(false))]
pub struct CmdArgs {
    /// Show a single snapshot with a given ID
    #[arg(value_parser, group = "filter")]
    pub snapshot: Option<String>,

    /// Show dropped snapshots only
    #[arg(long, value_parser, default_value_t = false, group = "filter")]
    pub dropped: bool,

    /// Show active and dropped snapshots
    #[arg(long, value_parser, default_value_t = false, group = "filter")]
    pub all: bool,

    /// Show a compact list of snapshots
    #[arg(short, long)]
    pub compact: bool,

    /// Only consider snapshots with tags: tag[,tag,...]
    #[arg(long = "tags", value_parser)]
    pub tags_str: Option<String>,
}

pub fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let auth = utils::get_auth_from_file(&global_args.auth_file)?;
    let backend = new_backend_with_prompt(BackendOptions {
        repo_path: global_args.repo.clone(),
        ssh_pubkey: global_args.ssh_pubkey.clone(),
        ssh_privatekey: global_args.ssh_privatekey.clone(),
        dry_backend: false,
    })?;

    let config = RepoConfig {
        pack_size: (global_args.pack_size_mib * size::MiB as f32) as u64,
        use_cache: !global_args.no_cache,
    };
    let (repo, _, lock_handle) = Repository::try_open_with_lock(
        auth.as_ref(),
        global_args.key.as_ref(),
        backend,
        config,
        false,
    )?;

    let lock_handle_clone = lock_handle.clone();
    let _cleanup_handler = CleanupHandler::new(move || {
        lock_handle_clone.write().unlock();
    })?;

    let show_active = args.all || !args.dropped;
    let show_dropped = args.all || args.dropped;

    let mut snapshots_sorted = match &args.snapshot {
        None => {
            let mut snapshots = Vec::new();

            if show_active {
                let mut active_snapshots = SnapshotStreamer::new(repo.clone())?
                    .map(|(id, snapshot)| (id, snapshot, true))
                    .collect();
                snapshots.append(&mut active_snapshots);
            }

            if show_dropped {
                let mut dropped_snapshots = SnapshotStreamer::dropped(repo.clone())?
                    .map(|(id, snapshot)| (id, snapshot, false))
                    .collect();
                snapshots.append(&mut dropped_snapshots);
            }

            snapshots
        }
        Some(prefix) => {
            let (id, path) = repo
                .find(ContentIdType::Snapshot, prefix)
                .with_context(|| format!("Could not find snapshot {prefix}"))?;

            let ext = path.extension().and_then(|s| s.to_str());
            let active = match ext {
                Some(REPO_DROPPED_EXTENSION) => false,
                None => true,
                _ => bail!(
                    "Snapshot {} not found",
                    id.to_short_hex(SHORT_SNAPSHOT_ID_LEN)
                ),
            };

            let snapshot = repo.load_snapshot(&id, ext)?;
            vec![(id, snapshot, active)]
        }
    };

    if let Some(tags_str) = &args.tags_str {
        let tags = parse_tags(Some(tags_str));
        snapshots_sorted.retain(|(_id, sn, _active)| sn.has_tags(&tags));
    }
    snapshots_sorted.sort_unstable_by_key(|(_id, snapshot, _active)| snapshot.timestamp);

    if snapshots_sorted.is_empty() {
        ui::cli::log!("No snapshots found");
        return Ok(());
    }

    ui::cli::log!();
    if args.compact {
        log_snapshots_compact(&snapshots_sorted);
    } else {
        log_snapshots_full(&snapshots_sorted);
    }

    ui::cli::log!("{} snapshots", snapshots_sorted.len());

    Ok(())
}

fn log_snapshots_full(snapshots: &[(ID, Snapshot, bool)]) {
    let mut peekable_snapshots = snapshots.iter().peekable();
    while let Some((id, snapshot, active)) = peekable_snapshots.next() {
        if *active {
            ui::cli::log!("{}", id.to_hex().bold().yellow());
        } else {
            ui::cli::log!("{}", (id.to_hex() + " (dropped)").bold().dimmed());
        }

        ui::cli::log!(
            "{} {}",
            "Date:".bold(),
            utils::pretty_print_timestamp(&snapshot.timestamp)
        );
        ui::cli::log!(
            "{} {}",
            "Size:".bold(),
            utils::format_size(snapshot.summary.processed_bytes, 3)
        );
        ui::cli::log!("{} {}", "Root:".bold(), &snapshot.root.display());

        if let Some(hostname) = &snapshot.hostname {
            ui::cli::log!("{} {}", "Host:".bold(), hostname);
        }

        if let Some(username) = &snapshot.username {
            ui::cli::log!("{} {}", "User:".bold(), username);
        }

        if !snapshot.tags.is_empty() {
            ui::cli::log!(
                "{} {}",
                "Tags:".bold(),
                snapshot
                    .tags
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }

        ui::cli::log!();
        ui::cli::log!("{}", "Paths:".bold());
        for path in &snapshot.paths {
            // This should work, since all paths have the common root
            let relative_path = path
                .strip_prefix(&snapshot.root)
                .expect("Could not strip snapshot root from path");
            ui::cli::log!("  {}", relative_path.display());
        }

        if let Some(description) = &snapshot.description {
            ui::cli::log!();
            ui::cli::log!("{}", description);
        }

        if peekable_snapshots.peek().is_some() {
            ui::cli::log!();
        }
    }

    ui::cli::log!();
}
