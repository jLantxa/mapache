use anyhow::{Context, Result, bail};
use clap::{ArgGroup, Args};
use colored::Colorize;
use futures::StreamExt;
use serde::Serialize;

use crate::{
    backend::new_backend_with_prompt,
    commands::{cleanup::CleanupHandler, parse_tags},
    mapache::{ContentIdType, defaults::SHORT_SNAPSHOT_ID_LEN},
    repository::{
        repo::{REPO_DROPPED_EXTENSION, RepoConfig, Repository},
        snapshot::{SnapshotEntry, SnapshotEntryList, SnapshotStream},
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

const LOG_MSG: &str = "log";

pub async fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let auth = utils::get_auth_from_file(&global_args.auth_file)?;
    let backend = new_backend_with_prompt(global_args.backend_options(false)).await?;

    let config = RepoConfig {
        pack_size: (global_args.pack_size_mib * size::MiB as f32) as u64,
        use_cache: !global_args.no_cache,
        compression: global_args.compression_level,
    };
    let (repo, _, mut lock_handle) = Repository::try_open_with_lock(
        auth.as_ref(),
        global_args.key.as_ref(),
        backend,
        config,
        false,
        global_args.retry_lock_duration,
    )
    .await?;

    let cleanup_handler = CleanupHandler::new()?;
    cleanup_handler.add_lock(lock_handle.clone());

    let show_active = args.all || !args.dropped;
    let show_dropped = args.all || args.dropped;

    let mut snapshots_sorted = match &args.snapshot {
        None => {
            let mut snapshots = Vec::new();

            if show_active {
                let mut active_snapshots = SnapshotStream::new(repo.clone())
                    .await?
                    .map(|(id, snapshot)| SnapshotEntry {
                        id,
                        snapshot,
                        active: true,
                    })
                    .collect()
                    .await;
                snapshots.append(&mut active_snapshots);
            }

            if show_dropped {
                let mut dropped_snapshots = SnapshotStream::dropped(repo.clone())
                    .await?
                    .map(|(id, snapshot)| SnapshotEntry {
                        id,
                        snapshot,
                        active: false,
                    })
                    .collect()
                    .await;
                snapshots.append(&mut dropped_snapshots);
            }

            snapshots
        }
        Some(prefix) => {
            let (id, path) = repo
                .find(ContentIdType::Snapshot, prefix)
                .await
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

            let snapshot = repo.load_snapshot(&id, ext).await?;
            vec![SnapshotEntry {
                id,
                snapshot,
                active,
            }]
        }
    };

    if let Some(tags_str) = &args.tags_str {
        let tags = parse_tags(Some(tags_str));
        snapshots_sorted.retain(|e| e.snapshot.has_tags(&tags));
    }
    snapshots_sorted.sort_unstable_by_key(|e| e.snapshot.timestamp);

    if snapshots_sorted.is_empty() {
        ui::cli::log!("No snapshots found");
        lock_handle.unlock().await;
        return Ok(());
    }

    if !global_args.json {
        ui::cli::log!();
        if args.compact {
            log_snapshots_compact(&snapshots_sorted);
        } else {
            log_snapshots_full(&snapshots_sorted);
        }

        ui::cli::log!("{} snapshots", snapshots_sorted.len());
    } else {
        ui::json_reporter::emit_static(
            LOG_MSG,
            &MsgSnapshots {
                snapshots: snapshots_sorted,
            },
        );
    }

    lock_handle.unlock().await;

    Ok(())
}

fn log_snapshots_full(snapshots: &SnapshotEntryList) {
    let mut peekable_snapshots = snapshots.iter().peekable();
    while let Some(entry) = peekable_snapshots.next() {
        let id = &entry.id;
        let snapshot = &entry.snapshot;
        let active = entry.active;

        if active {
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
            utils::format_size_binary(snapshot.summary.processed_bytes, 3)
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

#[derive(Serialize)]
struct MsgSnapshots {
    snapshots: SnapshotEntryList,
}
