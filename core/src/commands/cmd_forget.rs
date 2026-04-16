use anyhow::{Result, bail};
use chrono::{Duration, Local};
use clap::{ArgGroup, Parser};
use colored::Colorize;
use serde::Serialize;

use crate::{
    backend::new_backend_with_prompt,
    commands::{self, cleanup::CleanupHandler, parse_tags},
    mapache::{ContentIdType, ID, defaults::DEFAULT_GC_TOLERANCE},
    repository::{
        repo::{REPO_DROPPED_EXTENSION, RepoConfig, Repository},
        retention::{RetentionRule, apply_retention_rules},
        snapshot::{SnapshotEntryList, SnapshotStream},
    },
    ui::{self, log_snapshots_compact},
    utils::{self, collections::IdSet, size},
};

use super::GlobalArgs;

// Define argument groups for mutual exclusivity and multiple selection
#[derive(Parser, Debug)]
#[clap(group = ArgGroup::new("policy").multiple(false))] // Either forget OR retention_rules, but not both
#[clap(group = ArgGroup::new("retention_rules").multiple(true))] // Allow multiple --keep-* rules
#[clap(
    about = "Remove snapshots from the repository",
    long_about = "Remove snapshots from the repository and apply retention policies. \
                  When applying retention rules, snapshots are kept as long as there is at \
                  least one rule that applies."
)]
pub struct CmdArgs {
    /// Forget specific snapshots by their IDs.
    #[arg(value_parser, value_delimiter = ' ', group = "policy")]
    pub forget: Vec<String>,

    /// Delete the snapshot without staging.
    #[arg(long, value_parser, default_value_t = false)]
    pub force: bool,

    /// Only consider snapshots with any tag from the list: tag[,tag,...]
    #[arg(long = "tags", value_parser)]
    pub tags_str: Option<String>,

    /// Keep the last N snapshots.
    #[arg(long, group = "retention_rules")]
    pub keep_last: Option<usize>,

    /// Keep snapshots within a specified duration (e.g., '1d', '2w', '3m', '4y', '5h', '6s').
    #[arg(long, value_parser = utils::parse_duration_string, group = "retention_rules")]
    pub keep_within: Option<Duration>,

    /// Keep N yearly snapshots. N must be greater than 1 or "all".
    #[arg(long, value_parser = parse_retention_number, group = "retention_rules")]
    pub keep_yearly: Option<usize>,

    /// Keep N monthly snapshots. N must be greater than 1 or "all".
    #[arg(long, value_parser = parse_retention_number, group = "retention_rules")]
    pub keep_monthly: Option<usize>,

    /// Keep N weekly snapshots. N must be greater than 1 or "all".
    #[arg(long, value_parser = parse_retention_number, group = "retention_rules")]
    pub keep_weekly: Option<usize>,

    /// Keep N daily snapshots. N must be greater than 1 or "all".
    #[arg(long, value_parser = parse_retention_number, group = "retention_rules")]
    pub keep_daily: Option<usize>,

    /// Keep all snapshots with tags
    #[arg(long = "keep-tags", value_parser, group = "retention_rules")]
    pub keep_tags_str: Option<String>,

    /// Perform a dry run: show which snapshots would be removed without actually removing them.
    #[arg(long)]
    pub dry_run: bool,

    // -- Garbage collector --
    /// Run the garbage collector after this command
    #[arg(long = "clean")]
    pub run_gc: bool,

    /// Garbage tolerance. The percentage [0-100] of garbage to tolerate in a
    /// pack file before repacking.
    #[clap(short, long, default_value_t = DEFAULT_GC_TOLERANCE)]
    pub tolerance: f32,
}

pub fn parse_retention_number(s: &str) -> Result<usize> {
    if s == "all" {
        Ok(usize::MAX)
    } else {
        let n = s.parse::<isize>();
        match n {
            Ok(num) => {
                if num > 0 {
                    Ok(num as usize)
                } else {
                    bail!("N must be greater than 0")
                }
            }
            Err(_) => bail!("{s} is not a number"),
        }
    }
}

const FORGET_MSG: &str = "forget";

pub async fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let auth = utils::get_auth(&global_args.auth_file)?;
    let backend = new_backend_with_prompt(global_args.backend_options(args.dry_run)).await?;

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
        true,
        global_args.retry_lock_duration,
    )
    .await?;

    let cleanup_handler = CleanupHandler::new()?;
    cleanup_handler.add_lock(lock_handle.clone());

    let mut snapshots_sorted: SnapshotEntryList = SnapshotStream::new(repo.clone())
        .await?
        .collect_entries(true)
        .await?;

    if let Some(tags) = &args.tags_str {
        let tags = parse_tags(Some(tags));
        snapshots_sorted.retain(|e| e.snapshot.has_tags(&tags));
    }
    snapshots_sorted.sort_unstable_by_key(|e| e.snapshot.timestamp);

    let mut ids_to_keep: IdSet<ID> = IdSet::default();

    if !args.forget.is_empty() {
        let mut forget_ids = IdSet::default();
        for prefix in &args.forget {
            let (id, _) = repo.find(ContentIdType::Snapshot, prefix).await?;
            forget_ids.insert(id);
        }
        for e in &snapshots_sorted {
            if !forget_ids.contains(&e.id) {
                ids_to_keep.insert(e.id);
            }
        }
    } else {
        let mut retention_rules = Vec::new();
        if let Some(n) = args.keep_last {
            retention_rules.push(RetentionRule::KeepLast(n));
        }
        if let Some(d) = args.keep_within {
            retention_rules.push(RetentionRule::KeepWithin(d));
        }
        if let Some(n) = args.keep_yearly {
            retention_rules.push(RetentionRule::KeepYearly(n));
        }
        if let Some(n) = args.keep_monthly {
            retention_rules.push(RetentionRule::KeepMonthly(n));
        }
        if let Some(n) = args.keep_weekly {
            retention_rules.push(RetentionRule::KeepWeekly(n));
        }
        if let Some(n) = args.keep_daily {
            retention_rules.push(RetentionRule::KeepDaily(n));
        }
        if let Some(tags_str) = &args.keep_tags_str {
            let keep_tags = parse_tags(Some(tags_str));
            retention_rules.push(RetentionRule::KeepTags(keep_tags));
        }

        if retention_rules.is_empty() {
            bail!("At least one retention rule must be used.");
        }

        ids_to_keep = apply_retention_rules(&snapshots_sorted, &retention_rules, Local::now());
    }

    let mut kept_snapshots = Vec::new();
    let mut removed_snapshots = Vec::new();
    for entry in snapshots_sorted.into_iter() {
        if !ids_to_keep.contains(&entry.id) {
            removed_snapshots.push(entry);
        } else {
            kept_snapshots.push(entry);
        };
    }

    if !args.dry_run && !removed_snapshots.is_empty() {
        for entry in &removed_snapshots {
            if args.force {
                repo.delete_file(ContentIdType::Snapshot, &entry.id, None)
                    .await?;
            } else {
                repo.set_extension(
                    ContentIdType::Snapshot,
                    &entry.id,
                    Some(REPO_DROPPED_EXTENSION),
                )
                .await?;
            }
        }
    }

    if global_args.json {
        ui::json_reporter::emit_static(
            FORGET_MSG,
            &MsgForget {
                kept: kept_snapshots,
                removed: removed_snapshots,
            },
        );
    } else {
        ui::cli::log!();
        ui::cli::log!("{}", "Snapshots to keep:".bold());
        log_snapshots_compact(&kept_snapshots);

        if !removed_snapshots.is_empty() {
            ui::cli::log!("{}", "Snapshots to remove:".bold());
            log_snapshots_compact(&removed_snapshots);
        }

        let count_str = utils::format_count(removed_snapshots.len(), "snapshot", "snapshots");
        if args.dry_run {
            ui::cli::log!("This would remove {}", count_str);
        } else {
            ui::cli::log!("Removed {}", count_str);
        }
    }

    if args.run_gc {
        let gc_args = commands::cmd_clean::CmdArgs {
            tolerance: args.tolerance,
            dry_run: args.dry_run,
            no_repack: false,
        };

        if !global_args.json {
            ui::cli::log!();
            ui::cli::log!("Running garbage collector...");
        }

        repo.reload_master_index().await?; // We need to load the index for the garbage collector
        commands::cmd_clean::run_with_repo(global_args, &gc_args, repo).await?;
    }

    lock_handle.unlock().await;

    Ok(())
}

#[derive(Serialize)]
struct MsgForget {
    kept: SnapshotEntryList,
    removed: SnapshotEntryList,
}
