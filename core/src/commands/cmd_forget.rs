use std::sync::Arc;

use anyhow::{Result, bail};
use chrono::Local;
use clap::{ArgGroup, Parser};
use colored::Colorize;
use serde::{Deserialize, Serialize};

use crate::{
    backend::new_backend_with_prompt,
    commands::{
        self, GlobalArgs, Merge, ToExitCode, cleanup::CleanupHandler, fail, parse_tags,
        with_repository_lock,
    },
    mapache::{ContentIdType, ID, defaults::DEFAULT_GC_TOLERANCE},
    repository::{
        repo::{REPO_DROPPED_EXTENSION, Repository},
        retention::{RetentionRule, apply_retention_rules, filter_snapshots_by_hosts},
        snapshot::{SnapshotEntryList, SnapshotStream},
    },
    ui::{self, cli::log_snapshots_compact},
    utils::{self, collections::IdSet},
};

#[derive(Debug, Clone, Copy)]
pub enum ForgetError {
    RepoOpenFail = 10,
    InvalidRule = 20,
    ForgetFailed = 30,
    Interrupted = 130,
}

impl ToExitCode for ForgetError {
    fn to_exit_code(&self) -> i32 {
        *self as i32
    }
}

// Define argument groups for mutual exclusivity and multiple selection
#[derive(Parser, Debug, Clone, Deserialize, Default)]
#[clap(group = ArgGroup::new("policy").multiple(false))] // Either forget OR retention_rules, but not both
#[clap(group = ArgGroup::new("retention_rules").multiple(true))] // Allow multiple --keep-* rules
#[clap(
    about = "Remove snapshots from the repository",
    long_about = "Remove snapshots from the repository and apply retention policies. \
                  When applying retention rules, snapshots are kept as long as there is at \
                  least one rule that applies."
)]
#[serde(default, rename_all = "kebab-case")]
pub struct CmdArgs {
    /// Forget specific snapshots by their IDs.
    #[arg(value_parser, value_delimiter = ' ', group = "policy")]
    pub forget: Vec<String>,

    /// Delete the snapshot without staging.
    #[arg(long, value_parser)]
    pub force: bool,

    /// Only consider snapshots with any tag from the list: tag[,tag,...]
    #[arg(long = "tags", value_parser)]
    pub tags_str: Option<String>,

    /// Only consider snapshots from these hosts.
    #[arg(long = "host", value_parser)]
    pub hosts: Vec<String>,

    /// Keep the last N snapshots.
    #[arg(long, group = "retention_rules")]
    pub keep_last: Option<usize>,

    /// Keep snapshots within a specified duration (e.g., '1d', '2w', '3m', '4y', '5h', '6s').
    #[arg(long, value_parser = utils::parse_duration_string, group = "retention_rules")]
    #[serde(deserialize_with = "deserialize_duration_opt")]
    pub keep_within: Option<chrono::Duration>,

    /// Keep N yearly snapshots. N must be greater than 1 or "all".
    #[arg(long, value_parser = parse_retention_number, group = "retention_rules")]
    #[serde(deserialize_with = "deserialize_retention_opt")]
    pub keep_yearly: Option<usize>,

    /// Keep N monthly snapshots. N must be greater than 1 or "all".
    #[arg(long, value_parser = parse_retention_number, group = "retention_rules")]
    #[serde(deserialize_with = "deserialize_retention_opt")]
    pub keep_monthly: Option<usize>,

    /// Keep N weekly snapshots. N must be greater than 1 or "all".
    #[arg(long, value_parser = parse_retention_number, group = "retention_rules")]
    #[serde(deserialize_with = "deserialize_retention_opt")]
    pub keep_weekly: Option<usize>,

    /// Keep N daily snapshots. N must be greater than 1 or "all".
    #[arg(long, value_parser = parse_retention_number, group = "retention_rules")]
    #[serde(deserialize_with = "deserialize_retention_opt")]
    pub keep_daily: Option<usize>,

    /// Keep N hourly snapshots. N must be greater than 1 or "all".
    #[arg(long, value_parser = parse_retention_number, group = "retention_rules")]
    #[serde(deserialize_with = "deserialize_retention_opt")]
    pub keep_hourly: Option<usize>,

    /// Keep all snapshots with tags
    #[arg(long = "keep-tags", value_parser, group = "retention_rules")]
    pub keep_tags: Option<String>,

    /// Always keep at least N snapshots (after applying retention rules).
    #[arg(long, value_parser = parse_retention_number)]
    #[serde(deserialize_with = "deserialize_retention_opt")]
    pub keep_min: Option<usize>,

    /// Perform a dry run: show which snapshots would be removed without actually removing them.
    #[arg(long)]
    pub dry_run: bool,

    // -- Garbage collector --
    /// Run the garbage collector after this command
    #[arg(long = "clean")]
    pub run_gc: bool,

    /// Garbage tolerance. The percentage [0-100] of garbage to tolerate in a
    /// pack file before repacking.
    #[clap(short, long)]
    pub tolerance: Option<f32>,
}

impl Merge for CmdArgs {
    fn merge(&mut self, other: Self) {
        if !other.forget.is_empty() {
            self.forget = other.forget;
        }
        // skip: force
        if other.tags_str.is_some() {
            self.tags_str = other.tags_str;
        }
        if !other.hosts.is_empty() {
            self.hosts = other.hosts;
        }
        if other.keep_last.is_some() {
            self.keep_last = other.keep_last;
        }
        if other.keep_within.is_some() {
            self.keep_within = other.keep_within;
        }
        if other.keep_yearly.is_some() {
            self.keep_yearly = other.keep_yearly;
        }
        if other.keep_monthly.is_some() {
            self.keep_monthly = other.keep_monthly;
        }
        if other.keep_weekly.is_some() {
            self.keep_weekly = other.keep_weekly;
        }
        if other.keep_daily.is_some() {
            self.keep_daily = other.keep_daily;
        }
        if other.keep_hourly.is_some() {
            self.keep_hourly = other.keep_hourly;
        }
        if other.keep_tags.is_some() {
            self.keep_tags = other.keep_tags;
        }
        if other.keep_min.is_some() {
            self.keep_min = other.keep_min;
        }
        // skip: dry_run
        // skip: run_gc
        if other.tolerance.is_some() {
            self.tolerance = other.tolerance;
        }
    }
}

fn deserialize_duration_opt<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<chrono::Duration>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt = Option::<String>::deserialize(deserializer)?;
    opt.map(|s| utils::parse_duration_string(&s).map_err(serde::de::Error::custom))
        .transpose()
}

fn deserialize_retention_opt<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<usize>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum RetentionValue {
        Number(usize),
        String(String),
    }

    let opt = Option::<RetentionValue>::deserialize(deserializer)?;
    match opt {
        Some(RetentionValue::Number(n)) => {
            if n > 0 {
                Ok(Some(n))
            } else {
                Err(serde::de::Error::custom(
                    "retention number must be greater than 0",
                ))
            }
        }
        Some(RetentionValue::String(s)) => parse_retention_number(&s)
            .map(Some)
            .map_err(serde::de::Error::custom),
        None => Ok(None),
    }
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
    tracing::info!(target: "forget", "Starting forget command");

    let dry_run = args.dry_run;
    let run_gc = args.run_gc;
    let json_output = global_args.json;

    // Phase 1: forget under its own lock.
    with_repository_lock(
        global_args.auth_file.as_ref(),
        global_args.key.as_ref(),
        new_backend_with_prompt(global_args.backend_options(dry_run))
            .await
            .map_err(|e| {
                fail(
                    format!("Failed to initialize backend: {}", e),
                    ForgetError::ForgetFailed,
                )
            })?,
        global_args.to_repo_config(),
        true,
        global_args.retry_lock_duration,
        |repo, _secure_storage, lock_handle| async move {
            let cleanup_handler = CleanupHandler::new().map_err(|e| {
                fail(
                    format!("Failed to initialize cleanup handler: {}", e),
                    ForgetError::ForgetFailed,
                )
            })?;
            cleanup_handler.add_lock(lock_handle);

            forget_phase(repo, args, json_output, &cleanup_handler).await
        },
    )
    .await?;

    // Phase 2: optional post-forget GC. Delegated to `cmd_clean::run`, which
    // acquires and releases its own lock.
    if run_gc {
        tracing::info!(target: "forget", "Post-forget GC requested");
        if !json_output {
            ui::cli::log!();
            ui::cli::log!("Running garbage collector...");
        }

        let gc_args = commands::cmd_clean::CmdArgs {
            tolerance: args.tolerance.unwrap_or(DEFAULT_GC_TOLERANCE * 100.0),
            dry_run,
            no_repack: false,
        };
        commands::cmd_clean::run(global_args, &gc_args).await?;
    }

    Ok(())
}

/// Forget phase: load snapshots, apply retention/forget rules, mark or delete
/// the removed ones, and print the result. Runs under the caller's
/// `CleanupHandler` so interrupts are detected at the end.
async fn forget_phase(
    repo: Arc<Repository>,
    args: &CmdArgs,
    json_output: bool,
    cleanup_handler: &CleanupHandler,
) -> Result<()> {
    let dry_run = args.dry_run;

    tracing::info!(target: "forget", "Loading snapshots");
    let mut snapshots_sorted: SnapshotEntryList = SnapshotStream::new(repo.clone())
        .await
        .map_err(|e| {
            fail(
                format!("Failed to load snapshots: {}", e),
                ForgetError::ForgetFailed,
            )
        })?
        .collect_entries(true)
        .await?;

    if let Some(tags) = &args.tags_str {
        let tags = parse_tags(Some(tags));
        snapshots_sorted.retain(|e| e.snapshot.has_tags(&tags));
    }

    if !args.hosts.is_empty() {
        let filtered = filter_snapshots_by_hosts(snapshots_sorted.iter(), &args.hosts);
        let filtered_ids: IdSet<ID> = filtered.iter().map(|e| e.id).collect();
        snapshots_sorted.retain(|e| filtered_ids.contains(&e.id));
    }

    snapshots_sorted.sort_unstable_by_key(|e| e.snapshot.timestamp);

    let mut ids_to_keep: IdSet<ID> = IdSet::default();

    if !args.forget.is_empty() {
        tracing::info!(target: "forget", "Forgetting specific snapshots: {:?}", args.forget);
        let mut forget_ids = IdSet::default();
        for prefix in &args.forget {
            let (id, _) = repo
                .find(ContentIdType::Snapshot, prefix)
                .await
                .map_err(|_e| {
                    fail(
                        format!("Snapshot not found: {}", prefix),
                        ForgetError::ForgetFailed,
                    )
                })?;
            forget_ids.insert(id);
        }
        for e in &snapshots_sorted {
            if !forget_ids.contains(&e.id) {
                ids_to_keep.insert(e.id);
            }
        }
    } else {
        tracing::info!(target: "forget", "Applying retention rules");
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
        if let Some(n) = args.keep_hourly {
            retention_rules.push(RetentionRule::KeepHourly(n));
        }
        if let Some(tags_str) = &args.keep_tags {
            let keep_tags = parse_tags(Some(tags_str));
            retention_rules.push(RetentionRule::KeepTags(keep_tags));
        }

        if retention_rules.is_empty() {
            return Err(fail(
                "At least one retention rule must be used.",
                ForgetError::InvalidRule,
            ));
        }

        ids_to_keep = apply_retention_rules(
            &snapshots_sorted.iter().collect::<Vec<_>>(),
            &retention_rules,
            args.keep_min,
            Local::now(),
        );
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

    tracing::info!(target: "forget", "Kept {} snapshots, removed {} snapshots", kept_snapshots.len(), removed_snapshots.len());

    if !dry_run && !removed_snapshots.is_empty() {
        tracing::info!(target: "forget", "Updating repository (removing {} snapshots)", removed_snapshots.len());
        for entry in &removed_snapshots {
            tracing::info!(target: "forget", "Removing snapshot {} ({:?}, tags={:?})", entry.id, entry.snapshot.timestamp, entry.snapshot.tags);
            if args.force {
                repo.delete_file(ContentIdType::Snapshot, &entry.id, None)
                    .await
                    .map_err(|e| {
                        fail(
                            format!("Failed to delete snapshot: {}", e),
                            ForgetError::ForgetFailed,
                        )
                    })?;
            } else {
                repo.set_extension(
                    ContentIdType::Snapshot,
                    &entry.id,
                    Some(REPO_DROPPED_EXTENSION),
                )
                .await
                .map_err(|e| {
                    fail(
                        format!("Failed to mark snapshot for deletion: {}", e),
                        ForgetError::ForgetFailed,
                    )
                })?;
            }
        }
    }

    if json_output {
        ui::json::emit_static(
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
        if dry_run {
            ui::cli::log!("This would remove {}", count_str);
        } else {
            ui::cli::log!("Removed {}", count_str);
        }
    }

    if cleanup_handler.is_interrupted() {
        if !json_output {
            ui::cli::log!("Forget interrupted by user.");
        }
        return Err(fail(
            "Forget interrupted by user.",
            ForgetError::Interrupted,
        ));
    }

    Ok(())
}

#[derive(Serialize)]
struct MsgForget {
    kept: SnapshotEntryList,
    removed: SnapshotEntryList,
}
