use std::{io, sync::Arc};

use chrono::Local;
use clap::{ArgGroup, Parser};
use serde::{Deserialize, Serialize};

use crate::{
    backend::new_backend_with_prompt,
    commands::{
        self, GlobalArgs, HookArgs, Merge, ToExitCode, cleanup::CleanupHandler, merge_opt,
        parse_tags, with_repository_lock,
    },
    common::{ContentIdType, ID, defaults::DEFAULT_GC_TOLERANCE, error::MapacheError, hooks},
    repository::{
        repo::{REPO_DROPPED_EXTENSION, Repository},
        retention::{RetentionRule, apply_retention_rules, filter_snapshots_by_hosts},
        snapshot::{SnapshotEntryList, SnapshotStream},
    },
    ui::{
        self,
        cli::{color::Colorize, log_snapshots_compact},
    },
    utils::{self, collections::IdSet},
};

#[derive(Debug, thiserror::Error)]
pub enum ForgetError {
    #[error("failed to open repository: {0}")]
    RepoOpenFail(String),
    #[error("invalid retention rule: {0}")]
    InvalidRule(String),
    #[error("forget failed: {0}")]
    ForgetFailed(String),
    #[error("forget interrupted by user")]
    Interrupted,
    #[error(transparent)]
    Repo(#[from] MapacheError),
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl ToExitCode for ForgetError {
    fn to_exit_code(&self) -> i32 {
        match self {
            ForgetError::RepoOpenFail(_) => 10,
            ForgetError::InvalidRule(_) => 20,
            ForgetError::ForgetFailed(_) => 30,
            ForgetError::Interrupted => 130,
            ForgetError::Repo(_) => 1,
            ForgetError::Io(_) => 1,
        }
    }
}

// Define argument groups for mutual exclusivity and multiple selection
#[derive(Parser, Debug, Clone, Serialize, Deserialize, Default)]
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

    /// Keep the last N snapshots. N must be greater than 0 or "all".
    #[arg(long, value_parser = parse_retention_number, group = "retention_rules")]
    #[serde(deserialize_with = "deserialize_retention_opt")]
    pub keep_last: Option<usize>,

    /// Keep snapshots within a specified duration (e.g., '1d', '2w', '3m', '4y', '5h', '6s').
    #[arg(long, value_parser = utils::parse_duration_string, group = "retention_rules")]
    #[serde(deserialize_with = "deserialize_duration_opt")]
    pub keep_within: Option<chrono::Duration>,

    /// Keep N yearly snapshots. N must be greater than 0 or "all".
    #[arg(long, value_parser = parse_retention_number, group = "retention_rules")]
    #[serde(deserialize_with = "deserialize_retention_opt")]
    pub keep_yearly: Option<usize>,

    /// Keep N monthly snapshots. N must be greater than 0 or "all".
    #[arg(long, value_parser = parse_retention_number, group = "retention_rules")]
    #[serde(deserialize_with = "deserialize_retention_opt")]
    pub keep_monthly: Option<usize>,

    /// Keep N weekly snapshots. N must be greater than 0 or "all".
    #[arg(long, value_parser = parse_retention_number, group = "retention_rules")]
    #[serde(deserialize_with = "deserialize_retention_opt")]
    pub keep_weekly: Option<usize>,

    /// Keep N daily snapshots. N must be greater than 0 or "all".
    #[arg(long, value_parser = parse_retention_number, group = "retention_rules")]
    #[serde(deserialize_with = "deserialize_retention_opt")]
    pub keep_daily: Option<usize>,

    /// Keep N hourly snapshots. N must be greater than 0 or "all".
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

    #[clap(flatten)]
    pub hook_args: HookArgs,
}

impl CmdArgs {
    pub(crate) fn template() -> Self {
        Self {
            forget: Vec::new(),
            force: false,
            tags_str: None,
            hosts: Vec::new(),
            keep_last: Some(10),
            keep_within: None,
            keep_yearly: Some(1),
            keep_monthly: Some(6),
            keep_weekly: Some(4),
            keep_daily: Some(7),
            keep_hourly: None,
            keep_tags: Some("important,archive".to_string()),
            keep_min: None,
            dry_run: false,
            run_gc: false,
            tolerance: Some(0.0),
            hook_args: HookArgs::default(),
        }
    }
}

impl Merge for CmdArgs {
    fn merge(&mut self, other: Self) {
        if self.forget.is_empty() {
            self.forget = other.forget;
        }
        // skip: force
        merge_opt(&mut self.tags_str, other.tags_str);
        if self.hosts.is_empty() {
            self.hosts = other.hosts;
        }
        merge_opt(&mut self.keep_last, other.keep_last);
        merge_opt(&mut self.keep_within, other.keep_within);
        merge_opt(&mut self.keep_yearly, other.keep_yearly);
        merge_opt(&mut self.keep_monthly, other.keep_monthly);
        merge_opt(&mut self.keep_weekly, other.keep_weekly);
        merge_opt(&mut self.keep_daily, other.keep_daily);
        merge_opt(&mut self.keep_hourly, other.keep_hourly);
        merge_opt(&mut self.keep_tags, other.keep_tags);
        merge_opt(&mut self.keep_min, other.keep_min);
        // skip: dry_run
        // skip: run_gc
        merge_opt(&mut self.tolerance, other.tolerance);
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

pub fn parse_retention_number(s: &str) -> std::result::Result<usize, String> {
    if s == "all" {
        Ok(usize::MAX)
    } else {
        let n = s
            .parse::<isize>()
            .map_err(|_| format!("'{s}' is not a valid number"))?;
        if n > 0 {
            Ok(n as usize)
        } else {
            Err("retention number must be greater than 0".to_string())
        }
    }
}

const FORGET_MSG: &str = "forget";

pub async fn run(
    global_args: &GlobalArgs,
    args: &CmdArgs,
    cmd_hooks: Option<&crate::common::config::CommandHooks>,
) -> Result<(), ForgetError> {
    tracing::info!(target: "forget", "Starting forget command");

    let dry_run = args.dry_run;
    let run_gc = args.run_gc;
    let json_output = global_args.json;

    let repo_result = with_repository_lock(
        global_args.auth_file.as_ref(),
        global_args.key.as_ref(),
        new_backend_with_prompt(global_args.backend_options(dry_run)).await?,
        global_args.to_repo_config(),
        true,
        global_args.retry_lock_duration,
        global_args.no_lock,
        |repo, _secure_storage, lock_handle| async move {
            let repo = repo.clone();

            // Phase 1: forget
            let cleanup_handler = CleanupHandler::new();
            cleanup_handler.add_lock(lock_handle.clone());

            hooks::run_command_pre(
                cmd_hooks,
                "forget",
                &global_args.repo,
                args.hook_args.pre_hook.as_deref(),
                dry_run,
            )
            .await?;

            forget_phase(repo.clone(), args, json_output, &cleanup_handler).await?;
            drop(cleanup_handler);

            // Phase 2: optional post-forget GC, reusing the same repo/lock.
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
                    hook_args: HookArgs::default(),
                };
                commands::cmd_clean::run_with_repo(json_output, &gc_args, repo, lock_handle)
                    .await
                    .map_err(|e| ForgetError::ForgetFailed(format!("gc failed: {e}")))?;
            }

            Ok(())
        },
    )
    .await;

    hooks::run_command_post(
        cmd_hooks,
        "forget",
        &global_args.repo,
        &repo_result,
        args.hook_args.post_hook.as_deref(),
        dry_run,
    )
    .await;

    repo_result
}

/// Forget phase: load snapshots, apply retention/forget rules, mark or delete
/// the removed ones, and print the result. Runs under the caller's
/// `CleanupHandler` so interrupts are detected at the end.
async fn forget_phase(
    repo: Arc<Repository>,
    args: &CmdArgs,
    json_output: bool,
    cleanup_handler: &CleanupHandler,
) -> Result<(), ForgetError> {
    let dry_run = args.dry_run;

    tracing::info!(target: "forget", "Loading snapshots");
    let mut snapshots_sorted: SnapshotEntryList = SnapshotStream::new(repo.clone())
        .await
        .map_err(|e| ForgetError::ForgetFailed(format!("failed to load snapshots: {}", e.inner())))?
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
                .map_err(|_e| ForgetError::ForgetFailed(format!("snapshot not found: {prefix}")))?;
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
            return Err(ForgetError::InvalidRule(
                "at least one retention rule must be used.".to_string(),
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
                        ForgetError::ForgetFailed(format!(
                            "failed to delete snapshot: {}",
                            e.inner()
                        ))
                    })?;
            } else {
                repo.set_extension(
                    ContentIdType::Snapshot,
                    &entry.id,
                    Some(REPO_DROPPED_EXTENSION),
                )
                .await
                .map_err(|e| {
                    ForgetError::ForgetFailed(format!(
                        "failed to mark snapshot for deletion: {}",
                        e.inner()
                    ))
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
        tracing::info!(target: "forget", "Forget interrupted by user");
        return Err(ForgetError::Interrupted);
    }

    Ok(())
}

#[derive(Serialize)]
struct MsgForget {
    kept: SnapshotEntryList,
    removed: SnapshotEntryList,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    // `--keep-last 0` must be rejected, exactly like the other `--keep-*` rules.
    // Previously `keep_last` used a plain integer parser with no lower bound, so
    // `--keep-last 0` produced `KeepLast(0)`, which keeps zero snapshots and (with
    // no `--keep-min`) silently forgets every snapshot in the repository.
    #[test]
    fn keep_last_rejects_zero() {
        let err = CmdArgs::try_parse_from(["forget", "--keep-last", "0"])
            .expect_err("--keep-last 0 must be rejected");
        assert!(
            err.to_string().contains("greater than 0"),
            "unexpected error message: {err}"
        );
    }

    #[test]
    fn keep_last_rejects_negative() {
        assert!(
            CmdArgs::try_parse_from(["forget", "--keep-last", "-3"]).is_err(),
            "--keep-last -3 must be rejected"
        );
    }

    #[test]
    fn keep_last_accepts_all() {
        let args = CmdArgs::try_parse_from(["forget", "--keep-last", "all"])
            .expect("--keep-last all must parse");
        assert_eq!(args.keep_last, Some(usize::MAX));
    }

    #[test]
    fn keep_last_accepts_positive() {
        let args = CmdArgs::try_parse_from(["forget", "--keep-last", "5"])
            .expect("--keep-last 5 must parse");
        assert_eq!(args.keep_last, Some(5));
    }
}
