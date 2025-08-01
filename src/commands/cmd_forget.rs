// mapache is an incremental backup tool
// Copyright (C) 2025  Javier Lancha Vázquez <javier.lancha@gmail.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use anyhow::{Result, bail};
use chrono::{DateTime, Datelike, Duration, Local};
use clap::{ArgGroup, Parser};
use colored::Colorize;

use crate::backend::new_backend_with_prompt;
use crate::commands::cleanup::CleanupHandler;
use crate::commands::parse_tags;
use crate::global::defaults::DEFAULT_GC_TOLERANCE;
use crate::global::{FileType, ID};
use crate::repository::repo::{RepoConfig, Repository};
use crate::repository::snapshot::{Snapshot, SnapshotStreamer};
use crate::ui::log_snapshots_compact;
use crate::utils::size;
use crate::{commands, ui, utils};

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

    /// Verify that all referenced IDs are stored in the index without reading the data.
    #[clap(long, default_value_t = false)]
    pub verify: bool,
}

// Snapshot retention rules.
// The rules are applied as a union. Snapshots are kept as long as there is at least
// one rule that applies. For example, KeepLast(4) will keep the last 4 snapshots
// (after applying filtering), but if the 5th snapshot has a tag contained in
// KeepTags(tags), the 5th snapshot is kept as well.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetentionRule {
    /// Keep the last N snapshots.
    KeepLast(usize),
    /// Keep snapshots within a specified duration from the present.
    KeepWithin(Duration),
    /// Keep N yearly snapshots.
    KeepYearly(usize),
    /// Keep N monthly snapshots.
    KeepMonthly(usize),
    /// Keep N weekly snapshots.
    KeepWeekly(usize),
    /// Keep N daily snapshots.
    KeepDaily(usize),
    /// Keep snapshots with tag
    KeepTags(BTreeSet<String>),
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
            Err(_) => bail!("{} is not a number", s),
        }
    }
}

pub fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let pass = utils::get_password_from_file(&global_args.password_file)?;
    let backend = new_backend_with_prompt(global_args, args.dry_run)?;

    let config = RepoConfig {
        pack_size: (global_args.pack_size_mib * size::MiB as f32) as u64,
    };
    let (repo, _, lock_handle) =
        Repository::try_open_with_lock(pass, global_args.key.as_ref(), backend, config, true)?;

    let lock_handle_clone = lock_handle.clone();
    let _cleanup_handler = CleanupHandler::new(move || {
        let _ = lock_handle_clone.write().unlock();
    })?;

    // All sapshots, filter by tags and sorted by timestamp
    let mut snapshots_sorted: Vec<(ID, Snapshot)> = SnapshotStreamer::new(repo.clone())?.collect();
    if let Some(tags) = &args.tags_str {
        let tags = parse_tags(Some(tags));
        snapshots_sorted.retain(|(_id, sn)| sn.has_tags(&tags));
    }
    snapshots_sorted.sort_by_key(|(_id, snapshot)| snapshot.timestamp);

    let mut ids_to_keep: HashSet<ID> = HashSet::new();

    if !args.forget.is_empty() {
        let mut forget_ids = HashSet::new();
        for prefix in &args.forget {
            let (id, _) = repo.find(FileType::Snapshot, prefix)?;
            forget_ids.insert(id);
        }

        for (id, _snapshot) in &snapshots_sorted {
            if !forget_ids.contains(id) {
                ids_to_keep.insert(id.clone());
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
    for (id, snapshot) in snapshots_sorted.into_iter() {
        if !ids_to_keep.contains(&id) {
            removed_snapshots.push((id, snapshot));
        } else {
            kept_snapshots.push((id, snapshot));
        };
    }

    ui::cli::log!();
    ui::cli::log!("{}", "Snapshots to keep:".bold());
    log_snapshots_compact(&kept_snapshots);

    if !removed_snapshots.is_empty() {
        ui::cli::log!("{}", "Snapshots to remove:".bold());
        log_snapshots_compact(&removed_snapshots);
    }

    if !args.dry_run {
        let num_removed_snapshots = removed_snapshots.len();
        for (id, _) in removed_snapshots {
            repo.remove_snapshot(&id)?;
        }

        ui::cli::log!(
            "Removed {}",
            utils::format_count(num_removed_snapshots, "snapshot", "snapshots")
        );
    } else {
        ui::cli::log!(
            "This would remove {}",
            utils::format_count(removed_snapshots.len(), "snapshot", "snapshots")
        );
    }

    // Run the garbage collector
    if args.run_gc {
        let gc_args = commands::cmd_clean::CmdArgs {
            tolerance: args.tolerance,
            dry_run: args.dry_run,
            verify: args.verify,
        };

        ui::cli::log!();
        ui::cli::log!("Running garbage collector...");
        commands::cmd_clean::run_with_repo(global_args, &gc_args, repo)?;
    }

    Ok(())
}

/// Applies retention policies to a sorted list of snapshots and returns the IDs of snapshots to keep.
///
/// `snapshots_sorted`: A vector of (ID, Snapshot) tuples, sorted in ascending order by timestamp.
/// `policies`: A slice of `RetentionRule` to apply.
/// `now`: The current time to use for `KeepWithin` policy (useful for testing).
pub fn apply_retention_rules(
    snapshots_sorted: &[(ID, Snapshot)],
    rules: &[RetentionRule],
    now: DateTime<Local>,
) -> HashSet<ID> {
    let mut snapshots_to_keep: HashSet<ID> = HashSet::new();

    // The policies should be applied in a way that later policies don't override earlier ones
    // if a snapshot was already marked for keeping.
    // For simplicity, we'll collect all IDs to keep and then take the union.

    for rule in rules {
        match rule {
            RetentionRule::KeepLast(n) => {
                let num_to_keep = *n;
                for i in (0..snapshots_sorted.len()).rev().take(num_to_keep) {
                    snapshots_to_keep.insert(snapshots_sorted[i].0.clone());
                }
            }
            RetentionRule::KeepWithin(duration) => {
                let cutoff_time = now - *duration;
                for (id, snapshot) in snapshots_sorted.iter().rev() {
                    // Iterate in reverse for efficiency for "keep within"
                    if snapshot.timestamp >= cutoff_time {
                        snapshots_to_keep.insert(id.clone());
                    } else {
                        // Snapshots are sorted, so we can stop once we hit an older one
                        break;
                    }
                }
            }
            RetentionRule::KeepYearly(n) => {
                let mut kept_years: BTreeMap<i32, ID> = BTreeMap::new(); // Year -> latest snapshot ID for that year
                for (id, snapshot) in snapshots_sorted.iter().rev() {
                    let year = snapshot.timestamp.year();
                    kept_years.entry(year).or_insert(id.clone());
                }
                for (i, (_, id)) in kept_years.iter().rev().enumerate() {
                    // Iterate years in reverse
                    if i >= *n {
                        break;
                    }

                    snapshots_to_keep.insert(id.clone());
                }
            }
            RetentionRule::KeepMonthly(n) => {
                let mut kept_months: BTreeMap<(i32, u32), ID> = BTreeMap::new(); // (Year, Month) -> latest snapshot ID for that month
                for (id, snapshot) in snapshots_sorted.iter().rev() {
                    let year = snapshot.timestamp.year();
                    let month = snapshot.timestamp.month();
                    kept_months.entry((year, month)).or_insert(id.clone());
                }
                for (i, (_, id)) in kept_months.iter().rev().enumerate() {
                    // Iterate months in reverse
                    if i >= *n {
                        break;
                    }

                    snapshots_to_keep.insert(id.clone());
                }
            }
            RetentionRule::KeepWeekly(n) => {
                let mut kept_weeks: BTreeMap<(i32, u32), ID> = BTreeMap::new(); // (Year, ISO Week Number) -> latest snapshot ID
                for (id, snapshot) in snapshots_sorted.iter().rev() {
                    let iso_week = snapshot.timestamp.iso_week();
                    let year = iso_week.year();
                    let week = iso_week.week();
                    kept_weeks.entry((year, week)).or_insert(id.clone());
                }
                for (i, (_, id)) in kept_weeks.iter().rev().enumerate() {
                    if i >= *n {
                        break;
                    }

                    snapshots_to_keep.insert(id.clone());
                }
            }
            RetentionRule::KeepDaily(n) => {
                let mut kept_days: BTreeMap<(i32, u32, u32), ID> = BTreeMap::new(); // (Year, Month, Day) -> latest snapshot ID for that day
                for (id, snapshot) in snapshots_sorted.iter().rev() {
                    let year = snapshot.timestamp.year();
                    let month = snapshot.timestamp.month();
                    let day = snapshot.timestamp.day();
                    kept_days.entry((year, month, day)).or_insert(id.clone());
                }
                for (i, (_, id)) in kept_days.iter().rev().enumerate() {
                    if i >= *n {
                        break;
                    }

                    snapshots_to_keep.insert(id.clone());
                }
            }
            RetentionRule::KeepTags(tags) => {
                for (id, snapshot) in snapshots_sorted.iter() {
                    if snapshot.has_tags(tags) {
                        snapshots_to_keep.insert(id.clone());
                    }
                }
            }
        }
    }

    snapshots_to_keep
}
