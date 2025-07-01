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

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use anyhow::{Result, bail};
use chrono::{DateTime, Datelike, Duration, Local};
use clap::{ArgGroup, Parser};
use colored::Colorize;

use crate::backend::{make_dry_backend, new_backend_with_prompt};
use crate::global::defaults::DEFAULT_GC_TOLERANCE;
use crate::global::{self, FileType, ID};
use crate::repository::RepositoryBackend;
use crate::repository::snapshot::{Snapshot, SnapshotStreamer};
use crate::ui::table::{Alignment, Table};
use crate::{commands, repository, ui, utils};

use super::GlobalArgs;

// Define argument groups for mutual exclusivity and multiple selection
#[derive(Parser, Debug)]
#[clap(group = ArgGroup::new("policy").multiple(false))] // Either forget OR retention_rules, but not both
#[clap(group = ArgGroup::new("retention_rules").multiple(true))] // Allow multiple --keep-* rules
pub struct CmdArgs {
    /// Forget specific snapshots by their IDs.
    #[arg(value_parser, value_delimiter = ' ', group = "policy")]
    pub forget: Vec<String>,

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

    /// Perform a dry run: show which snapshots would be removed without actually removing them.
    #[arg(long)]
    pub dry_run: bool,

    // -- Garbage collector --
    /// Run the garbage collector after this command
    #[arg(long = "gc")]
    pub run_gc: bool,

    /// Garbage tolerance. The percentage [0-100] of garbage to tolerate in a
    /// pack file before repacking.
    #[clap(short, long, default_value_t = DEFAULT_GC_TOLERANCE)]
    pub tolerance: f32,
}

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
    let backend = new_backend_with_prompt(global_args)?;

    // If dry-run, wrap the backend inside the DryBackend
    let backend = make_dry_backend(backend, args.dry_run);

    let repo: Arc<dyn RepositoryBackend> =
        repository::try_open(pass, global_args.key.as_ref(), backend)?;

    let mut snapshots_sorted: Vec<(ID, Snapshot)> = SnapshotStreamer::new(repo.clone())?.collect();
    snapshots_sorted.sort_by_key(|(_id, snapshot)| snapshot.timestamp);

    let mut ids_to_keep: HashSet<ID> = HashSet::new();

    if !&args.forget.is_empty() {
        let mut forget_ids = HashSet::new();
        for prefix in &args.forget {
            let (id, _) = repo.find(FileType::Snapshot, prefix)?;
            forget_ids.insert(id);
        }
        for (id, _) in &snapshots_sorted {
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

        ids_to_keep = apply_retention_rules(&snapshots_sorted, &retention_rules, Local::now());
    }

    let mut removed_ids_table =
        Table::new_with_alignments(vec![Alignment::Left, Alignment::Center, Alignment::Right]);
    removed_ids_table.set_headers(vec![
        "ID".bold().to_string(),
        "Date".bold().to_string(),
        "Size".bold().to_string(),
    ]);

    let mut kept_ids_table =
        Table::new_with_alignments(vec![Alignment::Left, Alignment::Center, Alignment::Right]);
    kept_ids_table.set_headers(vec![
        "ID".bold().to_string(),
        "Date".bold().to_string(),
        "Size".bold().to_string(),
    ]);

    // Forget snapshots
    let mut removed_count = 0;
    for (id, snapshot) in snapshots_sorted {
        let table = if !ids_to_keep.contains(&id) {
            repo.remove_snapshot(&id)?;
            removed_count += 1;
            &mut removed_ids_table
        } else {
            &mut kept_ids_table
        };

        table.add_row(vec![
            id.to_short_hex(global::defaults::SHORT_SNAPSHOT_ID_LEN)
                .bold()
                .yellow()
                .to_string(),
            snapshot
                .timestamp
                .with_timezone(&Local)
                .format("%Y-%m-%d %H:%M:%S %Z")
                .to_string(),
            utils::format_size(snapshot.size()),
        ]);
    }

    ui::cli::log!();
    ui::cli::log!(
        "{}\n{}",
        "Snapshots to keep:".bold(),
        kept_ids_table.render()
    );

    if removed_count > 0 {
        ui::cli::log!(
            "{}\n{}",
            "Snapshots to remove:".bold(),
            removed_ids_table.render()
        );
    }

    if !args.dry_run {
        ui::cli::log!(
            "Removed {}",
            utils::format_count(removed_count, "snapshot", "snapshots")
        );
    } else {
        ui::cli::log!(
            "This would remove {}",
            utils::format_count(removed_count, "snapshot", "snapshots")
        );
    }

    // Run the garbage collector
    if args.run_gc {
        let gc_args = commands::cmd_gc::CmdArgs {
            tolerance: args.tolerance,
            dry_run: args.dry_run,
        };

        ui::cli::log!();
        ui::cli::log!("Running garbage collector...");
        commands::cmd_gc::run_with_repo(global_args, &gc_args, repo)?;
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
        }
    }

    snapshots_to_keep
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use chrono::{NaiveDate, TimeZone};

    use super::*;

    fn test_now() -> DateTime<Local> {
        Local.from_utc_datetime(
            &NaiveDate::from_ymd_opt(2025, 5, 25)
                .unwrap()
                .and_hms_opt(21, 58, 0)
                .unwrap(),
        )
    }

    fn create_mock_snapshots() -> Vec<(ID, Snapshot)> {
        let snapshots = vec![
            // Daily snapshots for a few days
            (
                ID::from_hex("0000000000000000000000000000000000000000000000000000000000000000")
                    .unwrap(),
                Snapshot {
                    timestamp: Local
                        .from_local_datetime(
                            &NaiveDate::from_ymd_opt(2023, 1, 1)
                                .unwrap()
                                .and_hms_opt(0, 0, 0)
                                .unwrap(),
                        )
                        .unwrap()
                        + Duration::days(21),
                    tree: ID::from_hex(
                        "0000000000000000000000000000000000000000000000000000000000000000",
                    )
                    .unwrap(),
                    root: PathBuf::from("/"),
                    paths: vec![],
                    description: None,
                    summary: Default::default(),
                },
            ),
            (
                ID::from_hex("0000000000000000000000000000000000000000000000000000000000000001")
                    .unwrap(),
                Snapshot {
                    timestamp: Local
                        .from_local_datetime(
                            &NaiveDate::from_ymd_opt(2023, 1, 1)
                                .unwrap()
                                .and_hms_opt(0, 0, 0)
                                .unwrap(),
                        )
                        .unwrap()
                        + Duration::days(1),
                    tree: ID::from_hex(
                        "0000000000000000000000000000000000000000000000000000000000000001",
                    )
                    .unwrap(),
                    root: PathBuf::from("/"),
                    paths: vec![],
                    description: None,
                    summary: Default::default(),
                },
            ),
            (
                ID::from_hex("0000000000000000000000000000000000000000000000000000000000000002")
                    .unwrap(),
                Snapshot {
                    timestamp: Local
                        .from_local_datetime(
                            &NaiveDate::from_ymd_opt(2023, 1, 1)
                                .unwrap()
                                .and_hms_opt(0, 0, 0)
                                .unwrap(),
                        )
                        .unwrap()
                        + Duration::days(2),
                    tree: ID::from_hex(
                        "0000000000000000000000000000000000000000000000000000000000000002",
                    )
                    .unwrap(),
                    root: PathBuf::from("/"),
                    paths: vec![],
                    description: None,
                    summary: Default::default(),
                },
            ),
            (
                ID::from_hex("0000000000000000000000000000000000000000000000000000000000000003")
                    .unwrap(),
                Snapshot {
                    timestamp: Local
                        .from_local_datetime(
                            &NaiveDate::from_ymd_opt(2023, 1, 1)
                                .unwrap()
                                .and_hms_opt(0, 0, 0)
                                .unwrap(),
                        )
                        .unwrap()
                        + Duration::days(3),
                    tree: ID::from_hex(
                        "0000000000000000000000000000000000000000000000000000000000000003",
                    )
                    .unwrap(),
                    root: PathBuf::from("/"),
                    paths: vec![],
                    description: None,
                    summary: Default::default(),
                },
            ),
            (
                ID::from_hex("0000000000000000000000000000000000000000000000000000000000000004")
                    .unwrap(),
                Snapshot {
                    timestamp: Local
                        .from_local_datetime(
                            &NaiveDate::from_ymd_opt(2023, 1, 1)
                                .unwrap()
                                .and_hms_opt(0, 0, 0)
                                .unwrap(),
                        )
                        .unwrap()
                        + Duration::days(4),
                    tree: ID::from_hex(
                        "0000000000000000000000000000000000000000000000000000000000000004",
                    )
                    .unwrap(),
                    root: PathBuf::from("/"),
                    paths: vec![],
                    description: None,
                    summary: Default::default(),
                },
            ),
            // Weekly snapshots (e.g., one per week, starting from week 1, 2023)
            (
                ID::from_hex("0000000000000000000000000000000000000000000000000000000000000005")
                    .unwrap(),
                Snapshot {
                    // End of Week 1
                    timestamp: Local
                        .from_local_datetime(
                            &NaiveDate::from_ymd_opt(2023, 1, 1)
                                .unwrap()
                                .and_hms_opt(0, 0, 0)
                                .unwrap(),
                        )
                        .unwrap()
                        + Duration::days(7),
                    tree: ID::from_hex(
                        "0000000000000000000000000000000000000000000000000000000000000005",
                    )
                    .unwrap(),
                    root: PathBuf::from("/"),
                    paths: vec![],
                    description: None,
                    summary: Default::default(),
                },
            ),
            (
                ID::from_hex("0000000000000000000000000000000000000000000000000000000000000006")
                    .unwrap(),
                Snapshot {
                    // End of Week 2
                    timestamp: Local
                        .from_local_datetime(
                            &NaiveDate::from_ymd_opt(2023, 1, 1)
                                .unwrap()
                                .and_hms_opt(0, 0, 0)
                                .unwrap(),
                        )
                        .unwrap()
                        + Duration::days(14),
                    tree: ID::from_hex(
                        "0000000000000000000000000000000000000000000000000000000000000006",
                    )
                    .unwrap(),
                    root: PathBuf::from("/"),
                    paths: vec![],
                    description: None,
                    summary: Default::default(),
                },
            ),
            (
                ID::from_hex("0000000000000000000000000000000000000000000000000000000000000106")
                    .unwrap(),
                Snapshot {
                    // Week 3
                    timestamp: Local
                        .from_local_datetime(
                            &NaiveDate::from_ymd_opt(2023, 1, 1)
                                .unwrap()
                                .and_hms_opt(0, 0, 0)
                                .unwrap(),
                        )
                        .unwrap()
                        + Duration::days(15),
                    tree: ID::from_hex(
                        "0000000000000000000000000000000000000000000000000000000000000106",
                    )
                    .unwrap(),
                    root: PathBuf::from("/"),
                    paths: vec![],
                    description: None,
                    summary: Default::default(),
                },
            ),
            (
                ID::from_hex("0000000000000000000000000000000000000000000000000000000000000206")
                    .unwrap(),
                Snapshot {
                    // Week 3
                    timestamp: Local
                        .from_local_datetime(
                            &NaiveDate::from_ymd_opt(2023, 1, 1)
                                .unwrap()
                                .and_hms_opt(0, 0, 0)
                                .unwrap(),
                        )
                        .unwrap()
                        + Duration::days(16),
                    tree: ID::from_hex(
                        "0000000000000000000000000000000000000000000000000000000000000206",
                    )
                    .unwrap(),
                    root: PathBuf::from("/"),
                    paths: vec![],
                    description: None,
                    summary: Default::default(),
                },
            ),
            (
                ID::from_hex("0000000000000000000000000000000000000000000000000000000000000007")
                    .unwrap(),
                Snapshot {
                    // End of Week 3
                    timestamp: Local
                        .from_local_datetime(
                            &NaiveDate::from_ymd_opt(2023, 1, 1)
                                .unwrap()
                                .and_hms_opt(0, 0, 0)
                                .unwrap(),
                        )
                        .unwrap()
                        + Duration::days(21),
                    tree: ID::from_hex(
                        "0000000000000000000000000000000000000000000000000000000000000007",
                    )
                    .unwrap(),
                    root: PathBuf::from("/"),
                    paths: vec![],
                    description: None,
                    summary: Default::default(),
                },
            ),
            // Monthly snapshots
            (
                ID::from_hex("0000000000000000000000000000000000000000000000000000000000000008")
                    .unwrap(),
                Snapshot {
                    // End of Jan
                    timestamp: Local
                        .from_local_datetime(
                            &NaiveDate::from_ymd_opt(2023, 1, 28)
                                .unwrap()
                                .and_hms_opt(23, 59, 59)
                                .unwrap(),
                        )
                        .unwrap(),
                    tree: ID::from_hex(
                        "0000000000000000000000000000000000000000000000000000000000000008",
                    )
                    .unwrap(),
                    root: PathBuf::from("/"),
                    paths: vec![],
                    description: None,
                    summary: Default::default(),
                },
            ),
            (
                ID::from_hex("0000000000000000000000000000000000000000000000000000000000000009")
                    .unwrap(),
                Snapshot {
                    // End of Feb
                    timestamp: Local
                        .from_local_datetime(
                            &NaiveDate::from_ymd_opt(2023, 2, 28)
                                .unwrap()
                                .and_hms_opt(23, 59, 0)
                                .unwrap(),
                        )
                        .unwrap(),
                    tree: ID::from_hex(
                        "0000000000000000000000000000000000000000000000000000000000000009",
                    )
                    .unwrap(),
                    root: PathBuf::from("/"),
                    paths: vec![],
                    description: None,
                    summary: Default::default(),
                },
            ),
            // Yearly snapshots
            (
                ID::from_hex("000000000000000000000000000000000000000000000000000000000000000A")
                    .unwrap(),
                Snapshot {
                    // End of 2023
                    timestamp: Local
                        .from_local_datetime(
                            &NaiveDate::from_ymd_opt(2023, 12, 31)
                                .unwrap()
                                .and_hms_opt(23, 59, 0)
                                .unwrap(),
                        )
                        .unwrap(),
                    tree: ID::from_hex(
                        "000000000000000000000000000000000000000000000000000000000000000A",
                    )
                    .unwrap(),
                    root: PathBuf::from("/"),
                    paths: vec![],
                    description: None,
                    summary: Default::default(),
                },
            ),
            (
                ID::from_hex("000000000000000000000000000000000000000000000000000000000000000B")
                    .unwrap(),
                Snapshot {
                    // End of 2024
                    timestamp: Local
                        .from_local_datetime(
                            &NaiveDate::from_ymd_opt(2024, 12, 31)
                                .unwrap()
                                .and_hms_opt(23, 59, 0)
                                .unwrap(),
                        )
                        .unwrap(),
                    tree: ID::from_hex(
                        "000000000000000000000000000000000000000000000000000000000000000B",
                    )
                    .unwrap(),
                    root: PathBuf::from("/"),
                    paths: vec![],
                    description: None,
                    summary: Default::default(),
                },
            ),
            (
                ID::from_hex("000000000000000000000000000000000000000000000000000000000000000C")
                    .unwrap(),
                Snapshot {
                    // Current time (for testing KeepWithin)
                    timestamp: Local
                        .from_local_datetime(
                            &NaiveDate::from_ymd_opt(2025, 5, 25)
                                .unwrap()
                                .and_hms_opt(20, 29, 46)
                                .unwrap(),
                        )
                        .unwrap(),
                    tree: ID::from_hex(
                        "000000000000000000000000000000000000000000000000000000000000000C",
                    )
                    .unwrap(),
                    root: PathBuf::from("/"),
                    paths: vec![],
                    description: None,
                    summary: Default::default(),
                },
            ),
        ];

        snapshots
    }

    #[test]
    fn test_keep_last() {
        let snapshots = create_mock_snapshots();

        let rules = vec![RetentionRule::KeepLast(3)];

        let keep_ids = apply_retention_rules(&snapshots, &rules, test_now());

        let mut expected_keep_ids: HashSet<ID> = HashSet::new();
        for i in (0..snapshots.len()).rev().take(3) {
            expected_keep_ids.insert(snapshots[i].0.clone());
        }

        assert_eq!(keep_ids, expected_keep_ids);
    }

    #[test]
    fn test_keep_yearly() {
        let snapshots = create_mock_snapshots();
        let rules = vec![RetentionRule::KeepYearly(3)];

        let kept_ids = apply_retention_rules(&snapshots, &rules, test_now());

        let expected_ids: HashSet<ID> = [
            // 2023
            ID::from_hex("000000000000000000000000000000000000000000000000000000000000000A")
                .unwrap(),
            // 2024
            ID::from_hex("000000000000000000000000000000000000000000000000000000000000000B")
                .unwrap(),
            // 2025
            ID::from_hex("000000000000000000000000000000000000000000000000000000000000000C")
                .unwrap(),
        ]
        .iter()
        .cloned()
        .collect();

        assert_eq!(kept_ids, expected_ids);
    }

    #[test]
    fn test_keep_monthly() {
        let snapshots = create_mock_snapshots();
        let rules = vec![RetentionRule::KeepMonthly(4)];

        let kept_ids = apply_retention_rules(&snapshots, &rules, test_now());

        let expected_ids: HashSet<ID> = [
            // 2023
            ID::from_hex("0000000000000000000000000000000000000000000000000000000000000009")
                .unwrap(),
            ID::from_hex("000000000000000000000000000000000000000000000000000000000000000A")
                .unwrap(),
            // 2024
            ID::from_hex("000000000000000000000000000000000000000000000000000000000000000B")
                .unwrap(),
            // 2025
            ID::from_hex("000000000000000000000000000000000000000000000000000000000000000C")
                .unwrap(),
        ]
        .iter()
        .cloned()
        .collect();

        assert_eq!(kept_ids, expected_ids);
    }

    #[test]
    fn test_keep_weekly() {
        let snapshots = create_mock_snapshots();
        let rules = vec![RetentionRule::KeepWeekly(5)];

        let kept_ids = apply_retention_rules(&snapshots, &rules, test_now());

        let expected_ids: HashSet<ID> = [
            // 2023
            ID::from_hex("0000000000000000000000000000000000000000000000000000000000000008")
                .unwrap(),
            ID::from_hex("0000000000000000000000000000000000000000000000000000000000000009")
                .unwrap(),
            ID::from_hex("000000000000000000000000000000000000000000000000000000000000000A")
                .unwrap(),
            // 2024
            ID::from_hex("000000000000000000000000000000000000000000000000000000000000000B")
                .unwrap(),
            // 2025
            ID::from_hex("000000000000000000000000000000000000000000000000000000000000000C")
                .unwrap(),
        ]
        .iter()
        .cloned()
        .collect();

        assert_eq!(kept_ids, expected_ids);
    }

    #[test]
    fn test_keep_daily() {
        let snapshots = create_mock_snapshots();
        let rules = vec![RetentionRule::KeepDaily(8)];

        let kept_ids = apply_retention_rules(&snapshots, &rules, test_now());

        let expected_ids: HashSet<ID> = [
            // 2023
            ID::from_hex("0000000000000000000000000000000000000000000000000000000000000106")
                .unwrap(),
            ID::from_hex("0000000000000000000000000000000000000000000000000000000000000206")
                .unwrap(),
            ID::from_hex("0000000000000000000000000000000000000000000000000000000000000007")
                .unwrap(),
            ID::from_hex("0000000000000000000000000000000000000000000000000000000000000008")
                .unwrap(),
            ID::from_hex("0000000000000000000000000000000000000000000000000000000000000009")
                .unwrap(),
            ID::from_hex("000000000000000000000000000000000000000000000000000000000000000A")
                .unwrap(),
            // 2024
            ID::from_hex("000000000000000000000000000000000000000000000000000000000000000B")
                .unwrap(),
            // 2025
            ID::from_hex("000000000000000000000000000000000000000000000000000000000000000C")
                .unwrap(),
        ]
        .iter()
        .cloned()
        .collect();

        assert_eq!(kept_ids, expected_ids);
    }

    #[test]
    fn test_keep_within() {
        let snapshots = create_mock_snapshots();
        let rules = vec![RetentionRule::KeepWithin(Duration::days(2 * 365))];

        let kept_ids = apply_retention_rules(&snapshots, &rules, test_now());

        let expected_ids: HashSet<ID> = [
            // 2023
            ID::from_hex("000000000000000000000000000000000000000000000000000000000000000A")
                .unwrap(),
            // 2024
            ID::from_hex("000000000000000000000000000000000000000000000000000000000000000B")
                .unwrap(),
            // 2025
            ID::from_hex("000000000000000000000000000000000000000000000000000000000000000C")
                .unwrap(),
        ]
        .iter()
        .cloned()
        .collect();

        assert_eq!(kept_ids, expected_ids);
    }

    #[test]
    fn test_keep_multiple_rules() {
        let snapshots = create_mock_snapshots();
        let rules = vec![
            RetentionRule::KeepLast(4),
            RetentionRule::KeepWithin(Duration::days(2 * 365)),
            RetentionRule::KeepYearly(3),
        ];

        let kept_ids = apply_retention_rules(&snapshots, &rules, test_now());

        let expected_ids: HashSet<ID> = [
            // 2023
            ID::from_hex("0000000000000000000000000000000000000000000000000000000000000009")
                .unwrap(),
            ID::from_hex("000000000000000000000000000000000000000000000000000000000000000A")
                .unwrap(),
            // 2024
            ID::from_hex("000000000000000000000000000000000000000000000000000000000000000B")
                .unwrap(),
            // 2025
            ID::from_hex("000000000000000000000000000000000000000000000000000000000000000C")
                .unwrap(),
        ]
        .iter()
        .cloned()
        .collect();

        assert_eq!(kept_ids, expected_ids);
    }
}
