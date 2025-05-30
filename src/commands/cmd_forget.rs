// [backup] is an incremental backup tool
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

use anyhow::Result;
use chrono::{DateTime, Datelike, Duration, Local};
use clap::{ArgGroup, Parser};

use crate::backend::{make_dry_backend, new_backend_with_prompt};
use crate::global::{FileType, ID};
use crate::repository::snapshot::Snapshot;
use crate::{repository, ui, utils};

use super::GlobalArgs;

// Define argument groups for mutual exclusivity and multiple selection
#[derive(Parser, Debug)]
#[clap(group = ArgGroup::new("policy").multiple(false))] // Either forget OR retention_rules, but not both
#[clap(group = ArgGroup::new("retention_rules").multiple(true))] // Allow multiple --keep-* rules
pub struct CmdArgs {
    /// Forget specific snapshots by their IDs.
    #[arg(long, value_delimiter = ' ', group = "policy")]
    pub forget: Vec<String>,

    /// Keep the last N snapshots.
    #[arg(long, help = "Keep the last N snapshots.", group = "retention_rules")]
    pub keep_last: Option<usize>,

    /// Keep snapshots within a specified duration (e.g., '1d', '2w', '3m', '4y', '5h', '6s').
    #[arg(long, value_parser = utils::parse_duration_string, group = "retention_rules")]
    pub keep_within: Option<Duration>,

    /// Keep N yearly snapshots.
    #[arg(long, help = "Keep N yearly snapshots.", group = "retention_rules")]
    pub keep_yearly: Option<usize>,

    /// Keep N monthly snapshots.
    #[arg(long, help = "Keep N monthly snapshots.", group = "retention_rules")]
    pub keep_monthly: Option<usize>,

    /// Keep N weekly snapshots.
    #[arg(long, help = "Keep N weekly snapshots.", group = "retention_rules")]
    pub keep_weekly: Option<usize>,

    /// Keep N daily snapshots.
    #[arg(long, help = "Keep N daily snapshots.", group = "retention_rules")]
    pub keep_daily: Option<usize>,

    /// Perform a dry run: show which snapshots would be removed without actually removing them.
    #[arg(long)]
    pub dry_run: bool,
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

pub fn run(global: &GlobalArgs, args: &CmdArgs) -> Result<()> {
    let backend = new_backend_with_prompt(&global.repo)?;
    let repo_password = ui::cli::request_repo_password();

    // If dry-run, wrap the backend inside the DryBackend
    let backend = make_dry_backend(backend, args.dry_run);

    let repo = repository::try_open(repo_password, global.key.as_ref(), backend)?;
    let snapshots_sorted = repo.load_all_snapshots_sorted()?;

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

    // Forget snapshots
    let mut removed_count = 0;
    for (id, _) in snapshots_sorted {
        if !ids_to_keep.contains(&id) {
            repo.remove_snapshot(&id)?;
            removed_count += 1;
        }
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
                println!("{:#?}", kept_years);
                let mut count = 0;
                for (_, id) in kept_years.iter().rev() {
                    // Iterate years in reverse
                    if count < *n {
                        snapshots_to_keep.insert(id.clone());
                        count += 1;
                    } else {
                        break;
                    }
                }
            }
            RetentionRule::KeepMonthly(n) => {
                let mut kept_months: BTreeMap<(i32, u32), ID> = BTreeMap::new(); // (Year, Month) -> latest snapshot ID for that month
                for (id, snapshot) in snapshots_sorted.iter().rev() {
                    let year = snapshot.timestamp.year();
                    let month = snapshot.timestamp.month();
                    kept_months.entry((year, month)).or_insert(id.clone());
                }
                let mut count = 0;
                for (_, id) in kept_months.iter().rev() {
                    // Iterate months in reverse
                    if count < *n {
                        snapshots_to_keep.insert(id.clone());
                        count += 1;
                    } else {
                        break;
                    }
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
                let mut count = 0;
                for (_, id) in kept_weeks.iter().rev() {
                    if count < *n {
                        snapshots_to_keep.insert(id.clone());
                        count += 1;
                    } else {
                        break;
                    }
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
                let mut count = 0;
                for (_, id) in kept_days.iter().rev() {
                    if count < *n {
                        snapshots_to_keep.insert(id.clone());
                        count += 1;
                    } else {
                        break;
                    }
                }
            }
        }
    }

    snapshots_to_keep
}

#[cfg(test)]
mod test {
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
        let mut snapshots = Vec::new();

        // Daily snapshots for a few days
        snapshots.push((
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
        ));
        snapshots.push((
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
        ));
        snapshots.push((
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
        ));
        snapshots.push((
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
        ));
        snapshots.push((
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
        ));

        // Weekly snapshots (e.g., one per week, starting from week 1, 2023)
        snapshots.push((
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
        ));
        snapshots.push((
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
        ));
        snapshots.push((
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
        ));

        // Monthly snapshots
        snapshots.push((
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
        ));
        snapshots.push((
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
        ));

        // Yearly snapshots
        snapshots.push((
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
        ));
        snapshots.push((
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
        ));
        snapshots.push((
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
        ));

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
            ID::from_hex("0000000000000000000000000000000000000000000000000000000000000005")
                .unwrap(),
            ID::from_hex("0000000000000000000000000000000000000000000000000000000000000006")
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
