use std::{
    collections::{BTreeSet, HashMap, HashSet},
    hash::Hash,
};

use chrono::{DateTime, Datelike, Duration, Local, NaiveDate, TimeZone};

use crate::{mapache::ID, repository::snapshot::Snapshot};

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

/// Applies retention policies to a sorted list of snapshots and returns the IDs of snapshots to keep.
///
/// `snapshots_sorted`: A vector of (ID, Snapshot) tuples, sorted in ascending order by timestamp.
/// `policies`: A slice of `RetentionRule` to apply.
/// `now`: The current time to use for `KeepWithin` policy.
pub fn apply_retention_rules(
    snapshots_sorted: &[(ID, Snapshot, bool)],
    rules: &[RetentionRule],
    now: DateTime<Local>,
) -> HashSet<ID> {
    let mut snapshots_to_keep: HashSet<ID> = HashSet::new();

    // The date part of 'now' for reference
    let now_date = now.date_naive();
    let timezone = now.timezone();

    // Policies are applied sequentially, and the results are unioned.
    for rule in rules {
        let ids_to_keep = match rule {
            RetentionRule::KeepLast(n) => {
                // Keep the N most recent snapshots, leveraging the reverse iterator on the sorted list.
                snapshots_sorted
                    .iter()
                    .rev()
                    .take(*n)
                    .map(|(id, _, _active)| *id)
                    .collect()
            }
            RetentionRule::KeepWithin(duration) => {
                // Keep all snapshots newer than the cutoff time (using the exact `now`).
                let cutoff_time = now - *duration;
                snapshots_sorted
                    .iter()
                    .filter(|(_id, snapshot, _active)| snapshot.timestamp >= cutoff_time)
                    .map(|(id, _, _active)| *id)
                    .collect()
            }
            RetentionRule::KeepYearly(n) => {
                // Cutoff is Jan 1st of the year (N-1) years ago.
                let target_year = now.year() - ((*n - 1) as i32);
                let naive_date = NaiveDate::from_ymd_opt(target_year, 1, 1).unwrap();
                let naive_datetime = naive_date.and_hms_opt(0, 0, 0).unwrap();

                let cutoff = timezone
                    .from_local_datetime(&naive_datetime)
                    .earliest()
                    .unwrap_or_else(|| timezone.from_local_datetime(&naive_datetime).unwrap());

                keep_latest_per_period(snapshots_sorted, |s| s.timestamp.year(), cutoff)
            }
            RetentionRule::KeepMonthly(n) => {
                // Cutoff is the 1st of the month N months ago.
                let mut target_date: NaiveDate = now_date.with_day(1).unwrap();

                // Manual month decrement logic remains, as this requires calendar arithmetic
                for _ in 1..*n {
                    let month = target_date.month();
                    let year = target_date.year();
                    if month == 1 {
                        target_date = target_date
                            .with_year(year - 1)
                            .unwrap()
                            .with_month(12)
                            .unwrap();
                    } else {
                        target_date = target_date.with_month(month - 1).unwrap();
                    }
                }

                let naive_datetime = target_date.and_hms_opt(0, 0, 0).unwrap();

                let cutoff = timezone
                    .from_local_datetime(&naive_datetime)
                    .earliest()
                    .unwrap_or_else(|| timezone.from_local_datetime(&naive_datetime).unwrap());

                keep_latest_per_period(
                    snapshots_sorted,
                    |s| (s.timestamp.year(), s.timestamp.month()),
                    cutoff,
                )
            }
            RetentionRule::KeepWeekly(n) => {
                // Cutoff is the Monday 00:00 of the week N weeks ago (ISO 8601 start of week).
                let current_monday_date =
                    now_date - Duration::days(now_date.weekday().num_days_from_monday() as i64);

                let target_monday_date = current_monday_date - Duration::weeks((*n - 1) as i64);

                let naive_datetime = target_monday_date.and_hms_opt(0, 0, 0).unwrap();

                let cutoff = timezone
                    .from_local_datetime(&naive_datetime)
                    .earliest()
                    .unwrap_or_else(|| timezone.from_local_datetime(&naive_datetime).unwrap());

                keep_latest_per_period(
                    snapshots_sorted,
                    |s| (s.timestamp.year(), s.timestamp.iso_week().week()),
                    cutoff,
                )
            }
            RetentionRule::KeepDaily(n) => {
                // Calculate midnight of the current day for the anchor.
                let now_midnight = now_date
                    .and_hms_opt(0, 0, 0)
                    .unwrap()
                    .and_local_timezone(timezone)
                    .unwrap();

                // Cutoff is midnight of the day N days ago.
                let cutoff = now_midnight - Duration::days((*n - 1) as i64);

                keep_latest_per_period(snapshots_sorted, |s| s.timestamp.date_naive(), cutoff)
            }
            RetentionRule::KeepTags(tags) => {
                // Keep all snapshots that match the required tags.
                snapshots_sorted
                    .iter()
                    .filter(|(_id, snapshot, _active)| snapshot.has_tags(tags))
                    .map(|(id, _, _active)| *id)
                    .collect()
            }
        };

        // Combine the results: if any rule dictates a snapshot must be kept, it is kept.
        snapshots_to_keep.extend(ids_to_keep);
    }

    snapshots_to_keep
}

/// Generic helper function to abstract the common logic for period-based retention.
///
/// It finds the latest snapshot for each unique period (defined by `key_extractor`)
/// and then keeps the latest `n` of those periods.
fn keep_latest_per_period<K, F>(
    snapshots_sorted: &[(ID, Snapshot, bool)],
    key_extractor: F,
    cut_off: DateTime<Local>,
) -> HashSet<ID>
where
    K: Eq + Hash,
    F: Fn(&Snapshot) -> K,
{
    let mut kept_periods: HashMap<K, ID> = HashMap::new();

    for (id, snapshot, _active) in snapshots_sorted.iter().rev() {
        let key = key_extractor(snapshot);

        if snapshot.timestamp >= cut_off {
            kept_periods.entry(key).or_insert(*id);
        }
    }

    kept_periods.values().copied().collect::<HashSet<ID>>()
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, path::PathBuf};

    use chrono::{DateTime, Duration, Local, NaiveDate, NaiveDateTime, TimeZone};

    use super::*;

    /// Creates a mock snapshot ID from a u32 value in the hex string.
    fn create_id(val: u32) -> ID {
        // With this, we can create enumerated snapshot IDs without specifying all the ID bits.
        // Creates IDs like "0...00", "0...01", "0...0A", "0...0B", etc.
        let hex_str = format!("{:0>64X}", val);
        ID::from_hex(&hex_str).unwrap()
    }

    /// Creates a Local DateTime from a ymd_hms tuple, adding an optional duration.
    fn create_datetime(
        ymd: (i32, u32, u32),
        hms: (u32, u32, u32),
        days_offset: i64,
    ) -> DateTime<Local> {
        let naive_dt: NaiveDateTime = NaiveDate::from_ymd_opt(ymd.0, ymd.1, ymd.2)
            .unwrap()
            .and_hms_opt(hms.0, hms.1, hms.2)
            .unwrap();

        Local.from_local_datetime(&naive_dt).unwrap() + Duration::days(days_offset)
    }

    /// Creates a standard mock snapshot (ID and Snapshot struct).
    fn create_snapshot(
        id_val: u32,
        timestamp: DateTime<Local>,
        tags: &[&str],
    ) -> (ID, Snapshot, bool) {
        let snapshot_id = create_id(id_val);
        (
            snapshot_id,
            Snapshot {
                timestamp,
                parent: None,
                tree: snapshot_id,
                root: PathBuf::from("/"),
                paths: Vec::new(),
                tags: tags.iter().map(|s| s.to_string()).collect(),
                description: None,
                summary: Default::default(),
                hostname: None,
                username: None,
            },
            true,
        )
    }

    fn test_now() -> DateTime<Local> {
        // Base reference time for cutoffs: 2025-05-25 21:58:00 (Sunday)
        create_datetime((2025, 5, 25), (21, 58, 0), 0)
    }

    fn create_mock_snapshots() -> Vec<(ID, Snapshot, bool)> {
        let mut snapshots = vec![
            create_snapshot(0, create_datetime((2021, 12, 31), (23, 59, 59), 0), &[]),
            create_snapshot(1, create_datetime((2022, 1, 1), (0, 0, 0), 0), &[]),
            create_snapshot(2, create_datetime((2023, 1, 1), (0, 0, 0), 2), &[]), // 2023-01-03
            create_snapshot(3, create_datetime((2023, 1, 1), (0, 0, 0), 3), &[]), // 2023-01-04
            create_snapshot(4, create_datetime((2023, 1, 1), (0, 0, 0), 4), &[]), // 2023-01-05
            create_snapshot(5, create_datetime((2023, 1, 1), (0, 0, 0), 7), &[]), // 2023-01-08
            create_snapshot(6, create_datetime((2023, 1, 1), (0, 0, 0), 14), &[]), // 2023-01-15
            create_snapshot(7, create_datetime((2023, 1, 1), (0, 0, 0), 15), &[]), // 2023-01-16
            create_snapshot(8, create_datetime((2023, 1, 1), (0, 0, 0), 16), &[]), // 2023-01-17
            create_snapshot(9, create_datetime((2023, 1, 1), (0, 0, 0), 21), &[]), // 2023-01-22
            create_snapshot(
                10,
                create_datetime((2023, 1, 1), (0, 0, 0), 23), // 2023-01-24
                &["tag0", "tag1"],
            ),
            create_snapshot(11, create_datetime((2023, 1, 28), (23, 59, 59), 0), &[]), // 2023-01-28
            create_snapshot(12, create_datetime((2023, 2, 28), (23, 59, 0), 0), &[]),  // 2023-02-28
            create_snapshot(13, create_datetime((2023, 12, 31), (23, 59, 0), 0), &[]), // 2023-12-31
            create_snapshot(14, create_datetime((2024, 12, 31), (23, 59, 0), 0), &[]), // 2024-12-31
            create_snapshot(15, create_datetime((2024, 12, 31), (23, 59, 59), 0), &[]), // 2024-12-31
            create_snapshot(16, create_datetime((2025, 1, 1), (0, 0, 1), 0), &[]),
            create_snapshot(17, create_datetime((2025, 4, 13), (23, 59, 59), 0), &[]), // 2025-04-13
            create_snapshot(18, create_datetime((2025, 4, 14), (0, 0, 1), 0), &[]),    // 2025-04-14
            create_snapshot(19, create_datetime((2025, 5, 1), (12, 0, 0), 0), &[]),    // 2025-05-01
            create_snapshot(20, create_datetime((2025, 5, 25), (20, 29, 46), 0), &[]), // 2025-05-25
            create_snapshot(21, create_datetime((2025, 5, 25), (20, 30, 0), 0), &[]),  // 2025-05-25
            create_snapshot(22, create_datetime((2025, 5, 25), (21, 57, 59), 0), &[]), // 2025-05-25 (Just before 'now')
        ];

        assert_no_duplicate_ids(&snapshots);

        snapshots.sort_by_key(|(_, snapshot, _)| snapshot.timestamp);

        snapshots
    }

    /// Assert that the snapshots don't have duplicate IDs
    fn assert_no_duplicate_ids(snapshots: &[(ID, Snapshot, bool)]) {
        let count = snapshots.len();
        let unique_ids: HashSet<&ID> = snapshots.iter().map(|(id, _, _active)| id).collect();

        assert_eq!(
            unique_ids.len(),
            count,
            "Duplicate ID found in mock snapshots. Total snapshots: {}, unique IDs: {}",
            count,
            unique_ids.len()
        );
    }

    // Helper function to create the expected ID HashSet
    fn create_expected_ids(id_vals: &[u32]) -> HashSet<ID> {
        id_vals.iter().map(|&v| create_id(v)).collect()
    }

    #[test]
    fn test_keep_last() {
        let snapshots = create_mock_snapshots();
        let rules = vec![RetentionRule::KeepLast(4)];
        let kept_ids = apply_retention_rules(&snapshots, &rules, test_now());

        // Keeps the last four
        let expected_keep_ids = create_expected_ids(&[22, 21, 20, 19]);

        assert_eq!(kept_ids, expected_keep_ids);
    }

    #[test]
    fn test_keep_yearly() {
        let snapshots = create_mock_snapshots();

        let rules = vec![RetentionRule::KeepYearly(5)];
        let kept_ids = apply_retention_rules(&snapshots, &rules, test_now());

        let expected_ids = create_expected_ids(&[
            0,  // Latest in 2021
            1,  // Latest in 2022
            13, // Latest in 2023
            15, // Latest in 2024
            22, // Latest in 2025
        ]);

        assert_eq!(kept_ids, expected_ids);
    }

    #[test]
    fn test_keep_monthly() {
        let snapshots = create_mock_snapshots();
        let rules = vec![RetentionRule::KeepMonthly(5)];
        let kept_ids = apply_retention_rules(&snapshots, &rules, test_now());

        let expected_ids = create_expected_ids(&[
            16, // Latest in Jan 2025
            18, // Latest in Apr 2025
            22, // Latest in May 2025
        ]);

        assert_eq!(kept_ids, expected_ids);
    }

    #[test]
    fn test_keep_weekly() {
        let snapshots = create_mock_snapshots();
        let rules = vec![RetentionRule::KeepWeekly(8)];
        let kept_ids = apply_retention_rules(&snapshots, &rules, test_now());

        let expected_ids = create_expected_ids(&[
            17, // Latest in Week 15 (Apr 13)
            18, // Latest in Week 16 (Apr 14)
            19, // Latest in Week 18 (May 1)
            22, // Latest in Week 21 (May 25)
        ]);

        assert_eq!(kept_ids, expected_ids);
    }

    #[test]
    fn test_keep_daily() {
        let snapshots = create_mock_snapshots();
        let rules = vec![RetentionRule::KeepDaily(10)];
        let kept_ids = apply_retention_rules(&snapshots, &rules, test_now());

        let expected_ids = create_expected_ids(&[22]);

        assert_eq!(kept_ids, expected_ids);
    }

    #[test]
    fn test_keep_within() {
        let snapshots = create_mock_snapshots();
        // 2 years duration
        let rules = vec![RetentionRule::KeepWithin(Duration::days(2 * 365))];
        let kept_ids = apply_retention_rules(&snapshots, &rules, test_now());

        // Cutoff: 2023-05-25 21:58:00
        let expected_ids = create_expected_ids(&[13, 14, 15, 16, 17, 18, 19, 20, 21, 22]);

        assert_eq!(kept_ids, expected_ids);
    }

    #[test]
    fn test_keep_tags() {
        let snapshots = create_mock_snapshots();
        let rules = vec![RetentionRule::KeepTags(
            ["tag0"].into_iter().map(|s| s.to_string()).collect(),
        )];
        let kept_ids = apply_retention_rules(&snapshots, &rules, test_now());

        let expected_ids = create_expected_ids(&[10]);

        assert_eq!(kept_ids, expected_ids);
    }

    #[test]
    fn test_keep_multiple_rules() {
        let snapshots = create_mock_snapshots();
        let rules = vec![
            RetentionRule::KeepLast(3),    // Keeps {22, 21, 20}
            RetentionRule::KeepMonthly(5), // Keeps {16, 18, 22}
            RetentionRule::KeepTags(["tag1"].into_iter().map(|s| s.to_string()).collect()), // Keeps {10}
        ];
        let kept_ids = apply_retention_rules(&snapshots, &rules, test_now());

        // Union: {10, 16, 18, 20, 21, 22}
        let expected_ids = create_expected_ids(&[
            10, // Kept by KeepTags("tag1")
            16, // Kept by KeepMonthly(5)
            18, // Kept by KeepMonthly(5)
            20, // Kept by KeepLast(3)
            21, // Kept by KeepLast(3)
            22, // Kept by multiple rules
        ]);

        assert_eq!(kept_ids, expected_ids);
    }
}
