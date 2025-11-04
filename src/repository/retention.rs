use std::collections::{BTreeMap, BTreeSet, HashSet};

use chrono::{DateTime, Datelike, Duration, Local};

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
/// `now`: The current time to use for `KeepWithin` policy (useful for testing).
pub fn apply_retention_rules(
    snapshots_sorted: &[(ID, Snapshot, bool)],
    rules: &[RetentionRule],
    now: DateTime<Local>,
) -> HashSet<ID> {
    let mut snapshots_to_keep: HashSet<ID> = HashSet::new();

    // Policies are applied sequentially, and the results are unioned.
    for rule in rules {
        let new_ids_to_keep = match rule {
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
                // Keep all snapshots newer than the cutoff time.
                let cutoff_time = now - *duration;
                snapshots_sorted
                    .iter()
                    .filter(|(_id, snapshot, _active)| snapshot.timestamp >= cutoff_time)
                    .map(|(id, _, _active)| *id)
                    .collect()
            }

            RetentionRule::KeepYearly(n) => keep_latest_per_period(
                snapshots_sorted,
                *n,
                |s| s.timestamp.year(), // Key: Year (i32)
            ),
            RetentionRule::KeepMonthly(n) => keep_latest_per_period(
                snapshots_sorted,
                *n,
                |s| (s.timestamp.year(), s.timestamp.month()), // Key: (Year, Month)
            ),
            RetentionRule::KeepWeekly(n) => keep_latest_per_period(
                snapshots_sorted,
                *n,
                |s| (s.timestamp.iso_week().year(), s.timestamp.iso_week().week()), // Key: (ISO Year, ISO Week Number)
            ),
            RetentionRule::KeepDaily(n) => keep_latest_per_period(
                snapshots_sorted,
                *n,
                |s| (s.timestamp.year(), s.timestamp.month(), s.timestamp.day()), // Key: (Year, Month, Day)
            ),

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
        snapshots_to_keep.extend(new_ids_to_keep);
    }

    snapshots_to_keep
}

/// Generic helper function to abstract the common logic for period-based retention.
///
/// It finds the latest snapshot for each unique period (defined by `key_extractor`)
/// and then keeps the latest `n` of those periods.
fn keep_latest_per_period<K, F>(
    snapshots_sorted: &[(ID, Snapshot, bool)],
    n: usize,
    key_extractor: F,
) -> HashSet<ID>
where
    K: Ord,
    F: Fn(&Snapshot) -> K,
{
    let mut kept_periods: BTreeMap<K, ID> = BTreeMap::new();

    for (id, snapshot, _active) in snapshots_sorted.iter().rev() {
        let key = key_extractor(snapshot);
        kept_periods.entry(key).or_insert(*id);
    }

    let mut ids_to_keep: HashSet<ID> = HashSet::new();

    // Keep the N latest periods.
    for (i, (_, id)) in kept_periods.iter().rev().enumerate() {
        if i >= n {
            break;
        }
        ids_to_keep.insert(*id);
    }

    ids_to_keep
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, path::PathBuf};

    use chrono::{DateTime, Duration, Local, NaiveDate, NaiveDateTime, TimeZone};

    use crate::{mapache::ID, repository::snapshot::Snapshot};

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
        create_datetime((2025, 5, 25), (21, 58, 0), 0)
    }

    fn create_mock_snapshots() -> Vec<(ID, Snapshot, bool)> {
        // Base date for offsets: 2023-01-01 00:00:00
        let base_ymd = (2023, 1, 1);
        let base_hms = (0, 0, 0);

        let snapshots = vec![
            // ID 0 to 4: Daily-like snapshots
            create_snapshot(
                0,
                create_datetime(base_ymd, base_hms, 21),
                &["tag0", "tag1"],
            ),
            create_snapshot(1, create_datetime(base_ymd, base_hms, 1), &["tag0"]),
            create_snapshot(2, create_datetime(base_ymd, base_hms, 2), &[]),
            create_snapshot(3, create_datetime(base_ymd, base_hms, 3), &[]),
            create_snapshot(4, create_datetime(base_ymd, base_hms, 4), &[]),
            // ID 5 to 7, 0xD, 0xE: Weekly-like snapshots
            create_snapshot(5, create_datetime(base_ymd, base_hms, 7), &[]),
            create_snapshot(6, create_datetime(base_ymd, base_hms, 14), &[]),
            create_snapshot(0xD, create_datetime(base_ymd, base_hms, 15), &[]),
            create_snapshot(0xE, create_datetime(base_ymd, base_hms, 16), &[]),
            create_snapshot(7, create_datetime(base_ymd, base_hms, 21), &[]),
            // ID 8 to 9: Monthly-like snapshots
            create_snapshot(8, create_datetime((2023, 1, 28), (23, 59, 59), 0), &[]),
            create_snapshot(9, create_datetime((2023, 2, 28), (23, 59, 0), 0), &[]),
            // ID 0xA to 0xC: Yearly-like snapshots and current time
            create_snapshot(0xA, create_datetime((2023, 12, 31), (23, 59, 0), 0), &[]),
            create_snapshot(0xB, create_datetime((2024, 12, 31), (23, 59, 0), 0), &[]),
            create_snapshot(0xC, create_datetime((2025, 5, 25), (20, 29, 46), 0), &[]),
        ];

        assert_no_duplicate_ids(&snapshots);

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
        let rules = vec![RetentionRule::KeepLast(3)];
        let kept_ids = apply_retention_rules(&snapshots, &rules, test_now());

        // KeepLast logic needs to reference the actual snapshots vector indices
        let mut expected_keep_ids: HashSet<ID> = HashSet::new();
        // The last three snapshots in the vector are 0xC, 0xB, 0xA
        for i in (0..snapshots.len()).rev().take(3) {
            expected_keep_ids.insert(snapshots[i].0.clone());
        }

        assert_eq!(kept_ids, expected_keep_ids);
    }

    #[test]
    fn test_keep_yearly() {
        let snapshots = create_mock_snapshots();
        let rules = vec![RetentionRule::KeepYearly(3)];
        let kept_ids = apply_retention_rules(&snapshots, &rules, test_now());

        let expected_ids = create_expected_ids(&[
            0xA, // 2023
            0xB, // 2024
            0xC, // 2025
        ]);

        assert_eq!(kept_ids, expected_ids);
    }

    #[test]
    fn test_keep_monthly() {
        let snapshots = create_mock_snapshots();
        let rules = vec![RetentionRule::KeepMonthly(4)];
        let kept_ids = apply_retention_rules(&snapshots, &rules, test_now());

        let expected_ids = create_expected_ids(&[
            9,   // Feb 2023
            0xA, // Dec 2023
            0xB, // Dec 2024
            0xC, // May 2025
        ]);

        assert_eq!(kept_ids, expected_ids);
    }

    #[test]
    fn test_keep_weekly() {
        let snapshots = create_mock_snapshots();
        let rules = vec![RetentionRule::KeepWeekly(5)];
        let kept_ids = apply_retention_rules(&snapshots, &rules, test_now());

        let expected_ids = create_expected_ids(&[
            8,   // Jan 2023
            9,   // Feb 2023
            0xA, // Dec 2023
            0xB, // Dec 2024
            0xC, // May 2025
        ]);

        assert_eq!(kept_ids, expected_ids);
    }

    #[test]
    fn test_keep_daily() {
        let snapshots = create_mock_snapshots();
        let rules = vec![RetentionRule::KeepDaily(8)];
        let kept_ids = apply_retention_rules(&snapshots, &rules, test_now());

        let expected_ids = create_expected_ids(&[
            0xD, // Day 15
            0xE, // Day 16
            7,   // Day 21
            8,   // Jan 28
            9,   // Feb 28
            0xA, // Dec 31, 2023
            0xB, // Dec 31, 2024
            0xC, // May 25, 2025
        ]);

        assert_eq!(kept_ids, expected_ids);
    }

    #[test]
    fn test_keep_within() {
        let snapshots = create_mock_snapshots();
        let rules = vec![RetentionRule::KeepWithin(Duration::days(2 * 365))];
        let kept_ids = apply_retention_rules(&snapshots, &rules, test_now());

        let expected_ids = create_expected_ids(&[
            0xA, // Dec 31, 2023
            0xB, // Dec 31, 2024
            0xC, // May 25, 2025
        ]);

        assert_eq!(kept_ids, expected_ids);
    }

    #[test]
    fn test_keep_tags() {
        let snapshots = create_mock_snapshots();
        let rules = vec![RetentionRule::KeepTags(
            ["tag0"].into_iter().map(|s| s.to_string()).collect(),
        )];
        let kept_ids = apply_retention_rules(&snapshots, &rules, test_now());

        let expected_ids = create_expected_ids(&[
            0, // Has tags ["tag0", "tag1"]
            1, // Has tag ["tag0"]
        ]);

        assert_eq!(kept_ids, expected_ids);
    }

    #[test]
    fn test_keep_multiple_rules() {
        let snapshots = create_mock_snapshots();
        let rules = vec![
            RetentionRule::KeepLast(4),
            RetentionRule::KeepWithin(Duration::days(2 * 365)),
            RetentionRule::KeepYearly(3),
            RetentionRule::KeepTags(["tag1"].into_iter().map(|s| s.to_string()).collect()),
        ];
        let kept_ids = apply_retention_rules(&snapshots, &rules, test_now());

        let expected_ids = create_expected_ids(&[
            0,   // Kept by KeepTags("tag1")
            9,   // Kept by KeepLast(4)
            0xA, // Kept by multiple rules
            0xB, // Kept by multiple rules
            0xC, // Kept by multiple rules
        ]);

        assert_eq!(kept_ids, expected_ids);
    }
}
