use std::{
    collections::{BTreeSet, HashMap},
    hash::Hash,
};

use chrono::{
    DateTime, Datelike, Duration, Local, NaiveDate, NaiveDateTime, TimeZone, Timelike, Utc,
};

use crate::{
    common::ID,
    repository::snapshot::{Snapshot, SnapshotEntry},
    utils::collections::IdSet,
};

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
    /// Keep N hourly snapshots.
    KeepHourly(usize),
    /// Keep snapshots with tag
    KeepTags(BTreeSet<String>),
}

/// Applies retention rules to a sorted list of snapshots and returns the IDs of snapshots to keep.
///
/// `snapshots_sorted`: A vector of (ID, Snapshot) tuples, sorted in ascending order by timestamp.
/// `rules`: A slice of `RetentionRule` to apply.
/// `now`: The current time to use for `KeepWithin` rules.
pub fn apply_retention_rules(
    snapshots_sorted: &[&SnapshotEntry],
    rules: &[RetentionRule],
    keep_min: Option<usize>,
    now: DateTime<Local>,
) -> IdSet<ID> {
    let mut snapshots_to_keep: IdSet<ID> = IdSet::default();

    // The date part of 'now' for reference
    let now_date = now.date_naive();
    let timezone = now.timezone();

    // The "all" retention value (`--keep-yearly all` etc.) is parsed as `usize::MAX`.
    // For a period rule it means "keep the latest snapshot of every period present",
    // which `keep_latest_per_period` produces when the cutoff is far enough in the
    // past to include every snapshot. Computing the cutoff arithmetically for
    // `usize::MAX` instead overflows (`(n - 1) as i32` wraps to a small negative
    // number, pushing the cutoff into the future) and would keep nothing.
    let all_cutoff: DateTime<Local> = DateTime::<Utc>::MIN_UTC.with_timezone(&timezone);

    // Returns `all_cutoff` for the "all" sentinel, otherwise calls `compute_cutoff()`
    // and falls back to `now` on `None`.
    fn period_cutoff(
        n: usize,
        all_cutoff: DateTime<Local>,
        now: DateTime<Local>,
        compute_cutoff: impl FnOnce() -> Option<DateTime<Local>>,
    ) -> DateTime<Local> {
        if n == usize::MAX {
            all_cutoff
        } else {
            compute_cutoff().unwrap_or(now)
        }
    }

    // Converts a NaiveDateTime to DateTime<Local>, handling DST gaps by
    // retrying with a 1-hour offset and falling back to UTC as a last resort.
    fn resolve_local(timezone: Local, dt: NaiveDateTime) -> DateTime<Local> {
        timezone
            .from_local_datetime(&dt)
            .earliest()
            .unwrap_or_else(|| {
                timezone
                    .from_local_datetime(&(dt + Duration::hours(1)))
                    .earliest()
                    .unwrap_or(dt.and_utc().with_timezone(&timezone))
            })
    }

    // Rules are applied sequentially, and the results are unioned.
    for rule in rules {
        let ids_to_keep = match rule {
            RetentionRule::KeepLast(n) => {
                // Keep the N most recent snapshots, leveraging the reverse iterator on the sorted list.
                snapshots_sorted
                    .iter()
                    .rev()
                    .take(*n)
                    .map(|e| e.id)
                    .collect()
            }
            RetentionRule::KeepWithin(duration) => {
                // Keep all snapshots newer than the cutoff time (using the exact `now`).
                let cutoff_time = now - *duration;
                snapshots_sorted
                    .iter()
                    .filter(|e| e.snapshot.timestamp >= cutoff_time)
                    .map(|e| e.id)
                    .collect()
            }
            RetentionRule::KeepYearly(n) => {
                // Cutoff is Jan 1st of the year (N-1) years ago.
                let cutoff = period_cutoff(*n, all_cutoff, now, || {
                    let target_year = now.year() - ((*n - 1) as i32);
                    NaiveDate::from_ymd_opt(target_year, 1, 1)
                        .and_then(|d| d.and_hms_opt(0, 0, 0))
                        .map(|dt| resolve_local(timezone, dt))
                });

                keep_latest_per_period(snapshots_sorted, |s| s.timestamp.year(), cutoff)
            }
            RetentionRule::KeepMonthly(n) => {
                // Cutoff is the 1st of the month N months ago.
                let cutoff = period_cutoff(*n, all_cutoff, now, || {
                    let mut target_date: NaiveDate = now_date.with_day(1)?;
                    for _ in 1..*n {
                        let month = target_date.month();
                        let year = target_date.year();
                        if month == 1 {
                            target_date = target_date.with_year(year - 1)?.with_month(12)?;
                        } else {
                            target_date = target_date.with_month(month - 1)?;
                        }
                    }
                    let dt = target_date.and_hms_opt(0, 0, 0)?;
                    Some(resolve_local(timezone, dt))
                });

                keep_latest_per_period(
                    snapshots_sorted,
                    |s| (s.timestamp.year(), s.timestamp.month()),
                    cutoff,
                )
            }
            RetentionRule::KeepWeekly(n) => {
                // Cutoff is the Monday 00:00 of the week N weeks ago (ISO 8601 start of week).
                let cutoff = period_cutoff(*n, all_cutoff, now, || {
                    let current_monday_date =
                        now_date - Duration::days(now_date.weekday().num_days_from_monday() as i64);
                    let target_monday_date = current_monday_date - Duration::weeks((*n - 1) as i64);
                    target_monday_date
                        .and_hms_opt(0, 0, 0)
                        .map(|dt| resolve_local(timezone, dt))
                });

                keep_latest_per_period(
                    snapshots_sorted,
                    |s| (s.timestamp.year(), s.timestamp.iso_week().week()),
                    cutoff,
                )
            }
            RetentionRule::KeepDaily(n) => {
                // Calculate midnight of the current day for the anchor.
                let cutoff = period_cutoff(*n, all_cutoff, now, || {
                    let naive_midnight = now_date.and_hms_opt(0, 0, 0)?;
                    let now_midnight = resolve_local(timezone, naive_midnight);
                    // Cutoff is midnight of the day N days ago.
                    Some(now_midnight - Duration::days((*n - 1) as i64))
                });

                keep_latest_per_period(snapshots_sorted, |s| s.timestamp.date_naive(), cutoff)
            }
            RetentionRule::KeepHourly(n) => {
                let cutoff = period_cutoff(*n, all_cutoff, now, || {
                    // Truncate 'now' to the start of the current hour.
                    let now_truncated = now
                        .with_minute(0)
                        .and_then(|d| d.with_second(0))
                        .unwrap_or(now);
                    // Cutoff is the start of the hour N hours ago.
                    Some(now_truncated - Duration::hours((*n - 1) as i64))
                });

                keep_latest_per_period(
                    snapshots_sorted,
                    |s| (s.timestamp.date_naive(), s.timestamp.hour()),
                    cutoff,
                )
            }
            RetentionRule::KeepTags(tags) => {
                // Keep all snapshots that match the required tags.
                snapshots_sorted
                    .iter()
                    .filter(|e| e.snapshot.has_tags(tags))
                    .map(|e| e.id)
                    .collect()
            }
        };

        // Combine the results: if any rule dictates a snapshot must be kept, it is kept.
        snapshots_to_keep.extend(ids_to_keep);
    }

    // Ensure minimum number of snapshots are kept
    if let Some(min) = keep_min {
        let target = min.min(snapshots_sorted.len());
        if snapshots_to_keep.len() < target {
            for entry in snapshots_sorted.iter().rev() {
                if snapshots_to_keep.len() >= target {
                    break;
                }
                snapshots_to_keep.insert(entry.id);
            }
        }
    }

    snapshots_to_keep
}

/// Filters snapshots to only include those from the specified hosts.
/// Snapshots without a hostname are excluded when any host filter is active.
pub fn filter_snapshots_by_hosts<'a>(
    snapshots: impl Iterator<Item = &'a SnapshotEntry>,
    hosts: &[String],
) -> Vec<&'a SnapshotEntry> {
    if hosts.is_empty() {
        return snapshots.collect();
    }
    snapshots
        .filter(|e| {
            e.snapshot
                .hostname
                .as_ref()
                .is_some_and(|h| hosts.contains(h))
        })
        .collect()
}

/// Generic helper function to abstract the common logic for period-based retention.
///
/// It finds the latest snapshot for each unique period (defined by `key_extractor`)
/// and then keeps snapshots that are within the cutoff.
fn keep_latest_per_period<K, F>(
    snapshots_sorted: &[&SnapshotEntry],
    key_extractor: F,
    cut_off: DateTime<Local>,
) -> IdSet<ID>
where
    K: Eq + Hash,
    F: Fn(&Snapshot) -> K,
{
    let mut kept_periods: HashMap<K, ID> = HashMap::new();

    for entry in snapshots_sorted.iter().rev() {
        let key = key_extractor(&entry.snapshot);

        if entry.snapshot.timestamp >= cut_off {
            kept_periods.entry(key).or_insert(entry.id);
        }
    }

    kept_periods.values().copied().collect::<IdSet<ID>>()
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, path::PathBuf};

    use chrono::{DateTime, Duration, Local, NaiveDate, NaiveDateTime, TimeZone};

    use super::*;
    use crate::repository::snapshot::SnapshotEntryList;

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
        hostname: Option<&str>,
    ) -> SnapshotEntry {
        let snapshot_id = create_id(id_val);
        SnapshotEntry {
            id: snapshot_id,
            snapshot: Snapshot {
                timestamp,
                parent: None,
                tree: snapshot_id,
                root: PathBuf::from("/"),
                paths: Vec::new(),
                tags: tags.iter().map(|s| s.to_string()).collect(),
                description: None,
                summary: Default::default(),
                hostname: hostname.map(|s| s.to_string()),
                username: None,
                version: None,
            },
            active: true,
        }
    }

    fn test_now() -> DateTime<Local> {
        // Base reference time for cutoffs: 2025-05-25 21:58:00 (Sunday)
        create_datetime((2025, 5, 25), (21, 58, 0), 0)
    }

    /// Helper: build a snapshot entry from compact arguments.
    fn snap(
        id: u32,
        ymd: (i32, u32, u32),
        hms: (u32, u32, u32),
        days_offset: i64,
        tags: &[&str],
        host: Option<&str>,
    ) -> SnapshotEntry {
        create_snapshot(id, create_datetime(ymd, hms, days_offset), tags, host)
    }

    /// Expanded mock dataset (~50 snapshots) for comprehensive retention testing.
    ///
    /// Reference time (`test_now()`): 2025-05-25 21:58:00 (Sunday).
    ///
    /// Coverage:
    /// - Years:  2021, 2022, 2023, 2024, 2025
    /// - Months: Jan-Dec 2023, Jan-Dec 2024, Jan-May 2025
    /// - Weeks:  weeks 15-21 of 2025 (multiple snapshots per week)
    /// - Days:   May 12-25 2025 (most days have a snapshot)
    /// - Hours:  last 6 hours of test_now()
    /// - Tags:   "release", "backup", "important", "archive"
    /// - Hosts:  "server", "laptop"
    fn create_mock_snapshots() -> SnapshotEntryList {
        let mut snapshots = vec![
            // ── 2021 ────────────────────────────────────────────
            snap(0, (2021, 12, 31), (23, 59, 59), 0, &["archive"], None),
            // ── 2022 ────────────────────────────────────────────
            snap(1, (2022, 1, 1), (0, 0, 0), 0, &["release"], Some("server")),
            snap(2, (2022, 6, 15), (12, 0, 0), 0, &[], None),
            // ── 2023 ────────────────────────────────────────────
            snap(3, (2023, 1, 1), (0, 0, 0), 0, &["release"], Some("server")),
            snap(4, (2023, 1, 15), (8, 0, 0), 0, &[], None),
            snap(5, (2023, 1, 31), (23, 59, 59), 0, &[], None),
            snap(6, (2023, 2, 15), (12, 0, 0), 0, &[], None),
            snap(7, (2023, 3, 10), (10, 0, 0), 0, &[], None),
            snap(8, (2023, 4, 1), (0, 0, 0), 0, &[], None),
            snap(9, (2023, 5, 20), (18, 0, 0), 0, &["backup"], None),
            snap(10, (2023, 6, 1), (0, 0, 0), 0, &[], None),
            snap(11, (2023, 7, 4), (12, 0, 0), 0, &[], None),
            snap(12, (2023, 8, 15), (6, 0, 0), 0, &[], None),
            snap(13, (2023, 9, 30), (23, 59, 59), 0, &[], None),
            snap(14, (2023, 10, 1), (0, 0, 0), 0, &[], None),
            snap(15, (2023, 11, 15), (12, 0, 0), 0, &[], None),
            snap(16, (2023, 12, 25), (0, 0, 0), 0, &[], None),
            snap(17, (2023, 12, 31), (23, 59, 59), 0, &[], None),
            // ── 2024 ────────────────────────────────────────────
            snap(18, (2024, 1, 1), (0, 0, 0), 0, &["release"], None),
            snap(19, (2024, 3, 15), (12, 0, 0), 0, &[], None),
            snap(20, (2024, 6, 1), (0, 0, 0), 0, &["backup"], None),
            snap(21, (2024, 9, 1), (0, 0, 0), 0, &[], None),
            snap(22, (2024, 12, 31), (23, 59, 59), 0, &[], None),
            // ── 2025 ────────────────────────────────────────────
            snap(23, (2025, 1, 1), (0, 0, 1), 0, &["release"], Some("laptop")),
            snap(24, (2025, 2, 1), (0, 0, 0), 0, &["backup"], None),
            snap(25, (2025, 3, 15), (12, 0, 0), 0, &[], None),
            snap(26, (2025, 4, 13), (23, 59, 59), 0, &[], None),
            snap(27, (2025, 4, 14), (0, 0, 1), 0, &[], None),
            snap(28, (2025, 4, 28), (10, 0, 0), 0, &[], None),
            snap(29, (2025, 5, 1), (12, 0, 0), 0, &["backup"], None),
            snap(30, (2025, 5, 5), (8, 0, 0), 0, &[], None),
            snap(31, (2025, 5, 7), (8, 0, 0), 0, &[], None),
            snap(32, (2025, 5, 8), (8, 0, 0), 0, &[], None),
            snap(33, (2025, 5, 12), (8, 0, 0), 0, &[], Some("server")),
            snap(34, (2025, 5, 13), (8, 0, 0), 0, &[], None),
            snap(35, (2025, 5, 14), (12, 0, 0), 0, &[], None),
            snap(
                36,
                (2025, 5, 15),
                (18, 0, 0),
                0,
                &["backup"],
                Some("laptop"),
            ),
            snap(37, (2025, 5, 18), (10, 0, 0), 0, &[], None),
            snap(38, (2025, 5, 19), (10, 0, 0), 0, &[], None),
            snap(39, (2025, 5, 20), (10, 0, 0), 0, &["important"], None),
            snap(40, (2025, 5, 21), (10, 0, 0), 0, &[], None),
            snap(41, (2025, 5, 22), (10, 0, 0), 0, &[], None),
            snap(42, (2025, 5, 23), (10, 0, 0), 0, &["important"], None),
            snap(43, (2025, 5, 24), (10, 0, 0), 0, &[], None),
            // Hourly: last 6 hours
            snap(44, (2025, 5, 25), (16, 0, 0), 0, &[], None),
            snap(45, (2025, 5, 25), (17, 0, 0), 0, &[], None),
            snap(46, (2025, 5, 25), (18, 0, 0), 0, &[], None),
            snap(47, (2025, 5, 25), (19, 0, 0), 0, &[], None),
            snap(48, (2025, 5, 25), (20, 0, 0), 0, &[], None),
            snap(49, (2025, 5, 25), (21, 0, 0), 0, &[], None),
        ];

        assert_no_duplicate_ids(&snapshots);

        snapshots.sort_by_key(|s| s.snapshot.timestamp);

        snapshots
    }

    /// Assert that the snapshots don't have duplicate IDs
    fn assert_no_duplicate_ids(snapshots: &[SnapshotEntry]) {
        let count = snapshots.len();
        let unique_ids: HashSet<&ID> = snapshots.iter().map(|s| &s.id).collect();

        assert_eq!(
            unique_ids.len(),
            count,
            "Duplicate ID found in mock snapshots. Total snapshots: {}, unique IDs: {}",
            count,
            unique_ids.len()
        );
    }

    // Helper function to create the expected ID HashSet
    fn create_expected_ids(id_vals: &[u32]) -> IdSet<ID> {
        id_vals.iter().map(|&v| create_id(v)).collect()
    }

    #[test]
    fn test_keep_last() {
        let snapshots = create_mock_snapshots();
        let rules = vec![RetentionRule::KeepLast(4)];
        let kept_ids = apply_retention_rules(
            &snapshots.iter().collect::<Vec<_>>(),
            &rules,
            None,
            test_now(),
        );

        // Keeps the 4 most recent snapshots
        let expected_keep_ids = create_expected_ids(&[46, 47, 48, 49]);

        assert_eq!(kept_ids, expected_keep_ids);
    }

    #[test]
    fn test_keep_yearly() {
        let snapshots = create_mock_snapshots();

        let rules = vec![RetentionRule::KeepYearly(5)];
        let kept_ids = apply_retention_rules(
            &snapshots.iter().collect::<Vec<_>>(),
            &rules,
            None,
            test_now(),
        );

        let expected_ids = create_expected_ids(&[
            0,  // Latest in 2021
            2,  // Latest in 2022
            17, // Latest in 2023
            22, // Latest in 2024
            49, // Latest in 2025
        ]);

        assert_eq!(kept_ids, expected_ids);
    }

    #[test]
    fn test_keep_monthly() {
        let snapshots = create_mock_snapshots();
        let rules = vec![RetentionRule::KeepMonthly(5)];
        let kept_ids = apply_retention_rules(
            &snapshots.iter().collect::<Vec<_>>(),
            &rules,
            None,
            test_now(),
        );

        // Cutoff: 2025-01-01 00:00:00. Latest snapshot per month since then.
        let expected_ids = create_expected_ids(&[
            23, // Latest in Jan 2025
            24, // Latest in Feb 2025
            25, // Latest in Mar 2025
            28, // Latest in Apr 2025
            49, // Latest in May 2025
        ]);

        assert_eq!(kept_ids, expected_ids);
    }

    #[test]
    fn test_keep_weekly() {
        let snapshots = create_mock_snapshots();
        let rules = vec![RetentionRule::KeepWeekly(8)];
        let kept_ids = apply_retention_rules(
            &snapshots.iter().collect::<Vec<_>>(),
            &rules,
            None,
            test_now(),
        );

        // Cutoff: Monday of week 8 weeks before week 21 (2025-03-31).
        // Latest snapshot per ISO week within the window.
        let expected_ids = create_expected_ids(&[
            26, // Latest in Week 15 (Apr 7-13)
            27, // Latest in Week 16 (Apr 14-20)
            29, // Latest in Week 18 (Apr 28 - May 4)
            32, // Latest in Week 19 (May 5-11)
            37, // Latest in Week 20 (May 12-18)
            49, // Latest in Week 21 (May 19-25)
        ]);

        assert_eq!(kept_ids, expected_ids);
    }

    #[test]
    fn test_keep_daily() {
        let snapshots = create_mock_snapshots();
        let rules = vec![RetentionRule::KeepDaily(10)];
        let kept_ids = apply_retention_rules(
            &snapshots.iter().collect::<Vec<_>>(),
            &rules,
            None,
            test_now(),
        );

        // Cutoff: midnight of 10 days before test_now() = 2025-05-16 00:00:00.
        // Latest snapshot per calendar day since then.
        // Note: May 16 and May 17 have no snapshots, so only 8 days are represented.
        let expected_ids = create_expected_ids(&[
            37, // Latest on May 18
            38, // Latest on May 19
            39, // Latest on May 20
            40, // Latest on May 21
            41, // Latest on May 22
            42, // Latest on May 23
            43, // Latest on May 24
            49, // Latest on May 25 (21:00 — only the latest hour kept)
        ]);

        assert_eq!(kept_ids, expected_ids);
    }

    #[test]
    fn test_keep_within() {
        let snapshots = create_mock_snapshots();

        // Keep within 1 day: cutoff = test_now() - 1 day = 2025-05-24 21:58:00
        let rules = vec![RetentionRule::KeepWithin(Duration::days(1))];
        let kept_ids = apply_retention_rules(
            &snapshots.iter().collect::<Vec<_>>(),
            &rules,
            None,
            test_now(),
        );
        let expected_ids = create_expected_ids(&[44, 45, 46, 47, 48, 49]);
        assert_eq!(kept_ids, expected_ids);

        // Keep within 30 days: cutoff = test_now() - 30 days = 2025-04-25 21:58:00
        let rules = vec![RetentionRule::KeepWithin(Duration::days(30))];
        let kept_ids = apply_retention_rules(
            &snapshots.iter().collect::<Vec<_>>(),
            &rules,
            None,
            test_now(),
        );
        // All snapshots from Apr 28 onward
        let expected_ids = create_expected_ids(&[
            28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49,
        ]);
        assert_eq!(kept_ids, expected_ids);
    }

    #[test]
    fn test_keep_tags() {
        let snapshots = create_mock_snapshots();
        let rules = vec![RetentionRule::KeepTags(
            ["release"].into_iter().map(|s| s.to_string()).collect(),
        )];
        let kept_ids = apply_retention_rules(
            &snapshots.iter().collect::<Vec<_>>(),
            &rules,
            None,
            test_now(),
        );

        let expected_ids = create_expected_ids(&[1, 3, 18, 23]);

        assert_eq!(kept_ids, expected_ids);
    }

    #[test]
    fn test_keep_multiple_rules() {
        let snapshots = create_mock_snapshots();
        let rules = vec![
            RetentionRule::KeepLast(3),    // Keeps {47, 48, 49}
            RetentionRule::KeepMonthly(5), // Keeps {23, 24, 25, 28, 49}
            RetentionRule::KeepTags(["important"].into_iter().map(|s| s.to_string()).collect()), // Keeps {39, 42}
        ];
        let kept_ids = apply_retention_rules(
            &snapshots.iter().collect::<Vec<_>>(),
            &rules,
            None,
            test_now(),
        );

        // Union: {23, 24, 25, 28, 39, 42, 47, 48, 49}
        let expected_ids = create_expected_ids(&[
            23, // KeepMonthly
            24, // KeepMonthly
            25, // KeepMonthly
            28, // KeepMonthly
            39, // KeepTags("important")
            42, // KeepTags("important")
            47, // KeepLast(3)
            48, // KeepLast(3)
            49, // KeepLast(3) + KeepMonthly
        ]);

        assert_eq!(kept_ids, expected_ids);
    }

    #[test]
    fn test_keep_hourly() {
        let now = test_now();
        let snapshots = [
            create_snapshot(0, now - Duration::hours(5), &[], None),
            create_snapshot(1, now - Duration::hours(4), &[], None),
            create_snapshot(2, now - Duration::hours(3), &[], None),
            create_snapshot(3, now - Duration::hours(2), &[], None),
            create_snapshot(4, now - Duration::hours(1), &[], None),
            create_snapshot(5, now, &[], None),
        ];

        let rules = vec![RetentionRule::KeepHourly(3)];
        let kept_ids =
            apply_retention_rules(&snapshots.iter().collect::<Vec<_>>(), &rules, None, now);

        // Keeps the latest snapshot per hour for the last 3 hours
        let expected_ids = create_expected_ids(&[3, 4, 5]);
        assert_eq!(kept_ids, expected_ids);
    }

    #[test]
    fn test_filter_by_hosts() {
        let snapshots = [
            create_snapshot(0, test_now() - Duration::days(2), &[], Some("server-a")),
            create_snapshot(1, test_now() - Duration::days(1), &[], Some("server-b")),
            create_snapshot(2, test_now(), &[], Some("server-a")),
            create_snapshot(3, test_now() - Duration::hours(1), &[], None),
        ];

        let filtered = filter_snapshots_by_hosts(snapshots.iter(), &["server-a".to_string()]);
        assert_eq!(filtered.len(), 2);
        assert_eq!(filtered[0].id, create_id(0));
        assert_eq!(filtered[1].id, create_id(2));

        let filtered_multi = filter_snapshots_by_hosts(
            snapshots.iter(),
            &["server-a".to_string(), "server-b".to_string()],
        );
        assert_eq!(filtered_multi.len(), 3);

        let filtered_empty = filter_snapshots_by_hosts(snapshots.iter(), &[]);
        assert_eq!(filtered_empty.len(), 4);
    }

    #[test]
    fn test_keep_min_override() {
        let snapshots = create_mock_snapshots();

        // KeepLast(2) would keep only {48, 49}
        let rules = vec![RetentionRule::KeepLast(2)];
        let kept_ids = apply_retention_rules(
            &snapshots.iter().collect::<Vec<_>>(),
            &rules,
            Some(5),
            test_now(),
        );

        // With keep_min=5, should have 5 snapshots: {48, 49} + {47, 46, 45}
        assert_eq!(kept_ids.len(), 5);
        assert!(kept_ids.contains(&create_id(49)));
        assert!(kept_ids.contains(&create_id(48)));
        assert!(kept_ids.contains(&create_id(47)));
        assert!(kept_ids.contains(&create_id(46)));
        assert!(kept_ids.contains(&create_id(45)));
    }

    #[test]
    fn test_keep_min_no_override_when_enough() {
        let snapshots = create_mock_snapshots();

        // KeepLast(10) already keeps more than 5
        let rules = vec![RetentionRule::KeepLast(10)];
        let kept_ids = apply_retention_rules(
            &snapshots.iter().collect::<Vec<_>>(),
            &rules,
            Some(5),
            test_now(),
        );

        // Should keep 10, not reduced to 5
        assert_eq!(kept_ids.len(), 10);
    }

    #[test]
    fn test_keep_yearly_all_keeps_latest_per_year() {
        // "all" is parsed as usize::MAX by the CLI (`--keep-yearly all`). It must
        // keep the latest snapshot of EVERY year present (deduping within a year),
        // not keep zero and not keep every snapshot.
        let snapshots = create_mock_snapshots();
        let rules = vec![RetentionRule::KeepYearly(usize::MAX)];
        let kept_ids = apply_retention_rules(
            &snapshots.iter().collect::<Vec<_>>(),
            &rules,
            None,
            test_now(),
        );

        let expected_ids = create_expected_ids(&[
            0,  // Latest in 2021
            2,  // Latest in 2022
            17, // Latest in 2023
            22, // Latest in 2024
            49, // Latest in 2025
        ]);
        assert_eq!(kept_ids, expected_ids);
    }

    #[test]
    fn test_period_rules_all_keep_every_isolated_period() {
        // Each snapshot is one year apart, so every one is alone in its own
        // year / month / week / day / hour bucket. With the "all" value
        // (usize::MAX) every period rule must therefore keep all of them.
        let now = test_now();
        let snapshots = [
            create_snapshot(0, now - Duration::days(365 * 4), &[], None),
            create_snapshot(1, now - Duration::days(365 * 3), &[], None),
            create_snapshot(2, now - Duration::days(365 * 2), &[], None),
            create_snapshot(3, now - Duration::days(365), &[], None),
            create_snapshot(4, now, &[], None),
        ];
        let all_ids = create_expected_ids(&[0, 1, 2, 3, 4]);

        for rule in [
            RetentionRule::KeepYearly(usize::MAX),
            RetentionRule::KeepMonthly(usize::MAX),
            RetentionRule::KeepWeekly(usize::MAX),
            RetentionRule::KeepDaily(usize::MAX),
            RetentionRule::KeepHourly(usize::MAX),
        ] {
            let kept_ids = apply_retention_rules(
                &snapshots.iter().collect::<Vec<_>>(),
                std::slice::from_ref(&rule),
                None,
                now,
            );
            assert_eq!(
                kept_ids, all_ids,
                "rule {rule:?} with \"all\" must keep every period"
            );
        }
    }

    #[test]
    fn test_keep_min_none() {
        let snapshots = create_mock_snapshots();

        let rules = vec![RetentionRule::KeepLast(2)];
        let kept_ids = apply_retention_rules(
            &snapshots.iter().collect::<Vec<_>>(),
            &rules,
            None,
            test_now(),
        );

        // No minimum, so just KeepLast(2)
        assert_eq!(kept_ids.len(), 2);
    }

    #[test]
    fn test_keep_min_no_rules_match() {
        let snapshots = create_mock_snapshots();

        // No rules match any snapshot, keep_min=1
        let rules = vec![];
        let kept_ids = apply_retention_rules(
            &snapshots.iter().collect::<Vec<_>>(),
            &rules,
            Some(1),
            test_now(),
        );

        assert_eq!(
            kept_ids.len(),
            1,
            "keep_min=1 with no matching rules should keep 1 snapshot"
        );
        assert!(
            kept_ids.contains(&create_id(49)),
            "Should keep the newest snapshot"
        );
    }

    // ── Edge-case tests ─────────────────────────────────────────────

    #[test]
    fn test_empty_snapshot_list_with_keep_min() {
        let empty: Vec<&SnapshotEntry> = vec![];
        let rules = vec![RetentionRule::KeepLast(10)];
        let kept_ids = apply_retention_rules(&empty, &rules, Some(5), test_now());
        // keep_min is capped to total count (0), so nothing is kept.
        assert!(kept_ids.is_empty());
    }

    #[test]
    fn test_keep_last_exceeds_count() {
        let snapshots = create_mock_snapshots();
        let rules = vec![RetentionRule::KeepLast(100)];
        let kept_ids = apply_retention_rules(
            &snapshots.iter().collect::<Vec<_>>(),
            &rules,
            None,
            test_now(),
        );
        // KeepLast(100) with 50 snapshots keeps all 50.
        assert_eq!(kept_ids.len(), 50);
    }

    #[test]
    fn test_keep_min_exceeds_total() {
        let snapshots = create_mock_snapshots();
        // KeepLast(2) keeps 2, but keep_min=200 asks for more than total.
        let rules = vec![RetentionRule::KeepLast(2)];
        let kept_ids = apply_retention_rules(
            &snapshots.iter().collect::<Vec<_>>(),
            &rules,
            Some(200),
            test_now(),
        );
        // keep_min is capped at total count (50).
        assert_eq!(kept_ids.len(), 50);
    }

    #[test]
    fn test_keep_min_with_period_rule() {
        let snapshots = create_mock_snapshots();
        // KeepMonthly(2) keeps {28, 49} (latest in Apr and May 2025).
        // keep_min=10 forces 10 total: the 2 from the rule + 8 more from the end.
        let rules = vec![RetentionRule::KeepMonthly(2)];
        let kept_ids = apply_retention_rules(
            &snapshots.iter().collect::<Vec<_>>(),
            &rules,
            Some(10),
            test_now(),
        );
        assert_eq!(kept_ids.len(), 10);
        // The 2 from KeepMonthly must be included.
        assert!(kept_ids.contains(&create_id(28)));
        assert!(kept_ids.contains(&create_id(49)));
    }

    #[test]
    fn test_keep_tags_multiple_tags() {
        let snapshots = create_mock_snapshots();
        // "release" tags: 1, 3, 18, 23
        // "important" tags: 39, 42
        // Union should keep all 6.
        let rules = vec![RetentionRule::KeepTags(
            ["release", "important"]
                .into_iter()
                .map(|s| s.to_string())
                .collect(),
        )];
        let kept_ids = apply_retention_rules(
            &snapshots.iter().collect::<Vec<_>>(),
            &rules,
            None,
            test_now(),
        );
        let expected_ids = create_expected_ids(&[1, 3, 18, 23, 39, 42]);
        assert_eq!(kept_ids, expected_ids);
    }

    #[test]
    fn test_keep_within_zero() {
        let snapshots = create_mock_snapshots();
        // KeepWithin(Duration::zero()) keeps only snapshots >= now.
        // test_now() = 2025-05-25 21:58:00, and the latest snapshot is at 21:00.
        // No snapshot is >= now, so nothing is kept.
        let rules = vec![RetentionRule::KeepWithin(Duration::zero())];
        let kept_ids = apply_retention_rules(
            &snapshots.iter().collect::<Vec<_>>(),
            &rules,
            None,
            test_now(),
        );
        assert!(kept_ids.is_empty());
    }

    #[test]
    fn test_keep_within_all() {
        let snapshots = create_mock_snapshots();
        // KeepWithin(very large) keeps all snapshots.
        let rules = vec![RetentionRule::KeepWithin(Duration::days(99999))];
        let kept_ids = apply_retention_rules(
            &snapshots.iter().collect::<Vec<_>>(),
            &rules,
            None,
            test_now(),
        );
        assert_eq!(kept_ids.len(), 50);
    }

    #[test]
    fn test_year_boundary() {
        let now = test_now();
        // Two snapshots: one just before midnight Dec 31, one just after midnight Jan 1.
        let snapshots = [
            create_snapshot(
                0,
                create_datetime((2024, 12, 31), (23, 59, 59), 0),
                &[],
                None,
            ),
            create_snapshot(1, create_datetime((2025, 1, 1), (0, 0, 1), 0), &[], None),
        ];
        // KeepYearly(2) keeps the latest per year: both are in different years.
        let rules = vec![RetentionRule::KeepYearly(2)];
        let kept_ids =
            apply_retention_rules(&snapshots.iter().collect::<Vec<_>>(), &rules, None, now);
        assert_eq!(kept_ids, create_expected_ids(&[0, 1]));
    }

    #[test]
    fn test_month_boundary() {
        let now = test_now();
        // Two snapshots at the April/May boundary: just before and just after midnight.
        let snapshots = [
            create_snapshot(
                0,
                create_datetime((2025, 4, 30), (23, 59, 59), 0),
                &[],
                None,
            ),
            create_snapshot(1, create_datetime((2025, 5, 1), (0, 0, 1), 0), &[], None),
        ];
        // KeepMonthly(2) keeps the latest per month: both are in different months.
        let rules = vec![RetentionRule::KeepMonthly(2)];
        let kept_ids =
            apply_retention_rules(&snapshots.iter().collect::<Vec<_>>(), &rules, None, now);
        assert_eq!(kept_ids, create_expected_ids(&[0, 1]));
    }

    #[test]
    fn test_keep_last_one() {
        let snapshots = create_mock_snapshots();
        let rules = vec![RetentionRule::KeepLast(1)];
        let kept_ids = apply_retention_rules(
            &snapshots.iter().collect::<Vec<_>>(),
            &rules,
            None,
            test_now(),
        );
        // Exactly the newest snapshot.
        assert_eq!(kept_ids.len(), 1);
        assert!(kept_ids.contains(&create_id(49)));
    }
}
