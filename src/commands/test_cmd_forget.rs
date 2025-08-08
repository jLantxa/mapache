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

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeSet, HashSet},
        path::PathBuf,
    };

    use chrono::{DateTime, Duration, Local, NaiveDate, TimeZone};

    use crate::{
        commands::cmd_forget::{RetentionRule, apply_retention_rules},
        global::ID,
        repository::snapshot::Snapshot,
    };

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
                    parent: None,
                    tree: ID::from_hex(
                        "0000000000000000000000000000000000000000000000000000000000000000",
                    )
                    .unwrap(),
                    root: PathBuf::from("/"),
                    paths: Vec::new(),
                    exclude: Vec::new(),
                    tags: ["tag0".to_string(), "tag1".to_string()]
                        .into_iter()
                        .map(|s| s.to_string())
                        .collect(),
                    description: None,
                    summary: Default::default(),
                    hostname: None,
                    username: None,
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
                    parent: None,
                    tree: ID::from_hex(
                        "0000000000000000000000000000000000000000000000000000000000000001",
                    )
                    .unwrap(),
                    root: PathBuf::from("/"),
                    paths: Vec::new(),
                    exclude: Vec::new(),
                    tags: ["tag0".to_string()]
                        .into_iter()
                        .map(|s| s.to_string())
                        .collect(),
                    description: None,
                    summary: Default::default(),
                    hostname: None,
                    username: None,
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
                    parent: None,
                    tree: ID::from_hex(
                        "0000000000000000000000000000000000000000000000000000000000000002",
                    )
                    .unwrap(),
                    root: PathBuf::from("/"),
                    paths: Vec::new(),
                    exclude: Vec::new(),
                    tags: BTreeSet::new(),
                    description: None,
                    summary: Default::default(),
                    hostname: None,
                    username: None,
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
                    parent: None,
                    tree: ID::from_hex(
                        "0000000000000000000000000000000000000000000000000000000000000003",
                    )
                    .unwrap(),
                    root: PathBuf::from("/"),
                    paths: Vec::new(),
                    exclude: Vec::new(),
                    tags: BTreeSet::new(),
                    description: None,
                    summary: Default::default(),
                    hostname: None,
                    username: None,
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
                    parent: None,
                    tree: ID::from_hex(
                        "0000000000000000000000000000000000000000000000000000000000000004",
                    )
                    .unwrap(),
                    root: PathBuf::from("/"),
                    paths: Vec::new(),
                    exclude: Vec::new(),
                    tags: BTreeSet::new(),
                    description: None,
                    summary: Default::default(),
                    hostname: None,
                    username: None,
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
                    parent: None,
                    tree: ID::from_hex(
                        "0000000000000000000000000000000000000000000000000000000000000005",
                    )
                    .unwrap(),
                    root: PathBuf::from("/"),
                    paths: Vec::new(),
                    exclude: Vec::new(),
                    tags: BTreeSet::new(),
                    description: None,
                    summary: Default::default(),
                    hostname: None,
                    username: None,
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
                    parent: None,
                    tree: ID::from_hex(
                        "0000000000000000000000000000000000000000000000000000000000000006",
                    )
                    .unwrap(),
                    root: PathBuf::from("/"),
                    paths: Vec::new(),
                    exclude: Vec::new(),
                    tags: BTreeSet::new(),
                    description: None,
                    summary: Default::default(),
                    hostname: None,
                    username: None,
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
                    parent: None,
                    tree: ID::from_hex(
                        "0000000000000000000000000000000000000000000000000000000000000106",
                    )
                    .unwrap(),
                    root: PathBuf::from("/"),
                    paths: Vec::new(),
                    exclude: Vec::new(),
                    tags: BTreeSet::new(),
                    description: None,
                    summary: Default::default(),
                    hostname: None,
                    username: None,
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
                    parent: None,
                    tree: ID::from_hex(
                        "0000000000000000000000000000000000000000000000000000000000000206",
                    )
                    .unwrap(),
                    root: PathBuf::from("/"),
                    paths: Vec::new(),
                    exclude: Vec::new(),
                    tags: BTreeSet::new(),
                    description: None,
                    summary: Default::default(),
                    hostname: None,
                    username: None,
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
                    parent: None,
                    tree: ID::from_hex(
                        "0000000000000000000000000000000000000000000000000000000000000007",
                    )
                    .unwrap(),
                    root: PathBuf::from("/"),
                    paths: Vec::new(),
                    exclude: Vec::new(),
                    tags: BTreeSet::new(),
                    description: None,
                    summary: Default::default(),
                    hostname: None,
                    username: None,
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
                    parent: None,
                    tree: ID::from_hex(
                        "0000000000000000000000000000000000000000000000000000000000000008",
                    )
                    .unwrap(),
                    root: PathBuf::from("/"),
                    paths: Vec::new(),
                    exclude: Vec::new(),
                    tags: BTreeSet::new(),
                    description: None,
                    summary: Default::default(),
                    hostname: None,
                    username: None,
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
                    parent: None,
                    tree: ID::from_hex(
                        "0000000000000000000000000000000000000000000000000000000000000009",
                    )
                    .unwrap(),
                    root: PathBuf::from("/"),
                    paths: Vec::new(),
                    exclude: Vec::new(),
                    tags: BTreeSet::new(),
                    description: None,
                    summary: Default::default(),
                    hostname: None,
                    username: None,
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
                    parent: None,
                    tree: ID::from_hex(
                        "000000000000000000000000000000000000000000000000000000000000000A",
                    )
                    .unwrap(),
                    root: PathBuf::from("/"),
                    paths: Vec::new(),
                    exclude: Vec::new(),
                    tags: BTreeSet::new(),
                    description: None,
                    summary: Default::default(),
                    hostname: None,
                    username: None,
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
                    parent: None,
                    tree: ID::from_hex(
                        "000000000000000000000000000000000000000000000000000000000000000B",
                    )
                    .unwrap(),
                    root: PathBuf::from("/"),
                    paths: Vec::new(),
                    exclude: Vec::new(),
                    tags: BTreeSet::new(),
                    description: None,
                    summary: Default::default(),
                    hostname: None,
                    username: None,
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
                    parent: None,
                    tree: ID::from_hex(
                        "000000000000000000000000000000000000000000000000000000000000000C",
                    )
                    .unwrap(),
                    root: PathBuf::from("/"),
                    paths: Vec::new(),
                    exclude: Vec::new(),
                    tags: BTreeSet::new(),
                    description: None,
                    summary: Default::default(),
                    hostname: None,
                    username: None,
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
    fn test_keep_tags() {
        let snapshots = create_mock_snapshots();
        let rules = vec![RetentionRule::KeepTags(
            ["tag0"].into_iter().map(|s| s.to_string()).collect(),
        )];

        let kept_ids = apply_retention_rules(&snapshots, &rules, test_now());

        let expected_ids: HashSet<ID> = [
            ID::from_hex("0000000000000000000000000000000000000000000000000000000000000000")
                .unwrap(),
            ID::from_hex("0000000000000000000000000000000000000000000000000000000000000001")
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
            RetentionRule::KeepTags(["tag1"].into_iter().map(|s| s.to_string()).collect()),
        ];

        let kept_ids = apply_retention_rules(&snapshots, &rules, test_now());

        let expected_ids: HashSet<ID> = [
            // Has tag1
            ID::from_hex("0000000000000000000000000000000000000000000000000000000000000000")
                .unwrap(),
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
