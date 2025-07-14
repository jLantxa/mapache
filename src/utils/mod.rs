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

pub mod indexset;
pub mod url;

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow};
use blake3::Hasher;
use chrono::{DateTime, Duration, Local};

use crate::global::Hash256;

// --- Constants ---

#[allow(non_upper_case_globals)]
pub mod size {
    pub const KiB: u64 = 1024;
    pub const MiB: u64 = KiB * 1024;
    pub const GiB: u64 = MiB * 1024;
    pub const TiB: u64 = GiB * 1024;

    pub const KB: u64 = 1000;
    pub const MB: u64 = KB * 1000;
    pub const GB: u64 = MB * 1000;
    pub const TB: u64 = GB * 1000;
}

// --- Password ---

pub fn get_password_from_file(password_file_path: &Option<PathBuf>) -> Result<Option<String>> {
    password_file_path
        .as_ref()
        .map(|path| {
            std::fs::read_to_string(path).with_context(|| {
                format!("Could not read repository password from {}", path.display())
            })
        })
        .transpose() // Converts Option<Result<T, E>> to Result<Option<T>, E>
}

// --- Hashing ---

/// Calculates the 256-bit BLAKE3 hash of a byte array.
#[inline]
pub fn calculate_hash<T: AsRef<[u8]>>(data: T) -> Hash256 {
    let mut hasher = Hasher::new();
    hasher.update(data.as_ref());
    hasher.finalize().into()
}

// --- Formatting ---

/// Formats a byte count into a human-readable string with binary prefixes (KiB, MiB, etc.).
#[allow(non_upper_case_globals)]
pub fn format_size(bytes: u64, precision: usize) -> String {
    if bytes >= size::TiB {
        format!("{:.precision$} TiB", (bytes as f64) / (size::TiB as f64))
    } else if bytes >= size::GiB {
        format!("{:.precision$} GiB", (bytes as f64) / (size::GiB as f64))
    } else if bytes >= size::MiB {
        format!("{:.precision$} MiB", (bytes as f64) / (size::MiB as f64))
    } else if bytes >= size::KiB {
        format!("{:.precision$} KiB", (bytes as f64) / (size::KiB as f64))
    } else {
        format!("{bytes} B")
    }
}

/// Formats a count with appropriate singular or plural suffix.
pub fn format_count<T>(count: T, singular: &str, plural: &str) -> String
where
    T: std::fmt::Display + PartialEq + From<u8>,
{
    if count == T::from(1) {
        format!("{count} {singular}")
    } else {
        format!("{count} {plural}")
    }
}

/// Converts a byte slice to its hexadecimal string representation.
pub fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        s.push_str(&format!("{byte:02x}"));
    }
    s
}

/// Pretty prints a `SystemTime` into a human-readable string,
/// defaulting to "%Y-%m-%d %H:%M:%S" format if not specified.
///
/// Returns an `Err` if the `SystemTime` is before the Unix epoch.
pub fn pretty_print_system_time(time: SystemTime, format_str: Option<&str>) -> Result<String> {
    time.duration_since(UNIX_EPOCH)
        .with_context(|| format!("SystemTime {time:?} is before UNIX EPOCH"))?;

    let format = format_str.unwrap_or("%Y-%m-%d %H:%M:%S");

    let datetime_local: DateTime<Local> = time.into();
    Ok(datetime_local.format(format).to_string())
}

pub fn pretty_print_timestamp(timestamp: &DateTime<Local>) -> String {
    timestamp
        .with_timezone(&Local)
        .format("%Y-%m-%d %H:%M:%S %Z")
        .to_string()
}

// --- Path Utilities ---

/// Calculates the longest common prefix for a set of paths.
/// Returns an empty PathBuf if the input is empty or if no common prefix exists.
/// If `strict_prefix` is true, the LCP of a single path is itself,
/// otherwise, the LCP is the parent:
///
/// - `true`: a/b/c -> a/b/c
/// - `false`: a/b/c -> a/b
pub fn calculate_lcp(paths: &[PathBuf], strict_prefix: bool) -> PathBuf {
    if paths.is_empty() {
        return PathBuf::new();
    } else if paths.len() == 1 {
        if strict_prefix {
            return paths[0].clone();
        } else {
            return extract_parent(&paths[0]).expect("Path should have a parent");
        }
    }

    let mut common_prefix = PathBuf::new();
    let mut iterators: Vec<_> = paths.iter().map(|p| p.components()).collect();

    'outer: loop {
        let current_components: Vec<_> = iterators.iter_mut().map(|it| it.next()).collect();

        if current_components.iter().any(Option::is_none) {
            break 'outer;
        }

        let first_comp = current_components[0].as_ref().unwrap();
        let all_match = current_components[1..]
            .iter()
            .all(|comp_opt| comp_opt.as_ref().is_some_and(|comp| comp.eq(first_comp)));

        if all_match {
            common_prefix.push(first_comp);
        } else {
            break 'outer;
        }
    }

    common_prefix
}

/// Extracts the parent path of a given path.
/// Returns `None` if the path has no parent (e.g., "/" or "file.txt" in current dir).
#[inline]
pub fn extract_parent(path: &Path) -> Option<PathBuf> {
    path.parent().map(PathBuf::from)
}

/// For each directory between `root` and any of the `paths`,
/// return how many *distinct* direct children each intermediate directory has,
/// and how many *distinct* direct children the `root` itself has.
///
/// The returned `BTreeMap` keys are the intermediate parent paths.
/// The `usize` value is the count of distinct direct children under that parent.
/// The first element of the tuple is the count of distinct direct children under `root`.
pub fn get_intermediate_paths(root: &Path, paths: &[PathBuf]) -> (usize, BTreeMap<PathBuf, usize>) {
    let mut children_map: BTreeMap<PathBuf, BTreeSet<PathBuf>> = BTreeMap::new();
    let mut unique_root_children: BTreeSet<PathBuf> = BTreeSet::new();

    for full_path in paths {
        let mut current_ancestor = full_path.as_path();

        let mut direct_root_child: Option<&Path> = None;

        while let Some(parent) = current_ancestor.parent() {
            if parent <= root {
                if parent == root {
                    direct_root_child = Some(current_ancestor);
                }
                break;
            }

            children_map
                .entry(parent.to_path_buf())
                .or_default()
                .insert(current_ancestor.to_path_buf());

            current_ancestor = parent;
        }

        if let Some(child_of_root) = direct_root_child {
            unique_root_children.insert(child_of_root.to_path_buf());
        }
    }

    let root_children_count = unique_root_children.len();

    let intermediate_counts = children_map
        .into_iter()
        .map(|(path, set_of_children)| (path, set_of_children.len()))
        .collect();

    (root_children_count, intermediate_counts)
}

/// Filters a path based on include and exclude rules.
/// Returns `true` if the path should be included, `false` otherwise.
/// Exclude rules take precedence over include rules.
///
/// # Arguments
/// * `path` - The path to filter.
/// * `include` - An optional slice of paths to include. If `None`, all paths are implicitly included
///   unless excluded. If `Some`, the path must either be an include path, a descendant
///   of an include path, or an ancestor of an include path.
///
/// * `exclude` - An optional slice of paths to exclude. If `Some`, the path is excluded if it
///   starts with any of the exclude paths.
///
/// # Behavior
/// 1. If `path` starts with any `exclude_path`, it's excluded (returns `false`).
/// 2. If `include_paths` is `Some`:
///    a. The path is included only if it is either:
///      - An include path itself (`path == in_path`).
///      - A descendant of an include path (`path.starts_with(in_path)`).
///      - An ancestor of an include path (`in_path.starts_with(path)`).
///
///    b. If it doesn't satisfy any of these conditions for any `include_path`, it's excluded (returns `false`).
/// 3. If `include_paths` is `None` and not excluded by step 1, it's included (returns `true`).
pub fn filter_path(
    path: &Path,
    include: Option<&Vec<PathBuf>>,
    exclude: Option<&Vec<PathBuf>>,
) -> bool {
    if let Some(exclude_paths) = exclude {
        for ex_path in exclude_paths {
            if path.starts_with(ex_path) {
                return false;
            }
        }
    }

    if let Some(include_paths) = include {
        let is_included = include_paths
            .iter()
            .any(|in_path| path.starts_with(in_path) || in_path.starts_with(path));

        if !is_included {
            return false;
        }
    }

    true
}

// --- Duration Utilities ---

/// Pretty prints a `std::time::Duration` in a human-readable format.
/// Attempts to show up to two most significant units.
pub fn pretty_print_duration(duration: std::time::Duration) -> String {
    let total_seconds = duration.as_secs();
    let milliseconds = duration.subsec_millis();

    let days = total_seconds / (24 * 3600);
    let rem_seconds = total_seconds % (24 * 3600);
    let hours = rem_seconds / 3600;
    let rem_seconds = rem_seconds % 3600;
    let minutes = rem_seconds / 60;
    let seconds = rem_seconds % 60;

    let mut parts = Vec::with_capacity(2);

    if days > 0 {
        parts.push(format!("{days}d"));
    }
    if (hours > 0 || (days > 0 && minutes > 0) || (days > 0 && seconds > 0 && parts.is_empty()))
        && parts.len() < 2
    {
        parts.push(format!("{hours}h"));
    }
    if (minutes > 0 || (hours > 0 && seconds > 0 && parts.is_empty())) && parts.len() < 2 {
        parts.push(format!("{minutes}m"));
    }
    if (seconds > 0 || (minutes > 0 && milliseconds > 0 && parts.is_empty())) && parts.len() < 2 {
        parts.push(format!("{seconds}s"));
    }
    if parts.is_empty() && milliseconds > 0 {
        parts.push(format!("{milliseconds}ms"));
    }

    if parts.is_empty() {
        "0s".to_string()
    } else {
        parts.join(" ")
    }
}

/// Parses a duration string (e.g., "1d", "2w", "3m", "4y", "5h", "6s") into a `chrono::Duration`.
/// Supports combinations like "1d12h".
///
/// # Supported Units:
/// - `s`: seconds
/// - `m`: minutes
/// - `h`: hours
/// - `d`: days
/// - `w`: weeks
/// - `y`: years (approximated as 365 days)
pub fn parse_duration_string(s: &str) -> Result<Duration> {
    let mut total_duration = Duration::seconds(0);
    let mut current_num_str = String::new();
    let chars = s.chars().peekable();

    for c in chars {
        if c.is_ascii_digit() {
            current_num_str.push(c);
        } else {
            if current_num_str.is_empty() {
                return Err(anyhow!(
                    "Invalid duration format: unit '{}' without preceding number in \"{}\"",
                    c,
                    s
                ));
            }

            let num = current_num_str
                .parse::<i64>()
                .with_context(|| format!("Failed to parse number before unit '{c}' in \"{s}\""))?;

            match c {
                's' => total_duration += Duration::seconds(num),
                'm' => total_duration += Duration::minutes(num),
                'h' => total_duration += Duration::hours(num),
                'd' => total_duration += Duration::days(num),
                'w' => total_duration += Duration::weeks(num),
                'y' => total_duration += Duration::days(num * 365),
                _ => return Err(anyhow!("Invalid duration unit: '{}' in \"{}\"", c, s)),
            }
            current_num_str.clear();
        }
    }

    if !current_num_str.is_empty() {
        return Err(anyhow!(
            "Invalid duration format: trailing number '{}' without unit in \"{}\"",
            current_num_str,
            s
        ));
    }

    Ok(total_duration)
}

// --- Permissions Utilities ---

/// Converts a Unix file mode (as `u32`) into a human-readable permission string
/// (e.g., "-rwxr-xr-x" like `ls -l`).
pub fn mode_to_permissions_string(mode: u32) -> String {
    let mut s = String::with_capacity(10);

    let file_type_mask = 0o170000;
    match mode & file_type_mask {
        0o100000 => s.push('-'),
        0o040000 => s.push('d'),
        0o120000 => s.push('l'),
        0o010000 => s.push('p'),
        0o020000 => s.push('c'),
        0o060000 => s.push('b'),
        0o140000 => s.push('s'),
        _ => s.push('?'),
    }

    let get_rwx_char =
        |mode_val: u32, read_bit: u32, write_bit: u32, exec_bit: u32, special_bit: u32| {
            let mut char_arr = ['-', '-', '-'];

            if (mode_val & read_bit) != 0 {
                char_arr[0] = 'r';
            }
            if (mode_val & write_bit) != 0 {
                char_arr[1] = 'w';
            }

            if (mode_val & exec_bit) != 0 {
                if (mode_val & special_bit) != 0 {
                    char_arr[2] = if special_bit == 0o1000 { 't' } else { 's' };
                } else {
                    char_arr[2] = 'x';
                }
            } else if (mode_val & special_bit) != 0 {
                char_arr[2] = if special_bit == 0o1000 { 'T' } else { 'S' };
            } else {
                char_arr[2] = '-';
            }
            char_arr
        };

    s.extend(get_rwx_char(mode, 0o400, 0o200, 0o100, 0o4000).iter());

    s.extend(get_rwx_char(mode, 0o040, 0o020, 0o010, 0o2000).iter());

    s.extend(get_rwx_char(mode, 0o004, 0o002, 0o001, 0o1000).iter());

    s
}

// --- Tests ---
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_hash() {
        let data = br#"
             Lorem ipsum dolor sit amet, consectetur adipisici elit, sed eiusmod tempor incidunt
             ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation
             ullamco laboris nisi ut aliquid ex ea commodi consequat. Quis aute iure reprehenderit in
             voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint obcaecat
             cupiditat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.
             "#;
        let hash = calculate_hash(data);

        assert_eq!(
            bytes_to_hex(&hash),
            "28ff314ca7c551552d4d2f4be86fd2348749ace0fbda1a051038bdb493c10a4d"
        );
    }

    #[test]
    fn test_format_size() {
        // With one decimal
        assert_eq!(format_size(0, 1), "0 B");
        assert_eq!(format_size(1, 1), "1 B");
        assert_eq!(format_size(324, 1), "324 B");
        assert_eq!(format_size(1_205, 1), "1.2 KiB");
        assert_eq!(format_size(124_112, 1), "121.2 KiB");
        assert_eq!(format_size(1_045_024, 1), "1020.5 KiB");
        assert_eq!(format_size(12_995_924, 1), "12.4 MiB");
        assert_eq!(format_size(1_500_000_000, 1), "1.4 GiB");
        assert_eq!(format_size(2_100_000_100_000, 1), "1.9 TiB");

        // With two decimals
        assert_eq!(format_size(0, 2), "0 B");
        assert_eq!(format_size(1, 2), "1 B");
        assert_eq!(format_size(324, 2), "324 B");
        assert_eq!(format_size(1_205, 2), "1.18 KiB");
        assert_eq!(format_size(124_112, 2), "121.20 KiB");
        assert_eq!(format_size(1_045_024, 2), "1020.53 KiB");
        assert_eq!(format_size(12_995_924, 2), "12.39 MiB");
        assert_eq!(format_size(1_500_000_000, 2), "1.40 GiB");
        assert_eq!(format_size(2_100_000_100_000, 2), "1.91 TiB");

        // With three decimals
        assert_eq!(format_size(0, 3), "0 B");
        assert_eq!(format_size(1, 3), "1 B");
        assert_eq!(format_size(324, 3), "324 B");
        assert_eq!(format_size(1_205, 3), "1.177 KiB");
        assert_eq!(format_size(124_112, 3), "121.203 KiB");
        assert_eq!(format_size(1_045_024, 3), "1020.531 KiB");
        assert_eq!(format_size(12_995_924, 3), "12.394 MiB");
        assert_eq!(format_size(1_500_000_000, 3), "1.397 GiB");
        assert_eq!(format_size(2_100_000_100_000, 3), "1.910 TiB");
    }

    #[test]
    fn test_calculate_lcp() {
        let paths: Vec<PathBuf> = vec![];
        assert_eq!(calculate_lcp(&paths, true), PathBuf::new());

        let paths = vec![PathBuf::from("/home/user/docs")];
        assert_eq!(
            calculate_lcp(&paths, true),
            PathBuf::from("/home/user/docs")
        );

        let paths = vec![PathBuf::from("/home/user/docs")];
        assert_eq!(calculate_lcp(&paths, false), PathBuf::from("/home/user"));

        let paths = vec![
            PathBuf::from("/home/user/a"),
            PathBuf::from("/home/user/b/file.txt"),
            PathBuf::from("/home/user/c"),
        ];
        assert_eq!(calculate_lcp(&paths, true), PathBuf::from("/home/user"));

        let paths = vec![
            PathBuf::from("/home/user/docs"),
            PathBuf::from("/etc"),
            PathBuf::from("/var/log"),
        ];
        assert_eq!(calculate_lcp(&paths, true), PathBuf::from("/"));

        let paths = vec![
            PathBuf::from("a/b/c"),
            PathBuf::from("a/b/d"),
            PathBuf::from("a/b"),
        ];
        assert_eq!(calculate_lcp(&paths, true), PathBuf::from("a/b"));

        let paths = vec![PathBuf::from("a/b"), PathBuf::from("x/y")];
        assert_eq!(calculate_lcp(&paths, true), PathBuf::new());

        let paths = vec![PathBuf::from("/home/user/a"), PathBuf::from("a")];
        assert_eq!(calculate_lcp(&paths, true), PathBuf::new());
    }

    #[test]
    fn test_bytes_to_hex() {
        let bytes: [u8; 32] = [
            0x1a, 0x2b, 0x3c, 0x4d, 0x5e, 0x6f, 0x7a, 0x8b, 0x9c, 0x0d, 0x1e, 0x2f, 0x3a, 0x4b,
            0x5c, 0x6d, 0x7e, 0x8f, 0x9a, 0x0b, 0x1c, 0x2d, 0x3e, 0x4f, 0x5a, 0x6b, 0x7c, 0x8d,
            0x9e, 0x0f, 0x10, 0x21,
        ];
        let hex_str = bytes_to_hex(&bytes);
        assert_eq!(
            hex_str,
            "1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f9a0b1c2d3e4f5a6b7c8d9e0f1021"
        );
    }

    #[test]
    fn test_get_intermediate_paths() {
        let root = PathBuf::from("/");
        let paths = vec![
            PathBuf::from("/a/b/c"),
            PathBuf::from("/a/b/d"),
            PathBuf::from("/a/e"),
        ];
        let (root_children_count, intermediate_paths) = get_intermediate_paths(&root, &paths);
        let mut expected = BTreeMap::new();
        expected.insert(PathBuf::from("/a"), 2);
        expected.insert(PathBuf::from("/a/b"), 2);
        assert_eq!(root_children_count, 1);
        assert_eq!(intermediate_paths, expected);

        // Test with root as a subpath
        let root = PathBuf::from("/a");
        let paths = vec![
            PathBuf::from("/a/b/c"),
            PathBuf::from("/a/b/d"),
            PathBuf::from("/a/e"),
        ];
        let (root_children_count, intermediate_paths) = get_intermediate_paths(&root, &paths);
        let mut expected = BTreeMap::new();
        expected.insert(PathBuf::from("/a/b"), 2);
        assert_eq!(root_children_count, 2); // 'b' and 'e' are direct children of '/a'
        assert_eq!(intermediate_paths, expected);
    }

    #[test]
    fn test_mode_to_permissions_string() {
        assert_eq!(mode_to_permissions_string(0o100755), "-rwxr-xr-x");
        assert_eq!(mode_to_permissions_string(0o100644), "-rw-r--r--");
        assert_eq!(mode_to_permissions_string(0o100700), "-rwx------");
        assert_eq!(mode_to_permissions_string(0o100000), "----------");
        assert_eq!(mode_to_permissions_string(0o040755), "drwxr-xr-x");
        assert_eq!(mode_to_permissions_string(0o040700), "drwx------");
        assert_eq!(mode_to_permissions_string(0o120777), "lrwxrwxrwx");
        assert_eq!(mode_to_permissions_string(0o104755), "-rwsr-xr-x");
        assert_eq!(mode_to_permissions_string(0o102755), "-rwxr-sr-x");
        assert_eq!(mode_to_permissions_string(0o041777), "drwxrwxrwt");
        assert_eq!(mode_to_permissions_string(0o104644), "-rwSr--r--");
        assert_eq!(mode_to_permissions_string(0o102644), "-rw-r-Sr--");
        assert_eq!(mode_to_permissions_string(0o041644), "drw-r--r-T");
        assert_eq!(mode_to_permissions_string(0o020666), "crw-rw-rw-");
        assert_eq!(mode_to_permissions_string(0o060660), "brw-rw----");
        assert_eq!(mode_to_permissions_string(0o010666), "prw-rw-rw-");
        assert_eq!(mode_to_permissions_string(0o140666), "srw-rw-rw-");
        assert_eq!(mode_to_permissions_string(0o000755), "?rwxr-xr-x");
    }

    #[test]
    fn test_pretty_print_duration() {
        assert_eq!(
            pretty_print_duration(std::time::Duration::from_secs(0)),
            "0s"
        );
        assert_eq!(
            pretty_print_duration(std::time::Duration::from_millis(500)),
            "500ms"
        );
        assert_eq!(
            pretty_print_duration(std::time::Duration::from_secs(1)),
            "1s"
        );
        assert_eq!(
            pretty_print_duration(std::time::Duration::from_secs(59)),
            "59s"
        );
        assert_eq!(
            pretty_print_duration(std::time::Duration::from_secs(60)),
            "1m"
        );
        assert_eq!(
            pretty_print_duration(std::time::Duration::from_secs(61)),
            "1m 1s"
        );
        assert_eq!(
            pretty_print_duration(std::time::Duration::from_secs(3599)),
            "59m 59s"
        );
        assert_eq!(
            pretty_print_duration(std::time::Duration::from_secs(3600)),
            "1h"
        );
        assert_eq!(
            pretty_print_duration(std::time::Duration::from_secs(3601)),
            "1h 1s"
        );
        assert_eq!(
            pretty_print_duration(std::time::Duration::from_secs(3660)),
            "1h 1m"
        );
        assert_eq!(
            pretty_print_duration(std::time::Duration::from_secs(86399)),
            "23h 59m"
        );
        assert_eq!(
            pretty_print_duration(std::time::Duration::from_secs(86400)),
            "1d"
        );
        assert_eq!(
            pretty_print_duration(std::time::Duration::from_secs(86401)),
            "1d 1s"
        );
        assert_eq!(
            pretty_print_duration(std::time::Duration::new(86400 * 2 + 3600 + 60 + 1, 0)),
            "2d 1h"
        );
        assert_eq!(
            pretty_print_duration(std::time::Duration::new(86400 * 7, 0)),
            "7d"
        ); // 1 week
    }

    #[test]
    fn test_parse_duration_string() {
        assert_eq!(parse_duration_string("1s").unwrap(), Duration::seconds(1));
        assert_eq!(parse_duration_string("1m").unwrap(), Duration::minutes(1));
        assert_eq!(parse_duration_string("1h").unwrap(), Duration::hours(1));
        assert_eq!(parse_duration_string("1d").unwrap(), Duration::days(1));
        assert_eq!(parse_duration_string("1w").unwrap(), Duration::weeks(1));
        assert_eq!(parse_duration_string("1y").unwrap(), Duration::days(365));
        assert_eq!(
            parse_duration_string("100s").unwrap(),
            Duration::seconds(100)
        );
        assert_eq!(
            parse_duration_string("2m30s").unwrap(),
            Duration::minutes(2) + Duration::seconds(30)
        );
        assert_eq!(
            parse_duration_string("1d12h30m").unwrap(),
            Duration::days(1) + Duration::hours(12) + Duration::minutes(30)
        );
        assert_eq!(
            parse_duration_string("1y2w3d4h5m6s").unwrap(),
            Duration::days(365)
                + Duration::weeks(2)
                + Duration::days(3)
                + Duration::hours(4)
                + Duration::minutes(5)
                + Duration::seconds(6)
        );

        // Test invalid formats
        assert!(parse_duration_string("1").is_err());
        assert!(parse_duration_string("s").is_err());
        assert!(parse_duration_string("1as").is_err());
        assert!(parse_duration_string("1d1").is_err());
        assert!(parse_duration_string("1d 2h").is_err()); // spaces are not supported
    }

    #[test]
    fn test_filter_path() {
        let path1 = PathBuf::from("/a/b/c");
        let path2 = PathBuf::from("/x/y/z");
        let path3 = PathBuf::from("/a/b");
        let path4 = PathBuf::from("/a/b/c/d");

        // No include/exclude
        assert!(filter_path(&path1, None, None));

        // Exclude only
        assert!(!filter_path(&path1, None, Some(&vec![PathBuf::from("/a")])));
        assert!(filter_path(&path2, None, Some(&vec![PathBuf::from("/a")])));
        assert!(!filter_path(
            &path4,
            None,
            Some(&vec![PathBuf::from("/a/b/c")])
        ));

        // Include only
        assert!(filter_path(&path1, Some(&vec![PathBuf::from("/a")]), None));
        assert!(!filter_path(&path2, Some(&vec![PathBuf::from("/a")]), None));
        assert!(filter_path(
            &path3,
            Some(&vec![PathBuf::from("/a/b/c")]),
            None
        ));
        assert!(filter_path(
            &path4,
            Some(&vec![PathBuf::from("/a/b/c")]),
            None
        ));

        // Exclude and Include
        // Exclude takes precedence
        assert!(!filter_path(
            &path1,
            Some(&vec![PathBuf::from("/a")]),
            Some(&vec![PathBuf::from("/a/b")])
        ));
        assert!(!filter_path(
            &path2,
            Some(&vec![PathBuf::from("/a")]),
            Some(&vec![PathBuf::from("/a")])
        ));
        assert!(filter_path(
            &path1,
            Some(&vec![PathBuf::from("/a/b/c")]),
            Some(&vec![PathBuf::from("/x")])
        ))
    }
}
