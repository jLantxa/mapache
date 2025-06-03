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

pub mod indexset;
pub mod url;

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow};
use blake3::Hasher;
use chrono::{DateTime, Local};

use crate::global::Hash256;

/// Calculates the 256-bit hash of a byte array
pub fn calculate_hash<T: AsRef<[u8]>>(data: T) -> Hash256 {
    let mut hasher = Hasher::new();
    hasher.update(data.as_ref());
    hasher.finalize().into()
}

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

#[allow(non_upper_case_globals)]
pub fn format_size(bytes: u64) -> String {
    if bytes >= size::TiB {
        return format!("{:.2} TiB", (bytes as f64) / (size::TiB as f64));
    } else if bytes >= size::GiB {
        return format!("{:.2} GiB", (bytes as f64) / (size::GiB as f64));
    } else if bytes >= size::MiB {
        return format!("{:.2} MiB", (bytes as f64) / (size::MiB as f64));
    } else if bytes >= size::KiB {
        return format!("{:.2} KiB", (bytes as f64) / (size::KiB as f64));
    } else {
        return format!("{} B", bytes);
    }
}

/// Calculates the longest common prefix for a set of paths
pub fn calculate_lcp(paths: &[PathBuf]) -> PathBuf {
    if paths.is_empty() {
        return PathBuf::new();
    }
    if paths.len() == 1 {
        return paths[0].clone();
    }

    let first_path = &paths[0];
    let mut common_prefix = PathBuf::new();

    for (i, first_comp) in first_path.components().enumerate() {
        let is_common = paths[1..].iter().all(|other_path| {
            other_path
                .components()
                .nth(i)
                .map_or(false, |other_comp| first_comp.eq(&other_comp))
        });

        if is_common {
            common_prefix.push(first_comp);
        } else {
            break;
        }
    }

    common_prefix
}

pub fn pretty_print_duration(duration: std::time::Duration) -> String {
    let total_seconds = duration.as_secs();
    let milliseconds = duration.subsec_millis();

    let days = total_seconds / (24 * 3600);
    let hours = (total_seconds % (24 * 3600)) / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;

    const MAX_NUM_PARTS: usize = 2;
    let mut parts = Vec::new();

    if days > 0 {
        parts.push(format!("{}d", days));
    }
    if hours > 0 || (days > 0 && (minutes > 0 || seconds > 0 || milliseconds > 0)) {
        parts.push(format!("{}h", hours));
    }
    if parts.len() < MAX_NUM_PARTS && minutes > 0
        || (hours > 0 && (seconds > 0 || milliseconds > 0))
    {
        parts.push(format!("{}m", minutes));
    }
    if parts.len() < MAX_NUM_PARTS && seconds > 0 || (minutes > 0 && milliseconds > 0) {
        parts.push(format!("{}s", seconds));
    }
    if parts.is_empty() && (milliseconds > 0) {
        parts.push(format!("{}ms", milliseconds));
    }

    if parts.is_empty() {
        "0s".to_string()
    } else {
        parts.join(" ")
    }
}

/// Parses a duration string (e.g., "1d", "2w", "3m", "4y", "5h", "6s") into a chrono::Duration.
/// Supports combinations like "1d12h".
pub fn parse_duration_string(s: &str) -> Result<chrono::Duration> {
    let mut total_duration = chrono::Duration::seconds(0);
    let mut current_num_str = String::new();

    for c in s.chars() {
        if c.is_ascii_digit() {
            current_num_str.push(c);
        } else {
            if let Ok(num) = current_num_str.parse::<i64>() {
                match c {
                    's' => total_duration = total_duration + chrono::Duration::seconds(num),
                    'm' => total_duration = total_duration + chrono::Duration::minutes(num),
                    'h' => total_duration = total_duration + chrono::Duration::hours(num),
                    'd' => total_duration = total_duration + chrono::Duration::days(num),
                    'w' => total_duration = total_duration + chrono::Duration::weeks(num),
                    'y' => total_duration = total_duration + chrono::Duration::days(num * 365), // Approximation for years
                    _ => return Err(anyhow!("Invalid duration unit: {}", c).into()),
                }
                current_num_str.clear();
            } else {
                return Err(anyhow!("Invalid duration format: {}", s).into());
            }
        }
    }

    // Handle any remaining number at the end (e.g., "123" without a unit, or if the last unit is parsed)
    if !current_num_str.is_empty() {
        return Err(anyhow!(
            "Invalid duration format: trailing number without unit in {}",
            s
        )
        .into());
    }

    Ok(total_duration)
}

pub fn format_count<T>(count: T, singular: &str, plural: &str) -> String
where
    T: std::fmt::Display + PartialEq + From<usize>,
{
    if count == T::from(1) {
        return format!("{} {}", count, singular);
    } else {
        return format!("{} {}", count, plural);
    }
}

// Extracts the parent of a path
pub fn extract_parent(path: &Path) -> Option<PathBuf> {
    path.parent().map(|p| p.to_path_buf())
}

/// For each directory between `root` and any of the `paths`,
/// return how many *distinct* children each directory has
/// and how many *distinct* children the root has.
pub fn intermediate_paths(root: &Path, paths: &[PathBuf]) -> (usize, BTreeMap<PathBuf, usize>) {
    let mut children_map: BTreeMap<PathBuf, BTreeSet<PathBuf>> = BTreeMap::new();
    let mut unique_root_children: BTreeSet<PathBuf> = BTreeSet::new();

    for full_path in paths {
        let mut current = full_path.clone();

        let mut potential_direct_root_child = None;

        while let Some(parent) = extract_parent(&current) {
            if parent <= *root {
                if parent == *root {
                    potential_direct_root_child = Some(current.clone());
                }
                break;
            }

            children_map
                .entry(parent.clone())
                .or_insert_with(BTreeSet::new)
                .insert(current.clone());

            current = parent;
        }

        if let Some(direct_child) = potential_direct_root_child {
            unique_root_children.insert(direct_child);
        }
    }

    let root_children_count = unique_root_children.len();

    let intermediate_paths = children_map
        .into_iter()
        .map(|(path, set_of_children)| (path, set_of_children.len()))
        .collect();

    (root_children_count, intermediate_paths)
}

pub fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{:02x}", byte)).collect()
}

pub fn filter_path(path: &Path, include: Option<&[PathBuf]>, exclude: Option<&[PathBuf]>) -> bool {
    if let Some(exclude_paths) = exclude {
        for ex_path in exclude_paths {
            if path.starts_with(ex_path) {
                return false;
            }
        }
    }

    if let Some(include_paths) = include {
        for in_path in include_paths {
            if !path.starts_with(in_path) && !in_path.starts_with(path) {
                return false;
            }
        }
    }

    true
}

/// Pretty prints a SystemTime
pub fn pretty_print_system_time(time: SystemTime, format_str: Option<String>) -> Result<String> {
    // Attempt to get the duration since the Unix epoch.
    // This handles cases where SystemTime might be before 1970-01-01.
    let _duration_since_epoch = time
        .duration_since(UNIX_EPOCH)
        .with_context(|| format!("SystemTime is before UNIX EPOCH"))?;

    let format = format_str.unwrap_or_else(|| "%Y-%m-%d %H:%M:%S".to_string());

    // Convert SystemTime to DateTime<Local>
    let datetime_local: DateTime<Local> = time.into();
    Ok(datetime_local.format(&format).to_string())
}

pub fn mode_to_permissions_string(mode: u32) -> String {
    let mut s = String::with_capacity(10);

    let file_type = mode & 0o170000; // Mask for file type bits
    match file_type {
        0o100000 => s.push('-'), // Regular file
        0o040000 => s.push('d'), // Directory
        0o120000 => s.push('l'), // Symbolic link
        0o010000 => s.push('p'), // Named pipe (FIFO)
        0o020000 => s.push('c'), // Character device
        0o060000 => s.push('b'), // Block device
        0o140000 => s.push('s'), // Socket
        _ => s.push('?'),        // Unknown file type
    }

    let get_rwx =
        |mode_val: u32, read_bit: u32, write_bit: u32, exec_bit: u32, special_bit: u32| {
            let mut part = ['-', '-', '-'];

            if (mode_val & read_bit) != 0 {
                part[0] = 'r';
            }
            if (mode_val & write_bit) != 0 {
                part[1] = 'w';
            }

            if (mode_val & exec_bit) != 0 {
                if (mode_val & special_bit) != 0 {
                    if special_bit == 0o1000 {
                        part[2] = 't';
                    } else {
                        part[2] = 's';
                    }
                } else {
                    part[2] = 'x';
                }
            } else {
                if (mode_val & special_bit) != 0 {
                    if special_bit == 0o1000 {
                        part[2] = 'T';
                    } else {
                        part[2] = 'S';
                    }
                } else {
                    part[2] = '-';
                }
            }
            part
        };

    let owner_rwx = get_rwx(mode, 0o400, 0o200, 0o100, 0o4000);
    s.extend(owner_rwx.iter());

    let group_rwx = get_rwx(mode, 0o040, 0o020, 0o010, 0o2000);
    s.extend(group_rwx.iter());

    // 4. Others permissions (rwx, potentially t/T for Sticky
    let others_rwx = get_rwx(mode, 0o004, 0o002, 0o001, 0o1000);
    s.extend(others_rwx.iter());

    s
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test consistency of the hash function
    #[test]
    fn test_hash_function() {
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
        assert_eq!(format_size(1), "1 B");
        assert_eq!(format_size(324), "324 B");
        assert_eq!(format_size(1_205), "1.18 KiB");
        assert_eq!(format_size(124_112), "121.20 KiB");
        assert_eq!(format_size(1_045_024), "1020.53 KiB");
        assert_eq!(format_size(12_995_924), "12.39 MiB");
        assert_eq!(format_size(1_500_000_000), "1.40 GiB");
        assert_eq!(format_size(2_100_000_100_000), "1.91 TiB");
    }

    #[test]
    fn test_calculate_lcp() {
        let paths: Vec<PathBuf> = vec![];
        assert_eq!(calculate_lcp(&paths), PathBuf::new());

        let paths = vec![PathBuf::from("/home/user/docs")];
        assert_eq!(calculate_lcp(&paths), PathBuf::from("/home/user/docs"));

        let paths = vec![
            PathBuf::from("/home/user/a"),
            PathBuf::from("/home/user/b/file.txt"),
            PathBuf::from("/home/user/c"),
        ];
        assert_eq!(calculate_lcp(&paths), PathBuf::from("/home/user"));

        let paths = vec![
            PathBuf::from("/home/user/docs"),
            PathBuf::from("/etc"),
            PathBuf::from("/var/log"),
        ];
        assert_eq!(calculate_lcp(&paths), PathBuf::from("/"));

        let paths = vec![
            PathBuf::from("a/b/c"),
            PathBuf::from("a/b/d"),
            PathBuf::from("a/b"),
        ];
        assert_eq!(calculate_lcp(&paths), PathBuf::from("a/b"));

        let paths = vec![PathBuf::from("a/b"), PathBuf::from("x/y")];
        assert_eq!(calculate_lcp(&paths), PathBuf::new()); // LCP of a/b and x/y is ""

        let paths = vec![PathBuf::from("/home/user/a"), PathBuf::from("a")];
        assert_eq!(calculate_lcp(&paths), PathBuf::new());
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
    fn test_intermediate_paths() {
        let root = PathBuf::from("/");
        let paths = vec![
            PathBuf::from("/a/b/c"),
            PathBuf::from("/a/b/d"),
            PathBuf::from("/a/e"),
        ];

        let (root_children_count, intermediate_paths) = intermediate_paths(&root, &paths);

        let mut expected = BTreeMap::new();
        expected.insert(PathBuf::from("/a"), 2);
        expected.insert(PathBuf::from("/a/b"), 2);

        assert_eq!(root_children_count, 1);
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
}
