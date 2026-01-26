pub mod collections;
pub mod filter;
pub mod url;

use std::{
    collections::BTreeMap,
    ffi::OsString,
    path::{MAIN_SEPARATOR, Path, PathBuf, is_separator},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow};
use blake3::Hasher;
use chrono::{DateTime, Duration, Local};
use rustc_hash::{FxHashMap, FxHashSet};

use crate::{mapache::Hash256, repository::repo::Auth};

// --- Constants ---

/// Size units.
#[allow(non_upper_case_globals)]
pub mod size {
    // As of 2025, the TB / TiB units are biggest units that one would
    // normally expect in a backup. Sizes bigger than the TB / TiB will still
    // be represented using those units. If this changes in the future, make
    // sure to add the appropriate units here and the formatting functions.

    // Binary units.
    pub const KiB: u64 = 1024;
    pub const MiB: u64 = KiB * 1024;
    pub const GiB: u64 = MiB * 1024;
    pub const TiB: u64 = GiB * 1024;

    // Decimal units.
    pub const kB: u64 = 1000;
    pub const MB: u64 = kB * 1000;
    pub const GB: u64 = MB * 1000;
    pub const TB: u64 = GB * 1000;
}

// --- Password ---

pub fn get_auth_from_file(password_file_path: &Option<PathBuf>) -> Result<Option<Auth>> {
    match password_file_path {
        None => Ok(None),
        Some(path) => {
            let text = std::fs::read_to_string(path).with_context(|| {
                format!("Could not read repository password from {}", path.display())
            })?;

            // Procesa el texto para obtener el username y la password
            let mut lines = text.lines();
            let username = lines
                .next()
                .ok_or_else(|| anyhow::anyhow!("File {} is empty", path.display()))?
                .to_string();

            let password = lines
                .next()
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "File {} is missing the password on the second line",
                        path.display()
                    )
                })?
                .to_string();

            Ok(Some(Auth { username, password }))
        }
    }
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
pub fn format_size_binary(bytes: u64, precision: usize) -> String {
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

/// Formats a byte count into a human-readable string with decimal prefixes (kB, MB, etc.).
#[allow(non_upper_case_globals)]
pub fn format_size_decimal(bytes: u64, precision: usize) -> String {
    if bytes >= size::TB {
        format!("{:.precision$} TB", (bytes as f64) / (size::TB as f64))
    } else if bytes >= size::GB {
        format!("{:.precision$} GB", (bytes as f64) / (size::GB as f64))
    } else if bytes >= size::MB {
        format!("{:.precision$} MB", (bytes as f64) / (size::MB as f64))
    } else if bytes >= size::kB {
        format!("{:.precision$} kB", (bytes as f64) / (size::kB as f64))
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
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
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
    // parent dir -> set of *child names* (single component) directly under that parent
    let mut children_map: FxHashMap<PathBuf, FxHashSet<OsString>> = FxHashMap::default();
    let mut unique_root_children: FxHashSet<OsString> = FxHashSet::default();

    for full_path in paths {
        let mut cur = full_path.as_path();
        let mut direct_root_child_name: Option<OsString> = None;

        while let Some(parent) = cur.parent() {
            // Stop when we reached (or crossed) root.
            // Note: this relies on your existing ordering semantics for normalized absolute paths.
            if parent <= root {
                if parent == root
                    && let Some(name) = cur.file_name()
                {
                    direct_root_child_name = Some(name.to_os_string());
                }
                break;
            }

            // Insert the child name under its parent.
            if let Some(name) = cur.file_name() {
                children_map
                    .entry(parent.to_path_buf())
                    .or_default()
                    .insert(name.to_os_string());
            }

            cur = parent;
        }

        if let Some(name) = direct_root_child_name {
            unique_root_children.insert(name);
        }
    }

    let root_children_count = unique_root_children.len();

    // Convert to BTreeMap for sorted keys (preserves your original return type / ordering)
    let mut intermediate_counts = BTreeMap::new();
    intermediate_counts.extend(children_map.into_iter().map(|(p, set)| (p, set.len())));

    (root_children_count, intermediate_counts)
}

/// Abbreviates a path to fit within `max_len`, keeping the first and last components.
///
/// Long paths are shortened by replacing middle components with `"..."`.
/// Always preserves the root (if any) and the filename.
pub fn abbreviate_path(path: &Path, max_len: usize) -> String {
    let path_str = path.to_string_lossy();
    if path_str.is_empty() {
        return String::new();
    }

    // If it already fits, return as-is
    if path_str.len() <= max_len {
        return path_str.into_owned();
    }

    let components: Vec<_> = path.components().collect();
    if components.len() <= 2 {
        return path_str.into_owned();
    }

    let first = components[0].as_os_str().to_string_lossy();
    let last = components.last().unwrap().as_os_str().to_string_lossy();
    let ellipsis = "...";

    // base_res_len handles: [first][sep?][...][sep][last]
    let needs_sep_after_first = !first.ends_with(is_separator);
    let mut base_res_len = first.len() + ellipsis.len() + last.len() + 1;
    if needs_sep_after_first {
        base_res_len += 1;
    }

    if base_res_len > max_len {
        return path_str.into_owned();
    }

    let mut middle_parts = Vec::new();
    let mut current_len = base_res_len;

    // --- FIX START ---
    // If we have a Windows prefix followed by a root (e.g., C:\),
    // we skip the RootDir component to avoid double slashes.
    let skip_count = if components.len() > 1
        && matches!(components[0], std::path::Component::Prefix(_))
        && matches!(components[1], std::path::Component::RootDir)
    {
        2
    } else {
        1
    };

    let middle_count = components.len().saturating_sub(1 + skip_count);

    for component in components.iter().skip(skip_count).take(middle_count) {
        // --- FIX END ---
        let comp_str = component.as_os_str().to_string_lossy();
        let added_len = comp_str.len() + 1; // component + separator

        if current_len + added_len <= max_len {
            current_len += added_len;
            middle_parts.push(comp_str);
        } else {
            // We've reached the length limit; the rest will be replaced by ellipsis.
            break;
        }
    }

    let mut result = String::with_capacity(current_len);
    result.push_str(&first);

    for comp in middle_parts {
        if !result.ends_with(is_separator) {
            result.push(MAIN_SEPARATOR);
        }
        result.push_str(&comp);
    }

    if !result.ends_with(is_separator) {
        result.push(MAIN_SEPARATOR);
    }
    result.push_str(ellipsis);

    if !result.ends_with(is_separator) {
        result.push(MAIN_SEPARATOR);
    }
    result.push_str(&last);

    result
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
                    "Invalid duration format: unit '{c}' without preceding number in \"{s}\""
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
                _ => return Err(anyhow!("Invalid duration unit: '{c}' in \"{s}\"")),
            }
            current_num_str.clear();
        }
    }

    if !current_num_str.is_empty() {
        return Err(anyhow!(
            "Invalid duration format: trailing number '{current_num_str}' without unit in \"{s}\""
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

// --- Others ---

/// Returns the hostname and username
pub fn get_system_info() -> (Option<String>, Option<String>) {
    let hostname = hostname::get().ok().and_then(|hn| hn.into_string().ok());
    let username = if cfg!(windows) {
        std::env::var("USERNAME").ok()
    } else {
        std::env::var("USER").ok()
    };
    (hostname, username)
}

/// Returns the cummulative size of all files below a base path.
pub fn dir_size(path: &Path) -> Result<u64> {
    let mut total_size = 0;

    if !path.is_dir() {
        if path.is_file() {
            return Ok(path.metadata()?.len());
        }
        return Ok(0);
    }

    for entry in std::fs::read_dir(path)? {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };

        let path = entry.path();
        let metadata = match std::fs::metadata(&path) {
            Ok(m) => m,
            Err(_) => continue,
        };

        if metadata.is_file() {
            total_size += metadata.len();
        } else if metadata.is_dir() {
            total_size += dir_size(&path)?;
        }
    }

    Ok(total_size)
}

/// Counts entries in a directory based on a custom filtering function.
///
/// The filter closure receives a reference to a fs::DirEntry and should return
/// 'true' if the entry should be counted.
pub fn count_entries<F>(dir_path: &Path, filter: F) -> Result<usize>
where
    F: Fn(&std::fs::DirEntry) -> bool,
{
    let entries = dir_path.read_dir()?;
    let mut count = 0;

    for entry in entries {
        if let Ok(entry) = entry
            && filter(&entry)
        {
            count += 1;
        }
    }

    Ok(count)
}

/// Counts files in a directory.
pub fn count_files(dir_path: &Path) -> Result<usize> {
    count_entries(dir_path, |entry| entry.path().is_file())
}

// --- Tests ---
#[cfg(test)]
mod tests {
    use chrono::{NaiveDateTime, TimeZone};

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
    fn test_format_size_binary() {
        // With one decimal
        assert_eq!(format_size_binary(0, 1), "0 B");
        assert_eq!(format_size_binary(1, 1), "1 B");
        assert_eq!(format_size_binary(324, 1), "324 B");
        assert_eq!(format_size_binary(1_205, 1), "1.2 KiB");
        assert_eq!(format_size_binary(124_112, 1), "121.2 KiB");
        assert_eq!(format_size_binary(1_045_024, 1), "1020.5 KiB");
        assert_eq!(format_size_binary(12_995_924, 1), "12.4 MiB");
        assert_eq!(format_size_binary(1_500_000_000, 1), "1.4 GiB");
        assert_eq!(format_size_binary(2_100_000_100_000, 1), "1.9 TiB");

        // With two decimals
        assert_eq!(format_size_binary(0, 2), "0 B");
        assert_eq!(format_size_binary(1, 2), "1 B");
        assert_eq!(format_size_binary(324, 2), "324 B");
        assert_eq!(format_size_binary(1_205, 2), "1.18 KiB");
        assert_eq!(format_size_binary(124_112, 2), "121.20 KiB");
        assert_eq!(format_size_binary(1_045_024, 2), "1020.53 KiB");
        assert_eq!(format_size_binary(12_995_924, 2), "12.39 MiB");
        assert_eq!(format_size_binary(1_500_000_000, 2), "1.40 GiB");
        assert_eq!(format_size_binary(2_100_000_100_000, 2), "1.91 TiB");

        // With three decimals
        assert_eq!(format_size_binary(0, 3), "0 B");
        assert_eq!(format_size_binary(1, 3), "1 B");
        assert_eq!(format_size_binary(324, 3), "324 B");
        assert_eq!(format_size_binary(1_205, 3), "1.177 KiB");
        assert_eq!(format_size_binary(124_112, 3), "121.203 KiB");
        assert_eq!(format_size_binary(1_045_024, 3), "1020.531 KiB");
        assert_eq!(format_size_binary(12_995_924, 3), "12.394 MiB");
        assert_eq!(format_size_binary(1_500_000_000, 3), "1.397 GiB");
        assert_eq!(format_size_binary(2_100_000_100_000, 3), "1.910 TiB");
    }

    #[test]
    fn test_format_size_decimal() {
        // With one decimal
        assert_eq!(format_size_decimal(0, 1), "0 B");
        assert_eq!(format_size_decimal(1, 1), "1 B");
        assert_eq!(format_size_decimal(324, 1), "324 B");
        assert_eq!(format_size_decimal(1_205, 1), "1.2 kB");
        assert_eq!(format_size_decimal(124_112, 1), "124.1 kB");
        assert_eq!(format_size_decimal(1_045_024, 1), "1.0 MB");
        assert_eq!(format_size_decimal(12_995_924, 1), "13.0 MB");
        assert_eq!(format_size_decimal(1_500_000_000, 1), "1.5 GB");
        assert_eq!(format_size_decimal(2_100_000_100_000, 1), "2.1 TB");

        // With two decimals
        assert_eq!(format_size_decimal(0, 2), "0 B");
        assert_eq!(format_size_decimal(1, 2), "1 B");
        assert_eq!(format_size_decimal(324, 2), "324 B");
        assert_eq!(format_size_decimal(1_205, 2), "1.21 kB");
        assert_eq!(format_size_decimal(124_112, 2), "124.11 kB");
        assert_eq!(format_size_decimal(1_045_024, 2), "1.05 MB");
        assert_eq!(format_size_decimal(12_995_924, 2), "13.00 MB");
        assert_eq!(format_size_decimal(1_500_000_000, 2), "1.50 GB");
        assert_eq!(format_size_decimal(2_100_000_100_000, 2), "2.10 TB");

        // With three decimals
        assert_eq!(format_size_decimal(0, 3), "0 B");
        assert_eq!(format_size_decimal(1, 3), "1 B");
        assert_eq!(format_size_decimal(324, 3), "324 B");
        assert_eq!(format_size_decimal(1_205, 3), "1.205 kB");
        assert_eq!(format_size_decimal(124_112, 3), "124.112 kB");
        assert_eq!(format_size_decimal(1_045_024, 3), "1.045 MB");
        assert_eq!(format_size_decimal(12_995_924, 3), "12.996 MB");
        assert_eq!(format_size_decimal(1_500_000_000, 3), "1.500 GB");
        assert_eq!(format_size_decimal(2_100_000_100_000, 3), "2.100 TB");
    }

    #[test]
    fn test_format_count() {
        assert_eq!(format_count(0, "thing", "things"), "0 things");
        assert_eq!(format_count(1, "thing", "things"), "1 thing");
        assert_eq!(format_count(2, "thing", "things"), "2 things");
        assert_eq!(format_count(500, "thing", "things"), "500 things");
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
    fn test_pretty_print_system_time() -> Result<()> {
        let naive_str = "2025-12-01 18:00";
        let naive_datetime = NaiveDateTime::parse_from_str(naive_str, "%Y-%m-%d %H:%M")?;
        let datetime_with_local_offset: DateTime<Local> =
            Local.from_local_datetime(&naive_datetime).unwrap();
        let time: SystemTime = datetime_with_local_offset.into();

        assert_eq!(pretty_print_system_time(time, None)?, "2025-12-01 18:00:00");
        assert_eq!(
            pretty_print_system_time(time, Some("%Y-%m-%d %H:%M:%S"))?,
            "2025-12-01 18:00:00"
        );
        assert_eq!(
            pretty_print_system_time(time, Some("%Y-%m-%d %H:%M:%S"))?,
            "2025-12-01 18:00:00"
        );
        assert_eq!(
            pretty_print_system_time(time, Some("%Y-%m-%d"))?,
            "2025-12-01"
        );
        assert_eq!(
            pretty_print_system_time(time, Some("%d/%m/%Y %H:%M:%S"))?,
            "01/12/2025 18:00:00"
        );

        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn test_abbreviate_path_unix() {
        // Note: On Unix, the first component of "/home/user" is "/"

        assert_eq!(abbreviate_path(Path::new("short/path"), 5), "short/path");

        let path = Path::new("/home/user/some/path/to/a/file.txt");
        // first="/" + middle="home" + "..." + last="file.txt" = "/home/.../file.txt"
        assert_eq!(abbreviate_path(path, 20), "/home/.../file.txt");
        assert_eq!(abbreviate_path(path, 30), "/home/user/some/.../file.txt");

        let long_path_str = "/home/user/projects/backup_tool/src/core/permissions.rs";
        let long_path = Path::new(long_path_str);

        assert_eq!(
            abbreviate_path(long_path, 30),
            "/home/user/.../permissions.rs"
        );
    }

    #[cfg(windows)]
    #[test]
    fn test_abbreviate_path_windows() {
        let path = Path::new(r"C:\Users\Admin\Documents\file.txt");
        let result = abbreviate_path(path, 20);
        // Should handle C:\ without doubling the backslash
        assert_eq!(result, r"C:\...\file.txt");
        assert!(!result.contains(r"\\"));

        let path = Path::new(r"C:\Users\Admin\Documents\file.txt");
        let result = abbreviate_path(path, 25);
        // Should handle C:\ without doubling the backslash
        assert_eq!(result, r"C:\Users\...\file.txt");
        assert!(!result.contains(r"\\"));

        let path = Path::new(r"C:\Users\Admin\Documents\file.txt");
        let result = abbreviate_path(path, 30);
        // Should handle C:\ without doubling the backslash
        assert_eq!(result, r"C:\Users\Admin\...\file.txt");
        assert!(!result.contains(r"\\"));
    }
}
