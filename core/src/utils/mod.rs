pub mod collections;

use std::{
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Duration, Local};

use crate::{
    mapache::vars::{PASSWORD_ENVVAR, USERNAME_ENVVAR, get_envvar},
    repository::repo::Auth,
};
use zeroize::Zeroizing;

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

/// Reads authentication credentials (username and password) from a file or environment variables.
/// The file should contain the username on the first line and the password on the second.
/// If no file is provided, it checks the MAPACHE_USERNAME and MAPACHE_PASSWORD environment variables.
pub fn get_auth(password_file_path: &Option<PathBuf>) -> Result<Option<Auth>> {
    if let Some(path) = password_file_path {
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

        Ok(Some(Auth {
            username,
            password: Zeroizing::new(password),
        }))
    } else {
        let username = get_envvar(USERNAME_ENVVAR);
        let password = get_envvar(PASSWORD_ENVVAR);

        match (username, password) {
            (Some(u), Some(p)) => Ok(Some(Auth {
                username: u,
                password: Zeroizing::new(p),
            })),
            _ => Ok(None),
        }
    }
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

// --- Duration Utilities ---

/// Pretty prints a `std::time::Duration` in a human-readable format.
/// Attempts to show up to two most significant units.
/// Milliseconds are only shown if the total duration is less than one second.
pub fn pretty_print_duration(duration: std::time::Duration) -> String {
    let total_seconds = duration.as_secs();
    let milliseconds = duration.subsec_millis();

    // Handle the absolute zero case
    if total_seconds == 0 && milliseconds == 0 {
        return "0s".to_string();
    }

    let days = total_seconds / 86_400;
    let hours = (total_seconds % 86_400) / 3_600;
    let minutes = (total_seconds % 3_600) / 60;
    let seconds = total_seconds % 60;

    let mut parts = Vec::with_capacity(2);

    if days > 0 {
        parts.push(format!("{days}d"));
    }

    if hours > 0 && parts.len() < 2 {
        parts.push(format!("{hours}h"));
    }

    if minutes > 0 && parts.len() < 2 {
        parts.push(format!("{minutes}m"));
    }

    if seconds > 0 && parts.len() < 2 {
        parts.push(format!("{seconds}s"));
    }

    // Only show ms if we haven't reached a full second yet
    if total_seconds == 0 && milliseconds > 0 {
        parts.push(format!("{milliseconds}ms"));
    }

    parts.join(" ")
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

/// Returns the system's hostname and the current user's name.
pub fn get_system_info() -> (Option<String>, Option<String>) {
    let hostname = hostname::get().ok().and_then(|hn| hn.into_string().ok());
    let username = if cfg!(windows) {
        std::env::var("USERNAME").ok()
    } else {
        std::env::var("USER").ok()
    };
    (hostname, username)
}

/// Returns the cumulative size of all files within a directory and its subdirectories.
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

/// Counts entries in a directory that satisfy a given filter predicate.
///
/// The filter closure receives a reference to a `std::fs::DirEntry` and should return
/// `true` if the entry should be counted.
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

/// Counts the number of regular files within a directory (non-recursive).
pub fn count_files(dir_path: &Path) -> Result<usize> {
    count_entries(dir_path, |entry| entry.path().is_file())
}

// --- Tests ---
/// Joins a relative path to a base path, ensuring the result is within the base path.
/// This prevents path traversal attacks.
pub fn secure_join(base: &Path, relative: &Path) -> Result<PathBuf> {
    if relative.is_absolute() {
        bail!("Relative path cannot be absolute: {}", relative.display());
    }

    let joined = base.join(relative);
    let normalized = crate::fs::get_absolute_normalized_path(&joined)?;
    let normalized_base = crate::fs::get_absolute_normalized_path(base)?;

    if !normalized.starts_with(&normalized_base) {
        bail!(
            "Path traversal detected: {} is outside of {}",
            normalized.display(),
            normalized_base.display()
        );
    }

    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDateTime, TimeZone};

    use super::*;

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
            pretty_print_duration(std::time::Duration::from_millis(1500)),
            "1s"
        );
        assert_eq!(
            pretty_print_duration(std::time::Duration::from_secs(59)),
            "59s"
        );
        assert_eq!(
            pretty_print_duration(std::time::Duration::from_millis(59500)),
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
        assert_eq!(
            pretty_print_duration(std::time::Duration::from_secs(3600 * 24 + 3600)),
            "1d 1h"
        );
        assert_eq!(
            pretty_print_duration(std::time::Duration::from_secs(3600 * 24 + 60)),
            "1d 1m"
        );
        assert_eq!(
            pretty_print_duration(std::time::Duration::from_secs(3600 * 24 + 1)),
            "1d 1s"
        );
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

    #[test]
    fn test_get_auth() -> Result<()> {
        let tmp_dir = tempfile::tempdir()?;
        let auth_file = tmp_dir.path().join("auth");

        // Write a valid auth file
        std::fs::write(&auth_file, "user1\npass123")?;

        let auth = get_auth(&Some(auth_file.clone()))?.unwrap();
        assert_eq!(auth.username, "user1");
        assert_eq!(*auth.password, "pass123");

        // Test empty file
        std::fs::write(&auth_file, "")?;
        assert!(get_auth(&Some(auth_file.clone())).is_err());

        // Test missing password
        std::fs::write(&auth_file, "user1")?;
        assert!(get_auth(&Some(auth_file.clone())).is_err());

        // Test None input (no env vars)
        unsafe {
            std::env::remove_var(USERNAME_ENVVAR);
            std::env::remove_var(PASSWORD_ENVVAR);
        }
        assert!(get_auth(&None)?.is_none());

        // Test environment variables
        unsafe {
            std::env::set_var(USERNAME_ENVVAR, "env_user");
            std::env::set_var(PASSWORD_ENVVAR, "env_pass");
        }
        let auth = get_auth(&None)?.unwrap();
        assert_eq!(auth.username, "env_user");
        assert_eq!(*auth.password, "env_pass");

        // Cleanup
        unsafe {
            std::env::remove_var(USERNAME_ENVVAR);
            std::env::remove_var(PASSWORD_ENVVAR);
        }

        Ok(())
    }

    #[test]
    fn test_get_system_info() {
        let _ = get_system_info();
    }

    #[test]
    fn test_directory_ops() -> Result<()> {
        let tmp_dir = tempfile::tempdir()?;
        let d = tmp_dir.path();

        let f1 = d.join("f1");
        std::fs::write(&f1, "hello")?;

        let f2 = d.join("f2");
        std::fs::write(&f2, "world!!")?;

        let sub_dir = d.join("sub");
        std::fs::create_dir(&sub_dir)?;
        let f3 = sub_dir.join("f3");
        std::fs::write(&f3, "mapache")?;

        assert_eq!(count_files(d)?, 2); // Only files in d
        assert_eq!(dir_size(d)?, 5 + 7 + 7); // size of f1 + f2 + f3

        assert_eq!(dir_size(&f1)?, 5);

        Ok(())
    }
}
