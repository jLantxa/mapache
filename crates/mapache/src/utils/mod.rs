pub mod base64;
pub mod binary;
pub mod collections;
pub mod rate_estimator;

use std::{
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::common::error::{MapacheError, Result};
use chrono::{DateTime, Duration, Local};
use zeroize::Zeroizing;

use crate::{
    common::vars::{PASSWORD_ENVVAR, USERNAME_ENVVAR, get_envvar},
    fs,
    repository::repo::Auth,
};

// --- Constants ---

/// Size units.
#[allow(non_upper_case_globals)]
pub(crate) mod size {
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
pub(crate) fn get_auth(password_file_path: &Option<PathBuf>) -> Result<Option<Auth>> {
    if let Some(path) = password_file_path {
        let text = std::fs::read_to_string(path)?;

        // Parse the text to extract the username and password
        let mut lines = text.lines();
        let username = lines
            .next()
            .ok_or_else(|| MapacheError::Config(format!("file {} is empty", path.display())))?
            .to_string();

        let password = lines
            .next()
            .ok_or_else(|| {
                MapacheError::Config(format!(
                    "file {} is missing the password on the second line",
                    path.display()
                ))
            })?
            .to_string();

        // Remove from process env so child processes (hooks, etc.) can't see it.
        // SAFETY: called during single-threaded startup; no other thread reads these vars.
        unsafe {
            std::env::remove_var(USERNAME_ENVVAR);
            std::env::remove_var(PASSWORD_ENVVAR);
        }

        Ok(Some(Auth {
            username,
            password: Zeroizing::new(password),
        }))
    } else {
        let username = get_envvar(USERNAME_ENVVAR);
        let password = get_envvar(PASSWORD_ENVVAR);

        // Remove from process env so child processes can't see it.
        // SAFETY: called during single-threaded startup; no other thread reads these vars.
        unsafe {
            if username.is_some() {
                std::env::remove_var(USERNAME_ENVVAR);
            }
            if password.is_some() {
                std::env::remove_var(PASSWORD_ENVVAR);
            }
        }

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
pub(crate) fn format_size_binary(bytes: u64, precision: usize) -> String {
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
pub(crate) fn format_count<T>(count: T, singular: &str, plural: &str) -> String
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
pub(crate) fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(char::from_digit((b >> 4).into(), 16).expect("nibble fits in radix 16"));
        s.push(char::from_digit((b & 0xf).into(), 16).expect("nibble fits in radix 16"));
    }
    s
}

/// Pretty prints a `SystemTime` into a human-readable string,
/// defaulting to "%Y-%m-%d %H:%M:%S" format if not specified.
///
/// Returns an `Err` if the `SystemTime` is before the Unix epoch.
pub(crate) fn pretty_print_system_time(
    time: SystemTime,
    format_str: Option<&str>,
) -> Result<String> {
    time.duration_since(UNIX_EPOCH).map_err(|e| {
        MapacheError::Integrity(format!("SystemTime {time:?} is before UNIX epoch: {e}"))
    })?;

    let format = format_str.unwrap_or("%Y-%m-%d %H:%M:%S");

    let datetime_local: DateTime<Local> = time.into();
    Ok(datetime_local.format(format).to_string())
}

pub(crate) fn pretty_print_timestamp(
    timestamp: &DateTime<Local>,
    format_str: Option<&str>,
) -> String {
    let format = format_str.unwrap_or("%a %Y-%m-%d %H:%M %:z");
    timestamp.format(format).to_string()
}

// --- Duration Utilities ---

/// Core duration decomposition: returns (days, hours, minutes, seconds, millis) from total seconds.
fn decompose_duration(total_seconds: u64, millis: u64) -> (u64, u64, u64, u64, u64) {
    let days = total_seconds / 86_400;
    let hours = (total_seconds % 86_400) / 3_600;
    let minutes = (total_seconds % 3_600) / 60;
    let seconds = total_seconds % 60;
    (days, hours, minutes, seconds, millis)
}

/// Formats decomposed duration parts into a human-readable string with up to `max_parts` entries.
/// If `show_ms` is true and total is sub-second, appends milliseconds.
fn format_duration_parts(
    days: u64,
    hours: u64,
    minutes: u64,
    seconds: u64,
    millis: u64,
    max_parts: usize,
    show_ms: bool,
) -> String {
    let mut parts = Vec::with_capacity(max_parts);
    if days > 0 {
        parts.push(format!("{days}d"));
    }
    if hours > 0 && parts.len() < max_parts {
        parts.push(format!("{hours}h"));
    }
    if minutes > 0 && parts.len() < max_parts {
        parts.push(format!("{minutes}m"));
    }
    if seconds > 0 && parts.len() < max_parts {
        parts.push(format!("{seconds}s"));
    }
    if show_ms && parts.is_empty() && millis > 0 {
        parts.push(format!("{millis}ms"));
    }
    if parts.is_empty() {
        "0s".to_string()
    } else {
        parts.join(" ")
    }
}

/// Pretty prints a `std::time::Duration` in a human-readable format.
/// Attempts to show up to two most significant units.
/// Milliseconds are only shown if the total duration is less than one second.
pub(crate) fn pretty_print_duration(duration: std::time::Duration) -> String {
    let total_seconds = duration.as_secs();
    let millis = duration.subsec_millis();
    if total_seconds == 0 && millis == 0 {
        return "0s".to_string();
    }
    let (d, h, m, s, ms) = decompose_duration(total_seconds, millis as u64);
    format_duration_parts(d, h, m, s, ms, 2, total_seconds == 0)
}

/// Pretty prints a `chrono::Duration` in a human-readable format.
/// Shows up to `max_parts` most significant units.
pub(crate) fn pretty_print_duration_chrono(duration: chrono::Duration, max_parts: usize) -> String {
    let total_seconds = duration.num_seconds().unsigned_abs();
    let millis = (duration.num_milliseconds() as u64) % 1000;
    let (d, h, m, s, ms) = decompose_duration(total_seconds, millis);
    format_duration_parts(d, h, m, s, ms, max_parts, false)
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
pub(crate) fn parse_duration_string(s: &str) -> Result<Duration> {
    let mut total_duration = Duration::seconds(0);
    let mut current_num_str = String::new();
    let chars = s.chars();

    for c in chars {
        if c.is_ascii_digit() {
            current_num_str.push(c);
        } else {
            if current_num_str.is_empty() {
                return Err(MapacheError::Format(format!(
                    "invalid duration format: unit '{c}' without preceding number in \"{s}\""
                )));
            }

            let num = current_num_str.parse::<i64>().map_err(|e| {
                MapacheError::Format(format!(
                    "failed to parse number before unit '{c}' in \"{s}\": {e}"
                ))
            })?;

            match c {
                's' => total_duration += Duration::seconds(num),
                'm' => total_duration += Duration::minutes(num),
                'h' => total_duration += Duration::hours(num),
                'd' => total_duration += Duration::days(num),
                'w' => total_duration += Duration::weeks(num),
                'y' => total_duration += Duration::days(num * 365),
                _ => {
                    return Err(MapacheError::Format(format!(
                        "invalid duration unit: '{c}' in \"{s}\""
                    )));
                }
            }
            current_num_str.clear();
        }
    }

    if !current_num_str.is_empty() {
        return Err(MapacheError::Format(format!(
            "invalid duration format: trailing number '{current_num_str}' without unit in \"{s}\""
        )));
    }

    Ok(total_duration)
}

/// Parses a bandwidth string (e.g., "10MB/s", "500KB/s", "1G") into bytes per second.
pub(crate) fn parse_bandwidth(s: &str) -> Result<u64> {
    let s = s.to_uppercase();
    let (num_str, unit) = if let Some(idx) = s.find(|c: char| !c.is_ascii_digit() && c != '.') {
        s.split_at(idx)
    } else {
        (s.as_str(), "")
    };

    let num: f64 = num_str
        .parse()
        .map_err(|e| MapacheError::Format(format!("invalid number: {num_str}: {e}")))?;

    let multiplier = match unit.trim() {
        "" | "B" | "B/S" => 1u64,
        "K" | "KIB" | "KIB/S" => size::KiB,
        "KB" | "KB/S" => size::kB,
        "M" | "MIB" | "MIB/S" => size::MiB,
        "MB" | "MB/S" => size::MB,
        "G" | "GIB" | "GIB/S" => size::GiB,
        "GB" | "GB/S" => size::GB,
        "T" | "TIB" | "TIB/S" => size::TiB,
        "TB" | "TB/S" => size::TB,
        _ => return Err(MapacheError::Format(format!("invalid unit: {unit}"))),
    };

    Ok((num * multiplier as f64) as u64)
}

// --- Permissions Utilities ---

/// Converts a Unix file mode (as `u32`) into a human-readable permission string
/// (e.g., "-rwxr-xr-x" like `ls -l`).
pub(crate) fn mode_to_permissions_string(mode: u32) -> String {
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

fn get_hostname() -> Option<String> {
    #[cfg(unix)]
    {
        let mut buf = [0u8; 256];
        let ret = unsafe {
            // SAFETY: FFI call to gethostname with valid buffer and length.
            libc::gethostname(buf.as_mut_ptr() as *mut libc::c_char, buf.len())
        };
        if ret != 0 {
            return None;
        }
        let len = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
        Some(String::from_utf8_lossy(&buf[..len]).into_owned())
    }
    #[cfg(windows)]
    {
        use windows_sys::Win32::System::SystemInformation::{
            ComputerNameDnsHostname, GetComputerNameExW,
        };
        let mut buf = [0u16; 256];
        let mut len = buf.len() as u32;
        // SAFETY: Windows FFI. buf is a valid 256-u16 stack array, len
        // is initialised to capacity. The call writes at most len u16s.
        let ret =
            unsafe { GetComputerNameExW(ComputerNameDnsHostname, buf.as_mut_ptr(), &mut len) };
        if ret == 0 {
            return None;
        }
        let s = String::from_utf16_lossy(&buf[..len as usize]);
        Some(s.trim_end_matches('\0').to_owned())
    }
}

/// Returns the system's hostname and the current user's name.
pub(crate) fn get_system_info() -> (Option<String>, Option<String>) {
    let hostname = get_hostname();
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

        let metadata = match entry.metadata() {
            Ok(m) => m,
            Err(_) => continue,
        };

        if metadata.is_file() {
            total_size += metadata.len();
        } else if metadata.is_dir() {
            total_size += dir_size(&entry.path())?;
        }
    }

    Ok(total_size)
}

/// Counts the number of regular files within a directory (non-recursive).
pub fn count_files(dir_path: &Path) -> Result<usize> {
    let entries = dir_path.read_dir()?;
    let mut count = 0;
    for entry in entries {
        if let Ok(entry) = entry
            && entry.file_type().map(|t| t.is_file()).unwrap_or(false)
        {
            count += 1;
        }
    }
    Ok(count)
}

// --- Tests ---
/// Joins a relative path to a base path, ensuring the result is within the base path.
/// This prevents path traversal attacks.
pub(crate) fn secure_join(base: &Path, relative: &Path) -> Result<PathBuf> {
    if relative.is_absolute() {
        return Err(MapacheError::Config(format!(
            "Relative path cannot be absolute: {}",
            relative.display()
        )));
    }

    let joined = base.join(relative);
    let normalized = fs::get_absolute_normalized_path(&joined)?;
    let normalized_base = fs::get_absolute_normalized_path(base)?;

    if !normalized.starts_with(&normalized_base) {
        return Err(MapacheError::Integrity(format!(
            "Path traversal detected: {} is outside of {}",
            normalized.display(),
            normalized_base.display()
        )));
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
    fn test_parse_bandwidth() {
        assert_eq!(parse_bandwidth("1000").unwrap(), 1000);
        assert_eq!(parse_bandwidth("1K").unwrap(), 1024);
        assert_eq!(parse_bandwidth("1KB").unwrap(), 1000);
        assert_eq!(parse_bandwidth("1M").unwrap(), 1024 * 1024);
        assert_eq!(parse_bandwidth("1MB").unwrap(), 1000 * 1000);
        assert_eq!(parse_bandwidth("1G").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_bandwidth("1GB").unwrap(), 1000 * 1000 * 1000);
        assert_eq!(
            parse_bandwidth("1.5M").unwrap(),
            (1.5 * 1024.0 * 1024.0) as u64
        );
        assert_eq!(parse_bandwidth("10MiB/s").unwrap(), 10 * 1024 * 1024);
        assert!(parse_bandwidth("abc").is_err());
        assert!(parse_bandwidth("10XX").is_err());
    }

    #[test]
    fn test_pretty_print_system_time() -> Result<()> {
        let naive_str = "2025-12-01 18:00";
        let naive_datetime = NaiveDateTime::parse_from_str(naive_str, "%Y-%m-%d %H:%M")
            .map_err(|e| MapacheError::Format(e.to_string()))?;
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
    fn test_pretty_print_timestamp() -> Result<()> {
        let naive_str = "2025-12-01 18:00";
        let naive_datetime = NaiveDateTime::parse_from_str(naive_str, "%Y-%m-%d %H:%M")
            .map_err(|e| MapacheError::Format(e.to_string()))?;
        let datetime: DateTime<Local> = Local.from_local_datetime(&naive_datetime).unwrap();

        let formatted = pretty_print_timestamp(&datetime, None);
        // format is "%a %Y-%m-%d %H:%M %:z"
        // 2025-12-01 is a Monday
        assert!(formatted.starts_with("Mon 2025-12-01 18:00"));
        // The timezone part depends on the local environment, so we just check it exists
        assert!(formatted.contains("+") || formatted.contains("-"));

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
            // SAFETY: Modifying environment variables in tests.
            std::env::remove_var(USERNAME_ENVVAR);
            std::env::remove_var(PASSWORD_ENVVAR);
        }
        assert!(get_auth(&None)?.is_none());

        // Test environment variables

        unsafe {
            // SAFETY: Modifying environment variables in tests.
            std::env::set_var(USERNAME_ENVVAR, "env_user");
            std::env::set_var(PASSWORD_ENVVAR, "env_pass");
        }
        let auth = get_auth(&None)?.unwrap();
        assert_eq!(auth.username, "env_user");
        assert_eq!(*auth.password, "env_pass");

        // Cleanup
        unsafe {
            // SAFETY: Modifying environment variables in tests.
            std::env::remove_var(USERNAME_ENVVAR);
            std::env::remove_var(PASSWORD_ENVVAR);
        }

        Ok(())
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
