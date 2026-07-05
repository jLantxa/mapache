use std::io::{self, BufRead, Write};

use crate::common::error::{MapacheError, Result};
use zeroize::{Zeroize, Zeroizing};

use crate::{
    common,
    repository::{repo::Auth, snapshot::SnapshotEntryList},
    ui::cli::{
        color::Colorize,
        table::{Alignment, Table},
    },
    utils,
};

pub mod bundle;
pub mod color;
pub mod gc;
pub mod restore;
pub mod snapshot;
pub mod table;

/// Logs a list of snapshots in the form of a compact table.
pub fn log_snapshots_compact(snapshots: &SnapshotEntryList) {
    let mut table = Table::new_with_alignments(vec![
        Alignment::Left,
        Alignment::Left,
        Alignment::Left,
        Alignment::Right,
        Alignment::Left,
    ]);

    table.set_headers(vec![
        "ID".bold().to_string(),
        "Date ▼".bold().to_string(),
        "Host".bold().to_string(),
        "Size".bold().to_string(),
        "Tags".bold().to_string(),
    ]);

    for entry in snapshots {
        let id_str = entry
            .id
            .to_short_hex(common::defaults::SHORT_SNAPSHOT_ID_LEN);
        let id_str = if entry.active {
            id_str.bold().yellow().to_string()
        } else {
            (id_str + " (dropped)").bold().dimmed().to_string()
        };

        table.add_row(vec![
            id_str,
            utils::pretty_print_timestamp(&entry.snapshot.timestamp, None),
            entry.snapshot.hostname.clone().unwrap_or_default(),
            utils::format_size_binary(entry.snapshot.size(), 3),
            entry
                .snapshot
                .tags
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join(", "),
        ]);
    }

    log!("{}", table.render());
}

fn read_line(prompt: &str) -> Result<String> {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    write!(stdout, "{prompt}: ")?;
    stdout.flush()?;

    let stdin = io::stdin();
    let mut line = String::new();
    stdin.lock().read_line(&mut line)?;
    Ok(line.trim().to_string())
}

#[cfg(unix)]
fn read_password_impl(prompt: &str) -> Result<Zeroizing<String>> {
    use libc::{ECHO, STDIN_FILENO, TCSANOW, tcgetattr, tcsetattr};

    let mut stdout = io::stdout().lock();
    write!(stdout, "{prompt}: ")?;
    stdout.flush()?;

    let mut termios = unsafe {
        // SAFETY: FFI call to tcgetattr with STDIN_FILENO and pointer to t.
        let mut t = std::mem::zeroed();
        if tcgetattr(STDIN_FILENO, &mut t) != 0 {
            return Err(MapacheError::Internal(
                "Failed to get terminal attributes".to_string(),
            ));
        }
        t
    };

    let original = termios;
    termios.c_lflag &= !ECHO;

    unsafe {
        // SAFETY: FFI call to tcsetattr to disable echoing.
        if tcsetattr(STDIN_FILENO, TCSANOW, &termios) != 0 {
            return Err(MapacheError::Internal(
                "Failed to set terminal attributes".to_string(),
            ));
        }
    }

    let mut password = String::new();
    let res = io::stdin().lock().read_line(&mut password);

    unsafe {
        // SAFETY: FFI call to tcsetattr to restore original terminal state.
        tcsetattr(STDIN_FILENO, TCSANOW, &original);
    }
    println!();

    res?;
    let trimmed = password.trim().to_string();
    password.zeroize();
    Ok(Zeroizing::new(trimmed))
}

#[cfg(windows)]
fn read_password_impl(prompt: &str) -> Result<Zeroizing<String>> {
    use windows_sys::Win32::System::Console::{
        ENABLE_ECHO_INPUT, GetConsoleMode, GetStdHandle, STD_INPUT_HANDLE, SetConsoleMode,
    };

    let mut stdout = io::stdout().lock();
    write!(stdout, "{prompt}: ")?;
    stdout.flush()?;

    // SAFETY: GetStdHandle returns a pseudo-handle (no ownership).
    // GetConsoleMode/SetConsoleMode operate on valid handles. Mode is
    // saved before modification and restored unconditionally.
    let handle = unsafe { GetStdHandle(STD_INPUT_HANDLE) };
    let mut mode = 0;
    // SAFETY: handle obtained above is valid for GetConsoleMode.
    unsafe {
        GetConsoleMode(handle, &mut mode);
    }

    let original_mode = mode;
    // SAFETY: same validated handle, setting no-echo for password input.
    unsafe {
        SetConsoleMode(handle, mode & !ENABLE_ECHO_INPUT);
    }

    let mut password = String::new();
    let res = io::stdin().lock().read_line(&mut password);

    // SAFETY: restores original mode regardless of read_line outcome.
    unsafe {
        SetConsoleMode(handle, original_mode);
    }

    res?;
    let trimmed = password.trim().to_string();
    password.zeroize();
    Ok(Zeroizing::new(trimmed))
}

#[cfg(not(any(unix, windows)))]
fn read_password_impl(prompt: &str) -> Result<Zeroizing<String>> {
    read_line(prompt).map(Zeroizing::new)
}

pub(crate) fn request_password(prompt: &str) -> Result<Zeroizing<String>> {
    read_password_impl(prompt)
}

pub(crate) fn request_input(prompt: &str) -> Result<Option<String>> {
    let input = read_line(prompt)?;
    if input.is_empty() {
        Ok(None)
    } else {
        Ok(Some(input))
    }
}

/// Requests a password with a prompt and confirmation.
pub(crate) fn request_new_password(prompt: &str, confirmation: &str) -> Result<Zeroizing<String>> {
    let pw = read_password_impl(prompt)?;
    let confirm = read_password_impl(confirmation)?;
    if *pw != *confirm {
        return Err(MapacheError::Internal("Passwords don't match".to_string()));
    }
    Ok(pw)
}

/// Requests new authentication data (username and password) with confirmation
pub(crate) fn request_new_auth() -> Result<Auth> {
    let username = read_line("Enter new username")?;
    let password = request_new_password("Enter new password", "Confirm password")?;
    Ok(Auth { username, password })
}

/// Requests authentication data (username and password)
pub(crate) fn request_auth() -> Result<Auth> {
    let username = read_line("Enter username")?;
    let password = request_password("Enter password")?;
    Ok(Auth { username, password })
}

#[macro_export]
macro_rules! log_with_level {
    ($min_level:expr, $($arg:tt)*) => {
        if $crate::common::global::GlobalOpts::verbosity() >= $min_level {
            println!($($arg)*)
        }
    };
}

#[macro_export]
macro_rules! log_always {
    ($($arg:tt)*) => {
        println!($($arg)*)
    }
}

#[macro_export]
macro_rules! error {
    ($($arg:tt)*) => {
        {
            use $crate::ui::cli::color::Colorize;
            eprintln!("{}: {:#}", "Error".red().bold(), format_args!($($arg)*))
        }
    };
}

#[macro_export]
macro_rules! warning {
    ($($arg:tt)*) => {
        {
            use $crate::ui::cli::color::Colorize;
            $crate::ui::cli::log_with_level!(1, "{}: {}", "Warning".yellow().bold(), format_args!($($arg)*))
        }
    };
}

#[macro_export]
macro_rules! log {
    ($($arg:tt)*) => { $crate::ui::cli::log_with_level!(1, $($arg)*) };
}

#[macro_export]
macro_rules! verbose_1 {
    ($($arg:tt)*) => { $crate::ui::cli::log_with_level!(2, $($arg)*) };
}

#[macro_export]
macro_rules! verbose_2 {
    ($($arg:tt)*) => { $crate::ui::cli::log_with_level!(3, $($arg)*) };
}

pub use error;
pub use log;
pub use log_with_level;
pub use verbose_1;
pub use verbose_2;
pub use warning;
