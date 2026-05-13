use std::io::{self, BufRead, Write};

use anyhow::Result;
use zeroize::Zeroizing;

use crate::repository::repo::Auth;

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
fn read_password_impl(prompt: &str) -> Result<String> {
    use libc::{ECHO, STDIN_FILENO, TCSANOW, tcgetattr, tcsetattr};

    let mut stdout = io::stdout().lock();
    write!(stdout, "{prompt}: ")?;
    stdout.flush()?;

    let mut termios = unsafe {
        let mut t = std::mem::zeroed();
        if tcgetattr(STDIN_FILENO, &mut t) != 0 {
            return Err(anyhow::anyhow!("Failed to get terminal attributes"));
        }
        t
    };

    let original = termios;
    termios.c_lflag &= !ECHO;

    unsafe {
        if tcsetattr(STDIN_FILENO, TCSANOW, &termios) != 0 {
            return Err(anyhow::anyhow!("Failed to set terminal attributes"));
        }
    }

    let mut password = String::new();
    let res = io::stdin().lock().read_line(&mut password);

    unsafe {
        tcsetattr(STDIN_FILENO, TCSANOW, &original);
    }
    println!();

    res?;
    Ok(password.trim().to_string())
}

#[cfg(windows)]
fn read_password_impl(prompt: &str) -> Result<String> {
    use windows_sys::Win32::System::Console::{
        ENABLE_ECHO_INPUT, GetConsoleMode, GetStdHandle, STD_INPUT_HANDLE, SetConsoleMode,
    };

    let mut stdout = io::stdout().lock();
    write!(stdout, "{prompt}: ")?;
    stdout.flush()?;

    let handle = unsafe { GetStdHandle(STD_INPUT_HANDLE) };
    let mut mode = 0;
    unsafe {
        GetConsoleMode(handle, &mut mode);
    }

    let original_mode = mode;
    unsafe {
        SetConsoleMode(handle, mode & !ENABLE_ECHO_INPUT);
    }

    let mut password = String::new();
    let res = io::stdin().lock().read_line(&mut password);

    unsafe {
        SetConsoleMode(handle, original_mode);
    }

    res?;
    Ok(password.trim().to_string())
}

#[cfg(not(any(unix, windows)))]
fn read_password_impl(prompt: &str) -> Result<String> {
    read_line(prompt)
}

pub(crate) fn request_password(prompt: &str) -> Result<Zeroizing<String>> {
    read_password_impl(prompt).map(Zeroizing::new)
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
    if pw != confirm {
        anyhow::bail!("Passwords don't match");
    }
    Ok(Zeroizing::new(pw))
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
        if $crate::mapache::global::GlobalOpts::verbosity() >= $min_level {
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
        eprintln!("{}: {}", "Error".red().bold(), format_args!($($arg)*))
    };
}

#[macro_export]
macro_rules! warning {
    ($($arg:tt)*) => {
        $crate::ui::cli::log_with_level!(1, "{}: {}", "Warning".yellow().bold(), format_args!($($arg)*))
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

pub use {error, log, log_with_level, verbose_1, verbose_2, warning};
