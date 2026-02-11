use crate::repository::repo::Auth;
use dialoguer::{Input, Password};

pub(crate) fn request_password(prompt: &str) -> String {
    Password::new()
        .with_prompt(prompt)
        .interact()
        .expect("Failed to read password")
}

pub(crate) fn request_input(prompt: &str) -> Option<String> {
    let input: String = Input::new()
        .with_prompt(prompt)
        .allow_empty(true)
        .interact()
        .expect("Failed to read input");

    if input.is_empty() {
        None
    } else {
        Some(input)
    }
}

/// Requests a password with a prompt and confirmation.
pub(crate) fn request_new_password(prompt: &str, confirmation: &str) -> String {
    Password::new()
        .with_prompt(prompt)
        .with_confirmation(confirmation, "Passwords don't match")
        .interact()
        .expect("Failed to read password")
}

/// Requests new authentication data (username and password) with confirmation
pub(crate) fn request_new_auth() -> Auth {
    let username: String = Input::new()
        .with_prompt("Enter new username")
        .interact()
        .expect("Failed to read username");

    let password = request_new_password("Enter new password", "Confirm password");
    Auth { username, password }
}

/// Requests authentication data (username and password)
pub(crate) fn request_auth() -> Auth {
    let username: String = Input::new()
        .with_prompt("Enter username")
        .interact()
        .expect("Failed to read username");

    let password = request_password("Enter password");
    Auth { username, password }
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
        eprintln!("\x1b[1;31mError:\x1b[0m {}", format_args!($($arg)*))
    };
}

#[macro_export]
macro_rules! warning {
    ($($arg:tt)*) => {
        $crate::ui::cli::log_with_level!(1, "\x1b[1;33mWarning:\x1b[0m {}", format_args!($($arg)*))
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
