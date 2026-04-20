use anyhow::Result;
use dialoguer::{Input, Password};

use crate::repository::repo::Auth;

pub(crate) fn request_password(prompt: &str) -> Result<String> {
    Password::new()
        .with_prompt(prompt)
        .interact()
        .map_err(Into::into)
}

pub(crate) fn request_input(prompt: &str) -> Result<Option<String>> {
    let input: String = Input::new()
        .with_prompt(prompt)
        .allow_empty(true)
        .interact()?;

    if input.is_empty() {
        Ok(None)
    } else {
        Ok(Some(input))
    }
}

/// Requests a password with a prompt and confirmation.
pub(crate) fn request_new_password(prompt: &str, confirmation: &str) -> Result<String> {
    Password::new()
        .with_prompt(prompt)
        .with_confirmation(confirmation, "Passwords don't match")
        .interact()
        .map_err(Into::into)
}

/// Requests new authentication data (username and password) with confirmation
pub(crate) fn request_new_auth() -> Result<Auth> {
    let username: String = Input::new().with_prompt("Enter new username").interact()?;

    let password = request_new_password("Enter new password", "Confirm password")?;
    Ok(Auth { username, password })
}

/// Requests authentication data (username and password)
pub(crate) fn request_auth() -> Result<Auth> {
    let username: String = Input::new().with_prompt("Enter username").interact()?;

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
