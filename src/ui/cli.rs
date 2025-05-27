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

use colored::Colorize;
use dialoguer::Password;

#[macro_export]
macro_rules! log {
    ($($arg:tt)*) => {
        println!($($arg)*)
    };
}
pub use log;

/// Prints a log with a green tag.
pub fn log_green(tag: &str, str: &str) {
    println!("{}: {}", tag.bold().green(), str);
}

/// Prints a log with a cyan tag.
pub fn log_cyan(tag: &str, str: &str) {
    println!("{}: {}", tag.bold().cyan(), str);
}

/// Prints a log with a purple tag.
pub fn log_purple(tag: &str, str: &str) {
    println!("{}: {}", tag.bold().purple(), str);
}

/// Prints a log with a yellow tag.
pub fn log_yellow(tag: &str, str: &str) {
    println!("{}: {}", tag.bold().yellow(), str);
}

/// Prints a log with a red tag.
pub fn log_red(tag: &str, str: &str) {
    println!("{}: {}", tag.bold().red(), str);
}

/// Prints a warning log (warning: ...)
pub fn log_warning(str: &str) {
    eprintln!("{}: {}", "Warning".bold().yellow(), str);
}

/// Prints an error log (error: ...)
pub fn log_error(str: &str) {
    eprintln!("{}: {}", "Error".bold().red(), str);
}

pub fn print_separator(character: char, count: usize) {
    let repeated_string: String = std::iter::repeat(character).take(count).collect();
    println!("{}", repeated_string);
}

/// Requests a new password with confirmation.
pub fn request_new_password() -> String {
    Password::new()
        .with_prompt("Enter new password")
        .with_confirmation("Confirm password", "Passwords mismatching")
        .interact()
        .unwrap()
}

/// Requests a repository password without confirmation.
#[inline]
pub fn request_repo_password() -> String {
    request_password("Enter password for repository")
}

/// Requests a password with a prompt without confirmation.
pub fn request_password(promt: &str) -> String {
    Password::new().with_prompt(promt).interact().unwrap()
}
