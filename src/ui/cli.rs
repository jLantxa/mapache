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

use dialoguer::Password;

/// Requests a password with a prompt without confirmation.
#[inline]
pub(crate) fn request_password(promt: &str) -> String {
    Password::new().with_prompt(promt).interact().unwrap()
}

/// Requests a password with a prompt and confirmation.
#[inline]
pub(crate) fn request_password_with_confirmation(
    prompt: &str,
    confirmation_prompt: &str,
    mismatch_err_prompt: &str,
) -> String {
    Password::new()
        .with_prompt(prompt)
        .with_confirmation(confirmation_prompt, mismatch_err_prompt)
        .interact()
        .unwrap()
}

#[macro_export]
macro_rules! log_with_level {
    ($min_level:expr, $($arg:tt)*) => {
        let current_verbosity = $crate::global::global_opts().as_ref().unwrap().verbosity;
        if current_verbosity >= $min_level {
            println!($($arg)*)
        }
    };
}

#[macro_export]
macro_rules! log_always {
    ($($arg:tt)*) => {
        println!($($arg)*)
    };
}

#[macro_export]
macro_rules! error {
    ($($arg:tt)*) => {
        $crate::ui::cli::log_always!(
            "{}{}Error:{} {}",
            "\x1b[1m",  // BOLD
            "\x1b[31m", // RED
            "\x1b[0m",  // RESET
            format!($($arg)*)
        );
    };
}

#[macro_export]
macro_rules! warning {
    ($($arg:tt)*) => {
        $crate::ui::cli::log_with_level!(
            1,
            "{}{}Warning:{} {}",
            "\x1b[1m",  // BOLD
            "\x1b[33m", // YELLOW
            "\x1b[0m",  // RESET
            format!($($arg)*)
        );
    };
}

#[macro_export]
macro_rules! log {
    ($($arg:tt)*) => {
        $crate::ui::cli::log_with_level!(1, $($arg)*);
    };
}

#[macro_export]
macro_rules! verbose_1 {
    ($($arg:tt)*) => {
        $crate::ui::cli::log_with_level!(2, $($arg)*);
    };
}

#[macro_export]
macro_rules! verbose_2 {
    ($($arg:tt)*) => {
       $crate::ui::cli::log_with_level!(3, $($arg)*);
    };
}

pub use {error, log, log_always, log_with_level, verbose_1, verbose_2, warning};
