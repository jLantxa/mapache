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

use clap::{Parser, Subcommand};
use colored::Colorize;
use dialoguer::Password;

use crate::cmd;

// CLI arguments
#[derive(Parser, Debug)]
#[clap(
    version = env!("CARGO_PKG_VERSION"), // Version from crate metadata
    about = "Incremental backup tool"
)]
pub struct Cli {
    // Subcommand
    #[command(subcommand)]
    pub command: Command,

    // Global arguments
    #[clap(flatten)]
    pub global_args: GlobalArgs,
}

// List of commands
#[derive(Subcommand, Debug)]
pub enum Command {
    #[clap(about = "Initialize a new repository")]
    Init(cmd::init::CmdArgs),

    #[clap(about = "Show all snapshots present in the repository")]
    Log(cmd::log::CmdArgs),

    #[clap(about = "Create a new snapshot")]
    Commit(cmd::commit::CmdArgs),

    #[clap(about = "Restores a snapshot")]
    Restore(cmd::restore::CmdArgs),
}

#[derive(Parser, Debug)]
pub struct GlobalArgs {
    /// Number of threads to use.
    /// Any positive integer greater than 0 or "max" to use the maximum number
    /// of logical CPU cores available on the system.
    #[clap(short, long, value_parser = parse_threads, default_value_t = 2)]
    pub threads: usize,

    /// Repository path
    #[clap(short, long, value_parser)]
    pub repo: String,
}

/// Custom parser function for the threads argument
fn parse_threads(s: &str) -> Result<usize, anyhow::Error> {
    match s {
        "max" => std::thread::available_parallelism()
            .map(|num| num.get())
            .map_err(|e| anyhow::anyhow!("Failed to determine available parallelism: {}", e)),

        _ => {
            let threads: usize = s
                .parse()
                .map_err(|e| anyhow::anyhow!("'{}' isn't a valid number for threads: {}", s, e))?;

            if threads == 0 {
                Err(anyhow::anyhow!("Number of threads must be greater than 0"))
            } else {
                Ok(threads)
            }
        }
    }
}

#[macro_export]
macro_rules! log {
    ($expr:expr) => {
        println!("{}", $expr);
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

/// Requests a new password with confirmation.
pub fn request_new_password() -> String {
    Password::new()
        .with_prompt("Enter new password")
        .with_confirmation("Confirm password", "Passwords mismatching")
        .interact()
        .unwrap()
}

/// Requests a pasword with no confirmation.
pub fn request_password() -> String {
    Password::new()
        .with_prompt("Enter password")
        .interact()
        .unwrap()
}
