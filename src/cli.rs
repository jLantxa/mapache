/*
 * [backup] is an incremental backup tool
 * Copyright (C) 2025  Javier Lancha Vázquez <javier.lancha@gmail.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

use clap::{Parser, Subcommand};
use colored::Colorize;

use crate::cmd;

/// CLI arguments
#[derive(Parser, Debug)]
pub struct Cli {
    // Subcommand
    #[command(subcommand)]
    pub command: Command,

    // Global arguments
    #[command(flatten)]
    pub global_args: GlobalArgs,
}

/// List of commands
#[derive(Subcommand, Debug)]
pub enum Command {
    /// Initialize a new repository
    Init(cmd::init::CmdArgs),
}

#[derive(Parser, Debug)]
pub struct GlobalArgs {
    /// Repository path
    #[clap(short, long, value_parser)]
    pub repo: String,
}

pub fn log_success(tag: &str, str: &str) {
    println!("{}: {}", tag.bold().green(), str);
}

pub fn log_info(tag: &str, str: &str) {
    println!("{}: {}", tag.bold().cyan(), str);
}

pub fn log_purple(tag: &str, str: &str) {
    println!("{}: {}", tag.bold().purple(), str);
}

pub fn log_warning(str: &str) {
    eprintln!("{}: {}", "Warning".bold().yellow(), str);
}

pub fn log_error(str: &str) {
    eprintln!("{}: {}", "Error".bold().red(), str);
}
