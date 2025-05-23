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

pub mod archiver;
pub mod backend;
pub mod backup;
pub mod cli;
pub mod commands;
pub mod repository;
pub mod restorer;
pub mod ui;

#[cfg(test)]
pub mod testing;
pub mod utils;

use anyhow::Result;
use clap::Parser;
use colored::Colorize;

fn run(args: &commands::Cli) -> Result<()> {
    match &args.command {
        commands::Command::Init(cmd_args) => commands::cmd_init::run(&args.global_args, cmd_args),
        commands::Command::Log(cmd_args) => commands::cmd_log::run(&args.global_args, cmd_args),
        commands::Command::Snapshot(cmd_args) => {
            commands::cmd_snapshot::run(&args.global_args, cmd_args)
        }
        commands::Command::Restore(cmd_args) => {
            commands::cmd_restore::run(&args.global_args, cmd_args)
        }
        commands::Command::Cat(cmd_args) => commands::cmd_cat::run(&args.global_args, cmd_args),
    }
}

fn main() -> Result<()> {
    let args = commands::Cli::parse();

    // Run the command
    if let Err(e) = run(&args) {
        cli::log_error(&e.to_string());
        cli::log!("Finished with {}", "Error".bold().red());
        std::process::exit(1);
    }

    Ok(())
}
