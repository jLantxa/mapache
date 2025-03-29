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

use anyhow::Result;
use clap::Parser;
use colored::Colorize;

use backup::cli::Cli;
use backup::{cli, cmd};

fn run(args: &Cli) -> Result<()> {
    match &args.command {
        cli::Command::Init(cmd_args) => cmd::init::run(&args.global_args, cmd_args),
    }
}

fn main() {
    let args = Cli::parse();

    if let Err(e) = run(&args) {
        cli::log_error(e.to_string().as_str());
        std::process::exit(1);
    }

    println!("{}", "Finished".bold().green());
}
