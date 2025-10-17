// mapache is a secure, de-duplicating, incremental backup tool.
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

use anyhow::Result;
use colored::Colorize;

use mapache::{commands, ui};

fn main() -> Result<()> {
    // Parse arguments and execute commands
    if let Err(e) = commands::parse_and_run() {
        ui::cli::error!("{}", e.to_string());
        ui::cli::log!();
        ui::cli::log!("Finished with {}", "Error".bold().red());
        std::process::exit(1);
    }

    Ok(())
}
