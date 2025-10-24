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
