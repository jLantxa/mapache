use anyhow::Result;

use mapache::{commands, ui};

fn main() -> Result<()> {
    // Parse arguments and execute commands
    if let Err(e) = commands::parse_and_run() {
        ui::cli::error!("{}", e.to_string());
        std::process::exit(1);
    }

    Ok(())
}
