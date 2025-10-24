use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Args, CommandFactory};
use clap_complete::Shell;

use crate::commands;

#[derive(Args, Debug)]
#[clap(about = "Generate autocompletion scripts")]
pub struct CmdArgs {
    /// Shell type (bash, zsh, fish, powershell, etc.)
    #[clap(long, value_parser)]
    pub shell: Shell,

    /// Output directory where the completion script will be written
    #[clap(long, value_parser)]
    pub path: PathBuf,
}

pub fn run(args: &CmdArgs) -> Result<()> {
    // Ensure directory exists
    std::fs::create_dir_all(&args.path)
        .with_context(|| format!("Failed to create directory: {}", args.path.display()))?;

    // Generate completion script
    let mut cmd = commands::Cli::command();
    let bin_name = &cmd.get_name().to_string();
    clap_complete::generate_to(args.shell, &mut cmd, bin_name, &args.path)
        .with_context(|| format!("Failed to generate completion for {:?}", args.shell))?;

    println!(
        "Completion script for {:?} written to {}",
        args.shell,
        args.path.display()
    );

    Ok(())
}
