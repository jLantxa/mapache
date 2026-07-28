use std::{io, path::PathBuf};

use clap::{Args, CommandFactory};
use clap_complete::Shell;

use crate::{commands, commands::ToExitCode, ui};

#[derive(Debug, thiserror::Error)]
pub enum CompletionError {
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl ToExitCode for CompletionError {
    fn to_exit_code(&self) -> i32 {
        match self {
            CompletionError::Io(_) => 1,
        }
    }
}

#[derive(Args, Debug, Clone)]
#[clap(about = "Generate autocompletion scripts")]
pub struct CmdArgs {
    /// Shell type (bash, zsh, fish, powershell, etc.)
    #[clap(long, value_parser)]
    pub shell: Shell,

    /// Output directory where the completion script will be written
    #[clap(long, value_parser)]
    pub path: PathBuf,
}

pub fn run(args: &CmdArgs) -> Result<(), CompletionError> {
    // Ensure directory exists
    std::fs::create_dir_all(&args.path).map_err(|e| {
        CompletionError::Io(io::Error::other(format!(
            "failed to create directory: {}: {}",
            args.path.display(),
            e
        )))
    })?;

    // Generate completion script
    let mut cmd = commands::Cli::command();
    let bin_name = &cmd.get_name().to_string();
    clap_complete::generate_to(args.shell, &mut cmd, bin_name, &args.path).map_err(|e| {
        CompletionError::Io(io::Error::other(format!(
            "failed to generate completion for {}: {}",
            args.shell, e
        )))
    })?;

    ui::cli::log!(
        "Completion script for {} written to {}",
        args.shell,
        args.path.display()
    );

    Ok(())
}
