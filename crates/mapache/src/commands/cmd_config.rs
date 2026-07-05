use std::io;

use clap::Args;

use crate::commands::ToExitCode;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl ToExitCode for ConfigError {
    fn to_exit_code(&self) -> i32 {
        match self {
            ConfigError::Io(_) => 1,
        }
    }
}

#[derive(Args, Debug)]
#[clap(about = "Manage configuration")]
pub struct CmdArgs {
    #[command(subcommand)]
    pub command: ConfigCommand,
}

#[derive(clap::Subcommand, Debug)]
pub enum ConfigCommand {
    /// Print a template TOML configuration file to stdout or a file
    Template {
        /// Output file path
        #[arg(short, long)]
        output: Option<std::path::PathBuf>,
    },
}

pub async fn run(args: &CmdArgs) -> Result<(), ConfigError> {
    match &args.command {
        ConfigCommand::Template { output } => {
            let config = crate::common::config::MapacheConfig::template();
            let toml_str = toml::to_string_pretty(&config)
                .map_err(|e| ConfigError::Io(io::Error::other(e)))?;

            // Comment out the generated TOML lines
            let mut template = String::from("# mapache configuration template\n");
            template.push_str(
                "# This file defines repository-wide settings, overridable via CLI flags.\n\n",
            );

            for line in toml_str.lines() {
                if line.starts_with('[') {
                    template.push_str(line);
                } else if line.is_empty() {
                    template.push('\n');
                } else {
                    template.push_str("# ");
                    template.push_str(line);
                }
                template.push('\n');
            }

            if let Some(path) = output {
                std::fs::write(path, template)?;
                println!("Template configuration written to {}", path.display());
            } else {
                println!("{}", template);
            }
        }
    }
    Ok(())
}
