use std::io;

use clap::Args;

use crate::{
    commands::ToExitCode,
    common::{config::MapacheConfig, parallelism, sysinfo},
    utils::format_size_binary,
};

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
    /// Show detected system resources and the resolved parallelism plan
    #[command(alias = "show")]
    Info,
    /// Print a template TOML configuration file to stdout or a file
    Template {
        /// Output file path
        #[arg(short, long)]
        output: Option<std::path::PathBuf>,
    },
}

pub async fn run(args: &CmdArgs) -> Result<(), ConfigError> {
    match &args.command {
        ConfigCommand::Info => {
            let profile = sysinfo::profile();
            let plan = parallelism::plan();
            let limiters = parallelism::limiter_status();

            println!("System");
            println!("  Logical CPUs:      {}", profile.logical_cpus);
            println!(
                "  Available memory:  {}{}",
                format_size_binary(profile.available_memory, 1),
                if profile.cgroup_cpu_limit || profile.cgroup_memory_limit {
                    " (cgroup-limited)"
                } else {
                    ""
                }
            );
            println!(
                "  Total memory:      {}",
                format_size_binary(profile.total_memory, 1)
            );
            println!(
                "  Memory budget:     {} (cpu scale {:.2})",
                format_size_binary(plan.memory_budget, 1),
                plan.cpu_scale
            );

            println!();
            println!("Runtime");
            println!(
                "  Async workers:          {}",
                parallelism::runtime_worker_threads()
            );
            println!(
                "  Blocking threads (max): {}",
                parallelism::runtime_max_blocking_threads()
            );

            println!();
            println!("Parallelism");
            println!("{}", plan.detailed_summary());

            println!();
            println!("Demand");
            println!("{}", plan.demand.detailed_summary());

            println!();
            println!("Limiters");
            println!(
                "  CPU:             {} / {}",
                limiters.cpu_used, limiters.cpu_capacity
            );
            println!(
                "  Writes:          {} / {}",
                limiters.writes_used, limiters.writes_capacity
            );
            println!(
                "  Restore decode:  {} / {}",
                format_size_binary(limiters.restore_decode_used_bytes, 1),
                format_size_binary(limiters.restore_decode_capacity_bytes, 1)
            );
            println!(
                "  Verify decode:   {} / {}",
                format_size_binary(limiters.verify_decode_used_bytes, 1),
                format_size_binary(limiters.verify_decode_capacity_bytes, 1)
            );
        }
        ConfigCommand::Template { output } => {
            let config = MapacheConfig::template();
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
