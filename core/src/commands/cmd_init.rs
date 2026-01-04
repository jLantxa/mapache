use anyhow::Result;
use clap::Args;
use colored::Colorize;

use crate::backend::new_backend_with_prompt;
use crate::repository::repo::Repository;
use crate::ui;
use crate::utils;

use super::GlobalArgs;

#[derive(Args, Debug)]
#[clap(about = "Initialize a new repository")]
pub struct CmdArgs {}

pub fn run(global_args: &GlobalArgs, _args: &CmdArgs) -> Result<()> {
    let auth = utils::get_auth_from_file(&global_args.auth_file)?;
    let backend = new_backend_with_prompt(global_args.backend_options(false))?;

    ui::cli::log!("Initializing a new repository in '{}'", &global_args.repo);
    Repository::init(auth.as_ref(), global_args.key.as_ref(), backend)?;

    ui::cli::warning!(
        "{}\n{}",
        "This password is the key to your repository and the only way to access your data.",
        "Don't forget it.".bold().green()
    );

    Ok(())
}
