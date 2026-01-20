use anyhow::Result;
use clap::Args;
use colored::Colorize;

use crate::backend::new_backend_with_prompt;
use crate::mapache::defaults::SHORT_REPO_ID_LEN;
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

    let manifest = Repository::init(auth.as_ref(), global_args.key.as_ref(), backend.clone())?;

    ui::cli::log!(
        "Created repo with id {} at {}\n",
        manifest.id().to_short_hex(SHORT_REPO_ID_LEN),
        global_args.repo
    );

    ui::cli::warning!(
        "This password is the key to your repository\nand the only way to access your data.\n{}",
        "Don't forget it.".bold().green()
    );

    Ok(())
}
