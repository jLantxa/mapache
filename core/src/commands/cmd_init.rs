use anyhow::Result;
use anyhow::bail;
use clap::Args;
use colored::Colorize;
use serde::Serialize;

use crate::mapache::ID;
use crate::ui::json_reporter::JsonReporter;
use crate::{
    backend::new_backend_with_prompt,
    mapache::defaults::SHORT_REPO_ID_LEN,
    repository::repo::Repository,
    ui::{self},
    utils,
};

use super::GlobalArgs;

#[derive(Args, Debug)]
#[clap(about = "Initialize a new repository")]
pub struct CmdArgs {}

const INIT_MSG: &str = "init";

pub fn run(global_args: &GlobalArgs, _args: &CmdArgs) -> Result<()> {
    let backend = new_backend_with_prompt(global_args.backend_options(false))?;

    // Create the repository root
    if backend.root_exists() {
        bail!("Cannot initialize a repository because a directory already exists");
    }

    let auth = utils::get_auth_from_file(&global_args.auth_file)?;
    let manifest = Repository::init(auth.as_ref(), global_args.key.as_ref(), backend.clone())?;

    if !global_args.json {
        ui::cli::log!(
            "Created repo with id {} at {}\n",
            manifest.id().to_short_hex(SHORT_REPO_ID_LEN),
            global_args.repo
        );

        ui::cli::warning!(
            "This password is the key to your repository\nand the only way to access your data.\n{}",
            "Don't forget it.".bold().green()
        );
    } else {
        JsonReporter::emit_static(
            INIT_MSG,
            &MsgInit {
                id: manifest.id(),
                path: &global_args.repo,
            },
        );
    }

    Ok(())
}

#[derive(Serialize)]
struct MsgInit<'a> {
    id: &'a ID,
    path: &'a str,
}
