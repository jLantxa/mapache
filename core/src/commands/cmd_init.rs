use anyhow::Result;
use clap::Args;
use colored::Colorize;
use serde::Serialize;

use crate::{
    backend::new_backend_with_prompt,
    commands::{ToExitCode, fail},
    mapache::{ID, defaults::SHORT_REPO_ID_LEN},
    repository::repo::Repository,
    ui::{self, json_reporter::emit_static},
    utils,
};

use super::GlobalArgs;

#[derive(Debug, Clone, Copy)]
pub enum InitError {
    AuthFail = 1,
    BackendError = 2,
    RepoInitError = 3,
}

impl ToExitCode for InitError {
    fn to_exit_code(&self) -> i32 {
        *self as i32
    }
}

#[derive(Args, Debug)]
#[clap(about = "Initialize a new repository")]
pub struct CmdArgs {}

const INIT_MSG: &str = "init";

pub async fn run(global_args: &GlobalArgs, _args: &CmdArgs) -> Result<()> {
    let backend = new_backend_with_prompt(global_args.backend_options(false))
        .await
        .map_err(|e| {
            fail(
                format!("Failed to initialize backend: {}", e),
                InitError::BackendError,
            )
        })?;

    let auth = match utils::get_auth(&global_args.auth_file)? {
        Some(a) => a,
        None => ui::cli::request_new_auth()
            .map_err(|_| fail("Authentication failed", InitError::AuthFail))?,
    };

    let manifest = Repository::init(&auth, global_args.key.as_ref(), backend.clone())
        .await
        .map_err(|e| {
            fail(
                format!(
                    "Failed to initialize repository in {:?}: {}",
                    global_args.repo, e
                ),
                InitError::RepoInitError,
            )
        })?;

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
        emit_static(
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
