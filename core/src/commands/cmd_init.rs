use anyhow::Result;
use clap::Args;
use colored::Colorize;
use serde::Serialize;

use crate::{
    backend::new_backend_with_prompt,
    commands::{GlobalArgs, ToExitCode, fail},
    mapache::{ID, defaults::SHORT_REPO_ID_LEN},
    repository::repo::Repository,
    ui::{self, json::emit_static},
    utils,
};

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

#[derive(Args, Debug, Clone)]
pub struct CmdArgs {}

const INIT_MSG: &str = "init";

pub async fn run(global_args: &GlobalArgs, _args: &CmdArgs) -> Result<()> {
    tracing::info!(target: "init", "Initializing repository at {}", global_args.repo);

    let backend = new_backend_with_prompt(global_args.backend_options(false))
        .await
        .map_err(|e| {
            tracing::error!(target: "init", "Backend initialization failed: {:#}", e);
            fail(
                format!("Failed to initialize backend: {:#}", e),
                InitError::BackendError,
            )
        })?;

    tracing::info!(target: "init", "Backend initialized");

    let auth = match utils::get_auth(&global_args.auth_file)? {
        Some(a) => a,
        None => ui::cli::request_new_auth().map_err(|_| {
            tracing::error!(target: "init", "Authentication failed");
            fail("Authentication failed", InitError::AuthFail)
        })?,
    };

    tracing::info!(target: "init", "Calling Repository::init");

    let manifest = Repository::init(&auth, global_args.key.as_ref(), backend.clone())
        .await
        .map_err(|e| {
            tracing::error!(target: "init", "Repository::init failed: {e}");
            fail(
                format!(
                    "Failed to initialize repository in {:?}: {}",
                    global_args.repo, e
                ),
                InitError::RepoInitError,
            )
        })?;

    tracing::info!(
        target: "init",
        "Repository {} initialized at {}",
        manifest.id().to_short_hex(SHORT_REPO_ID_LEN),
        global_args.repo
    );

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
