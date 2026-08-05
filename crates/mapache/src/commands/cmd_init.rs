use std::io;

use clap::Args;
use serde::Serialize;

use crate::{
    backend::new_backend_with_prompt,
    commands::{Compression, GlobalArgs, ToExitCode},
    common::{ID, defaults::SHORT_REPO_ID_LEN, error::MapacheError},
    repository::repo::{Repository, THIS_REPOSITORY_VERSION, warn_v1_deprecated},
    ui::{self, json::emit_static},
    utils,
};

#[derive(Debug, thiserror::Error)]
pub enum InitError {
    #[error("authentication failed: {0}")]
    AuthFail(String),
    #[error("backend initialization failed: {0}")]
    BackendError(String),
    #[error("repository initialization failed: {0}")]
    RepoInitError(String),
    #[error(transparent)]
    Repo(#[from] MapacheError),
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl ToExitCode for InitError {
    fn to_exit_code(&self) -> i32 {
        match self {
            InitError::AuthFail(_) => 1,
            InitError::BackendError(_) => 2,
            InitError::RepoInitError(_) => 3,
            InitError::Repo(_) => 1,
            InitError::Io(_) => 1,
        }
    }
}

#[derive(Args, Debug, Clone)]
#[clap(
    about = "Initialize a new repository",
    long_about = "Initialize a new repository at the path specified by --repo.\n\n\
        The repository stores encrypted, deduplicated backup data. You will be\n\
        prompted to set a username and password for authentication.\n\n\
        A repository can be a local directory, an SFTP server, or an S3-compatible\n\
        storage bucket. The storage backend is determined by the --repo URL scheme."
)]
pub struct CmdArgs {
    /// Repository format version (1 or 2)
    #[clap(long, default_value_t = THIS_REPOSITORY_VERSION)]
    pub format: u32,
}

const INIT_MSG: &str = "init";

pub async fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<(), InitError> {
    if args.format < 1 || args.format > 2 {
        return Err(InitError::RepoInitError(format!(
            "unsupported repository format: {} (supported: 1, 2)",
            args.format
        )));
    }

    // The v1 format has no per-blob compression marker: blobs are always
    // zstd-compressed, so `--compression none` cannot be honored.
    if args.format < 2 && matches!(global_args.compression_level, Compression::None) {
        return Err(InitError::RepoInitError(
            "compression 'none' is not supported in repository format v1; \
             use format 2 or a compression preset"
                .to_string(),
        ));
    }

    tracing::info!(target: "init", "Initializing repository at {}", global_args.repo);

    let backend = new_backend_with_prompt(global_args.backend_options(false))
        .await
        .map_err(|e| {
            tracing::error!(target: "init", "backend initialization failed: {:#}", e);
            InitError::BackendError(e.inner())
        })?;

    tracing::info!(target: "init", "Backend initialized");

    let auth = match utils::get_auth(&global_args.auth_file)? {
        Some(a) => a,
        None => ui::cli::request_new_auth().map_err(|e| {
            tracing::error!(target: "init", "authentication failed");
            InitError::AuthFail(e.to_string())
        })?,
    };

    tracing::info!(target: "init", "Calling Repository::init");

    let manifest = Repository::init(
        args.format,
        &auth,
        global_args.key.as_ref(),
        backend.clone(),
    )
    .await
    .map_err(|e| {
        tracing::error!(target: "init", "Repository::init failed: {e}");
        InitError::RepoInitError(e.inner())
    })?;

    tracing::info!(
        target: "init",
        "Repository {} initialized at {}",
        manifest.id().to_short_hex(SHORT_REPO_ID_LEN),
        global_args.repo
    );

    if !global_args.json {
        ui::cli::log!(
            "Created repo v{} with id {} at {}\n",
            args.format,
            manifest.id().to_short_hex(SHORT_REPO_ID_LEN),
            global_args.repo
        );

        if args.format == 1 {
            warn_v1_deprecated();
        }

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
