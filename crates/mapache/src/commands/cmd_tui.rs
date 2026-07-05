use std::io;

use clap::Args;

use crate::{
    backend::new_backend_with_prompt,
    commands::{self, GlobalArgs, ToExitCode, with_repository_lock},
    common::error::MapacheError,
    ui::tui,
};

#[derive(Debug, thiserror::Error)]
pub enum TuiError {
    #[error(transparent)]
    Repo(#[from] MapacheError),
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl ToExitCode for TuiError {
    fn to_exit_code(&self) -> i32 {
        match self {
            TuiError::Repo(_) => 1,
            TuiError::Io(_) => 1,
        }
    }
}

#[derive(Args, Debug, Clone)]
#[clap(about = "Launch interactive terminal user interface")]
pub struct CmdArgs;

pub async fn run(
    global_args: &GlobalArgs,
    _args: &CmdArgs,
    snapshot_config: Option<commands::cmd_snapshot::CmdArgs>,
    forget_config: Option<commands::cmd_forget::CmdArgs>,
) -> Result<(), TuiError> {
    let backend = new_backend_with_prompt(global_args.backend_options(false)).await?;
    let repo_path = global_args.repo.clone();

    with_repository_lock(
        global_args.auth_file.as_ref(),
        global_args.key.as_ref(),
        backend,
        global_args.to_repo_config(),
        false,
        global_args.retry_lock_duration,
        global_args.no_lock,
        |repo, _secure_storage, lock_handle| async move {
            repo.reload_master_index().await?;
            tui::run(repo, lock_handle, repo_path, snapshot_config, forget_config)
                .await
                .map_err(|e| TuiError::Repo(MapacheError::Internal(e.to_string())))
        },
    )
    .await
}
