#![cfg(feature = "tui")]
use anyhow::Result;
use clap::Args;

use crate::{
    backend::new_backend_with_prompt,
    commands::{GlobalArgs, with_repository_lock},
    ui::tui,
};

#[derive(Args, Debug, Clone)]
#[clap(about = "Launch interactive terminal user interface")]
pub struct CmdArgs;

pub async fn run(
    global_args: &GlobalArgs,
    _args: &CmdArgs,
    snapshot_config: Option<crate::commands::cmd_snapshot::CmdArgs>,
    forget_config: Option<crate::commands::cmd_forget::CmdArgs>,
) -> Result<()> {
    let backend = new_backend_with_prompt(global_args.backend_options(false)).await?;
    let repo_path = global_args.repo.clone();

    with_repository_lock(
        global_args.auth_file.as_ref(),
        global_args.key.as_ref(),
        backend,
        global_args.to_repo_config(),
        false,
        global_args.retry_lock_duration,
        |repo, _secure_storage, lock_handle| async move {
            repo.reload_master_index().await?;
            tui::run(repo, lock_handle, repo_path, snapshot_config, forget_config).await
        },
    )
    .await
}
