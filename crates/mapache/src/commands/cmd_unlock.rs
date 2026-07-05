use std::io;

use clap::Args;

use crate::{
    backend::new_backend_with_prompt,
    commands::{GlobalArgs, ToExitCode, open_repository},
    common::ContentIdType,
    repository::repo::RepoConfig,
    ui,
    utils::{self, size},
};

#[derive(Debug, thiserror::Error)]
pub enum UnlockError {
    #[error("failed to open repository: {0}")]
    RepoOpenFail(String),
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl ToExitCode for UnlockError {
    fn to_exit_code(&self) -> i32 {
        match self {
            UnlockError::RepoOpenFail(_) => 10,
            UnlockError::Io(_) => 1,
        }
    }
}

#[derive(Args, Debug, Clone)]
#[clap(about = "Remove existing locks")]
pub struct CmdArgs {
    #[clap(short, long, default_value_t = false)]
    pub force: bool,
}

pub async fn run(global_args: &GlobalArgs, args: &CmdArgs) -> Result<(), UnlockError> {
    let backend = new_backend_with_prompt(global_args.backend_options(false))
        .await
        .map_err(|e| {
            UnlockError::RepoOpenFail(format!("failed to initialize backend: {}", e.inner()))
        })?;

    let config = RepoConfig {
        pack_size: (global_args.pack_size_mib * size::MiB as f32) as u64,
        use_cache: !global_args.no_cache,
        compression: global_args.compression_level,
    };

    let (repo, _) = open_repository(
        global_args.auth_file.as_ref(),
        global_args.key.as_ref(),
        backend,
        config,
    )
    .await
    .map_err(|e| UnlockError::RepoOpenFail(e.inner()))?;

    let locks = repo
        .get_locks()
        .await
        .map_err(|e| UnlockError::RepoOpenFail(format!("failed to get locks: {}", e.inner())))?;
    let mut num_deleted_locks = 0;
    for lock in locks {
        if args.force || lock.is_expired() {
            tracing::info!(target: "unlock", "Deleting lock {}", lock.id().to_short_hex(8));
            repo.delete_file(ContentIdType::Lock, lock.id(), None)
                .await
                .map_err(|e| {
                    UnlockError::RepoOpenFail(format!("failed to delete lock: {}", e.inner()))
                })?;
            num_deleted_locks += 1;
        }
    }

    ui::cli::log!(
        "Deleted {}",
        utils::format_count(num_deleted_locks, "lock", "locks")
    );
    tracing::info!(target: "unlock", "Deleted {} locks", num_deleted_locks);

    Ok(())
}
