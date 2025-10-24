use std::{path::PathBuf, sync::Arc};

use anyhow::{Context, Result};

use mapache::{
    backend::localfs::LocalFS,
    repository::repo::{Auth, Repository},
};

mod test_cmd_amend;
mod test_cmd_clean;
mod test_cmd_init;
mod test_cmd_restore;
mod test_cmd_snapshot;
mod test_cmd_sync;

#[cfg(all(feature = "fuse", unix))]
mod test_cmd_mount;

const BACKUP_DATA_PATH: &str = "backup_data.tar.xz";

fn init_repo(auth: &Auth, repo_path: PathBuf) -> Result<()> {
    let backend = Arc::new(LocalFS::new(repo_path));
    Repository::init(Some(auth), None, backend).with_context(|| "Failed to init repo")
}
