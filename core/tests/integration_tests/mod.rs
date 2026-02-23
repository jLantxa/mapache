use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result};

use mapache::{
    backend::{StorageBackend, localfs::LocalFS, read_backend_dir},
    repository::repo::{Auth, Repository},
};

mod test_cmd_amend;
mod test_cmd_cat;
mod test_cmd_clean;
mod test_cmd_completion;
mod test_cmd_diff;
mod test_cmd_find;
mod test_cmd_forget;
mod test_cmd_init;
mod test_cmd_key;
mod test_cmd_lock;
mod test_cmd_log;
mod test_cmd_ls;
mod test_cmd_rebuild_index;
mod test_cmd_rechunk;
mod test_cmd_restore;
mod test_cmd_snapshot;
mod test_cmd_stats;
mod test_cmd_sync;
mod test_cmd_verify;

#[cfg(all(feature = "fuse", target_os = "linux"))]
mod test_cmd_mount;

const BACKUP_DATA_PATH: &str = "backup_data.tar.xz";

async fn init_repo(auth: &Auth, repo_path: PathBuf) -> Result<()> {
    let backend = Arc::new(LocalFS::new(repo_path));
    let _ = Repository::init(Some(auth), None, backend)
        .await
        .context("Failed to init repo")?;
    Ok(())
}

/// Remove all file nodes from a base directory. This is useful to remove all
/// index files or packs from the repository, without deleting the directories.
async fn delete_all_files_from(backend: &dyn StorageBackend, dir: &Path) -> Result<()> {
    let backend_objects = read_backend_dir(backend, &PathBuf::from(dir)).await?;

    for node in backend_objects {
        match node {
            mapache::backend::BackendNode::File(path) => backend.remove(&path).await?,
            mapache::backend::BackendNode::Dir(_) => (),
        }
    }

    Ok(())
}

fn set_write_permission<P: AsRef<Path>>(path: P, writable: bool) -> std::io::Result<()> {
    let metadata = std::fs::metadata(&path)?;
    let mut perms = metadata.permissions();

    perms.set_readonly(!writable);

    std::fs::set_permissions(&path, perms)
}

pub fn run_bin(args: &[&str]) -> Result<std::process::Output> {
    let bin_path = env!("CARGO_BIN_EXE_mapache");
    let mut cmd = std::process::Command::new(bin_path);
    cmd.args(args);
    let output = cmd.output().context("Failed to execute mapache binary")?;
    if !output.status.success() {
        eprintln!("Command failed with status: {}", output.status);
        eprintln!("stdout: {}", String::from_utf8_lossy(&output.stdout));
        eprintln!("stderr: {}", String::from_utf8_lossy(&output.stderr));
    }
    Ok(output)
}
