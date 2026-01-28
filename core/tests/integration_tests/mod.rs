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
mod test_cmd_clean;
mod test_cmd_init;
mod test_cmd_rebuild_index;
mod test_cmd_restore;
mod test_cmd_snapshot;
mod test_cmd_sync;
mod test_cmd_verify;

#[cfg(all(feature = "fuse", target_os = "linux"))]
mod test_cmd_mount;

const BACKUP_DATA_PATH: &str = "backup_data.tar.xz";

fn init_repo(auth: &Auth, repo_path: PathBuf) -> Result<()> {
    let backend = Arc::new(LocalFS::new(repo_path));
    let _ = Repository::init(Some(auth), None, backend).context("Failed to init repo")?;
    Ok(())
}

/// Remove all file nodes from a base directory. This is useful to remove all
/// index files or packs from the repository, without deleting the directories.
fn delete_all_files_from(backend: &dyn StorageBackend, dir: &Path) -> Result<()> {
    let backend_objects = read_backend_dir(backend, &PathBuf::from(dir))?;

    for node in backend_objects {
        match node {
            mapache::backend::BackendNode::File(path) => backend.remove(&path)?,
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
