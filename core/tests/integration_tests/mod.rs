use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result};
use tempfile::tempdir;

use mapache::{
    backend::{StorageBackend, localfs::LocalFS, read_backend_dir},
    commands::{Compression, GlobalArgs},
    mapache::{defaults::DEFAULT_DEFAULT_PACK_SIZE_MIB, global::set_global_opts_with_args},
    repository::repo::{Auth, Repository},
};

use crate::{TEST_QUIET, test_utils};

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
mod test_corrupt_repo;
mod test_lock_cleanup;
mod test_permission_denied;

#[cfg(all(feature = "fuse", target_os = "linux"))]
mod test_cmd_mount;

const BACKUP_DATA_PATH: &str = "backup_data.tar.xz";

pub fn assert_times_equal(t1: std::time::SystemTime, t2: std::time::SystemTime) {
    if t1 == t2 {
        return;
    }

    #[cfg(target_os = "windows")]
    {
        use std::time::UNIX_EPOCH;
        let d1 = t1.duration_since(UNIX_EPOCH).unwrap_or_default();
        let d2 = t2.duration_since(UNIX_EPOCH).unwrap_or_default();
        let diff = if d1 > d2 { d1 - d2 } else { d2 - d1 };
        if diff.as_secs() < 1 {
            return;
        }
    }

    assert_eq!(t1, t2);
}

pub struct TestContext {
    pub _tmp_dir: tempfile::TempDir,
    pub repo_path: PathBuf,
    pub auth: Auth,
    pub auth_file_path: PathBuf,
    pub global: GlobalArgs,
    pub backup_data_path: Option<PathBuf>,
}

impl TestContext {
    pub async fn new() -> Result<Self> {
        let tmp_dir = tempdir()?;
        let tmp_path = tmp_dir.path();
        let auth = Auth {
            username: "mapachito".to_string(),
            password: "password".to_string(),
        };
        let auth_file_path = tmp_path.join("auth");
        std::fs::write(
            &auth_file_path,
            format!("{}\n{}", auth.username, auth.password),
        )?;

        let repo_path = tmp_path.join("repo");

        let global = GlobalArgs {
            repo: repo_path.to_string_lossy().to_string(),
            auth_file: Some(auth_file_path.clone()),
            key: None,
            quiet: *TEST_QUIET,
            json: false,
            verbosity: Some(3),
            ssh_privatekey: None,
            pack_size_mib: DEFAULT_DEFAULT_PACK_SIZE_MIB,
            no_cache: true,
            retry_lock_duration: None,
            compression_level: Compression::Fastest,
            limit_upload: None,
            limit_download: None,
        };
        set_global_opts_with_args(&global);

        Ok(Self {
            _tmp_dir: tmp_dir,
            repo_path,
            auth,
            auth_file_path,
            global,
            backup_data_path: None,
        })
    }

    pub fn setup_backup_data(&mut self) -> Result<()> {
        let backup_data_path = test_utils::get_test_data_path(BACKUP_DATA_PATH);
        let backup_data_tmp_path = self._tmp_dir.path().join("backup");
        test_utils::extract_tar_xz_archive(&backup_data_path, &backup_data_tmp_path)?;
        self.backup_data_path = Some(backup_data_tmp_path);
        Ok(())
    }

    pub async fn init_repo(&self) -> Result<()> {
        init_repo(&self.auth, self.repo_path.clone()).await
    }
}

async fn init_repo(auth: &Auth, repo_path: PathBuf) -> Result<()> {
    let backend = Arc::new(LocalFS::new(repo_path));
    let _ = Repository::init(auth, None, backend)
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
            mapache::backend::BackendNode::File(path, _) => backend.remove(&path).await?,
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

    // Set --no-cache for commands requiring it.
    if !args.is_empty() {
        match args[0] {
            // These commands don't accept --no-cache
            "cache" | "completion" | "key" => (),
            _ => {
                let _ = cmd.arg("--no-cache");
            }
        }
    }

    println!("{cmd:?}");

    let output = cmd.output().context("Failed to execute mapache binary")?;
    if !output.status.success() {
        eprintln!("Command failed with status: {}", output.status);
        eprintln!("stdout: {}", String::from_utf8_lossy(&output.stdout));
        eprintln!("stderr: {}", String::from_utf8_lossy(&output.stderr));
    }
    Ok(output)
}
