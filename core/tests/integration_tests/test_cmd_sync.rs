#![cfg(test)]

mod tests {
    use std::{path::PathBuf, sync::Arc};

    use anyhow::{Context, Result};
    use mapache::{
        backend::{self, BackendNode, StorageBackend, localfs::LocalFS},
        commands::{
            self, Compression, GlobalArgs, UseSnapshot, cmd_snapshot,
            cmd_sync::{self},
        },
        mapache::{defaults::DEFAULT_DEFAULT_PACK_SIZE_MIB, global::set_global_opts_with_args},
        repository::repo::Auth,
    };

    use tempfile::tempdir;

    use crate::{
        TEST_QUIET,
        integration_tests::{BACKUP_DATA_PATH, init_repo},
        test_utils::{self},
    };

    #[tokio::test]
    async fn test_sync_no_delete() -> Result<()> {
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

        let backup_data_path = test_utils::get_test_data_path(BACKUP_DATA_PATH);
        let backup_data_tmp_path = tmp_path.join("backup");
        test_utils::extract_tar_xz_archive(&backup_data_path, &backup_data_tmp_path)?;

        let repo = String::from("repo");
        let repo_path = tmp_path.join(&repo);

        let global = GlobalArgs {
            repo: repo_path.to_string_lossy().to_string(),
            auth_file: Some(auth_file_path),
            key: None,
            quiet: *TEST_QUIET,
            json: false,
            verbosity: Some(3),
            ssh_pubkey: None,
            ssh_privatekey: None,
            pack_size_mib: DEFAULT_DEFAULT_PACK_SIZE_MIB,
            no_cache: true,
            retry_lock_duration: None,
            compression_level: Compression::Fastest,
        };
        set_global_opts_with_args(&global);

        // Init repo
        init_repo(&auth, repo_path.clone()).await?;

        // Run snapshot
        let snapshot_args = cmd_snapshot::CmdArgs {
            paths: vec![
                backup_data_tmp_path.join("0"),
                backup_data_tmp_path.join("1"),
                backup_data_tmp_path.join("2"),
                backup_data_tmp_path.join("file.txt"),
            ],
            as_root: false,
            exclude: None,
            tags_str: String::new(),
            description: None,
            no_parent: false,
            skip_if_unchanged: false,
            no_scan: true,
            parent: UseSnapshot::Latest,
            num_readers: 2,
            num_packers: 2,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&global, &snapshot_args)
            .await
            .context("Failed to run cmd_snapshot")?;

        let dst_repo_path = tmp_path.join("sync_dst");
        let sync_args = cmd_sync::CmdArgs {
            target: dst_repo_path.to_string_lossy().to_string(),
            delete: false,
            dst_ssh_pubkey: None,
            dst_ssh_privatekey: None,
        };
        cmd_sync::run(&global, &sync_args)
            .await
            .context("Failed to run cmd_sync")?;

        let src_backend = Arc::new(LocalFS::new(repo_path));
        let dst_backend = Arc::new(LocalFS::new(dst_repo_path));

        let forward_cmp = |n0: &BackendNode, n1: &BackendNode| n0.path().cmp(n1.path());
        let mut src_nodes =
            backend::read_backend_dir(src_backend.as_ref(), &PathBuf::new()).await?;
        let mut dst_nodes =
            backend::read_backend_dir(dst_backend.as_ref(), &PathBuf::new()).await?;
        src_nodes.sort_unstable_by(forward_cmp);
        dst_nodes.sort_unstable_by(forward_cmp);

        assert_eq!(src_nodes, dst_nodes);

        Ok(())
    }

    #[tokio::test]
    async fn test_sync_with_delete() -> Result<()> {
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

        let backup_data_path = test_utils::get_test_data_path(BACKUP_DATA_PATH);
        let backup_data_tmp_path = tmp_path.join("backup");
        test_utils::extract_tar_xz_archive(&backup_data_path, &backup_data_tmp_path)?;

        let repo = String::from("repo");
        let repo_path = tmp_path.join(&repo);

        let global = GlobalArgs {
            repo: repo_path.to_string_lossy().to_string(),
            auth_file: Some(auth_file_path),
            key: None,
            quiet: *TEST_QUIET,
            json: false,
            verbosity: Some(3),
            ssh_pubkey: None,
            ssh_privatekey: None,
            pack_size_mib: DEFAULT_DEFAULT_PACK_SIZE_MIB,
            no_cache: true,
            retry_lock_duration: None,
            compression_level: Compression::Fastest,
        };
        set_global_opts_with_args(&global);

        // Init repo
        init_repo(&auth, repo_path.clone()).await?;

        // Run snapshot
        let snapshot_args = cmd_snapshot::CmdArgs {
            paths: vec![
                backup_data_tmp_path.join("0"),
                backup_data_tmp_path.join("1"),
                backup_data_tmp_path.join("2"),
                backup_data_tmp_path.join("file.txt"),
            ],
            as_root: false,
            exclude: None,
            tags_str: String::new(),
            description: None,
            no_parent: false,
            skip_if_unchanged: false,
            no_scan: true,
            parent: UseSnapshot::Latest,
            num_readers: 2,
            num_packers: 2,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&global, &snapshot_args)
            .await
            .context("Failed to run cmd_snapshot")?;

        let dst_repo_path = tmp_path.join("sync_dst");

        let src_backend = Arc::new(LocalFS::new(repo_path));
        let dst_backend = Arc::new(LocalFS::new(dst_repo_path.clone()));
        dst_backend.create().await?;

        // Add some dummy files to dst repo
        std::fs::create_dir_all(dst_repo_path.join("snapshots"))?;
        std::fs::write(
            dst_repo_path.join("snapshots").join("dummy_snapshot"),
            b"Dummy content",
        )?;
        std::fs::create_dir_all(dst_repo_path.join("objects").join("ff"))?;
        std::fs::write(
            dst_repo_path.join("objects").join("ff").join("dummy_pack"),
            b"Dummy content",
        )?;
        std::fs::create_dir_all(dst_repo_path.join("index"))?;
        std::fs::write(
            dst_repo_path.join("index").join("dummy_index"),
            b"Dummy content",
        )?;

        let sync_args = cmd_sync::CmdArgs {
            target: dst_repo_path.to_string_lossy().to_string(),
            delete: true,
            dst_ssh_pubkey: None,
            dst_ssh_privatekey: None,
        };
        cmd_sync::run(&global, &sync_args)
            .await
            .context("Failed to run cmd_sync")?;

        let forward_cmp = |n0: &BackendNode, n1: &BackendNode| n0.path().cmp(n1.path());
        let mut src_nodes =
            backend::read_backend_dir(src_backend.as_ref(), &PathBuf::new()).await?;
        let mut dst_nodes =
            backend::read_backend_dir(dst_backend.as_ref(), &PathBuf::new()).await?;
        src_nodes.sort_unstable_by(forward_cmp);
        dst_nodes.sort_unstable_by(forward_cmp);

        assert_eq!(src_nodes, dst_nodes);

        Ok(())
    }
}
