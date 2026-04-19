#![cfg(test)]

mod tests {
    use std::{path::PathBuf, sync::Arc};

    use anyhow::{Context, Result};
    use mapache::{
        backend::{self, BackendNode, StorageBackend, localfs::LocalFS},
        commands::{
            self, UseSnapshot, cmd_snapshot,
            cmd_sync::{self},
        },
        repository::repo::LOCKS_DIR,
    };

    use crate::integration_tests::TestContext;

    #[tokio::test]
    async fn test_sync_no_delete() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        ctx.setup_backup_data()?;
        let backup_data_tmp_path = ctx.backup_data_path.as_ref().unwrap();

        // Init repo
        ctx.init_repo().await?;

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
            exclude_file: None,
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
        commands::cmd_snapshot::run(&ctx.global, &snapshot_args)
            .await
            .context("Failed to run cmd_snapshot")?;

        let dst_repo_path = ctx._tmp_dir.path().join("sync_dst");
        let sync_args = cmd_sync::CmdArgs {
            target: dst_repo_path.to_string_lossy().to_string(),
            delete: false,
            dst_ssh_privatekey: None,
            dry_run: false,
        };
        cmd_sync::run(&ctx.global, &sync_args)
            .await
            .context("Failed to run cmd_sync")?;

        let src_backend = Arc::new(LocalFS::new(ctx.repo_path));
        let dst_backend = Arc::new(LocalFS::new(dst_repo_path));

        let forward_cmp = |n0: &BackendNode, n1: &BackendNode| n0.path().cmp(n1.path());
        let mut src_nodes: Vec<BackendNode> =
            backend::read_backend_dir(src_backend.as_ref(), &PathBuf::new())
                .await?
                .into_iter()
                .filter(|n| !n.path().starts_with(LOCKS_DIR))
                .collect();
        let mut dst_nodes: Vec<BackendNode> =
            backend::read_backend_dir(dst_backend.as_ref(), &PathBuf::new())
                .await?
                .into_iter()
                .filter(|n| !n.path().starts_with(LOCKS_DIR))
                .collect();
        src_nodes.sort_unstable_by(forward_cmp);
        dst_nodes.sort_unstable_by(forward_cmp);

        assert_eq!(src_nodes, dst_nodes);

        Ok(())
    }

    #[tokio::test]
    async fn test_sync_with_delete() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        ctx.setup_backup_data()?;
        let backup_data_tmp_path = ctx.backup_data_path.as_ref().unwrap();

        // Init repo
        ctx.init_repo().await?;

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
            exclude_file: None,
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
        commands::cmd_snapshot::run(&ctx.global, &snapshot_args)
            .await
            .context("Failed to run cmd_snapshot")?;

        let dst_repo_path = ctx._tmp_dir.path().join("sync_dst");

        let src_backend = Arc::new(LocalFS::new(ctx.repo_path));
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
            dst_ssh_privatekey: None,
            dry_run: false,
        };
        cmd_sync::run(&ctx.global, &sync_args)
            .await
            .context("Failed to run cmd_sync")?;

        let forward_cmp = |n0: &BackendNode, n1: &BackendNode| n0.path().cmp(n1.path());
        let mut src_nodes: Vec<BackendNode> =
            backend::read_backend_dir(src_backend.as_ref(), &PathBuf::new())
                .await?
                .into_iter()
                .filter(|n| !n.path().starts_with(LOCKS_DIR))
                .collect();
        let mut dst_nodes: Vec<BackendNode> =
            backend::read_backend_dir(dst_backend.as_ref(), &PathBuf::new())
                .await?
                .into_iter()
                .filter(|n| !n.path().starts_with(LOCKS_DIR))
                .collect();
        src_nodes.sort_unstable_by(forward_cmp);
        dst_nodes.sort_unstable_by(forward_cmp);

        assert_eq!(src_nodes, dst_nodes);

        Ok(())
    }

    #[tokio::test]
    async fn test_sync_by_size() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        ctx.setup_backup_data()?;
        let backup_data_tmp_path = ctx.backup_data_path.as_ref().unwrap();

        // Init repo
        ctx.init_repo().await?;

        // Run snapshot to generate some files
        let snapshot_args = cmd_snapshot::CmdArgs {
            paths: vec![backup_data_tmp_path.join("file.txt")],
            as_root: false,
            exclude: None,
            exclude_file: None,
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
        commands::cmd_snapshot::run(&ctx.global, &snapshot_args)
            .await
            .context("Failed to run cmd_snapshot")?;

        let dst_repo_path = ctx._tmp_dir.path().join("sync_dst_size");
        let sync_args = cmd_sync::CmdArgs {
            target: dst_repo_path.to_string_lossy().to_string(),
            delete: false,
            dst_ssh_privatekey: None,
            dry_run: false,
        };

        // First sync
        cmd_sync::run(&ctx.global, &sync_args)
            .await
            .context("Failed to run first cmd_sync")?;

        // Find a pack file in the destination and corrupt its size
        let objects_dir = dst_repo_path.join("objects");
        let mut corrupted = false;
        for entry in std::fs::read_dir(objects_dir)? {
            let entry = entry?;
            #[allow(clippy::collapsible_if)]
            if entry.file_type()?.is_dir() {
                if let Some(sub_entry) = std::fs::read_dir(entry.path())?.next() {
                    let sub_entry = sub_entry?;
                    let path = sub_entry.path();
                    // Make writable first (mapache sets them readonly)
                    let mut perms = std::fs::metadata(&path)?.permissions();
                    #[allow(clippy::permissions_set_readonly_false)]
                    perms.set_readonly(false);
                    std::fs::set_permissions(&path, perms)?;

                    // Overwrite with different size
                    std::fs::write(&path, b"too short")?;
                    corrupted = true;
                }
            }
            if corrupted {
                break;
            }
        }
        assert!(corrupted, "Could not find any pack file to corrupt");

        // Second sync: should detect size difference and fix it
        cmd_sync::run(&ctx.global, &sync_args)
            .await
            .context("Failed to run second cmd_sync")?;

        let src_backend = Arc::new(LocalFS::new(ctx.repo_path));
        let dst_backend = Arc::new(LocalFS::new(dst_repo_path));

        let forward_cmp = |n0: &BackendNode, n1: &BackendNode| n0.path().cmp(n1.path());
        let mut src_nodes: Vec<BackendNode> =
            backend::read_backend_dir(src_backend.as_ref(), &PathBuf::new())
                .await?
                .into_iter()
                .filter(|n| !n.path().starts_with(LOCKS_DIR))
                .collect();
        let mut dst_nodes: Vec<BackendNode> =
            backend::read_backend_dir(dst_backend.as_ref(), &PathBuf::new())
                .await?
                .into_iter()
                .filter(|n| !n.path().starts_with(LOCKS_DIR))
                .collect();
        src_nodes.sort_unstable_by(forward_cmp);
        dst_nodes.sort_unstable_by(forward_cmp);

        // All nodes (including sizes) must be identical now
        assert_eq!(src_nodes, dst_nodes);

        Ok(())
    }
}
