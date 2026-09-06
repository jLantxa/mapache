#![cfg(test)]

mod tests {
    use std::{path::PathBuf, sync::Arc};

    use anyhow::Result;

    use mapache::{
        backend::{self, BackendNode, StorageBackend, localfs::LocalFS},
        repository::repo::LOCKS_DIR,
    };

    use crate::{
        integration_tests::{INTEGRATION_TEST_DATA, TestContext},
        synthetic::{Dataset, SyntheticData},
    };

    #[tokio::test]
    async fn test_sync_no_delete() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        // Init repo
        ctx.init_repo().await?;

        // Run snapshot
        ctx.snapshot_builder(vec![
            backup_data_tmp_path.join("0"),
            backup_data_tmp_path.join("1"),
            backup_data_tmp_path.join("2"),
            backup_data_tmp_path.join("file.txt"),
        ])
        .no_scan(true)
        .run(&ctx.global)
        .await?;

        let dst_repo_path = ctx._tmp_dir.path().join("sync_dst");
        ctx.sync_builder(dst_repo_path.to_string_lossy().to_string())
            .run(&ctx.global)
            .await?;

        let src_backend = Arc::new(LocalFS::new(ctx.repo_path.clone()));
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
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        // Init repo
        ctx.init_repo().await?;

        // Run snapshot
        ctx.snapshot_builder(vec![
            backup_data_tmp_path.join("0"),
            backup_data_tmp_path.join("1"),
            backup_data_tmp_path.join("2"),
            backup_data_tmp_path.join("file.txt"),
        ])
        .no_scan(true)
        .run(&ctx.global)
        .await?;

        let dst_repo_path = ctx._tmp_dir.path().join("sync_dst");

        let src_backend = Arc::new(LocalFS::new(ctx.repo_path.clone()));
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

        ctx.sync_builder(dst_repo_path.to_string_lossy().to_string())
            .delete(true)
            .run(&ctx.global)
            .await?;

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
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        // Init repo
        ctx.init_repo().await?;

        // Run snapshot to generate some files
        ctx.snapshot_builder(vec![backup_data_tmp_path.join("file.txt")])
            .no_scan(true)
            .run(&ctx.global)
            .await?;

        let dst_repo_path = ctx._tmp_dir.path().join("sync_dst_size");

        // First sync
        ctx.sync_builder(dst_repo_path.to_string_lossy().to_string())
            .run(&ctx.global)
            .await?;

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
        ctx.sync_builder(dst_repo_path.to_string_lossy().to_string())
            .run(&ctx.global)
            .await?;

        let src_backend = Arc::new(LocalFS::new(ctx.repo_path.clone()));
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

    /// Initialize an existing repo at the given path with the given format version.
    async fn init_repo_at_version(
        path: &std::path::Path,
        auth: &mapache::repository::repo::Auth,
        version: u32,
    ) -> Result<()> {
        let backend = Arc::new(LocalFS::new(path.to_path_buf()));
        let _ =
            mapache::repository::repo::Repository::init(version, auth, None, backend, None, false)
                .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_sync_version_mismatch_v1_to_v2() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        // Source repository is v1
        ctx.init_builder().format(1).run(&ctx.global).await?;
        ctx.snapshot_builder(vec![backup_data_tmp_path.join("file.txt")])
            .no_scan(true)
            .run(&ctx.global)
            .await?;

        // Destination is an existing v2 repository
        let dst_repo_path = ctx._tmp_dir.path().join("sync_dst_v2");
        init_repo_at_version(&dst_repo_path, &ctx.auth, 2).await?;

        let output = ctx.run_mapache(&["sync", "--target", &dst_repo_path.to_string_lossy()])?;

        assert!(
            !output.status.success(),
            "sync between different formats should fail\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let msg = String::from_utf8_lossy(&output.stderr);
        assert!(
            msg.contains("different formats") && msg.contains("v1") && msg.contains("v2"),
            "unexpected error message: {}",
            msg
        );
        Ok(())
    }
}
