#![cfg(test)]

mod tests {
    use std::sync::Arc;

    use anyhow::Result;

    use mapache::backend::{StorageBackend, localfs::LocalFS};

    use crate::{
        integration_tests::{INTEGRATION_TEST_DATA, TestContext},
        synthetic::{Dataset, SyntheticData},
    };

    fn get_snapshot_ids(repo_path: &std::path::Path) -> Result<Vec<String>> {
        let snapshots_dir = repo_path.join("snapshots");
        let mut ids: Vec<String> = std::fs::read_dir(&snapshots_dir)?
            .map(|res| res.map(|e| e.file_name().to_string_lossy().to_string()))
            .collect::<std::io::Result<Vec<_>>>()?;
        ids.sort();
        Ok(ids)
    }

    /// Initialize a repo at the given path with the test auth.
    async fn init_repo_at(
        path: &std::path::Path,
        auth: &mapache::repository::repo::Auth,
    ) -> Result<()> {
        let backend = Arc::new(LocalFS::new(path.to_path_buf()));
        let _ = mapache::repository::repo::Repository::init(auth, None, backend).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_copy_basic() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        // Init destination repo (will be accessed via -r)
        ctx.init_repo().await?;

        // Init source repo separately
        let src_repo_path = ctx._tmp_dir.path().join("copy_src");
        init_repo_at(&src_repo_path, &ctx.auth).await?;

        // Create a snapshot in source repo
        let mut src_global = ctx.global.clone();
        src_global.repo = src_repo_path.to_string_lossy().to_string();
        ctx.snapshot_builder(vec![
            backup_data_tmp_path.join("0"),
            backup_data_tmp_path.join("file.txt"),
        ])
        .no_scan(true)
        .run(&src_global)
        .await?;

        let src_ids = get_snapshot_ids(&src_repo_path)?;
        assert!(!src_ids.is_empty(), "Source should have snapshots");

        // Copy: -r (auto) = destination, --from = source
        let src_path_str = src_repo_path.to_string_lossy().to_string();
        ctx.run_mapache_ok(&["copy", "--from", &src_path_str])?;

        let dst_ids = get_snapshot_ids(&ctx.repo_path)?;
        assert_eq!(
            src_ids, dst_ids,
            "Destination should have same snapshots as source"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_copy_idempotent() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        ctx.init_repo().await?;

        let src_repo_path = ctx._tmp_dir.path().join("copy_src");
        init_repo_at(&src_repo_path, &ctx.auth).await?;

        let mut src_global = ctx.global.clone();
        src_global.repo = src_repo_path.to_string_lossy().to_string();
        ctx.snapshot_builder(vec![
            backup_data_tmp_path.join("0"),
            backup_data_tmp_path.join("file.txt"),
        ])
        .no_scan(true)
        .run(&src_global)
        .await?;

        let src_path_str = src_repo_path.to_string_lossy().to_string();

        // First copy
        ctx.run_mapache_ok(&["copy", "--from", &src_path_str])?;

        // Second copy should be a no-op (idempotent)
        ctx.run_mapache_ok(&["copy", "--from", &src_path_str])?;

        // Verify destination is still consistent
        let dst_backend = Arc::new(LocalFS::new(ctx.repo_path.clone()));
        let manifest_exists = dst_backend
            .path_exists(std::path::Path::new("manifest"))
            .await;
        assert!(manifest_exists, "Manifest should exist after copy");

        Ok(())
    }

    #[tokio::test]
    async fn test_copy_dry_run() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        ctx.init_repo().await?;

        let src_repo_path = ctx._tmp_dir.path().join("copy_src");
        init_repo_at(&src_repo_path, &ctx.auth).await?;

        let mut src_global = ctx.global.clone();
        src_global.repo = src_repo_path.to_string_lossy().to_string();
        ctx.snapshot_builder(vec![backup_data_tmp_path.join("file.txt")])
            .no_scan(true)
            .run(&src_global)
            .await?;

        let src_path_str = src_repo_path.to_string_lossy().to_string();
        let output = ctx.run_mapache(&["copy", "--from", &src_path_str, "--dry-run"])?;
        assert!(output.status.success(), "Dry run should succeed");

        // Destination should have no snapshots after dry run
        let dst_ids = get_snapshot_ids(&ctx.repo_path)?;
        assert!(
            dst_ids.is_empty(),
            "Destination should be empty after dry run"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_copy_with_snapshot_filter() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        ctx.init_repo().await?;

        let src_repo_path = ctx._tmp_dir.path().join("copy_src");
        init_repo_at(&src_repo_path, &ctx.auth).await?;

        let mut src_global = ctx.global.clone();
        src_global.repo = src_repo_path.to_string_lossy().to_string();

        // Create two snapshots
        ctx.snapshot_builder(vec![backup_data_tmp_path.join("file.txt")])
            .no_scan(true)
            .run(&src_global)
            .await?;

        ctx.snapshot_builder(vec![backup_data_tmp_path.join("0")])
            .no_scan(true)
            .run(&src_global)
            .await?;

        let src_ids = get_snapshot_ids(&src_repo_path)?;
        assert_eq!(src_ids.len(), 2, "Should have two snapshots");

        let src_path_str = src_repo_path.to_string_lossy().to_string();

        // Copy only the first snapshot by prefix
        let first_prefix = &src_ids[0][..8];
        ctx.run_mapache_ok(&["copy", "--from", &src_path_str, "--snapshot", first_prefix])?;

        let dst_ids = get_snapshot_ids(&ctx.repo_path)?;
        assert_eq!(dst_ids.len(), 1, "Should copy exactly one snapshot");
        assert!(
            dst_ids[0].starts_with(first_prefix),
            "Copied snapshot should match the requested prefix"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_copy_with_tag_filter() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        ctx.init_repo().await?;

        let src_repo_path = ctx._tmp_dir.path().join("copy_src");
        init_repo_at(&src_repo_path, &ctx.auth).await?;

        let mut src_global = ctx.global.clone();
        src_global.repo = src_repo_path.to_string_lossy().to_string();

        // Create a snapshot with a specific tag
        ctx.snapshot_builder(vec![backup_data_tmp_path.join("file.txt")])
            .no_scan(true)
            .tags("important".to_string())
            .run(&src_global)
            .await?;

        let src_path_str = src_repo_path.to_string_lossy().to_string();

        // Copy with matching tag
        ctx.run_mapache_ok(&["copy", "--from", &src_path_str, "--tags", "important"])?;

        let dst_ids = get_snapshot_ids(&ctx.repo_path)?;
        assert_eq!(dst_ids.len(), 1, "Should copy snapshot with matching tag");

        Ok(())
    }

    #[tokio::test]
    async fn test_copy_with_tag_filter_no_match() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        ctx.init_repo().await?;

        let src_repo_path = ctx._tmp_dir.path().join("copy_src");
        init_repo_at(&src_repo_path, &ctx.auth).await?;

        let mut src_global = ctx.global.clone();
        src_global.repo = src_repo_path.to_string_lossy().to_string();

        ctx.snapshot_builder(vec![backup_data_tmp_path.join("file.txt")])
            .no_scan(true)
            .tags("work".to_string())
            .run(&src_global)
            .await?;

        let src_path_str = src_repo_path.to_string_lossy().to_string();

        // Copy with non-matching tag
        let output =
            ctx.run_mapache(&["copy", "--from", &src_path_str, "--tags", "nonexistent"])?;
        assert!(
            output.status.success(),
            "Should succeed with no snapshots to copy"
        );

        let dst_ids = get_snapshot_ids(&ctx.repo_path)?;
        assert!(dst_ids.is_empty(), "Destination should have no snapshots");

        Ok(())
    }

    #[tokio::test]
    async fn test_copy_with_host_filter_no_match() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        ctx.init_repo().await?;

        let src_repo_path = ctx._tmp_dir.path().join("copy_src");
        init_repo_at(&src_repo_path, &ctx.auth).await?;

        let mut src_global = ctx.global.clone();
        src_global.repo = src_repo_path.to_string_lossy().to_string();

        ctx.snapshot_builder(vec![backup_data_tmp_path.join("file.txt")])
            .no_scan(true)
            .run(&src_global)
            .await?;

        let src_path_str = src_repo_path.to_string_lossy().to_string();

        // Copy with non-matching host
        let output = ctx.run_mapache(&[
            "copy",
            "--from",
            &src_path_str,
            "--host",
            "nonexistent-host",
        ])?;
        assert!(
            output.status.success(),
            "Should succeed with no snapshots to copy"
        );

        let dst_ids = get_snapshot_ids(&ctx.repo_path)?;
        assert!(dst_ids.is_empty(), "Destination should have no snapshots");

        Ok(())
    }

    #[tokio::test]
    async fn test_copy_multiple_snapshots() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        ctx.init_repo().await?;

        let src_repo_path = ctx._tmp_dir.path().join("copy_src");
        init_repo_at(&src_repo_path, &ctx.auth).await?;

        let mut src_global = ctx.global.clone();
        src_global.repo = src_repo_path.to_string_lossy().to_string();

        // Create three snapshots
        for path in [
            backup_data_tmp_path.join("file.txt"),
            backup_data_tmp_path.join("0"),
            backup_data_tmp_path.join("1"),
        ] {
            ctx.snapshot_builder(vec![path])
                .no_scan(true)
                .run(&src_global)
                .await?;
        }

        let src_ids = get_snapshot_ids(&src_repo_path)?;
        assert_eq!(src_ids.len(), 3, "Should have three snapshots");

        let src_path_str = src_repo_path.to_string_lossy().to_string();

        // Copy the first and third snapshot by prefix
        let prefix1 = &src_ids[0][..8];
        let prefix3 = &src_ids[2][..8];
        ctx.run_mapache_ok(&[
            "copy",
            "--from",
            &src_path_str,
            "--snapshot",
            prefix1,
            "--snapshot",
            prefix3,
        ])?;

        let dst_ids = get_snapshot_ids(&ctx.repo_path)?;
        assert_eq!(dst_ids.len(), 2, "Should copy exactly two snapshots");

        Ok(())
    }
}
