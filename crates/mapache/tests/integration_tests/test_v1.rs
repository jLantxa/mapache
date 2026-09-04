#![cfg(test)]

mod tests {
    use std::path::PathBuf;

    use anyhow::{Context, Result};

    use mapache::{commands::cmd_migrate, repository::repo::SNAPSHOTS_DIR};

    use crate::{
        integration_tests::{INTEGRATION_TEST_DATA, TestContext},
        synthetic::{Dataset, SyntheticData},
    };

    #[tokio::test]
    async fn test_v1_init_and_open() -> Result<()> {
        let ctx = TestContext::new().await?;

        ctx.init_builder()
            .format(1)
            .run(&ctx.global)
            .await
            .context("Failed to run cmd_init")?;

        assert!(ctx.repo_path.join("manifest").exists());
        assert!(ctx.repo_path.join("index").exists());
        assert!(ctx.repo_path.join("keys").exists());
        assert!(ctx.repo_path.join("snapshots").exists());
        assert!(ctx.repo_path.join("objects").exists());

        let backend = std::sync::Arc::new(mapache::backend::localfs::LocalFS::new(
            ctx.repo_path.clone(),
        ));
        mapache::repository::repo::Repository::try_open_with_lock(
            &ctx.auth,
            None,
            backend,
            mapache::common::defaults::TEST_REPO_CONFIG,
            false,
            None,
        )
        .await
        .context("Failed to open v1 repository")?;

        Ok(())
    }

    #[tokio::test]
    async fn test_v1_snapshot_and_restore() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        ctx.init_builder()
            .format(1)
            .run(&ctx.global)
            .await
            .context("Failed to init v1 repo")?;

        ctx.snapshot(vec![
            backup_data_tmp_path.join("file.txt"),
            backup_data_tmp_path.join("0"),
            backup_data_tmp_path.join("1"),
            backup_data_tmp_path.join("2"),
        ])
        .await
        .context("Failed to snapshot")?;

        let restore_path = ctx._tmp_dir.path().join("restore");
        ctx.restore_builder(restore_path.clone())
            .run(&ctx.global)
            .await
            .context("Failed to restore")?;

        let paths = vec![
            PathBuf::from("file.txt"),
            PathBuf::from("0"),
            PathBuf::from("0/file0.txt"),
            PathBuf::from("0/00"),
            PathBuf::from("0/00/file00.txt"),
            PathBuf::from("0/01"),
            PathBuf::from("0/01/file01a.txt"),
            PathBuf::from("0/01/file01b.txt"),
            PathBuf::from("1"),
            PathBuf::from("1/10"),
            PathBuf::from("1/10/file10.txt"),
            PathBuf::from("2"),
        ];

        for path in &paths {
            let backup_path = backup_data_tmp_path.join(path);
            let restored_path = restore_path.join(path);
            assert!(restored_path.exists(), "missing: {}", path.display());
            assert_eq!(
                restored_path.symlink_metadata()?.len(),
                backup_path.symlink_metadata()?.len(),
                "size mismatch: {}",
                path.display(),
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_v1_migrate_to_v2() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        // Init v1
        ctx.init_builder()
            .format(1)
            .run(&ctx.global)
            .await
            .context("Failed to init v1 repo")?;

        // Snapshot
        ctx.snapshot(vec![
            backup_data_tmp_path.join("file.txt"),
            backup_data_tmp_path.join("0"),
            backup_data_tmp_path.join("1"),
            backup_data_tmp_path.join("2"),
        ])
        .await
        .context("Failed to snapshot")?;

        // Migrate v1 -> v2
        cmd_migrate::run(&ctx.global, &cmd_migrate::CmdArgs { dry_run: false })
            .await
            .context("Migration failed")?;

        // Verify after migration
        ctx.verify_builder()
            .read_packs(true)
            .run(&ctx.global)
            .await
            .context("Verify failed after migration")?;

        // Restore after migration
        let restore_path = ctx._tmp_dir.path().join("restore_after_migrate");
        ctx.restore_builder(restore_path.clone())
            .run(&ctx.global)
            .await
            .context("Restore failed after migration")?;

        let paths = vec![
            PathBuf::from("file.txt"),
            PathBuf::from("0"),
            PathBuf::from("0/file0.txt"),
            PathBuf::from("1"),
            PathBuf::from("1/10"),
            PathBuf::from("1/10/file10.txt"),
            PathBuf::from("2"),
        ];

        for path in &paths {
            let backup_path = backup_data_tmp_path.join(path);
            let restored_path = restore_path.join(path);
            assert!(
                restored_path.exists(),
                "missing after migrate: {}",
                path.display()
            );
            assert_eq!(
                restored_path.symlink_metadata()?.len(),
                backup_path.symlink_metadata()?.len(),
                "size mismatch after migrate: {}",
                path.display(),
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_v1_migrate_preserves_dropped_snapshot() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        ctx.init_builder()
            .format(1)
            .run(&ctx.global)
            .await
            .context("Failed to init v1 repo")?;

        ctx.snapshot(vec![backup_data_tmp_path.join("file.txt")])
            .await
            .context("Failed to snapshot")?;
        let snapshot_id = ctx
            .get_snapshot_ids()?
            .into_iter()
            .next()
            .context("Snapshot was not created")?;

        ctx.forget_builder()
            .forget(vec![snapshot_id])
            .run(&ctx.global)
            .await
            .context("Failed to drop snapshot")?;

        cmd_migrate::run(&ctx.global, &cmd_migrate::CmdArgs { dry_run: false })
            .await
            .context("Migration failed")?;

        let snapshot_paths = std::fs::read_dir(ctx.repo_path.join(SNAPSHOTS_DIR))?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(snapshot_paths.len(), 1);
        assert_eq!(
            snapshot_paths[0]
                .extension()
                .and_then(|extension| extension.to_str()),
            Some("dropped")
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_v1_rejects_compression_none() -> Result<()> {
        let ctx = TestContext::new().await?;

        ctx.init_builder()
            .format(1)
            .run(&ctx.global)
            .await
            .context("Failed to init v1 repo")?;

        // `--compression none` is not representable in v1 (no per-blob marker).
        let mut global = ctx.global.clone();
        global.compression_level = mapache::commands::Compression::None;

        let res = ctx
            .snapshot_builder(vec![ctx.repo_path.join("manifest")])
            .run(&global)
            .await;

        assert!(
            res.is_err(),
            "snapshotting a v1 repo with --compression none should fail"
        );

        Ok(())
    }
}
