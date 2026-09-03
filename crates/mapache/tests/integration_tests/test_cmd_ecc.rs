#![cfg(test)]

mod tests {
    use std::{path::PathBuf, sync::Arc};

    use anyhow::{Context, Result};

    use mapache::{
        backend::{BackendNode, StorageBackend, localfs::LocalFS, read_backend_dir},
        commands::cmd_ecc::SubCmd,
        repository::{
            manifest::EccConfig,
            repo::{INDEX_DIR, OBJECTS_DIR, Repository, SNAPSHOTS_DIR},
        },
    };

    use crate::{
        integration_tests::{INTEGRATION_TEST_DATA, TestContext},
        synthetic::{Dataset, SyntheticData},
    };

    /// Count `.ecc` sidecar files under the given directory (recursively, iterative).
    async fn count_ecc_files(backend: &dyn StorageBackend, start_dir: &str) -> Result<usize> {
        let mut count = 0usize;
        let mut stack = vec![PathBuf::from(start_dir)];

        while let Some(dir) = stack.pop() {
            let entries = read_backend_dir(backend, &dir).await?;
            for entry in entries {
                match entry {
                    BackendNode::File(path, _) => {
                        if path.extension().is_some_and(|ext| ext == "ecc") {
                            count += 1;
                        }
                    }
                    BackendNode::Dir(subdir) => {
                        stack.push(subdir);
                    }
                }
            }
        }
        Ok(count)
    }

    /// Total `.ecc` files across packs, indices, and snapshots.
    async fn total_ecc_files(backend: &dyn StorageBackend) -> Result<usize> {
        let packs = count_ecc_files(backend, OBJECTS_DIR).await?;
        let indices = count_ecc_files(backend, INDEX_DIR).await?;
        let snapshots = count_ecc_files(backend, SNAPSHOTS_DIR).await?;
        Ok(packs + indices + snapshots)
    }

    #[tokio::test]
    async fn test_ecc_enable() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        // Init repo WITHOUT ECC.
        ctx.init_repo().await.context("init repo failed")?;

        // Create a snapshot.
        ctx.snapshot(vec![
            backup_data_tmp_path.join("file.txt"),
            backup_data_tmp_path.join("0"),
        ])
        .await
        .context("snapshot failed")?;

        // No sidecars yet.
        let backend = LocalFS::new(ctx.repo_path.clone());
        let ecc_count = total_ecc_files(&backend).await?;
        assert_eq!(ecc_count, 0, "no sidecars before enabling ECC");

        // Enable ECC with 10% overhead.
        ctx.ecc_builder(SubCmd::Enable { percent: 10 })
            .run(&ctx.global)
            .await
            .context("ecc enable failed")?;

        // Sidecars should now exist.
        let ecc_count = total_ecc_files(&backend).await?;
        assert!(ecc_count > 0, "sidecars should exist after enabling ECC");

        // Verify the repo still works.
        ctx.verify_builder()
            .read_packs(true)
            .run(&ctx.global)
            .await
            .context("verify after enable should pass")?;

        Ok(())
    }

    #[tokio::test]
    async fn test_ecc_enable_already_enabled() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        // Init repo WITH ECC.
        let ecc_config = EccConfig::from_overhead(10);
        let backend = Arc::new(LocalFS::new(ctx.repo_path.clone()));
        let _ = Repository::init(
            mapache::repository::repo::THIS_REPOSITORY_VERSION,
            &ctx.auth,
            None,
            backend.clone(),
            ecc_config,
            false,
        )
        .await
        .context("init with ECC failed")?;

        ctx.snapshot(vec![backup_data_tmp_path.join("file.txt")])
            .await
            .context("snapshot failed")?;

        // Enabling ECC again should fail.
        let result = ctx
            .ecc_builder(SubCmd::Enable { percent: 10 })
            .run(&ctx.global)
            .await;
        assert!(
            result.is_err(),
            "enable should fail when ECC is already enabled"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_ecc_disable() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        // Init repo WITH ECC.
        let ecc_config = EccConfig::from_overhead(10);
        let backend = Arc::new(LocalFS::new(ctx.repo_path.clone()));
        let _ = Repository::init(
            mapache::repository::repo::THIS_REPOSITORY_VERSION,
            &ctx.auth,
            None,
            backend.clone(),
            ecc_config,
            false,
        )
        .await
        .context("init with ECC failed")?;

        ctx.snapshot(vec![
            backup_data_tmp_path.join("file.txt"),
            backup_data_tmp_path.join("0"),
        ])
        .await
        .context("snapshot failed")?;

        // Sidecars should exist.
        let ecc_count = total_ecc_files(backend.as_ref()).await?;
        assert!(ecc_count > 0, "sidecars should exist before disabling ECC");

        // Disable ECC.
        ctx.ecc_builder(SubCmd::Disable)
            .run(&ctx.global)
            .await
            .context("ecc disable failed")?;

        // Sidecars should be gone.
        let ecc_count = total_ecc_files(backend.as_ref()).await?;
        assert_eq!(ecc_count, 0, "no sidecars after disabling ECC");

        Ok(())
    }

    #[tokio::test]
    async fn test_ecc_disable_not_enabled() -> Result<()> {
        let ctx = TestContext::new().await?;
        ctx.init_repo().await.context("init repo failed")?;

        // Disabling ECC when it's not enabled should fail.
        let result = ctx.ecc_builder(SubCmd::Disable).run(&ctx.global).await;
        assert!(
            result.is_err(),
            "disable should fail when ECC is not enabled"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_ecc_set_percent() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        // Init repo WITH ECC at 10%.
        let ecc_config = EccConfig::from_overhead(10);
        let backend = Arc::new(LocalFS::new(ctx.repo_path.clone()));
        let _ = Repository::init(
            mapache::repository::repo::THIS_REPOSITORY_VERSION,
            &ctx.auth,
            None,
            backend.clone(),
            ecc_config,
            false,
        )
        .await
        .context("init with ECC failed")?;

        ctx.snapshot(vec![
            backup_data_tmp_path.join("file.txt"),
            backup_data_tmp_path.join("0"),
        ])
        .await
        .context("snapshot failed")?;

        let ecc_before = total_ecc_files(backend.as_ref()).await?;
        assert!(ecc_before > 0, "sidecars should exist before set-percent");

        // Change ECC to 50%.
        ctx.ecc_builder(SubCmd::SetPercent { percent: 50 })
            .run(&ctx.global)
            .await
            .context("ecc set-percent failed")?;

        // Sidecars should still exist (regenerated with new K/P).
        let ecc_after = total_ecc_files(backend.as_ref()).await?;
        assert!(ecc_after > 0, "sidecars should exist after set-percent");
        assert_eq!(
            ecc_before, ecc_after,
            "same number of sidecars after set-percent"
        );

        // Verify the repo still works.
        ctx.verify_builder()
            .read_packs(true)
            .run(&ctx.global)
            .await
            .context("verify after set-percent should pass")?;

        Ok(())
    }

    #[tokio::test]
    async fn test_ecc_regenerate() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        // Init repo WITH ECC.
        let ecc_config = EccConfig::from_overhead(10);
        let backend = Arc::new(LocalFS::new(ctx.repo_path.clone()));
        let _ = Repository::init(
            mapache::repository::repo::THIS_REPOSITORY_VERSION,
            &ctx.auth,
            None,
            backend.clone(),
            ecc_config,
            false,
        )
        .await
        .context("init with ECC failed")?;

        ctx.snapshot(vec![
            backup_data_tmp_path.join("file.txt"),
            backup_data_tmp_path.join("0"),
        ])
        .await
        .context("snapshot failed")?;

        let ecc_before = total_ecc_files(backend.as_ref()).await?;
        assert!(ecc_before > 0, "sidecars should exist before regenerate");

        // Regenerate all sidecars.
        ctx.ecc_builder(SubCmd::Regenerate)
            .run(&ctx.global)
            .await
            .context("ecc regenerate failed")?;

        // Sidecars should exist with the same count.
        let ecc_after = total_ecc_files(backend.as_ref()).await?;
        assert!(ecc_after > 0, "sidecars should exist after regenerate");
        assert_eq!(
            ecc_before, ecc_after,
            "same number of sidecars after regenerate"
        );

        // Verify the repo still works.
        ctx.verify_builder()
            .read_packs(true)
            .run(&ctx.global)
            .await
            .context("verify after regenerate should pass")?;

        Ok(())
    }

    #[tokio::test]
    async fn test_ecc_regenerate_not_enabled() -> Result<()> {
        let ctx = TestContext::new().await?;
        ctx.init_repo().await.context("init repo failed")?;

        // Regenerating when ECC is not enabled should fail.
        let result = ctx.ecc_builder(SubCmd::Regenerate).run(&ctx.global).await;
        assert!(
            result.is_err(),
            "regenerate should fail when ECC is not enabled"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_ecc_enable_then_disable_roundtrip() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        ctx.init_repo().await.context("init repo failed")?;

        ctx.snapshot(vec![backup_data_tmp_path.join("file.txt")])
            .await
            .context("snapshot failed")?;

        let backend = LocalFS::new(ctx.repo_path.clone());

        // Enable ECC.
        ctx.ecc_builder(SubCmd::Enable { percent: 20 })
            .run(&ctx.global)
            .await?;
        assert!(total_ecc_files(&backend).await? > 0);

        // Disable ECC.
        ctx.ecc_builder(SubCmd::Disable).run(&ctx.global).await?;
        assert_eq!(total_ecc_files(&backend).await?, 0);

        // Enable again.
        ctx.ecc_builder(SubCmd::Enable { percent: 30 })
            .run(&ctx.global)
            .await?;
        assert!(total_ecc_files(&backend).await? > 0);

        // Verify still works.
        ctx.verify_builder()
            .read_packs(true)
            .run(&ctx.global)
            .await
            .context("verify after roundtrip should pass")?;

        Ok(())
    }

    #[tokio::test]
    async fn test_ecc_rejects_v1_repo() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        // Init repo as v1.
        ctx.init_builder().format(1).run(&ctx.global).await?;

        ctx.snapshot(vec![backup_data_tmp_path.join("file.txt")])
            .await
            .context("snapshot failed")?;

        // ECC should be rejected on v1 repos.
        let result = ctx
            .ecc_builder(SubCmd::Enable { percent: 10 })
            .run(&ctx.global)
            .await;
        assert!(result.is_err(), "ecc should be rejected on v1 repos");
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("v1"), "error should mention v1: {err_msg}");

        Ok(())
    }
}
