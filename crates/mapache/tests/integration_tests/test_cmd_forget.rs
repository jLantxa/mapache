#![cfg(test)]

mod tests {
    use anyhow::Result;

    use mapache::{commands::cmd_forget, repository::repo::SNAPSHOTS_DIR, utils};

    use crate::{
        integration_tests::{INTEGRATION_TEST_DATA, TestContext},
        synthetic::{Dataset, SyntheticData},
    };

    #[tokio::test]
    async fn test_cmd_forget_and_recall() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        // Init repo
        ctx.init_repo().await?;

        // Run snapshots
        ctx.snapshot_builder(vec![backup_data_tmp_path.join("file.txt")])
            .tags("tag1".to_string())
            .run(&ctx.global)
            .await?;

        ctx.snapshot_builder(vec![backup_data_tmp_path.join("file.txt")])
            .tags("tag2".to_string())
            .run(&ctx.global)
            .await?;

        let snapshots_dir = ctx.repo_path.join(SNAPSHOTS_DIR);
        assert_eq!(utils::count_files(&snapshots_dir)?, 2);

        // Get ID of one snapshot to forget
        let ids = ctx.get_snapshot_ids()?;
        let first_id = &ids[0];

        // Test cmd_forget
        ctx.forget_builder()
            .forget(vec![first_id.clone()])
            .run(&ctx.global)
            .await?;

        let snapshots = std::fs::read_dir(&snapshots_dir)?
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<_>, _>>()?;

        let dropped_count = snapshots
            .iter()
            .filter(|p| p.extension().is_some_and(|ext| ext == "dropped"))
            .count();
        assert_eq!(dropped_count, 1);

        // Test cmd_recall
        ctx.recall_builder(first_id.clone())
            .run(&ctx.global)
            .await?;

        let snapshots = std::fs::read_dir(&snapshots_dir)?
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<_>, _>>()?;
        let dropped_count = snapshots
            .iter()
            .filter(|p| p.extension().is_some_and(|ext| ext == "dropped"))
            .count();
        assert_eq!(dropped_count, 0);
        assert_eq!(snapshots.len(), 2);

        // Test cmd_forget with force
        ctx.forget_builder()
            .forget(vec![first_id.clone()])
            .force(true)
            .run(&ctx.global)
            .await?;

        assert_eq!(utils::count_files(&snapshots_dir)?, 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_forget_explicit_id_combines_with_retention_rules() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        ctx.init_repo().await?;

        for tag in ["tag1", "tag2"] {
            ctx.snapshot_builder(vec![backup_data_tmp_path.join("file.txt")])
                .tags(tag.to_string())
                .run(&ctx.global)
                .await?;
        }

        let snapshots_dir = ctx.repo_path.join(SNAPSHOTS_DIR);
        let ids = ctx.get_snapshot_ids()?;

        // Naming a snapshot must not discard the retention rules: `--keep-last 1`
        // still applies, and it wins over the explicit request for the snapshot
        // it covers. The other snapshot is dropped because no rule matches it.
        ctx.forget_builder()
            .forget(ids.clone())
            .keep_last(1)
            .force(true)
            .run(&ctx.global)
            .await?;

        assert_eq!(utils::count_files(&snapshots_dir)?, 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_forget_explicit_id_with_keep_tags_preserves_tagged_snapshot() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        ctx.init_repo().await?;

        ctx.snapshot_builder(vec![backup_data_tmp_path.join("file.txt")])
            .tags("important".to_string())
            .run(&ctx.global)
            .await?;

        ctx.snapshot_builder(vec![backup_data_tmp_path.join("file.txt")])
            .tags("ephemeral".to_string())
            .run(&ctx.global)
            .await?;

        let snapshots_dir = ctx.repo_path.join(SNAPSHOTS_DIR);
        let ids = ctx.get_snapshot_ids()?;

        // Name both snapshots for removal, but `--keep-tags important` must
        // protect the tagged one.  Only the untagged snapshot is removed.
        let args = cmd_forget::CmdArgs {
            forget: ids.clone(),
            keep_tags: Some("important".to_string()),
            force: true,
            ..Default::default()
        };
        cmd_forget::run(&ctx.global, &args, None).await?;

        assert_eq!(utils::count_files(&snapshots_dir)?, 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_forget_no_rules_no_ids_is_error() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        ctx.init_repo().await?;

        ctx.snapshot_builder(vec![backup_data_tmp_path.join("file.txt")])
            .run(&ctx.global)
            .await?;

        // Forgetting with neither explicit IDs nor retention rules must fail.
        let err = ctx
            .forget_builder()
            .run(&ctx.global)
            .await
            .expect_err("forget with no arguments must fail");

        assert!(
            err.downcast_ref::<cmd_forget::ForgetError>()
                .is_some_and(|e| matches!(e, cmd_forget::ForgetError::InvalidRule(_))),
            "expected InvalidRule, got: {err:#}"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_forget_explicit_target_excluded_by_host_filter() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        // Init repo
        ctx.init_repo().await?;

        // Run one snapshot
        ctx.snapshot_builder(vec![backup_data_tmp_path.join("file.txt")])
            .run(&ctx.global)
            .await?;

        let ids = ctx.get_snapshot_ids()?;
        let first_id = &ids[0];

        // Explicitly forget a snapshot that a --host filter excludes. Before the
        // fix, this silently no-op'd; it must now refuse to continue.
        let err = ctx
            .forget_builder()
            .forget(vec![first_id.clone()])
            .hosts(vec!["some-other-host".to_string()])
            .run(&ctx.global)
            .await
            .expect_err("forget must fail when the explicit target is excluded by --host");

        assert!(
            err.downcast_ref::<cmd_forget::ForgetError>()
                .is_some_and(|e| matches!(e, cmd_forget::ForgetError::ForgetFailed(_))),
            "expected ForgetFailed, got: {err:#}"
        );

        // No snapshot was actually forgotten.
        let snapshots_dir = ctx.repo_path.join(SNAPSHOTS_DIR);
        assert_eq!(utils::count_files(&snapshots_dir)?, 1);

        Ok(())
    }
}
