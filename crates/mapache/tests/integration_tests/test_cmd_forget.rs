#![cfg(test)]

mod tests {
    use anyhow::Result;

    use mapache::{repository::repo::SNAPSHOTS_DIR, utils};

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

        let msg = format!("{err:#}");
        assert!(
            msg.contains("excluded by the given --host/--tags filters"),
            "unexpected error: {msg}"
        );

        // No snapshot was actually forgotten.
        let snapshots_dir = ctx.repo_path.join(SNAPSHOTS_DIR);
        assert_eq!(utils::count_files(&snapshots_dir)?, 1);

        Ok(())
    }
}
