#![cfg(test)]

mod tests {
    use anyhow::Result;

    use crate::{
        integration_tests::{INTEGRATION_TEST_DATA, TestContext},
        synthetic::{Dataset, SyntheticData},
    };

    #[tokio::test]
    async fn test_run_log() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        // Init repo
        ctx.init_repo().await?;

        // Run snapshot
        ctx.snapshot_builder(vec![backup_data_tmp_path.join("file.txt")])
            .tags("tag1".to_string())
            .description("test description".to_string())
            .run(&ctx.global)
            .await?;

        // Test cmd_log via binary
        let stdout = ctx.run_mapache_ok(&["log"])?;
        assert!(stdout.contains("Date:"));
        assert!(stdout.contains("tag1"));
        assert!(stdout.contains("test description"));
        assert!(stdout.contains("1 snapshots"));

        // Test cmd_log --compact
        let stdout = ctx.run_mapache_ok(&["log", "--compact"])?;
        assert!(stdout.contains("tag1"));

        Ok(())
    }
}
