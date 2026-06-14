#![cfg(test)]

mod tests {
    use anyhow::Result;

    use crate::{
        integration_tests::{INTEGRATION_TEST_DATA, TestContext},
        synthetic::{Dataset, SyntheticData},
    };

    #[tokio::test]
    async fn test_run_find() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        // Init repo
        ctx.init_repo().await?;

        // Run snapshot
        ctx.snapshot(vec![backup_data_tmp_path.join("file.txt")])
            .await?;

        // Test cmd_find via binary
        let stdout = ctx.run_mapache_ok(&["find", "file.txt"])?;
        assert!(stdout.contains("Found in snapshot"));
        assert!(stdout.contains("file.txt"));

        // Test cmd_find non-existent
        let stdout = ctx.run_mapache_ok(&["find", "non-existent.file"])?;
        assert!(!stdout.contains("Found in snapshot"));

        Ok(())
    }
}
