#![cfg(test)]

mod tests {
    use anyhow::Result;

    use crate::integration_tests::TestContext;

    #[tokio::test]
    async fn test_run_cat() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        ctx.setup_backup_data()?;
        let backup_data_tmp_path = ctx.backup_data_path.clone().unwrap();

        // Init and snapshot
        ctx.init_repo().await?;
        ctx.snapshot(vec![backup_data_tmp_path.join("file.txt")])
            .await?;

        // Test cmd_cat manifest via binary
        let stdout = ctx.run_mapache_ok(&["cat", "manifest"])?;

        assert!(stdout.contains("\"version\""));
        assert!(stdout.contains("\"id\""));

        Ok(())
    }
}
