#![cfg(test)]

mod tests {
    use anyhow::Result;

    use crate::integration_tests::TestContext;

    #[tokio::test]
    async fn test_run_ls() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        ctx.setup_backup_data()?;
        let backup_data_tmp_path = ctx.backup_data_path.clone().unwrap();

        // Init and snapshot
        ctx.init_repo().await?;
        ctx.snapshot(vec![backup_data_tmp_path.join("file.txt")])
            .await?;

        // Test cmd_ls
        let stdout = ctx.run_mapache_ok(&["ls", "latest"])?;
        assert!(stdout.contains("file.txt"));

        // Test cmd_ls -l
        let stdout = ctx.run_mapache_ok(&["ls", "-l", "latest"])?;
        assert!(stdout.contains("file.txt"));

        #[cfg(not(windows))]
        assert!(stdout.contains("-rw"));

        Ok(())
    }
}
