#![cfg(test)]

mod tests {
    use crate::integration_tests::TestContext;
    use anyhow::Result;

    #[tokio::test]
    async fn test_run_diff() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        ctx.setup_backup_data()?;
        let backup_data_tmp_path = ctx.backup_data_path.clone().unwrap();

        // Init repo
        ctx.init_repo().await?;

        // Run snapshot 1
        ctx.snapshot(vec![backup_data_tmp_path.join("file.txt")])
            .await?;

        // Run snapshot 2 (modified)
        let file_path = backup_data_tmp_path.join("file.txt");
        std::fs::write(&file_path, "modified content")?;

        ctx.snapshot(vec![file_path]).await?;

        // Get IDs
        let ids = ctx.get_snapshot_ids()?;
        assert_eq!(ids.len(), 2);

        // Test cmd_diff via binary
        let stdout = ctx.run_mapache_ok(&["diff", &ids[0], &ids[1]])?;

        assert!(stdout.contains("M  file.txt") || stdout.contains("m  file.txt"));
        assert!(stdout.contains("Files"));
        assert!(stdout.contains("Size"));

        Ok(())
    }
}
