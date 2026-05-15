#![cfg(test)]

mod tests {
    use crate::integration_tests::TestContext;
    use anyhow::Result;

    #[tokio::test]
    async fn test_cmd_rechunk() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        ctx.setup_backup_data()?;
        let backup_data_tmp_path = ctx.backup_data_path.clone().unwrap();

        // Init repo
        ctx.init_repo().await?;

        // Run snapshot
        ctx.snapshot(vec![backup_data_tmp_path.join("file.txt")])
            .await?;

        // Run rechunk
        ctx.rechunk_builder().run(&ctx.global).await?;

        // Verify repo
        ctx.verify_builder()
            .read_packs(true)
            .fail_early(true)
            .run(&ctx.global)
            .await?;

        Ok(())
    }
}
