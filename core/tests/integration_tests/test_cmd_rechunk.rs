#![cfg(test)]

mod tests {
    use anyhow::Result;

    use crate::{
        integration_tests::{INTEGRATION_TEST_DATA, TestContext},
        synthetic::{Dataset, SyntheticData},
    };

    #[tokio::test]
    async fn test_cmd_rechunk() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

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
