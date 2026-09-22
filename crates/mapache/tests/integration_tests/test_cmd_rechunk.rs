#![cfg(test)]

mod tests {
    use anyhow::Result;
    use mapache::{repository::repo::SNAPSHOTS_DIR, utils};

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

        ctx.init_repo().await?;
        ctx.snapshot(vec![backup_data_tmp_path.join("file.txt")])
            .await?;

        ctx.rechunk_builder().run(&ctx.global).await?;

        ctx.verify_builder()
            .read_packs(true)
            .fail_early(true)
            .run(&ctx.global)
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_cmd_rechunk_noop_keeps_snapshot() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        ctx.init_repo().await?;
        ctx.snapshot(vec![backup_data_tmp_path.join("file.txt")])
            .await?;

        ctx.rechunk_builder().run(&ctx.global).await?;

        for _ in 0..2 {
            ctx.rechunk_builder().run(&ctx.global).await?;
            let snapshots_dir = ctx.repo_path.join(SNAPSHOTS_DIR);
            assert_eq!(
                utils::count_files(&snapshots_dir)?,
                1,
                "rechunk must not delete the snapshot when nothing changes"
            );
        }

        ctx.verify_builder()
            .read_packs(true)
            .fail_early(true)
            .run(&ctx.global)
            .await?;

        Ok(())
    }
}
