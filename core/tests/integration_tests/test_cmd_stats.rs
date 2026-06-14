#![cfg(test)]

mod tests {
    use anyhow::Result;

    use crate::{
        integration_tests::{INTEGRATION_TEST_DATA, TestContext},
        synthetic::{Dataset, SyntheticData},
    };

    #[tokio::test]
    async fn test_run_stats() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        // Init repo
        ctx.init_repo().await?;

        // Run snapshot
        ctx.snapshot_builder(vec![backup_data_tmp_path.join("file.txt")])
            .no_parent(true)
            .num_readers(1)
            .num_packers(1)
            .run(&ctx.global)
            .await?;

        // Test cmd_stats via binary
        let stdout = ctx.run_mapache_ok(&["stats"])?;

        assert!(stdout.contains("Packs:"));
        assert!(stdout.contains("Snapshots:"));
        assert!(stdout.contains("1 snapshot"));

        // Test cmd_stats --json
        let stdout = ctx.run_mapache_ok(&["stats", "--json"])?;

        let json: serde_json::Value = serde_json::from_str(&stdout)?;
        assert_eq!(json["msg_type"], "stats");
        assert_eq!(json["snapshots"]["count"], 1);

        Ok(())
    }
}
