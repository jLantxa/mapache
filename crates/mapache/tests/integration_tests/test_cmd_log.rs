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

        // Test cmd_log via binary, checking the structured JSON output
        let stdout = ctx.run_mapache_ok(&["log", "--json"])?;
        let json: serde_json::Value = serde_json::from_str(&stdout)?;
        assert_eq!(json["msg_type"], "log");
        let snapshots = json["snapshots"]
            .as_array()
            .expect("log output must have a snapshots array");
        assert_eq!(snapshots.len(), 1);
        let snapshot = &snapshots[0]["snapshot"];
        assert!(
            snapshot["tags"]
                .as_array()
                .expect("snapshot must have a tags array")
                .contains(&serde_json::json!("tag1"))
        );
        assert_eq!(snapshot["description"], "test description");

        // Test cmd_log --compact (human output)
        let stdout = ctx.run_mapache_ok(&["log", "--compact"])?;
        assert!(stdout.contains("tag1"));

        Ok(())
    }
}
