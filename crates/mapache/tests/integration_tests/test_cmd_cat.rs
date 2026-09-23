#![cfg(test)]

mod tests {
    use anyhow::Result;

    use crate::{
        integration_tests::{INTEGRATION_TEST_DATA, TestContext},
        synthetic::{Dataset, SyntheticData},
    };

    #[tokio::test]
    async fn test_run_cat() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        // Init and snapshot
        ctx.init_repo().await?;
        ctx.snapshot(vec![backup_data_tmp_path.join("file.txt")])
            .await?;

        // Test cmd_cat manifest via binary
        let stdout = ctx.run_mapache_ok(&["cat", "manifest"])?;
        let manifest: serde_json::Value = serde_json::from_str(&stdout)?;

        assert_eq!(
            manifest["version"],
            mapache::repository::repo::THIS_REPOSITORY_VERSION
        );
        assert!(manifest["id"].as_str().is_some_and(|id| !id.is_empty()));
        assert_eq!(manifest["hash_algorithm"], "blake3-256");

        Ok(())
    }
}
