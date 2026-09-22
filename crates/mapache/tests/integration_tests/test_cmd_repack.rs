#![cfg(test)]

mod tests {
    use std::sync::Arc;

    use anyhow::Result;

    use mapache::{backend::localfs::LocalFS, commands::Compression, repository::repo::Repository};

    use crate::{
        integration_tests::{INTEGRATION_TEST_DATA, TestContext},
        synthetic::{Dataset, SyntheticData},
    };

    #[tokio::test]
    async fn test_cmd_repack() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        ctx.init_repo().await?;
        ctx.global.compression_level = Compression::Fast;
        ctx.snapshot(vec![backup_data_tmp_path.join("file.txt")])
            .await?;

        let (repo, _) = Repository::try_open_unlocked(
            &ctx.auth,
            None,
            Arc::new(LocalFS::new(ctx.repo_path.clone())),
            ctx.global.to_repo_config(),
        )
        .await?;
        repo.reload_master_index().await?;

        // Verify that the snapshot contains compressed blobs
        let mut has_compressed_blob = false;
        repo.index()
            .for_each_id(|_, locator| has_compressed_blob |= locator.compressed)
            .await;
        assert!(has_compressed_blob, "repack must honor --compression fast");

        ctx.global.compression_level = Compression::None;
        ctx.repack_builder().run(&ctx.global).await?;

        ctx.verify_builder()
            .read_packs(true)
            .fail_early(true)
            .run(&ctx.global)
            .await?;

        // Reload the index: the in-memory copy is stale after repack.
        repo.reload_master_index().await?;

        // Now verify that the snapshot contains uncompressed blobs
        let mut has_compressed_blob = false;
        repo.index()
            .for_each_id(|_, locator| has_compressed_blob |= locator.compressed)
            .await;
        assert!(!has_compressed_blob, "repack must honor --compression none");

        Ok(())
    }
}
