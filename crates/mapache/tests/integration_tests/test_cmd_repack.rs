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

    #[tokio::test]
    async fn test_cmd_repack_preserves_zero_blobs() -> Result<()> {
        let ctx = TestContext::new().await?;
        ctx.init_repo().await?;

        let zero_data = vec![0u8; 8192];
        let zero_file = ctx._tmp_dir.path().join("zeros.bin");
        std::fs::write(&zero_file, &zero_data)?;

        ctx.snapshot_builder(vec![zero_file])
            .no_scan(true)
            .run(&ctx.global)
            .await?;

        ctx.repack_builder().run(&ctx.global).await?;
        ctx.verify_builder()
            .read_packs(true)
            .fail_early(true)
            .run(&ctx.global)
            .await?;

        let restore_path = ctx._tmp_dir.path().join("restore_zero_blob");
        ctx.restore_builder(restore_path.clone())
            .run(&ctx.global)
            .await?;

        assert_eq!(std::fs::read(restore_path.join("zeros.bin"))?, zero_data);

        Ok(())
    }
}
