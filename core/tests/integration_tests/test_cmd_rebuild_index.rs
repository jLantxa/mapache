#![cfg(test)]

mod tests {
    use std::{path::PathBuf, sync::Arc};

    use anyhow::{Context, Result};
    use mapache::{backend::localfs::LocalFS, repository::repo::INDEX_DIR};

    use crate::integration_tests::{TestContext, delete_all_files_from};

    #[tokio::test]
    async fn test_rebuild_index() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        ctx.setup_backup_data()?;
        let backup_data_tmp_path = ctx.backup_data_path.clone().unwrap();

        // Init repo
        ctx.init_repo().await?;

        // Run snapshot
        ctx.snapshot_builder(vec![
            backup_data_tmp_path.join("0"),
            backup_data_tmp_path.join("1"),
            backup_data_tmp_path.join("2"),
            backup_data_tmp_path.join("file.txt"),
        ])
        .no_scan(true)
        .run(&ctx.global)
        .await?;

        let verify_builder = ctx
            .verify_builder()
            .read_packs(true)
            .parallel(8)
            .fail_early(true);

        assert!(
            verify_builder.clone().run(&ctx.global).await.is_ok(),
            "First verify should pass"
        );

        let backend = Arc::new(LocalFS::new(ctx.repo_path.clone()));

        // Rebuild index and verify
        ctx.rebuild_index_builder().run(&ctx.global).await?;

        assert!(
            verify_builder.clone().run(&ctx.global).await.is_ok(),
            "Verify should pass after index is rebuilt"
        );

        // Delete the index to make it fail the next time.
        delete_all_files_from(backend.as_ref(), &PathBuf::from(INDEX_DIR))
            .await
            .context("Failed to remove the index")?;

        assert!(
            verify_builder.clone().run(&ctx.global).await.is_err(),
            "Verify should fail without an index"
        );

        // Rebuild index and verify
        ctx.rebuild_index_builder().run(&ctx.global).await?;

        assert!(
            verify_builder.run(&ctx.global).await.is_ok(),
            "Verify should pass after index is rebuilt"
        );

        Ok(())
    }
}
