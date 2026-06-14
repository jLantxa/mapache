#![cfg(test)]

mod tests {
    use std::sync::Arc;

    use anyhow::Result;

    use mapache::{
        mapache::defaults::TEST_REPO_CONFIG,
        repository::repo::{LOCKS_DIR, Repository},
    };

    use crate::integration_tests::TestContext;

    #[tokio::test]
    async fn test_cmd_unlock() -> Result<()> {
        let ctx = TestContext::new().await?;

        // Init repo
        ctx.init_repo().await?;

        let backend = Arc::new(mapache::backend::localfs::LocalFS::new(
            ctx.repo_path.clone(),
        ));

        // Open with lock and KEEP the handle alive to maintain the lock
        let (_repo, _, lock_handle) =
            Repository::try_open_with_lock(&ctx.auth, None, backend, TEST_REPO_CONFIG, true, None)
                .await?;

        let locks_dir = ctx.repo_path.join(LOCKS_DIR);
        assert_eq!(mapache::utils::count_files(&locks_dir)?, 1);

        // Test cmd_unlock without force (should not delete as it's not expired)
        ctx.unlock_builder().run(&ctx.global).await?;
        assert_eq!(mapache::utils::count_files(&locks_dir)?, 1);

        // Test cmd_unlock with force
        ctx.unlock_builder().force(true).run(&ctx.global).await?;
        assert_eq!(mapache::utils::count_files(&locks_dir)?, 0);

        lock_handle.unlock().await;

        Ok(())
    }
}
