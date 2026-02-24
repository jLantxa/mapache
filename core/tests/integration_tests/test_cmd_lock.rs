#![cfg(test)]

mod tests {
    use anyhow::{Context, Result};
    use mapache::{
        commands::{self, cmd_unlock},
        mapache::defaults::TEST_REPO_CONFIG,
        repository::repo::{LOCKS_DIR, Repository},
    };

    use std::sync::Arc;

    use crate::integration_tests::TestContext;

    #[tokio::test]
    async fn test_cmd_unlock() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        ctx.global.verbosity = Some(1);
        mapache::mapache::global::set_global_opts_with_args(&ctx.global);

        // Init repo
        ctx.init_repo().await?;

        let backend = Arc::new(mapache::backend::localfs::LocalFS::new(
            ctx.repo_path.clone(),
        ));

        // Open with lock and KEEP the handle alive to maintain the lock
        let (_repo, _, _lock_handle) = Repository::try_open_with_lock(
            Some(&ctx.auth),
            None,
            backend,
            TEST_REPO_CONFIG,
            true,
            None,
        )
        .await?;

        let locks_dir = ctx.repo_path.join(LOCKS_DIR);
        assert_eq!(mapache::utils::count_files(&locks_dir)?, 1);

        // Test cmd_unlock without force (should not delete as it's not expired)
        let unlock_args = cmd_unlock::CmdArgs { force: false };
        commands::cmd_unlock::run(&ctx.global, &unlock_args)
            .await
            .context("cmd_unlock failed")?;
        assert_eq!(mapache::utils::count_files(&locks_dir)?, 1);

        // Test cmd_unlock with force
        let unlock_args_force = cmd_unlock::CmdArgs { force: true };
        commands::cmd_unlock::run(&ctx.global, &unlock_args_force)
            .await
            .context("cmd_unlock force failed")?;
        assert_eq!(mapache::utils::count_files(&locks_dir)?, 0);

        Ok(())
    }
}
