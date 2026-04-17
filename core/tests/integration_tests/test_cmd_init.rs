#![cfg(test)]

mod tests {
    use std::sync::Arc;

    use mapache::{
        backend::localfs::LocalFS,
        commands::{self, cmd_init::CmdArgs},
        mapache::defaults::TEST_REPO_CONFIG,
        repository::repo::Repository,
    };

    use anyhow::{Context, Result};

    use crate::integration_tests::TestContext;

    #[tokio::test]
    async fn test_init() -> Result<()> {
        let ctx = TestContext::new().await?;

        let args = CmdArgs {};

        // Init repo
        commands::cmd_init::run(&ctx.global, &args)
            .await
            .context("Failed to run cmd_init")?;

        // Assert layout
        assert!(ctx.repo_path.join("manifest").exists());
        assert!(ctx.repo_path.join("index").exists());
        assert!(ctx.repo_path.join("keys").exists());
        assert!(ctx.repo_path.join("snapshots").exists());
        assert!(ctx.repo_path.join("objects").exists());
        for i in 0x00..=0xff {
            assert!(
                ctx.repo_path
                    .join("objects")
                    .join(format!("{i:02x}"))
                    .exists()
            );
        }

        let keys = ctx.repo_path.join("keys").read_dir()?;
        assert_eq!(keys.into_iter().count(), 1);

        // Try to open repo
        let backend = Arc::new(LocalFS::new(ctx.repo_path));
        Repository::try_open_with_lock(&ctx.auth, None, backend, TEST_REPO_CONFIG, false, None)
            .await
            .context("Failed to open repository")?;

        Ok(())
    }

    #[tokio::test]
    async fn test_init_and_open_with_ext_keyfile() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let keyfile_path = ctx._tmp_dir.path().join("ext_keyfile");

        ctx.global.key = Some(keyfile_path.clone());
        mapache::mapache::global::set_global_opts_with_args(&ctx.global);

        let args = CmdArgs {};

        // Init repo
        commands::cmd_init::run(&ctx.global, &args)
            .await
            .context("Failed to run cmd_init")?;

        // Assert layout
        assert!(ctx.repo_path.join("manifest").exists());
        assert!(ctx.repo_path.join("index").exists());
        assert!(ctx.repo_path.join("keys").exists());
        assert!(ctx.repo_path.join("snapshots").exists());
        assert!(ctx.repo_path.join("objects").exists());
        for i in 0x00..=0xff {
            assert!(
                ctx.repo_path
                    .join("objects")
                    .join(format!("{i:02x}"))
                    .exists()
            );
        }

        assert!(keyfile_path.exists());
        let keys = ctx.repo_path.join("keys").read_dir()?;
        assert_eq!(keys.into_iter().count(), 0);

        // Try to open repo
        let backend = Arc::new(LocalFS::new(ctx.repo_path));
        Repository::try_open_with_lock(
            &ctx.auth,
            Some(&keyfile_path),
            backend,
            TEST_REPO_CONFIG,
            false,
            None,
        )
        .await
        .context("Failed to open repository")?;

        Ok(())
    }
}
