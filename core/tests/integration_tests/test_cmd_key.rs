#![cfg(test)]

mod tests {
    use anyhow::Result;

    use mapache::repository::repo::KEYS_DIR;

    use crate::integration_tests::TestContext;

    #[tokio::test]
    async fn test_run_key_subcommands_and_check_stdout() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        ctx.global.verbosity = Some(1);
        mapache::mapache::global::set_global_opts_with_args(&ctx.global);

        // Init repo
        ctx.init_repo().await?;

        // Test key list
        let stdout = ctx.run_mapache_ok(&["key", "list"])?;

        assert!(stdout.contains("mapachito"));
        assert!(stdout.contains("Key ID"));

        // Test key delete
        let keys_dir = ctx.repo_path.join(KEYS_DIR);
        let keys = std::fs::read_dir(&keys_dir)?
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(keys.len(), 1);
        let key_id = keys[0].file_name().unwrap().to_str().unwrap().to_string();

        // Verification of 'delete' subcommand
        ctx.run_mapache_ok(&["key", "delete", &key_id])?;

        assert!(!keys_dir.join(&key_id).exists());

        Ok(())
    }
}
