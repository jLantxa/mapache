#![cfg(test)]

mod tests {
    use anyhow::{Context, Result};
    use mapache::commands::{self, UseSnapshot, cmd_rechunk, cmd_snapshot, cmd_verify};

    use crate::integration_tests::TestContext;

    #[tokio::test]
    async fn test_cmd_rechunk() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        ctx.setup_backup_data()?;
        let backup_data_tmp_path = ctx.backup_data_path.as_ref().unwrap();

        ctx.global.verbosity = Some(1);
        mapache::mapache::global::set_global_opts_with_args(&ctx.global);

        // Init repo
        ctx.init_repo().await?;

        // Run snapshot
        let snapshot_args = cmd_snapshot::CmdArgs {
            paths: vec![backup_data_tmp_path.join("file.txt")],
            as_root: false,
            exclude: None,
            tags_str: String::new(),
            description: None,
            no_parent: true,
            skip_if_unchanged: false,
            no_scan: false,
            parent: UseSnapshot::Latest,
            num_readers: 1,
            num_packers: 1,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&ctx.global, &snapshot_args).await?;

        // Run rechunk
        let rechunk_args = cmd_rechunk::CmdArgs {};
        commands::cmd_rechunk::run(&ctx.global, &rechunk_args)
            .await
            .context("cmd_rechunk failed")?;

        // Verify repo
        let verify_args = cmd_verify::CmdArgs {
            read_packs: true,
            parallel: 1,
            with_cache: false,
            fail_early: true,
            sample: None,
        };
        commands::cmd_verify::run(&ctx.global, &verify_args)
            .await
            .context("verify failed after rechunk")?;

        Ok(())
    }
}
