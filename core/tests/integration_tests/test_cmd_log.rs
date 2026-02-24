#![cfg(test)]

mod tests {
    use anyhow::Result;
    use mapache::commands::{self, UseSnapshot, cmd_snapshot};

    use crate::integration_tests::{TestContext, run_bin};

    #[tokio::test]
    async fn test_run_log() -> Result<()> {
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
            tags_str: "tag1".to_string(),
            description: Some("test description".to_string()),
            no_parent: true,
            skip_if_unchanged: false,
            no_scan: false,
            parent: UseSnapshot::Latest,
            num_readers: 1,
            num_packers: 1,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&ctx.global, &snapshot_args).await?;

        // Test cmd_log via binary
        let output = run_bin(&[
            "log",
            "--repo",
            &ctx.repo_path.to_string_lossy(),
            "--auth-file",
            &ctx.auth_file_path.to_string_lossy(),
        ])?;

        assert!(output.status.success());
        let stdout = String::from_utf8(output.stdout)?;
        assert!(stdout.contains("Date:"));
        assert!(stdout.contains("tag1"));
        assert!(stdout.contains("test description"));
        assert!(stdout.contains("1 snapshots"));

        // Test cmd_log --compact
        let output = run_bin(&[
            "log",
            "--compact",
            "--repo",
            &ctx.repo_path.to_string_lossy(),
            "--auth-file",
            &ctx.auth_file_path.to_string_lossy(),
        ])?;

        assert!(output.status.success());
        let stdout = String::from_utf8(output.stdout)?;
        assert!(stdout.contains("tag1"));

        Ok(())
    }
}
