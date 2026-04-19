#![cfg(test)]

mod tests {
    use anyhow::Result;
    use mapache::commands::{self, UseSnapshot, cmd_snapshot};

    use crate::integration_tests::{TestContext, run_bin};

    #[tokio::test]
    async fn test_run_stats() -> Result<()> {
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
            exclude_file: None,
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

        // Test cmd_stats via binary
        let output = run_bin(&[
            "stats",
            "--repo",
            &ctx.repo_path.to_string_lossy(),
            "--auth-file",
            &ctx.auth_file_path.to_string_lossy(),
        ])?;

        assert!(output.status.success());
        let stdout = String::from_utf8(output.stdout)?;
        assert!(stdout.contains("Packs:"));
        assert!(stdout.contains("Snapshots:"));
        assert!(stdout.contains("1 snapshot"));

        // Test cmd_stats --json
        let output = run_bin(&[
            "stats",
            "--json",
            "--repo",
            &ctx.repo_path.to_string_lossy(),
            "--auth-file",
            &ctx.auth_file_path.to_string_lossy(),
        ])?;

        assert!(output.status.success());
        let stdout = String::from_utf8(output.stdout)?;
        let json: serde_json::Value = serde_json::from_str(&stdout)?;
        assert_eq!(json["msg_type"], "stats");
        assert_eq!(json["snapshots"]["count"], 1);

        Ok(())
    }
}
