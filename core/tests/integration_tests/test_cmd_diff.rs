#![cfg(test)]

mod tests {
    use anyhow::Result;
    use mapache::commands::{self, UseSnapshot, cmd_snapshot};

    use crate::integration_tests::{TestContext, run_bin};

    #[tokio::test]
    async fn test_run_diff() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        ctx.setup_backup_data()?;
        let backup_data_tmp_path = ctx.backup_data_path.as_ref().unwrap();

        ctx.global.verbosity = Some(1);
        mapache::mapache::global::set_global_opts_with_args(&ctx.global);

        // Init repo
        ctx.init_repo().await?;

        // Run snapshot 1
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

        // Run snapshot 2 (modified)
        let file_path = backup_data_tmp_path.join("file.txt");
        std::fs::write(&file_path, "modified content")?;

        let snapshot_args_2 = cmd_snapshot::CmdArgs {
            paths: vec![file_path],
            as_root: false,
            exclude: None,
            exclude_file: None,
            tags_str: String::new(),
            description: None,
            no_parent: false,
            skip_if_unchanged: false,
            no_scan: false,
            parent: UseSnapshot::Latest,
            num_readers: 1,
            num_packers: 1,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&ctx.global, &snapshot_args_2).await?;

        // Get IDs
        let snapshots_dir = ctx.repo_path.join("snapshots");
        let snapshots = std::fs::read_dir(&snapshots_dir)?
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(snapshots.len(), 2);

        let id1 = snapshots[0]
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let id2 = snapshots[1]
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        // Test cmd_diff via binary
        let output = run_bin(&[
            "diff",
            &id1,
            &id2,
            "--repo",
            &ctx.repo_path.to_string_lossy(),
            "--auth-file",
            &ctx.auth_file_path.to_string_lossy(),
        ])?;

        assert!(output.status.success());
        let stdout = String::from_utf8(output.stdout)?;
        assert!(stdout.contains("M  file.txt") || stdout.contains("m  file.txt"));
        assert!(stdout.contains("Files"));
        assert!(stdout.contains("Size"));

        Ok(())
    }
}
