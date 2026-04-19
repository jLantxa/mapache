#![cfg(test)]

mod tests {
    use anyhow::{Context, Result};
    use mapache::{
        commands::{self, UseSnapshot, cmd_forget, cmd_recall, cmd_snapshot},
        repository::repo::SNAPSHOTS_DIR,
        utils,
    };

    use crate::integration_tests::TestContext;

    #[tokio::test]
    async fn test_cmd_forget_and_recall() -> Result<()> {
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
            tags_str: "tag1".to_string(),
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

        // Run snapshot 2
        let snapshot_args = cmd_snapshot::CmdArgs {
            paths: vec![backup_data_tmp_path.join("file.txt")],
            as_root: false,
            exclude: None,
            exclude_file: None,
            tags_str: "tag2".to_string(),
            description: None,
            no_parent: false,
            skip_if_unchanged: false,
            no_scan: false,
            parent: UseSnapshot::Latest,
            num_readers: 1,
            num_packers: 1,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&ctx.global, &snapshot_args).await?;

        let snapshots_dir = ctx.repo_path.join(SNAPSHOTS_DIR);
        assert_eq!(utils::count_files(&snapshots_dir)?, 2);

        // Get ID of one snapshot to forget
        let snapshots = std::fs::read_dir(&snapshots_dir)?
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<_>, _>>()?;
        let first_id = snapshots[0]
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        // Test cmd_forget with keep_last
        let forget_args = cmd_forget::CmdArgs {
            forget: vec![first_id.clone()],
            force: false,
            tags_str: None,
            keep_last: None,
            keep_within: None,
            keep_yearly: None,
            keep_monthly: None,
            keep_weekly: None,
            keep_daily: None,
            keep_tags_str: None,
            dry_run: false,
            run_gc: false,
            tolerance: 10.0,
        };
        commands::cmd_forget::run(&ctx.global, &forget_args)
            .await
            .context("cmd_forget failed")?;

        let snapshots = std::fs::read_dir(&snapshots_dir)?
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<_>, _>>()?;

        let dropped_count = snapshots
            .iter()
            .filter(|p| p.extension().is_some_and(|ext| ext == "dropped"))
            .count();
        assert_eq!(dropped_count, 1);

        // Test cmd_recall
        let recall_args = cmd_recall::CmdArgs {
            id: first_id.clone(),
        };
        commands::cmd_recall::run(&ctx.global, &recall_args)
            .await
            .context("cmd_recall failed")?;

        let snapshots = std::fs::read_dir(&snapshots_dir)?
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<_>, _>>()?;
        let dropped_count = snapshots
            .iter()
            .filter(|p| p.extension().is_some_and(|ext| ext == "dropped"))
            .count();
        assert_eq!(dropped_count, 0);
        assert_eq!(snapshots.len(), 2);

        // Test cmd_forget with force
        let forget_args = cmd_forget::CmdArgs {
            forget: vec![first_id],
            force: true,
            tags_str: None,
            keep_last: None,
            keep_within: None,
            keep_yearly: None,
            keep_monthly: None,
            keep_weekly: None,
            keep_daily: None,
            keep_tags_str: None,
            dry_run: false,
            run_gc: true,
            tolerance: 10.0,
        };
        commands::cmd_forget::run(&ctx.global, &forget_args)
            .await
            .context("cmd_forget force failed")?;

        assert_eq!(utils::count_files(&snapshots_dir)?, 1);

        Ok(())
    }
}
