#![cfg(test)]

mod tests {
    use anyhow::{Context, Result};
    use mapache::{
        commands::{
            self, Compression, GlobalArgs, UseSnapshot, cmd_forget, cmd_recall, cmd_snapshot,
        },
        mapache::{defaults::DEFAULT_DEFAULT_PACK_SIZE_MIB, global::set_global_opts_with_args},
        repository::repo::{Auth, SNAPSHOTS_DIR},
        utils,
    };

    use tempfile::tempdir;

    use crate::{
        TEST_QUIET,
        integration_tests::{BACKUP_DATA_PATH, init_repo},
        test_utils::{self},
    };

    #[tokio::test]
    async fn test_cmd_forget_and_recall() -> Result<()> {
        let tmp_dir = tempdir()?;
        let tmp_path = tmp_dir.path();
        let auth = Auth {
            username: "mapachito".to_string(),
            password: "password".to_string(),
        };
        let auth_file_path = tmp_path.join("auth");
        std::fs::write(
            &auth_file_path,
            format!("{}\n{}", auth.username, auth.password),
        )?;

        let backup_data_path = test_utils::get_test_data_path(BACKUP_DATA_PATH);
        let backup_data_tmp_path = tmp_path.join("backup");
        test_utils::extract_tar_xz_archive(&backup_data_path, &backup_data_tmp_path)?;

        let repo_path = tmp_path.join("repo");

        let global = GlobalArgs {
            repo: repo_path.to_string_lossy().to_string(),
            auth_file: Some(auth_file_path),
            key: None,
            quiet: *TEST_QUIET,
            json: false,
            verbosity: Some(1),
            ssh_privatekey: None,
            pack_size_mib: DEFAULT_DEFAULT_PACK_SIZE_MIB,
            no_cache: true,
            retry_lock_duration: None,
            compression_level: Compression::Fastest,
        };
        set_global_opts_with_args(&global);

        // Init repo
        init_repo(&auth, repo_path.clone()).await?;

        // Run snapshot 1
        let snapshot_args = cmd_snapshot::CmdArgs {
            paths: vec![backup_data_tmp_path.join("file.txt")],
            as_root: false,
            exclude: None,
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
        commands::cmd_snapshot::run(&global, &snapshot_args).await?;

        // Run snapshot 2
        let snapshot_args = cmd_snapshot::CmdArgs {
            paths: vec![backup_data_tmp_path.join("file.txt")],
            as_root: false,
            exclude: None,
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
        commands::cmd_snapshot::run(&global, &snapshot_args).await?;

        let snapshots_dir = repo_path.join(SNAPSHOTS_DIR);
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
        commands::cmd_forget::run(&global, &forget_args)
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
        commands::cmd_recall::run(&global, &recall_args)
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
        commands::cmd_forget::run(&global, &forget_args)
            .await
            .context("cmd_forget force failed")?;

        assert_eq!(utils::count_files(&snapshots_dir)?, 1);

        Ok(())
    }
}
