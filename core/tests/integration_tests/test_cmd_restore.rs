#![cfg(test)]

mod tests {
    use std::{
        path::PathBuf,
        time::{Duration, SystemTime},
    };

    use anyhow::{Context, Result};
    use filetime::{FileTime, set_file_times};
    use mapache::{
        commands::{self, UseSnapshot, cmd_restore, cmd_snapshot},
        restorer::Strategy,
    };

    use crate::integration_tests::TestContext;

    #[tokio::test]
    async fn test_restore_with_filter() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        ctx.setup_backup_data()?;
        let backup_data_tmp_path = ctx.backup_data_path.as_ref().unwrap();

        // Init repo
        ctx.init_repo().await?;

        // Run snapshot
        let snapshot_args = cmd_snapshot::CmdArgs {
            paths: vec![
                backup_data_tmp_path.join("0"),
                backup_data_tmp_path.join("1"),
                backup_data_tmp_path.join("2"),
                backup_data_tmp_path.join("file.txt"),
            ],
            as_root: false,
            exclude: Some(vec![
                backup_data_tmp_path
                    .join("0/01")
                    .to_string_lossy()
                    .into_owned(),
            ]),
            tags_str: String::new(),
            description: None,
            no_parent: false,
            skip_if_unchanged: false,
            no_scan: true,
            parent: UseSnapshot::Latest,
            num_readers: 2,
            num_packers: 2,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&ctx.global, &snapshot_args)
            .await
            .context("Failed to run cmd_snapshot")?;

        // Run restore
        let restore_path = ctx._tmp_dir.path().join("restore");
        let restore_args = cmd_restore::CmdArgs {
            target: restore_path.clone(),
            snapshot: UseSnapshot::Latest,
            dry_run: false,
            include: Some(vec!["0".to_string(), "1".to_string()]),
            exclude: Some(vec![
                String::from("0/00/file00.txt"),
                String::from("0/*.txt"),
            ]),
            strip_prefix: false,
            strategy: Strategy::Skip,

            quit_on_error: true,
            delete: false,
            no_preserve_root: false,
        };
        commands::cmd_restore::run(&ctx.global, &restore_args)
            .await
            .context("Failed to run cmd_restore")?;

        let restored_paths = vec![
            PathBuf::from("0"),
            PathBuf::from("0/00"),
            PathBuf::from("1"),
            PathBuf::from("1/10"),
        ];

        let excluded_paths = vec![
            PathBuf::from("0/file0.txt"),
            PathBuf::from("0/00/file00.txt"),
            PathBuf::from("2"),
            PathBuf::from("file.txt"),
        ];

        for path in &excluded_paths {
            let not_restored_path = restore_path.join(path);
            assert!(!not_restored_path.exists());
        }

        for path in &restored_paths {
            let backup_path = backup_data_tmp_path.join(path);
            let restored_path = restore_path.join(path);
            assert!(restored_path.exists());

            let restored_meta = restored_path.symlink_metadata()?;
            let backup_meta = backup_path.symlink_metadata()?;

            assert_eq!(restored_meta.modified()?, backup_meta.modified()?);

            if restored_path.is_file() {
                assert_eq!(std::fs::read(&restored_path)?, std::fs::read(&backup_path)?);
            }

            if !restore_path.is_dir() {
                // Excluded paths decrease the size of parent directories.
                // We only test the size of files in this case
                assert_eq!(restored_meta.len(), backup_meta.len());
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_restore_dry_run() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        ctx.setup_backup_data()?;
        let backup_data_tmp_path = ctx.backup_data_path.as_ref().unwrap();

        // Init repo
        ctx.init_repo().await?;

        // Run snapshot
        let snapshot_args = cmd_snapshot::CmdArgs {
            paths: vec![
                backup_data_tmp_path.join("0"),
                backup_data_tmp_path.join("1"),
                backup_data_tmp_path.join("2"),
                backup_data_tmp_path.join("file.txt"),
            ],
            as_root: false,
            exclude: None,
            tags_str: String::new(),
            description: None,
            no_parent: false,
            skip_if_unchanged: false,
            no_scan: true,
            parent: UseSnapshot::Latest,
            num_readers: 2,
            num_packers: 2,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&ctx.global, &snapshot_args)
            .await
            .context("Failed to run cmd_snapshot")?;

        // Run restore
        let restore_path = ctx._tmp_dir.path().join("restore");
        let restore_args = cmd_restore::CmdArgs {
            target: restore_path.clone(),
            snapshot: UseSnapshot::Latest,
            dry_run: true,
            include: None,
            exclude: None,
            strip_prefix: false,
            strategy: Strategy::Skip,

            quit_on_error: true,
            delete: false,
            no_preserve_root: false,
        };
        commands::cmd_restore::run(&ctx.global, &restore_args)
            .await
            .context("Failed to run cmd_restore")?;

        assert!(!restore_path.exists());

        Ok(())
    }

    #[tokio::test]
    async fn test_restore_strip_prefix() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        ctx.setup_backup_data()?;
        let backup_data_tmp_path = ctx.backup_data_path.as_ref().unwrap();

        // Init repo
        ctx.init_repo().await?;

        // Run snapshot
        let snapshot_args = cmd_snapshot::CmdArgs {
            paths: vec![
                backup_data_tmp_path.join("0"),
                backup_data_tmp_path.join("1"),
                backup_data_tmp_path.join("2"),
                backup_data_tmp_path.join("file.txt"),
            ],
            as_root: false,
            exclude: None,
            tags_str: String::new(),
            description: None,
            no_parent: false,
            skip_if_unchanged: false,
            no_scan: true,
            parent: UseSnapshot::Latest,
            num_readers: 2,
            num_packers: 2,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&ctx.global, &snapshot_args)
            .await
            .context("Failed to run cmd_snapshot")?;

        // Run restore 1
        let restore_path = ctx._tmp_dir.path().join("restore1");
        let restore_args = cmd_restore::CmdArgs {
            target: restore_path.clone(),
            snapshot: UseSnapshot::Latest,
            dry_run: false,
            include: Some(vec![
                "0/file0.txt".to_string(),
                "0/00/file00.txt".to_string(),
            ]),
            exclude: None,
            strip_prefix: true,
            strategy: Strategy::Skip,

            quit_on_error: true,
            delete: false,
            no_preserve_root: false,
        };
        commands::cmd_restore::run(&ctx.global, &restore_args)
            .await
            .context("Failed to run cmd_restore 1")?;

        let restored_paths = vec![PathBuf::from("file0.txt"), PathBuf::from("00/file00.txt")];
        for path in &restored_paths {
            let restored_path = restore_path.join(path);
            assert!(restored_path.exists());
        }

        // Run restore 2
        let restore_path = ctx._tmp_dir.path().join("restore2");
        let restore_args = cmd_restore::CmdArgs {
            target: restore_path.clone(),
            snapshot: UseSnapshot::Latest,
            dry_run: false,
            include: Some(vec!["0/00/file00.txt".to_string()]),
            exclude: None,
            strip_prefix: true,
            strategy: Strategy::Skip,

            quit_on_error: true,
            delete: false,
            no_preserve_root: false,
        };
        commands::cmd_restore::run(&ctx.global, &restore_args)
            .await
            .context("Failed to run cmd_restore 2")?;

        let restored_paths = vec![PathBuf::from("file00.txt")];
        for path in &restored_paths {
            let restored_path = restore_path.join(path);
            assert!(restored_path.exists());
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_restore_delete_default() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        ctx.setup_backup_data()?;
        let backup_data_tmp_path = ctx.backup_data_path.as_ref().unwrap();

        // Init repo
        ctx.init_repo().await?;

        // Run snapshot
        let snapshot_args = cmd_snapshot::CmdArgs {
            paths: vec![
                backup_data_tmp_path.join("0"),
                backup_data_tmp_path.join("1"),
                backup_data_tmp_path.join("2"),
                backup_data_tmp_path.join("file.txt"),
            ],
            as_root: false,
            exclude: None,
            tags_str: String::new(),
            description: None,
            no_parent: false,
            skip_if_unchanged: false,
            no_scan: true,
            parent: UseSnapshot::Latest,
            num_readers: 2,
            num_packers: 2,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&ctx.global, &snapshot_args)
            .await
            .context("Failed to run cmd_snapshot")?;

        // Run restore to create base files
        let restore_path = ctx._tmp_dir.path().join("restore");
        let restore_args = cmd_restore::CmdArgs {
            target: restore_path.clone(),
            snapshot: UseSnapshot::Latest,
            dry_run: false,
            include: None,
            exclude: None,
            strip_prefix: false,
            strategy: Strategy::Overwrite,

            quit_on_error: false,
            delete: false,
            no_preserve_root: false,
        };
        commands::cmd_restore::run(&ctx.global, &restore_args)
            .await
            .context("Failed to run cmd_restore (1/2)")?;

        // Create extra files
        std::fs::create_dir_all(restore_path.join("0"))?;
        std::fs::create_dir_all(restore_path.join("1").join("10"))?;
        std::fs::File::create(restore_path.join("extra_root.txt"))?;
        std::fs::create_dir_all(restore_path.join("extra_root_dir"))?;
        std::fs::File::create(restore_path.join("0").join("extra0.txt"))?;
        std::fs::File::create(restore_path.join("1").join("10").join("extra10.txt"))?;
        assert!(restore_path.join("extra_root.txt").exists());
        assert!(restore_path.join("extra_root_dir").exists());
        assert!(restore_path.join("0").join("extra0.txt").exists());
        assert!(
            restore_path
                .join("1")
                .join("10")
                .join("extra10.txt")
                .exists()
        );

        // Restore with --delete
        let restore_args = cmd_restore::CmdArgs {
            target: restore_path.clone(),
            snapshot: UseSnapshot::Latest,
            dry_run: false,
            include: None,
            exclude: None,
            strip_prefix: false,
            strategy: Strategy::Overwrite,

            quit_on_error: false,
            delete: true,
            no_preserve_root: false,
        };
        commands::cmd_restore::run(&ctx.global, &restore_args)
            .await
            .context("Failed to run cmd_restore (2/2)")?;

        let paths = vec![
            PathBuf::from("0"),
            PathBuf::from("0/file0.txt"),
            PathBuf::from("0/00"),
            PathBuf::from("0/00/file00.txt"),
            PathBuf::from("0/01"),
            PathBuf::from("0/01/file01a.txt"),
            PathBuf::from("0/01/file01b.txt"),
            PathBuf::from("1"),
            PathBuf::from("1/10"),
            PathBuf::from("2"),
            PathBuf::from("file.txt"),
        ];

        for path in &paths {
            let restored_path = restore_path.join(path);
            assert!(restored_path.exists());
        }

        // Assert that extra files were deleted, except at root level
        assert!(restore_path.join("extra_root.txt").exists()); // Not deleted (root level)
        assert!(restore_path.join("extra_root_dir").exists()); // Not deleted (root level)
        assert!(!restore_path.join("0").join("extra0.txt").exists());
        assert!(
            !restore_path
                .join("1")
                .join("10")
                .join("extra10.txt")
                .exists()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_restore_delete_default_with_include() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        ctx.setup_backup_data()?;
        let backup_data_tmp_path = ctx.backup_data_path.as_ref().unwrap();

        // Init repo
        ctx.init_repo().await?;

        // Run snapshot
        let snapshot_args = cmd_snapshot::CmdArgs {
            paths: vec![
                backup_data_tmp_path.join("0"),
                backup_data_tmp_path.join("1"),
                backup_data_tmp_path.join("2"),
                backup_data_tmp_path.join("file.txt"),
            ],
            as_root: false,
            exclude: None,
            tags_str: String::new(),
            description: None,
            no_parent: false,
            skip_if_unchanged: false,
            no_scan: true,
            parent: UseSnapshot::Latest,
            num_readers: 2,
            num_packers: 2,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&ctx.global, &snapshot_args)
            .await
            .context("Failed to run cmd_snapshot")?;

        // Run restore to create base files
        let restore_path = ctx._tmp_dir.path().join("restore");
        let restore_args = cmd_restore::CmdArgs {
            target: restore_path.clone(),
            snapshot: UseSnapshot::Latest,
            dry_run: false,
            include: None,
            exclude: None,
            strip_prefix: false,
            strategy: Strategy::Overwrite,

            quit_on_error: false,
            delete: false,
            no_preserve_root: false,
        };
        commands::cmd_restore::run(&ctx.global, &restore_args)
            .await
            .context("Failed to run cmd_restore (1/2)")?;

        // Create extra files
        std::fs::create_dir_all(restore_path.join("0"))?;
        std::fs::create_dir_all(restore_path.join("1").join("10"))?;
        std::fs::File::create(restore_path.join("extra_root.txt"))?;
        std::fs::create_dir_all(restore_path.join("extra_root_dir"))?;
        std::fs::File::create(restore_path.join("0").join("extra0.txt"))?;
        std::fs::File::create(restore_path.join("1").join("10").join("extra10.txt"))?;
        assert!(restore_path.join("extra_root.txt").exists());
        assert!(restore_path.join("extra_root_dir").exists());
        assert!(restore_path.join("0").join("extra0.txt").exists());
        assert!(
            restore_path
                .join("1")
                .join("10")
                .join("extra10.txt")
                .exists()
        );

        // Restore with --delete
        let restore_args = cmd_restore::CmdArgs {
            target: restore_path.clone(),
            snapshot: UseSnapshot::Latest,
            dry_run: false,
            include: Some(vec!["0".to_string()]),
            exclude: None,
            strip_prefix: false,
            strategy: Strategy::Overwrite,

            quit_on_error: false,
            delete: true,
            no_preserve_root: false,
        };
        commands::cmd_restore::run(&ctx.global, &restore_args)
            .await
            .context("Failed to run cmd_restore (2/2)")?;

        let paths = vec![
            PathBuf::from("0"),
            PathBuf::from("0/file0.txt"),
            PathBuf::from("0/00"),
            PathBuf::from("0/00/file00.txt"),
            PathBuf::from("0/01"),
            PathBuf::from("0/01/file01a.txt"),
            PathBuf::from("0/01/file01b.txt"),
        ];

        for path in &paths {
            let restored_path = restore_path.join(path);
            assert!(restored_path.exists());
        }

        // Assert that extra files were deleted, except at root level
        assert!(restore_path.join("extra_root.txt").exists()); // Not deleted (root level and includes)
        assert!(restore_path.join("extra_root_dir").exists()); // Not deleted (root level)
        assert!(!restore_path.join("0").join("extra0.txt").exists());
        assert!(
            restore_path
                .join("1")
                .join("10")
                .join("extra10.txt")
                .exists() // Not deleted as it is outside the includes
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_restore_delete_no_preserve_root() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        ctx.setup_backup_data()?;
        let backup_data_tmp_path = ctx.backup_data_path.as_ref().unwrap();

        // Init repo
        ctx.init_repo().await?;

        // Run snapshot
        let snapshot_args = cmd_snapshot::CmdArgs {
            paths: vec![
                backup_data_tmp_path.join("0"),
                backup_data_tmp_path.join("1"),
                backup_data_tmp_path.join("2"),
                backup_data_tmp_path.join("file.txt"),
            ],
            as_root: false,
            exclude: None,
            tags_str: String::new(),
            description: None,
            no_parent: false,
            skip_if_unchanged: false,
            no_scan: true,
            parent: UseSnapshot::Latest,
            num_readers: 2,
            num_packers: 2,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&ctx.global, &snapshot_args)
            .await
            .context("Failed to run cmd_snapshot")?;

        // Run restore to create base files
        let restore_path = ctx._tmp_dir.path().join("restore");
        let restore_args = cmd_restore::CmdArgs {
            target: restore_path.clone(),
            snapshot: UseSnapshot::Latest,
            dry_run: false,
            include: None,
            exclude: None,
            strip_prefix: false,
            strategy: Strategy::Overwrite,

            quit_on_error: false,
            delete: false,
            no_preserve_root: true,
        };
        commands::cmd_restore::run(&ctx.global, &restore_args)
            .await
            .context("Failed to run cmd_restore (1/2)")?;

        // Create extra files
        std::fs::create_dir_all(restore_path.join("0"))?;
        std::fs::create_dir_all(restore_path.join("1").join("10"))?;
        std::fs::File::create(restore_path.join("extra_root.txt"))?;
        std::fs::create_dir_all(restore_path.join("extra_root_dir"))?;
        std::fs::File::create(restore_path.join("0").join("extra0.txt"))?;
        std::fs::File::create(restore_path.join("1").join("10").join("extra10.txt"))?;
        assert!(restore_path.join("extra_root.txt").exists());
        assert!(restore_path.join("extra_root_dir").exists());
        assert!(restore_path.join("0").join("extra0.txt").exists());
        assert!(
            restore_path
                .join("1")
                .join("10")
                .join("extra10.txt")
                .exists()
        );

        // Restore with --delete
        let restore_args = cmd_restore::CmdArgs {
            target: restore_path.clone(),
            snapshot: UseSnapshot::Latest,
            dry_run: false,
            include: None,
            exclude: None,
            strip_prefix: false,
            strategy: Strategy::Overwrite,

            quit_on_error: false,
            delete: true,
            no_preserve_root: true,
        };
        commands::cmd_restore::run(&ctx.global, &restore_args)
            .await
            .context("Failed to run cmd_restore (2/2)")?;

        let paths = vec![
            PathBuf::from("0"),
            PathBuf::from("0/file0.txt"),
            PathBuf::from("0/00"),
            PathBuf::from("0/00/file00.txt"),
            PathBuf::from("0/01"),
            PathBuf::from("0/01/file01a.txt"),
            PathBuf::from("0/01/file01b.txt"),
            PathBuf::from("1"),
            PathBuf::from("1/10"),
            PathBuf::from("2"),
            PathBuf::from("file.txt"),
        ];

        for path in &paths {
            let restored_path = restore_path.join(path);
            assert!(restored_path.exists());
        }

        // Assert that extra files were deleted
        assert!(!restore_path.join("extra_root.txt").exists());
        assert!(!restore_path.join("extra_root_dir").exists());
        assert!(!restore_path.join("0").join("extra0.txt").exists());
        assert!(
            !restore_path
                .join("1")
                .join("10")
                .join("extra10.txt")
                .exists()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_restore_with_conflict_resolution() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        ctx.setup_backup_data()?;
        let backup_data_tmp_path = ctx.backup_data_path.as_ref().unwrap();

        // Init repo and create Snapshot 1
        ctx.init_repo().await?;

        let snapshot_args = cmd_snapshot::CmdArgs {
            paths: vec![backup_data_tmp_path.join("0")],
            as_root: false,
            exclude: None,
            tags_str: String::from("S1"),
            description: None,
            no_parent: false,
            skip_if_unchanged: false,
            no_scan: true,
            parent: UseSnapshot::Latest,
            num_readers: 1,
            num_packers: 1,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&ctx.global, &snapshot_args)
            .await
            .context("Failed to run cmd_snapshot S1")?;

        let restore_path = ctx._tmp_dir.path().join("restore_conflicts");
        let restore_args_initial = cmd_restore::CmdArgs {
            target: restore_path.clone(),
            snapshot: UseSnapshot::Latest,
            dry_run: false,
            include: None,
            exclude: None,
            strip_prefix: false,
            strategy: Strategy::Skip, // Strategy doesn't matter for initial restore

            quit_on_error: false,
            delete: false,
            no_preserve_root: false,
        };
        commands::cmd_restore::run(&ctx.global, &restore_args_initial)
            .await
            .context("Failed to run initial cmd_restore")?;

        let file_to_overwrite = restore_path.join("0").join("file0.txt");
        let file_to_skip = restore_path.join("0").join("00").join("file00.txt");
        let original_content =
            std::fs::read_to_string(backup_data_tmp_path.join("0").join("file0.txt"))?;

        // Change content and metadata for both local files
        let new_content_overwrite = "Conflict content for Overwrite test";
        std::fs::write(&file_to_overwrite, new_content_overwrite)?;

        let new_content_skip = "Conflict content for Skip test";
        std::fs::write(&file_to_skip, new_content_skip)?;

        let filetime = FileTime::from(SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000_000));
        set_file_times(&file_to_overwrite, filetime, filetime)?;
        set_file_times(&file_to_skip, filetime, filetime)?;

        let restore_args_overwrite = cmd_restore::CmdArgs {
            target: restore_path.clone(),
            snapshot: UseSnapshot::Latest,
            dry_run: false,
            include: Some(vec!["0/file0.txt".to_string()]),
            exclude: None,
            strip_prefix: false,
            strategy: Strategy::Overwrite,

            quit_on_error: false,
            delete: false,
            no_preserve_root: false,
        };
        commands::cmd_restore::run(&ctx.global, &restore_args_overwrite)
            .await
            .context("Failed to run cmd_restore Overwrite")?;

        // Verify that the file was overwritten
        assert_eq!(
            std::fs::read_to_string(&file_to_overwrite)?,
            original_content
        );

        let restore_args_skip = cmd_restore::CmdArgs {
            target: restore_path.clone(),
            snapshot: UseSnapshot::Latest,
            dry_run: false,
            include: Some(vec!["0/00/file00.txt".to_string()]),
            exclude: None,
            strip_prefix: false,
            strategy: Strategy::Skip,

            quit_on_error: false,
            delete: false,
            no_preserve_root: false,
        };
        commands::cmd_restore::run(&ctx.global, &restore_args_skip)
            .await
            .context("Failed to run cmd_restore Skip")?;

        // Verify that the local change was kept (skipped)
        assert_eq!(std::fs::read_to_string(&file_to_skip)?, new_content_skip);

        Ok(())
    }
}
