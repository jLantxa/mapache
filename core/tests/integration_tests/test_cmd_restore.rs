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

    use crate::integration_tests::{TestContext, assert_times_equal};

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
            preallocate: false,
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

            assert_times_equal(restored_meta.modified()?, backup_meta.modified()?);

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
            preallocate: false,
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
            preallocate: false,
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
            preallocate: false,
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
            preallocate: false,
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
            preallocate: false,
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
            preallocate: false,
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
            preallocate: false,
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
            preallocate: false,
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
            preallocate: false,
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
            preallocate: false,
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
            preallocate: false,
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
            preallocate: false,
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

    #[cfg(unix)]
    #[tokio::test]
    async fn test_xattrs() -> Result<()> {
        let ctx = TestContext::new().await?;
        let backup_path = ctx._tmp_dir.path().join("backup");
        std::fs::create_dir_all(&backup_path)?;

        // File with xattr
        let file_path = backup_path.join("file_with_xattr.txt");
        std::fs::write(&file_path, "hello xattr")?;
        let file_attr_name = "user.mapache_file_test";
        let file_attr_value = b"mapache_file_value";
        xattr::set(&file_path, file_attr_name, file_attr_value)?;

        // Directory with xattr
        let dir_path = backup_path.join("dir_with_xattr");
        std::fs::create_dir(&dir_path)?;
        let dir_attr_name = "user.mapache_dir_test";
        let dir_attr_value = b"mapache_dir_value";
        xattr::set(&dir_path, dir_attr_name, dir_attr_value)?;

        // Symlink with xattr (some OSs might not support xattrs on symlinks, but Linux generally does)
        let symlink_path = backup_path.join("symlink_with_xattr");
        std::os::unix::fs::symlink("file_with_xattr.txt", &symlink_path)?;
        let symlink_attr_name = "user.mapache_symlink_test";
        let symlink_attr_value = b"mapache_symlink_value";
        // standard variants do not follow symlinks by default in the xattr crate
        let symlink_xattr_supported =
            xattr::set(&symlink_path, symlink_attr_name, symlink_attr_value).is_ok();

        // Init repo
        ctx.init_repo().await?;

        // Run snapshot
        let snapshot_args = cmd_snapshot::CmdArgs {
            paths: vec![backup_path.clone()],
            as_root: true,
            exclude: None,
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
        commands::cmd_snapshot::run(&ctx.global, &snapshot_args).await?;

        // Restore
        let restore_path = ctx._tmp_dir.path().join("restore");
        let restore_args = cmd_restore::CmdArgs {
            preallocate: false,
            target: restore_path.clone(),
            snapshot: UseSnapshot::Latest,
            dry_run: false,
            include: None,
            exclude: None,
            strip_prefix: false,
            strategy: Strategy::Skip,
            quit_on_error: true,
            delete: false,
            no_preserve_root: false,
        };
        commands::cmd_restore::run(&ctx.global, &restore_args).await?;

        // Verify file xattr
        let restored_file_path = restore_path.join("file_with_xattr.txt");
        assert!(restored_file_path.exists());
        let restored_file_value = xattr::get(&restored_file_path, file_attr_name)?;
        assert_eq!(restored_file_value, Some(file_attr_value.to_vec()));

        // Verify directory xattr
        let restored_dir_path = restore_path.join("dir_with_xattr");
        assert!(restored_dir_path.exists());
        let restored_dir_value = xattr::get(&restored_dir_path, dir_attr_name)?;
        assert_eq!(restored_dir_value, Some(dir_attr_value.to_vec()));

        // Verify symlink xattr
        if symlink_xattr_supported {
            let restored_symlink_path = restore_path.join("symlink_with_xattr");
            assert!(restored_symlink_path.is_symlink());
            let restored_symlink_value = xattr::get(&restored_symlink_path, symlink_attr_name)?;
            assert_eq!(restored_symlink_value, Some(symlink_attr_value.to_vec()));
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_restore_sparse_vs_eager_allocation() -> Result<()> {
        let ctx = TestContext::new().await?;
        let backup_path = ctx._tmp_dir.path().join("backup");
        std::fs::create_dir_all(&backup_path)?;

        // Create a moderately-sized test file
        let file_path = backup_path.join("large_file.bin");
        let test_size = 10 * 1024 * 1024; // 10 MiB
        let test_data = vec![42u8; test_size];
        std::fs::write(&file_path, &test_data)?;

        // Init repo and snapshot
        ctx.init_repo().await?;

        let snapshot_args = cmd_snapshot::CmdArgs {
            paths: vec![file_path.clone()],
            as_root: false,
            exclude: None,
            tags_str: String::new(),
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
            .context("Failed to run cmd_snapshot")?;

        // Test 1: Restore with sparse allocation (default)
        let restore_sparse_path = ctx._tmp_dir.path().join("restore_sparse");
        let restore_args_sparse = cmd_restore::CmdArgs {
            preallocate: false,
            target: restore_sparse_path.clone(),
            snapshot: UseSnapshot::Latest,
            dry_run: false,
            include: None,
            exclude: None,
            strip_prefix: false,
            strategy: Strategy::Overwrite,
            quit_on_error: true,
            delete: false,
            no_preserve_root: false,
        };
        commands::cmd_restore::run(&ctx.global, &restore_args_sparse)
            .await
            .context("Failed to restore with sparse allocation")?;

        let restored_sparse_file = restore_sparse_path.join("large_file.bin");
        assert!(
            restored_sparse_file.exists(),
            "Sparse restore: file should exist at {:?}",
            restored_sparse_file
        );
        let sparse_content = std::fs::read(&restored_sparse_file)?;
        assert_eq!(sparse_content.len(), test_data.len());
        assert_eq!(sparse_content, test_data);
        let sparse_metadata = std::fs::metadata(&restored_sparse_file)?;
        assert_eq!(sparse_metadata.len(), test_size as u64);

        // Test 2: Restore with eager allocation
        let restore_eager_path = ctx._tmp_dir.path().join("restore_eager");
        let restore_args_eager = cmd_restore::CmdArgs {
            preallocate: true,
            target: restore_eager_path.clone(),
            snapshot: UseSnapshot::Latest,
            dry_run: false,
            include: None,
            exclude: None,
            strip_prefix: false,
            strategy: Strategy::Overwrite,
            quit_on_error: true,
            delete: false,
            no_preserve_root: false,
        };
        commands::cmd_restore::run(&ctx.global, &restore_args_eager)
            .await
            .context("Failed to restore with eager allocation")?;

        let restored_eager_file = restore_eager_path.join("large_file.bin");
        assert!(
            restored_eager_file.exists(),
            "Eager restore: file should exist at {:?}",
            restored_eager_file
        );
        let eager_content = std::fs::read(&restored_eager_file)?;
        assert_eq!(eager_content.len(), test_data.len());
        assert_eq!(eager_content, test_data);
        let eager_metadata = std::fs::metadata(&restored_eager_file)?;
        assert_eq!(eager_metadata.len(), test_size as u64);

        // Both files should have identical content
        assert_eq!(sparse_content, eager_content);

        // On Unix, check allocation: eager-allocated file should use more disk space
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let sparse_blocks = sparse_metadata.blocks();
            let eager_blocks = eager_metadata.blocks();
            // Eager allocation reserves actual blocks; sparse may use fewer initially
            // This is a best-effort check; some filesystems behave differently
            eprintln!(
                "Sparse file blocks: {}, Eager file blocks: {}",
                sparse_blocks, eager_blocks
            );
            assert!(
                eager_blocks > 0,
                "Eager-allocated file should have allocated blocks"
            );
        }

        Ok(())
    }
}
