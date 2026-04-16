#![cfg(test)]

mod tests {
    use std::path::PathBuf;

    use anyhow::{Context, Result};
    use mapache::{
        commands::{self, UseSnapshot, cmd_restore, cmd_snapshot},
        fs,
        repository::repo::SNAPSHOTS_DIR,
        restorer::Strategy,
        utils,
    };

    use crate::integration_tests::{TestContext, assert_times_equal, run_bin};

    #[tokio::test]
    async fn test_snapshot() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        ctx.setup_backup_data()?;
        let backup_data_tmp_path = ctx.backup_data_path.as_ref().unwrap();

        // Init repo
        ctx.init_repo().await?;

        let snapshots_dir = ctx.repo_path.join(SNAPSHOTS_DIR);
        assert_eq!(utils::count_files(&snapshots_dir)?, 0);
        let index_dir = ctx.repo_path.join("index");
        assert_eq!(utils::count_files(&index_dir)?, 0);

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
            no_scan: false,
            parent: UseSnapshot::Latest,
            num_readers: 2,
            num_packers: 2,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&ctx.global, &snapshot_args)
            .await
            .context("Failed to run cmd_snapshot")?;
        assert_eq!(utils::count_files(&snapshots_dir)?, 1);
        assert_ne!(utils::count_files(&index_dir)?, 0);

        // Run restore
        let restore_path = ctx._tmp_dir.path().join("restore");
        let restore_args = cmd_restore::CmdArgs {
            preallocate: false,
            target: restore_path.clone(),
            snapshot: UseSnapshot::Latest,
            dry_run: false,
            verify: false,
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

        let paths = vec![
            PathBuf::from("0"),
            PathBuf::from("0/file0.txt"),
            PathBuf::from("0/00"),
            PathBuf::from("0/00/file00.txt"),
            PathBuf::from("0/01"),
            #[cfg(not(target_os = "windows"))]
            PathBuf::from("0/l01"),
            PathBuf::from("0/01/file01a.txt"),
            PathBuf::from("0/01/file01b.txt"),
            PathBuf::from("1"),
            PathBuf::from("1/10"),
            PathBuf::from("1/10/file10.txt"),
            #[cfg(not(target_os = "windows"))]
            PathBuf::from("1/10/lfile10.txt"),
            PathBuf::from("2"),
            PathBuf::from("file.txt"),
        ];

        for path in &paths {
            let backup_path = backup_data_tmp_path.join(path);
            let restored_path = restore_path.join(path);
            assert!(restored_path.exists());

            let restored_meta = restored_path.symlink_metadata()?;
            let backup_meta = backup_path.symlink_metadata()?;

            assert_eq!(restored_meta.len(), backup_meta.len());

            #[cfg(unix)]
            if restored_path.is_symlink() {
                assert_eq!(restored_meta.modified()?, backup_meta.modified()?);
            }

            if restored_path.is_file() {
                assert_eq!(std::fs::read(&restored_path)?, std::fs::read(&backup_path)?);
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_snapshot_dry_run() -> Result<()> {
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
            no_scan: false,
            parent: UseSnapshot::Latest,
            num_readers: 2,
            num_packers: 2,
            dry_run: true,
        };
        commands::cmd_snapshot::run(&ctx.global, &snapshot_args)
            .await
            .context("Failed to run cmd_snapshot")?;

        // `snapshots` directory should be empty
        let snapshots_dir = ctx.repo_path.join("snapshots");
        assert_eq!(utils::count_files(&snapshots_dir)?, 0);

        // `index` directory should be empty
        let index_dir = ctx.repo_path.join("index");
        assert_eq!(utils::count_files(&index_dir)?, 0);

        // Run restore
        let restore_path = ctx._tmp_dir.path().join("restore");
        let restore_args = cmd_restore::CmdArgs {
            preallocate: false,
            target: restore_path.clone(),
            snapshot: UseSnapshot::Latest,
            dry_run: false,
            verify: false,
            include: None,
            exclude: None,
            strip_prefix: false,
            strategy: Strategy::Skip,

            quit_on_error: true,
            delete: false,
            no_preserve_root: false,
        };

        let restore_result = commands::cmd_restore::run(&ctx.global, &restore_args).await;
        assert!(restore_result.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_snapshot_with_exclude() -> Result<()> {
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
                backup_data_tmp_path
                    .join("0/00/*.txt")
                    .to_string_lossy()
                    .into_owned(),
            ]),
            tags_str: String::new(),
            description: None,
            no_parent: false,
            skip_if_unchanged: false,
            no_scan: false,
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
            verify: false,
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

        let paths = vec![
            PathBuf::from("0"),
            PathBuf::from("0/file0.txt"),
            PathBuf::from("0/00"),
            PathBuf::from("1"),
            PathBuf::from("1/10"),
            PathBuf::from("1/10/file10.txt"),
            #[cfg(not(target_os = "windows"))]
            PathBuf::from("1/10/lfile10.txt"),
            PathBuf::from("2"),
            PathBuf::from("file.txt"),
        ];

        // Excluded
        assert!(!restore_path.join("0/00/file00.txt").exists());

        for path in &paths {
            let backup_path = backup_data_tmp_path.join(path);
            let restored_path = restore_path.join(path);
            assert!(restored_path.exists());

            let restored_meta = restored_path.symlink_metadata()?;
            let backup_meta = backup_path.symlink_metadata()?;

            if restored_path.is_file() {
                assert_eq!(std::fs::read(&restored_path)?, std::fs::read(&backup_path)?);
            }

            if !restore_path.is_dir() {
                // Excluded paths decrease the size of parent directories.
                // We only test the size of files in this case
                assert_eq!(restored_meta.len(), backup_meta.len());
            }

            #[cfg(unix)]
            if restored_path.is_symlink() {
                assert_eq!(restored_meta.modified()?, backup_meta.modified()?);
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_snapshot_twice() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        ctx.setup_backup_data()?;
        let backup_data_tmp_path = ctx.backup_data_path.as_ref().unwrap();

        // Init repo
        ctx.init_repo().await?;

        let snapshots_dir = ctx.repo_path.join(SNAPSHOTS_DIR);
        assert_eq!(utils::count_files(&snapshots_dir)?, 0);

        // Run snapshot (1st)
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
            no_scan: false,
            parent: UseSnapshot::Latest,
            num_readers: 2,
            num_packers: 2,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&ctx.global, &snapshot_args)
            .await
            .context("Failed to run cmd_snapshot 1")?;
        assert_eq!(utils::count_files(&snapshots_dir)?, 1);

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
            no_scan: false,
            parent: UseSnapshot::Latest,
            num_readers: 2,
            num_packers: 2,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&ctx.global, &snapshot_args)
            .await
            .context("Failed to run cmd_snapshot 2")?;
        assert_eq!(utils::count_files(&snapshots_dir)?, 2);

        let restore_path = ctx._tmp_dir.path().join("restore");
        let restore_args = cmd_restore::CmdArgs {
            preallocate: false,
            target: restore_path.clone(),
            snapshot: UseSnapshot::Latest,
            dry_run: false,
            verify: false,
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
            let backup_path = backup_data_tmp_path.join(path);
            let restored_path = restore_path.join(path);
            assert!(restored_path.exists());

            let restored_meta = restored_path.symlink_metadata()?;
            let backup_meta = backup_path.symlink_metadata()?;

            assert_eq!(restored_meta.len(), backup_meta.len());
            assert_times_equal(restored_meta.modified()?, backup_meta.modified()?);

            if restored_path.is_file() {
                assert_eq!(std::fs::read(&restored_path)?, std::fs::read(&backup_path)?);
            }

            #[cfg(unix)]
            if restored_path.is_symlink() {
                assert_eq!(restored_meta.modified()?, backup_meta.modified()?);
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_snapshot_folder_as_root() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        ctx.setup_backup_data()?;
        let backup_data_tmp_path = ctx.backup_data_path.as_ref().unwrap();

        // Init repo
        ctx.init_repo().await?;

        // Run snapshot
        let snapshot_args = cmd_snapshot::CmdArgs {
            paths: vec![backup_data_tmp_path.join("0")],
            as_root: true,
            exclude: None,
            tags_str: String::new(),
            description: None,
            no_parent: false,
            skip_if_unchanged: false,
            no_scan: false,
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
            verify: false,
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

        let paths = vec![
            PathBuf::from("file0.txt"),
            PathBuf::from("00"),
            PathBuf::from("00/file00.txt"),
            PathBuf::from("01"),
            #[cfg(not(target_os = "windows"))]
            PathBuf::from("l01"),
            PathBuf::from("01/file01a.txt"),
            PathBuf::from("01/file01b.txt"),
        ];

        for path in &paths {
            // The source folder still has the "0" folder
            let backup_path = backup_data_tmp_path.join("0").join(path);
            let restored_path = restore_path.join(path);
            assert!(fs::path_exists(&restored_path).await);

            let restored_meta = restored_path.symlink_metadata()?;
            let backup_meta = backup_path.symlink_metadata()?;

            assert_eq!(restored_meta.len(), backup_meta.len());

            #[cfg(unix)]
            if restored_path.is_symlink() {
                assert_eq!(restored_meta.modified()?, backup_meta.modified()?);
            }

            if restored_path.is_file() {
                assert_eq!(std::fs::read(&restored_path)?, std::fs::read(&backup_path)?);
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_snapshot_intermediate_paths() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        ctx.setup_backup_data()?;
        let backup_data_tmp_path = ctx.backup_data_path.as_ref().unwrap();

        // Init repo
        ctx.init_repo().await?;

        // Run snapshot
        let snapshot_args = cmd_snapshot::CmdArgs {
            paths: vec![
                backup_data_tmp_path.join("0"),
                backup_data_tmp_path.join("1/10/file10.txt"), // 1/10 should be included as intermediate_paths
                backup_data_tmp_path.join("2"),
                backup_data_tmp_path.join("file.txt"),
            ],
            as_root: false,
            exclude: None,
            tags_str: String::new(),
            description: None,
            no_parent: false,
            skip_if_unchanged: false,
            no_scan: false,
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
            verify: false,
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

        let paths = vec![
            PathBuf::from("0"),
            PathBuf::from("0/file0.txt"),
            PathBuf::from("0/00"),
            PathBuf::from("0/00/file00.txt"),
            PathBuf::from("0/01"),
            #[cfg(not(target_os = "windows"))]
            PathBuf::from("0/l01"),
            PathBuf::from("0/01/file01a.txt"),
            PathBuf::from("0/01/file01b.txt"),
            PathBuf::from("1/10/file10.txt"),
            PathBuf::from("2"),
            PathBuf::from("file.txt"),
        ];

        for path in &paths {
            let backup_path = backup_data_tmp_path.join(path);
            let restored_path = restore_path.join(path);
            assert!(restored_path.exists());

            let restored_meta = restored_path.symlink_metadata()?;
            let backup_meta = backup_path.symlink_metadata()?;

            assert_eq!(restored_meta.len(), backup_meta.len());

            #[cfg(unix)]
            if restored_path.is_symlink() {
                assert_eq!(restored_meta.modified()?, backup_meta.modified()?);
            }

            if restored_path.is_file() {
                assert_eq!(std::fs::read(&restored_path)?, std::fs::read(&backup_path)?);
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_snapshot_skip_if_unchanged() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        ctx.setup_backup_data()?;
        let backup_data_tmp_path = ctx.backup_data_path.as_ref().unwrap();

        // Init repo
        ctx.init_repo().await?;

        let snapshots_dir = ctx.repo_path.join(SNAPSHOTS_DIR);
        assert_eq!(utils::count_files(&snapshots_dir)?, 0);

        // Run snapshot (1st)
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
            skip_if_unchanged: true,
            no_scan: false,
            parent: UseSnapshot::Latest,
            num_readers: 2,
            num_packers: 2,
            dry_run: false,
        };

        commands::cmd_snapshot::run(&ctx.global, &snapshot_args)
            .await
            .context("Failed to run cmd_snapshot 1")?;
        assert_eq!(utils::count_files(&snapshots_dir)?, 1);

        commands::cmd_snapshot::run(&ctx.global, &snapshot_args)
            .await
            .context("Failed to run cmd_snapshot 2")?;
        assert_eq!(utils::count_files(&snapshots_dir)?, 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_run_snapshot_metadata_and_check_log() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let backup_data_tmp_path = ctx._tmp_dir.path().join("backup");
        std::fs::create_dir(&backup_data_tmp_path)?;
        std::fs::write(backup_data_tmp_path.join("test.txt"), "content")?;

        ctx.init_repo().await?;

        ctx.global.verbosity = Some(1);
        mapache::mapache::global::set_global_opts_with_args(&ctx.global);

        // Run snapshot with tags and description
        let snapshot_args = cmd_snapshot::CmdArgs {
            paths: vec![backup_data_tmp_path.join("test.txt")],
            as_root: false,
            exclude: None,
            tags_str: "important,work".to_string(),
            description: Some("Detailed backup description".to_string()),
            no_parent: false,
            skip_if_unchanged: false,
            no_scan: false,
            parent: UseSnapshot::Latest,
            num_readers: 1,
            num_packers: 1,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&ctx.global, &snapshot_args).await?;

        // Verify via binary log output
        let output = run_bin(&[
            "log",
            "--repo",
            &ctx.repo_path.to_string_lossy(),
            "--auth-file",
            &ctx.auth_file_path.to_string_lossy(),
        ])?;

        assert!(output.status.success());
        let stdout = String::from_utf8(output.stdout)?;
        assert!(stdout.contains("Tags: important, work"));
        assert!(stdout.contains("Detailed backup description"));

        Ok(())
    }

    #[tokio::test]
    async fn test_run_snapshot_empty_dirs_and_restore() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let backup_path = ctx._tmp_dir.path().join("backup");
        let empty_dir = backup_path.join("empty_folder");
        std::fs::create_dir_all(&empty_dir)?;

        ctx.init_repo().await?;

        ctx.global.verbosity = Some(1);
        mapache::mapache::global::set_global_opts_with_args(&ctx.global);

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
            verify: false,
            include: None,
            exclude: None,
            strip_prefix: false,
            strategy: Strategy::Skip,
            quit_on_error: true,
            delete: false,
            no_preserve_root: false,
        };
        commands::cmd_restore::run(&ctx.global, &restore_args).await?;

        assert!(restore_path.join("empty_folder").is_dir());

        Ok(())
    }

    #[tokio::test]
    async fn test_run_snapshot_manual_parent() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let backup_path = ctx._tmp_dir.path().join("backup");
        std::fs::create_dir_all(&backup_path)?;
        std::fs::write(backup_path.join("f1.txt"), "v1")?;

        ctx.init_repo().await?;

        ctx.global.verbosity = Some(1);
        mapache::mapache::global::set_global_opts_with_args(&ctx.global);

        // Snapshot 1
        commands::cmd_snapshot::run(
            &ctx.global,
            &cmd_snapshot::CmdArgs {
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
            },
        )
        .await?;

        let snapshots_dir = ctx.repo_path.join(SNAPSHOTS_DIR);
        let snapshots = std::fs::read_dir(&snapshots_dir)?
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<_>, _>>()?;
        let first_id = snapshots[0]
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        // Snapshot 2 with manual parent
        commands::cmd_snapshot::run(
            &ctx.global,
            &cmd_snapshot::CmdArgs {
                paths: vec![backup_path.clone()],
                as_root: true,
                exclude: None,
                tags_str: String::new(),
                description: None,
                no_parent: false,
                skip_if_unchanged: false,
                no_scan: false,
                parent: UseSnapshot::SnapshotId(first_id.clone()),
                num_readers: 1,
                num_packers: 1,
                dry_run: false,
            },
        )
        .await?;

        // Verify parent ID via cat
        let _output = run_bin(&[
            "cat",
            "snapshot:latest",
            "--repo",
            &ctx.repo_path.to_string_lossy(),
            "--auth-file",
            &ctx.auth_file_path.to_string_lossy(),
        ]);

        let snapshots = std::fs::read_dir(&snapshots_dir)?
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<_>, _>>()?;

        // Find the one that is NOT first_id
        let second_id = snapshots
            .iter()
            .map(|p| p.file_name().unwrap().to_str().unwrap().to_string())
            .find(|id| id != &first_id)
            .unwrap();

        let output = run_bin(&[
            "cat",
            &format!("snapshot:{}", second_id),
            "--repo",
            &ctx.repo_path.to_string_lossy(),
            "--auth-file",
            &ctx.auth_file_path.to_string_lossy(),
        ])?;

        assert!(output.status.success());
        let stdout = String::from_utf8(output.stdout)?;
        assert!(stdout.contains(&format!("\"parent\": \"{}\"", first_id)));

        Ok(())
    }
}
