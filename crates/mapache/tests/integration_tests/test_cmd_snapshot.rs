#![cfg(test)]

mod tests {
    use std::path::PathBuf;

    use anyhow::{Context, Result};

    use mapache::{
        commands::{Compression, UseSnapshot},
        repository::repo::SNAPSHOTS_DIR,
        utils,
    };

    use crate::{
        integration_tests::{INTEGRATION_TEST_DATA, TestContext, assert_times_equal},
        synthetic::{Dataset, SyntheticData},
    };

    #[tokio::test]
    async fn test_snapshot() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        // Init repo
        ctx.init_repo().await?;

        let snapshots_dir = ctx.repo_path.join(SNAPSHOTS_DIR);
        assert_eq!(utils::count_files(&snapshots_dir)?, 0);
        let index_dir = ctx.repo_path.join("index");
        assert_eq!(utils::count_files(&index_dir)?, 0);

        // Run snapshot
        ctx.snapshot(vec![
            backup_data_tmp_path.join("0"),
            backup_data_tmp_path.join("1"),
            backup_data_tmp_path.join("2"),
            backup_data_tmp_path.join("file.txt"),
        ])
        .await?;

        assert_eq!(utils::count_files(&snapshots_dir)?, 1);
        assert_ne!(utils::count_files(&index_dir)?, 0);

        // Run restore
        let restore_path = ctx._tmp_dir.path().join("restore");
        ctx.restore_builder(restore_path.clone())
            .run(&ctx.global)
            .await?;

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
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        // Init repo
        ctx.init_repo().await?;

        // Run snapshot
        ctx.snapshot_builder(vec![
            backup_data_tmp_path.join("0"),
            backup_data_tmp_path.join("1"),
            backup_data_tmp_path.join("2"),
            backup_data_tmp_path.join("file.txt"),
        ])
        .dry_run(true)
        .run(&ctx.global)
        .await?;

        // `snapshots` directory should be empty
        let snapshots_dir = ctx.repo_path.join("snapshots");
        assert_eq!(utils::count_files(&snapshots_dir)?, 0);

        // `index` directory should be empty
        let index_dir = ctx.repo_path.join("index");
        assert_eq!(utils::count_files(&index_dir)?, 0);

        // Run restore
        let restore_path = ctx._tmp_dir.path().join("restore");
        let restore_result = ctx.restore_builder(restore_path).run(&ctx.global).await;
        assert!(restore_result.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_snapshot_with_exclude() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        // Init repo
        ctx.init_repo().await?;

        // Run snapshot
        ctx.snapshot_builder(vec![
            backup_data_tmp_path.join("0"),
            backup_data_tmp_path.join("1"),
            backup_data_tmp_path.join("2"),
            backup_data_tmp_path.join("file.txt"),
        ])
        .exclude(vec![
            backup_data_tmp_path
                .join("0/01")
                .to_string_lossy()
                .into_owned(),
            backup_data_tmp_path
                .join("0/00/*.txt")
                .to_string_lossy()
                .into_owned(),
        ])
        .run(&ctx.global)
        .await?;

        // Run restore
        let restore_path = ctx._tmp_dir.path().join("restore");
        ctx.restore_builder(restore_path.clone())
            .run(&ctx.global)
            .await?;

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

            if !restored_path.is_dir() {
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
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        // Init repo
        ctx.init_repo().await?;

        let snapshots_dir = ctx.repo_path.join(SNAPSHOTS_DIR);
        assert_eq!(utils::count_files(&snapshots_dir)?, 0);

        // Run snapshot (1st)
        ctx.snapshot_builder(vec![
            backup_data_tmp_path.join("0"),
            backup_data_tmp_path.join("1"),
            backup_data_tmp_path.join("2"),
            backup_data_tmp_path.join("file.txt"),
        ])
        .exclude(vec![
            backup_data_tmp_path
                .join("0/01")
                .to_string_lossy()
                .into_owned(),
        ])
        .run(&ctx.global)
        .await?;
        assert_eq!(utils::count_files(&snapshots_dir)?, 1);

        // Run snapshot (2nd)
        ctx.snapshot(vec![
            backup_data_tmp_path.join("0"),
            backup_data_tmp_path.join("1"),
            backup_data_tmp_path.join("2"),
            backup_data_tmp_path.join("file.txt"),
        ])
        .await?;
        assert_eq!(utils::count_files(&snapshots_dir)?, 2);

        let restore_path = ctx._tmp_dir.path().join("restore");
        ctx.restore_builder(restore_path.clone())
            .run(&ctx.global)
            .await?;

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
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        // Init repo
        ctx.init_repo().await?;

        // Run snapshot
        ctx.snapshot_builder(vec![backup_data_tmp_path.join("0")])
            .root(true)
            .run(&ctx.global)
            .await?;

        // Run restore
        let restore_path = ctx._tmp_dir.path().join("restore");
        ctx.restore_builder(restore_path.clone())
            .run(&ctx.global)
            .await?;

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
    async fn test_snapshot_intermediate_paths() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        // Init repo
        ctx.init_repo().await?;

        // Run snapshot
        ctx.snapshot(vec![
            backup_data_tmp_path.join("0"),
            backup_data_tmp_path.join("1/10/file10.txt"),
            backup_data_tmp_path.join("2"),
            backup_data_tmp_path.join("file.txt"),
        ])
        .await?;

        // Run restore
        let restore_path = ctx._tmp_dir.path().join("restore");
        ctx.restore_builder(restore_path.clone())
            .run(&ctx.global)
            .await?;

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
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        // Init repo
        ctx.init_repo().await?;

        let snapshots_dir = ctx.repo_path.join(SNAPSHOTS_DIR);
        assert_eq!(utils::count_files(&snapshots_dir)?, 0);

        // Run snapshot (1st)
        let builder = ctx
            .snapshot_builder(vec![
                backup_data_tmp_path.join("0"),
                backup_data_tmp_path.join("1"),
                backup_data_tmp_path.join("2"),
                backup_data_tmp_path.join("file.txt"),
            ])
            .skip_if_unchanged(true);

        builder.clone().run(&ctx.global).await?;
        assert_eq!(utils::count_files(&snapshots_dir)?, 1);

        builder.run(&ctx.global).await?;
        assert_eq!(utils::count_files(&snapshots_dir)?, 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_run_snapshot_metadata_and_check_log() -> Result<()> {
        let ctx = TestContext::new().await?;
        let backup_data_tmp_path = ctx._tmp_dir.path().join("backup");
        std::fs::create_dir(&backup_data_tmp_path)?;
        std::fs::write(backup_data_tmp_path.join("test.txt"), "content")?;

        ctx.init_repo().await?;

        // Run snapshot with tags and description
        ctx.snapshot_builder(vec![backup_data_tmp_path.join("test.txt")])
            .tags("important,work".to_string())
            .description("Detailed backup description".to_string())
            .run(&ctx.global)
            .await?;

        // Verify via binary log output
        let stdout = ctx.run_mapache_ok(&["log"])?;

        assert!(stdout.contains("Tags: important, work"));
        assert!(stdout.contains("Detailed backup description"));

        Ok(())
    }

    #[tokio::test]
    async fn test_run_snapshot_empty_dirs_and_restore() -> Result<()> {
        let ctx = TestContext::new().await?;
        let backup_path = ctx._tmp_dir.path().join("backup");
        let empty_dir = backup_path.join("empty_folder");
        std::fs::create_dir_all(&empty_dir)?;

        ctx.init_repo().await?;

        ctx.snapshot_builder(vec![backup_path.clone()])
            .root(true)
            .run(&ctx.global)
            .await?;

        // Restore
        let restore_path = ctx._tmp_dir.path().join("restore");
        ctx.restore_builder(restore_path.clone())
            .run(&ctx.global)
            .await?;

        assert!(restore_path.join("empty_folder").is_dir());

        Ok(())
    }

    #[tokio::test]
    async fn test_run_snapshot_manual_parent() -> Result<()> {
        let ctx = TestContext::new().await?;
        let backup_path = ctx._tmp_dir.path().join("backup");
        std::fs::create_dir_all(&backup_path)?;
        std::fs::write(backup_path.join("f1.txt"), "v1")?;

        ctx.init_repo().await?;

        // Snapshot 1
        ctx.snapshot_builder(vec![backup_path.clone()])
            .root(true)
            .run(&ctx.global)
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
        ctx.snapshot_builder(vec![backup_path.clone()])
            .root(true)
            .parent(UseSnapshot::SnapshotId(first_id.clone()))
            .run(&ctx.global)
            .await?;

        // Find the one that is NOT first_id
        let snapshots = std::fs::read_dir(&snapshots_dir)?
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<_>, _>>()?;

        let second_id = snapshots
            .iter()
            .map(|p| p.file_name().unwrap().to_str().unwrap().to_string())
            .find(|id| id != &first_id)
            .unwrap();

        // Verify parent ID via cat
        let stdout = ctx.run_mapache_ok(&["cat", &format!("snapshot:{}", second_id)])?;
        assert!(stdout.contains(&format!("\"parent\": \"{}\"", first_id)));

        Ok(())
    }

    #[tokio::test]
    async fn test_snapshot_with_atime_capture_and_restore() -> Result<()> {
        use std::time::{Duration, UNIX_EPOCH};

        let ctx = TestContext::new().await?;
        let backup_path = ctx._tmp_dir.path().join("backup");
        std::fs::create_dir_all(&backup_path)?;

        let file_path = backup_path.join("test.txt");
        std::fs::write(&file_path, "atime test content")?;

        let original_atime = UNIX_EPOCH + Duration::from_secs(1_000_000_000);
        let original_mtime = UNIX_EPOCH + Duration::from_secs(1_100_000_000);

        let ft_atime = mapache::fs::filetime::FileTime::from(original_atime);
        let ft_mtime = mapache::fs::filetime::FileTime::from(original_mtime);
        mapache::fs::filetime::set_file_times(&file_path, ft_atime, ft_mtime)?;

        ctx.init_repo().await?;

        ctx.snapshot_builder(vec![backup_path.clone()])
            .root(true)
            .with_atime(true)
            .run(&ctx.global)
            .await?;

        let restore_path = ctx._tmp_dir.path().join("restore");
        ctx.restore_builder(restore_path.clone())
            .run(&ctx.global)
            .await?;

        let restored_path = restore_path.join("test.txt");
        assert!(restored_path.exists());

        let restored_meta = restored_path.symlink_metadata()?;
        let restored_atime = restored_meta.accessed()?;
        let restored_mtime = restored_meta.modified()?;

        let atime_diff = restored_atime
            .duration_since(original_atime)
            .unwrap_or_else(|_| original_atime.duration_since(restored_atime).unwrap());
        assert!(
            atime_diff.as_secs() <= 1,
            "restored atime differs by {:?}",
            atime_diff
        );

        let mtime_diff = restored_mtime
            .duration_since(original_mtime)
            .unwrap_or_else(|_| original_mtime.duration_since(restored_mtime).unwrap());
        assert!(
            mtime_diff.as_secs() <= 1,
            "restored mtime differs by {:?}",
            mtime_diff
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_snapshot_without_atime_does_not_store_it() -> Result<()> {
        use std::time::{Duration, UNIX_EPOCH};

        let ctx = TestContext::new().await?;
        let backup_path = ctx._tmp_dir.path().join("backup");
        std::fs::create_dir_all(&backup_path)?;

        let file_path = backup_path.join("test.txt");
        std::fs::write(&file_path, "no atime test")?;

        let original_atime = UNIX_EPOCH + Duration::from_secs(1_000_000_000);
        let original_mtime = UNIX_EPOCH + Duration::from_secs(1_100_000_000);

        let ft_atime = mapache::fs::filetime::FileTime::from(original_atime);
        let ft_mtime = mapache::fs::filetime::FileTime::from(original_mtime);
        mapache::fs::filetime::set_file_times(&file_path, ft_atime, ft_mtime)?;

        ctx.init_repo().await?;

        ctx.snapshot_builder(vec![backup_path.clone()])
            .root(true)
            .with_atime(false)
            .run(&ctx.global)
            .await?;

        let restore_path = ctx._tmp_dir.path().join("restore");
        ctx.restore_builder(restore_path.clone())
            .run(&ctx.global)
            .await?;

        let restored_path = restore_path.join("test.txt");
        assert!(restored_path.exists());

        let restored_meta = restored_path.symlink_metadata()?;
        let restored_atime = restored_meta.accessed()?;
        let restored_mtime = restored_meta.modified()?;

        let mtime_diff = restored_mtime
            .duration_since(original_mtime)
            .unwrap_or_else(|_| original_mtime.duration_since(restored_mtime).unwrap());
        assert!(
            mtime_diff.as_secs() <= 1,
            "restored mtime differs by {:?}",
            mtime_diff
        );

        let atime_diff = restored_atime
            .duration_since(restored_mtime)
            .unwrap_or_else(|_| restored_mtime.duration_since(restored_atime).unwrap());
        assert!(
            atime_diff.as_secs() <= 1,
            "without --with-atime, restored atime should equal mtime"
        );

        Ok(())
    }

    /// End-to-end test: spawn mapache binary with --stdin, pipe data,
    /// and verify the snapshot is created.
    #[tokio::test]
    async fn test_stdin_snapshot_end_to_end() -> Result<()> {
        use std::io::Write;

        let ctx = TestContext::new().await?;
        ctx.init_repo().await?;

        let snapshots_dir = ctx.repo_path.join(SNAPSHOTS_DIR);
        let index_dir = ctx.repo_path.join("index");

        assert_eq!(mapache::utils::count_files(&snapshots_dir)?, 0);
        assert_eq!(mapache::utils::count_files(&index_dir)?, 0);

        // Spawn mapache with --stdin
        let bin = env!("CARGO_BIN_EXE_mapache");
        let mut cmd = std::process::Command::new(bin);
        // Global options (--repo, --auth-file, etc.) go AFTER the subcommand
        // because only --with-config is truly global in clap.
        cmd.arg("snapshot")
            .arg("--repo")
            .arg(ctx.repo_path.as_os_str())
            .arg("--auth-file")
            .arg(ctx.auth_file_path.as_os_str())
            .arg("--stdin")
            .arg("--description")
            .arg("stdin end-to-end test")
            .arg("--no-cache")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());

        let mut child = cmd.spawn().context("Failed to spawn mapache")?;

        // Pipe test data to stdin
        let test_data = b"Hello from stdin end-to-end test!";
        {
            let mut stdin_handle = child.stdin.take().expect("Failed to get stdin handle");
            stdin_handle.write_all(test_data)?;
        }

        let output = child.wait_with_output()?;

        // Check exit status and show any error output
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success(),
            "mapache snapshot --stdin failed:\nstderr: {stderr}\nstdout: {}",
            String::from_utf8_lossy(&output.stdout),
        );

        // Verify a snapshot was created
        assert_eq!(mapache::utils::count_files(&snapshots_dir)?, 1);
        assert_ne!(
            mapache::utils::count_files(&index_dir)?,
            0,
            "index should have been created"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_compression_none_snapshot_restore() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_path = ctx.setup_backup_data(&synthetic)?;

        ctx.init_repo().await?;

        let mut global = ctx.global.clone();
        global.compression_level = Compression::None;

        ctx.snapshot_builder(vec![
            backup_data_path.join("file.txt"),
            backup_data_path.join("0"),
            backup_data_path.join("1"),
            backup_data_path.join("2"),
        ])
        .run(&global)
        .await
        .context("snapshot with --compression none failed")?;

        let restore_path = ctx._tmp_dir.path().join("restore");
        ctx.restore_builder(restore_path.clone())
            .run(&global)
            .await
            .context("restore after --compression none failed")?;

        let paths = vec![
            PathBuf::from("file.txt"),
            PathBuf::from("0/file0.txt"),
            PathBuf::from("0/00/file00.txt"),
            PathBuf::from("0/01/file01a.txt"),
            PathBuf::from("0/01/file01b.txt"),
            PathBuf::from("1/10/file10.txt"),
        ];

        for path in &paths {
            let backup_file = backup_data_path.join(path);
            let restored_file = restore_path.join(path);
            assert!(
                restored_file.exists(),
                "missing after restore: {}",
                path.display()
            );
            assert_eq!(
                std::fs::read(&backup_file)?,
                std::fs::read(&restored_file)?,
                "content mismatch: {}",
                path.display()
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_compression_none_empty_file() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_path = ctx.setup_backup_data(&synthetic)?;

        ctx.init_repo().await?;

        let mut global = ctx.global.clone();
        global.compression_level = Compression::None;

        ctx.snapshot_builder(vec![backup_data_path.join("empty.txt")])
            .run(&global)
            .await
            .context("snapshot empty file with --compression none failed")?;

        let restore_path = ctx._tmp_dir.path().join("restore");
        ctx.restore_builder(restore_path.clone())
            .run(&global)
            .await
            .context("restore empty file with --compression none failed")?;

        let restored = std::fs::read(restore_path.join("empty.txt"))?;
        assert!(
            restored.is_empty(),
            "empty file should remain empty after restore"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_compression_none_large_file() -> Result<()> {
        let ctx = TestContext::new().await?;

        let data_dir = ctx._tmp_dir.path().join("backup");
        std::fs::create_dir_all(&data_dir)?;
        let pattern = b"ABCDEFGHIJKLMNOP";
        let mut content = Vec::with_capacity(pattern.len() * 10_000);
        for _ in 0..10_000 {
            content.extend_from_slice(pattern);
        }
        std::fs::write(data_dir.join("large.bin"), &content)?;

        ctx.init_repo().await?;

        let mut global = ctx.global.clone();
        global.compression_level = Compression::None;

        ctx.snapshot_builder(vec![data_dir.join("large.bin")])
            .run(&global)
            .await
            .context("snapshot large file with --compression none failed")?;

        let restore_path = ctx._tmp_dir.path().join("restore");
        ctx.restore_builder(restore_path.clone())
            .run(&global)
            .await
            .context("restore large file with --compression none failed")?;

        assert_eq!(std::fs::read(restore_path.join("large.bin"))?, content);

        Ok(())
    }

    #[tokio::test]
    async fn test_compression_none_unicode_paths() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_path = ctx.setup_backup_data(&synthetic)?;

        ctx.init_repo().await?;

        let mut global = ctx.global.clone();
        global.compression_level = Compression::None;

        ctx.snapshot_builder(vec![
            backup_data_path.join("🚀"),
            backup_data_path.join("日本語"),
        ])
        .run(&global)
        .await
        .context("snapshot unicode paths with --compression none failed")?;

        let restore_path = ctx._tmp_dir.path().join("restore");
        ctx.restore_builder(restore_path.clone())
            .run(&global)
            .await
            .context("restore unicode paths with --compression none failed")?;

        assert_eq!(
            std::fs::read(restore_path.join("🚀/mañana.txt"))?,
            "UTF-8 content: ñáóú\n".as_bytes()
        );
        assert_eq!(
            std::fs::read(restore_path.join("日本語/こんにちは.txt"))?,
            "こんにちは、世界！\n".as_bytes()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_compression_none_multiple_snapshots() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_path = ctx.setup_backup_data(&synthetic)?;

        ctx.init_repo().await?;

        let mut global = ctx.global.clone();
        global.compression_level = Compression::None;

        ctx.snapshot_builder(vec![backup_data_path.join("file.txt")])
            .run(&global)
            .await
            .context("first snapshot failed")?;

        ctx.snapshot_builder(vec![backup_data_path.join("file.txt")])
            .run(&global)
            .await
            .context("second snapshot failed")?;

        let restore_path = ctx._tmp_dir.path().join("restore");
        ctx.restore_builder(restore_path.clone())
            .run(&global)
            .await
            .context("restore failed")?;

        assert_eq!(
            std::fs::read(restore_path.join("file.txt"))?,
            b"This is a test file.\n"
        );

        ctx.verify_builder()
            .read_packs(true)
            .run(&global)
            .await
            .context("verify failed")?;

        Ok(())
    }

    #[tokio::test]
    async fn test_mixed_compression_across_snapshots() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_path = ctx.setup_backup_data(&synthetic)?;

        ctx.init_repo().await?;

        ctx.snapshot_builder(vec![
            backup_data_path.join("file.txt"),
            backup_data_path.join("0"),
            backup_data_path.join("1"),
        ])
        .no_scan(true)
        .run(&ctx.global)
        .await
        .context("snapshot with compression failed")?;

        let restore1 = ctx._tmp_dir.path().join("restore1");
        ctx.restore_builder(restore1.clone())
            .run(&ctx.global)
            .await
            .context("restore first snapshot failed")?;
        assert_eq!(
            std::fs::read(restore1.join("file.txt"))?,
            b"This is a test file.\n"
        );

        let mut global_no_comp = ctx.global.clone();
        global_no_comp.compression_level = Compression::None;

        ctx.snapshot_builder(vec![
            backup_data_path.join("file.txt"),
            backup_data_path.join("0"),
            backup_data_path.join("1"),
        ])
        .no_scan(true)
        .run(&global_no_comp)
        .await
        .context("snapshot without compression failed")?;

        let restore2 = ctx._tmp_dir.path().join("restore2");
        ctx.restore_builder(restore2.clone())
            .run(&ctx.global)
            .await
            .context("restore second snapshot failed")?;

        assert_eq!(
            std::fs::read(restore2.join("file.txt"))?,
            b"This is a test file.\n"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_zero_blob_dedup() -> Result<()> {
        let ctx = TestContext::new().await?;

        let data_dir = ctx._tmp_dir.path().join("backup");
        std::fs::create_dir_all(&data_dir)?;

        let zeros_small = vec![0u8; 4096];
        std::fs::write(data_dir.join("zeros_small.bin"), &zeros_small)?;

        let zeros_large = vec![0u8; 65536];
        std::fs::write(data_dir.join("zeros_large.bin"), &zeros_large)?;

        std::fs::write(data_dir.join("normal.bin"), b"normal content\n")?;

        ctx.init_repo().await?;

        ctx.snapshot_builder(vec![
            data_dir.join("zeros_small.bin"),
            data_dir.join("zeros_large.bin"),
            data_dir.join("normal.bin"),
        ])
        .no_scan(true)
        .run(&ctx.global)
        .await
        .context("snapshot with zero blobs failed")?;

        ctx.verify_builder()
            .read_packs(true)
            .run(&ctx.global)
            .await
            .context("verify failed with zero blobs")?;

        let restore_path = ctx._tmp_dir.path().join("restore");
        ctx.restore_builder(restore_path.clone())
            .run(&ctx.global)
            .await
            .context("restore with zero blobs failed")?;

        assert_eq!(
            std::fs::read(restore_path.join("zeros_small.bin"))?,
            zeros_small
        );
        assert_eq!(
            std::fs::read(restore_path.join("zeros_large.bin"))?,
            zeros_large
        );
        assert_eq!(
            std::fs::read(restore_path.join("normal.bin"))?,
            b"normal content\n"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_zero_blob_dedup_across_snapshots() -> Result<()> {
        let ctx = TestContext::new().await?;

        let data_dir = ctx._tmp_dir.path().join("backup");
        std::fs::create_dir_all(&data_dir)?;

        let zeros = vec![0u8; 8192];
        std::fs::write(data_dir.join("zeros.bin"), &zeros)?;

        ctx.init_repo().await?;

        ctx.snapshot_builder(vec![data_dir.join("zeros.bin")])
            .no_scan(true)
            .run(&ctx.global)
            .await
            .context("first snapshot failed")?;

        ctx.snapshot_builder(vec![data_dir.join("zeros.bin")])
            .no_scan(true)
            .run(&ctx.global)
            .await
            .context("second snapshot failed")?;

        ctx.verify_builder()
            .read_packs(true)
            .run(&ctx.global)
            .await
            .context("verify failed")?;

        let restore_path = ctx._tmp_dir.path().join("restore");
        ctx.restore_builder(restore_path.clone())
            .run(&ctx.global)
            .await
            .context("restore failed")?;

        assert_eq!(std::fs::read(restore_path.join("zeros.bin"))?, zeros);

        Ok(())
    }
}
