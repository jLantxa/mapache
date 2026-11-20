mod tests {
    use std::{
        io::{Read, Seek, SeekFrom},
        path::{Path, PathBuf},
        time::{Duration, Instant},
    };

    use anyhow::{Context, Result, bail};
    #[cfg(unix)]
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    use mapache::commands::{self, UseSnapshot, cmd_mount, cmd_snapshot};

    use crate::{
        integration_tests::{INTEGRATION_TEST_DATA, TestContext},
        synthetic::{Dataset, Entry, SyntheticData},
    };

    /// Real FUSE mounts are global resources; run these tests one at a time.
    static MOUNT_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    struct MountThreadGuard {
        mountpoint: PathBuf,
        mount_thread: Option<std::thread::JoinHandle<()>>,
    }

    impl Drop for MountThreadGuard {
        fn drop(&mut self) {
            let _ = mapache::commands::cmd_mount::MapacheFS::unmount(&self.mountpoint);
            if let Some(thread) = self.mount_thread.take() {
                // Wait a bit for the thread to finish after unmount.
                // If it doesn't finish quickly, retry unmount and wait again.
                for _ in 0..10 {
                    if thread.is_finished() {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
                if !thread.is_finished() {
                    let _ = mapache::commands::cmd_mount::MapacheFS::unmount(&self.mountpoint);
                    for _ in 0..40 {
                        if thread.is_finished() {
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(100));
                    }
                }
                let _ = thread.join();
            }
        }
    }

    async fn verify_paths(paths: &[PathBuf], source_base: &Path, mount_base: &Path) -> Result<()> {
        for path in paths {
            let source_path = source_base.join(path);
            let mount_path = mount_base.join(path);

            assert!(
                mount_path.exists(),
                "Path {} does not exist in mount",
                path.display()
            );

            let source_meta = std::fs::symlink_metadata(&source_path)?;
            let mount_meta = std::fs::symlink_metadata(&mount_path)?;

            assert_eq!(source_meta.file_type(), mount_meta.file_type());

            #[cfg(unix)]
            {
                assert_eq!(
                    source_meta.permissions().mode(),
                    mount_meta.permissions().mode(),
                    "permission bits differ for {}",
                    path.display()
                );
                assert_eq!(
                    source_meta.uid(),
                    mount_meta.uid(),
                    "owner differs for {}",
                    path.display()
                );
                assert_eq!(
                    source_meta.gid(),
                    mount_meta.gid(),
                    "group differs for {}",
                    path.display()
                );
            }

            if source_meta.is_file() {
                assert_eq!(source_meta.len(), mount_meta.len());

                let mut source_file = std::fs::File::open(&source_path)?;
                let mut mount_file = std::fs::File::open(&mount_path)?;

                let mut source_buf = Vec::new();
                let mut mount_buf = Vec::new();

                source_file.read_to_end(&mut source_buf)?;
                mount_file.read_to_end(&mut mount_buf)?;

                assert_eq!(source_buf, mount_buf);
            } else if source_meta.is_symlink() {
                let source_target = std::fs::read_link(&source_path)?;
                let mount_target = std::fs::read_link(&mount_path)?;
                assert_eq!(source_target, mount_target);
            }
        }

        Ok(())
    }

    fn snapshot_paths(backup_data: &Path) -> Vec<PathBuf> {
        vec![
            backup_data.join("0"),
            backup_data.join("1"),
            backup_data.join("2"),
            backup_data.join("file.txt"),
            backup_data.join("🚀"),
            backup_data.join("日本語"),
        ]
    }

    fn snapshot_args(paths: Vec<PathBuf>) -> cmd_snapshot::CmdArgs {
        cmd_snapshot::CmdArgs {
            paths,
            as_root: Some(false),
            exclude: None,
            exclude_file: None,
            exclude_if_present: None,
            exclude_caches: None,
            exclude_larger_than: None,
            files_from: None,
            hostname: None,
            tags_str: Some(String::new()),
            description: None,
            no_parent: false,
            skip_if_unchanged: Some(false),
            no_scan: Some(true),
            one_file_system: Some(false),
            use_fs_snapshot: Some(false),
            fail_on_skipped: Some(false),
            parent: Some(UseSnapshot::Latest),
            num_readers: Some(2),
            num_packers: Some(2),
            dry_run: false,
            with_atime: None,
            stdin: false,
            hook_args: Default::default(),
        }
    }

    fn mount_args(
        mountpoint: PathBuf,
        metadata_only: bool,
        create_mountpoint: bool,
    ) -> cmd_mount::CmdArgs {
        cmd_mount::CmdArgs {
            mountpoint,
            bundle: false,
            allow_other: false,
            create_mountpoint,
            metadata_only,
            data_cache_size_mib: 64.0_f32,
            internal_password: None,
        }
    }

    fn spawn_mount(ctx: &TestContext, args: cmd_mount::CmdArgs) -> MountThreadGuard {
        let mountpoint = args.mountpoint.clone();
        let global_clone = ctx.global.clone();
        let mount_thread = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async {
                if let Err(e) = commands::cmd_mount::run(&global_clone, &args).await {
                    eprintln!("Background mount task failed: {}", e);
                }
            });
        });
        MountThreadGuard {
            mountpoint,
            mount_thread: Some(mount_thread),
        }
    }

    async fn wait_for_path(path: &Path) -> Result<()> {
        let wait_start = Instant::now();
        while !mapache::fs::path_exists(path).await {
            tokio::time::sleep(Duration::from_millis(250)).await;
            if wait_start.elapsed() > Duration::from_secs(5) {
                bail!("Waiting for {} timed out", path.display());
            }
        }
        Ok(())
    }

    async fn wait_for_latest(mountpoint: &Path) -> Result<PathBuf> {
        let snapshot_path = mountpoint.join("snapshots/ids/latest");
        wait_for_path(&snapshot_path).await?;
        Ok(snapshot_path)
    }

    async fn start_mount(
        ctx: &TestContext,
        args: cmd_mount::CmdArgs,
    ) -> Result<(MountThreadGuard, PathBuf)> {
        let guard = spawn_mount(ctx, args);
        let snapshot_path = wait_for_latest(&guard.mountpoint).await?;
        Ok((guard, snapshot_path))
    }

    fn count_snapshot_entries(mountpoint: &Path, subdir: &str) -> Result<usize> {
        Ok(mountpoint
            .join("snapshots")
            .join(subdir)
            .read_dir()?
            .flatten()
            .filter(|entry| entry.file_name().to_string_lossy() != "latest")
            .count())
    }

    fn assert_access_denied<T>(result: std::io::Result<T>, operation: &str) {
        let err = match result {
            Ok(_) => panic!("{operation} unexpectedly succeeded"),
            Err(err) => err,
        };
        let expected = matches!(err.kind(), std::io::ErrorKind::PermissionDenied)
            || matches!(
                err.raw_os_error(),
                Some(code) if code == libc::EACCES || code == libc::EROFS || code == libc::EPERM
            );
        assert!(
            expected,
            "{operation} failed with an unexpected error: {err}"
        );
    }

    async fn assert_unmounted(mountpoint: &Path) -> Result<()> {
        for _ in 0..20 {
            if std::fs::read_dir(mountpoint)?.next().is_none() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let entries: Vec<_> = std::fs::read_dir(mountpoint)?
            .flatten()
            .map(|entry| entry.file_name())
            .collect();
        bail!("mountpoint is still mounted or not empty: {entries:?}");
    }

    #[tokio::test]
    async fn test_mount_basic() -> Result<()> {
        inner_test_mount(false).await
    }

    #[tokio::test]
    async fn test_mount_auto_create() -> Result<()> {
        inner_test_mount(true).await
    }

    async fn inner_test_mount(auto_mount: bool) -> Result<()> {
        let _lock = MOUNT_TEST_LOCK.lock().await;
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        // Init repo
        ctx.init_repo().await?;

        // Run snapshot
        let args = snapshot_args(snapshot_paths(&backup_data_tmp_path));
        cmd_snapshot::run(&ctx.global, &args, None)
            .await
            .context("Failed to run cmd_snapshot")?;

        let mountpoint = ctx._tmp_dir.path().join("mount");
        if !auto_mount {
            std::fs::create_dir_all(&mountpoint)?; // Create the mountpoint
        }

        let args = mount_args(mountpoint.clone(), false, auto_mount);
        let _guard = spawn_mount(&ctx, args);
        let snapshot_path = wait_for_latest(&mountpoint).await?;

        assert_eq!(count_snapshot_entries(&mountpoint, "ids")?, 1);

        let paths = vec![
            PathBuf::from("0"),
            PathBuf::from("0/file0.txt"),
            PathBuf::from("0/00"),
            PathBuf::from("0/00/file00.txt"),
            PathBuf::from("0/01"),
            PathBuf::from("0/l01"),
            PathBuf::from("0/01/file01a.txt"),
            PathBuf::from("0/01/file01b.txt"),
            PathBuf::from("1"),
            PathBuf::from("1/10"),
            PathBuf::from("1/10/file10.txt"),
            PathBuf::from("1/10/lfile10.txt"),
            PathBuf::from("2"),
            PathBuf::from("file.txt"),
            PathBuf::from("🚀"),
            PathBuf::from("🚀/mañana.txt"),
            PathBuf::from("日本語"),
            PathBuf::from("日本語/こんにちは.txt"),
        ];

        verify_paths(&paths, &backup_data_tmp_path, &snapshot_path).await
    }

    #[tokio::test]
    async fn test_mount_multiple_snapshots() -> Result<()> {
        let _lock = MOUNT_TEST_LOCK.lock().await;
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        // Init repo
        ctx.init_repo().await?;

        // Run snapshots
        let args = snapshot_args(snapshot_paths(&backup_data_tmp_path));
        cmd_snapshot::run(&ctx.global, &args, None)
            .await
            .context("Failed to run cmd_snapshot 1")?;

        // Avoid timestamps within one second
        std::thread::sleep(Duration::from_millis(1500));

        let args = snapshot_args(snapshot_paths(&backup_data_tmp_path));
        cmd_snapshot::run(&ctx.global, &args, None)
            .await
            .context("Failed to run cmd_snapshot 2")?;

        let mountpoint = ctx._tmp_dir.path().join("mount");
        let args = mount_args(mountpoint.clone(), false, true);
        let _guard = spawn_mount(&ctx, args);
        wait_for_latest(&mountpoint).await?;

        assert_eq!(count_snapshot_entries(&mountpoint, "ids")?, 2);
        assert_eq!(count_snapshot_entries(&mountpoint, "by_date")?, 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_mount_unmount() -> Result<()> {
        let _lock = MOUNT_TEST_LOCK.lock().await;
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        ctx.setup_backup_data(&synthetic)?;

        // Init repo
        ctx.init_repo().await?;

        let mountpoint = ctx._tmp_dir.path().join("mount");
        std::fs::create_dir_all(&mountpoint)?;

        let args = mount_args(mountpoint.clone(), false, false);
        let _guard = spawn_mount(&ctx, args);

        // With no snapshots there is no `latest` symlink, so wait for `snapshots/` itself.
        let snapshots_path = mountpoint.join("snapshots");
        wait_for_path(&snapshots_path).await?;
        assert!(
            snapshots_path.is_dir(),
            "snapshots directory is missing before unmount"
        );

        mapache::commands::cmd_mount::MapacheFS::unmount(&mountpoint)?;
        assert_unmounted(&mountpoint).await?;
        assert!(
            !snapshots_path.exists(),
            "FUSE contents are still visible after unmount"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_mount_rejects_writes() -> Result<()> {
        let _lock = MOUNT_TEST_LOCK.lock().await;
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        ctx.init_repo().await?;
        let args = snapshot_args(snapshot_paths(&backup_data_tmp_path));
        cmd_snapshot::run(&ctx.global, &args, None).await?;

        let mountpoint = ctx._tmp_dir.path().join("mount");
        let args = mount_args(mountpoint.clone(), false, true);
        let (_guard, snapshot_path) = start_mount(&ctx, args).await?;

        // The mount must still be readable.
        verify_paths(
            &[PathBuf::from("file.txt")],
            &backup_data_tmp_path,
            &snapshot_path,
        )
        .await?;

        let existing_file = snapshot_path.join("file.txt");
        assert_access_denied(
            std::fs::OpenOptions::new().write(true).open(&existing_file),
            "writing to an existing file",
        );
        assert_access_denied(
            std::fs::write(snapshot_path.join("new-file.txt"), b"nope"),
            "creating a file",
        );
        assert_access_denied(
            std::fs::create_dir(snapshot_path.join("new-dir")),
            "creating a directory",
        );
        assert_access_denied(std::fs::remove_file(&existing_file), "removing a file");
        assert_access_denied(
            std::fs::rename(&existing_file, snapshot_path.join("renamed.txt")),
            "renaming a file",
        );

        // Failed writes must not have changed what is visible.
        verify_paths(
            &[PathBuf::from("file.txt")],
            &backup_data_tmp_path,
            &snapshot_path,
        )
        .await?;
        assert!(!snapshot_path.join("new-file.txt").exists());
        assert!(!snapshot_path.join("new-dir").exists());

        Ok(())
    }

    #[tokio::test]
    async fn test_mount_metadata_only_denies_file_contents() -> Result<()> {
        let _lock = MOUNT_TEST_LOCK.lock().await;
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        ctx.init_repo().await?;
        let args = snapshot_args(snapshot_paths(&backup_data_tmp_path));
        cmd_snapshot::run(&ctx.global, &args, None).await?;

        let mountpoint = ctx._tmp_dir.path().join("mount");
        let args = mount_args(mountpoint.clone(), true, true);
        let (_guard, snapshot_path) = start_mount(&ctx, args).await?;

        // Metadata stays browsable, including lazy directory loading.
        let entries: Vec<_> = std::fs::read_dir(&snapshot_path)?
            .flatten()
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .collect();
        assert!(entries.iter().any(|name| name == "file.txt"));
        assert!(entries.iter().any(|name| name == "0"));
        assert!(std::fs::read_dir(snapshot_path.join("0"))?.next().is_some());

        // Symlinks are metadata too; their targets must still resolve.
        let link = snapshot_path.join("0/l01");
        assert_eq!(std::fs::read_link(&link)?, PathBuf::from("01"));
        assert!(std::fs::symlink_metadata(&link)?.file_type().is_symlink());

        // Regular file contents must be unavailable in metadata-only mode.
        assert_access_denied(
            std::fs::File::open(snapshot_path.join("file.txt")),
            "opening a file in metadata-only mode",
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_mount_large_file_random_access() -> Result<()> {
        let _lock = MOUNT_TEST_LOCK.lock().await;
        let mut ctx = TestContext::new().await?;
        let size = 2 * 1024 * 1024u64;
        let dataset = Dataset::new().add("big.bin", Entry::Random(size, 20_261_120));
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        ctx.init_repo().await?;
        let args = snapshot_args(vec![backup_data_tmp_path.join("big.bin")]);
        cmd_snapshot::run(&ctx.global, &args, None).await?;

        let mountpoint = ctx._tmp_dir.path().join("mount");
        let args = mount_args(mountpoint.clone(), false, true);
        let (_guard, snapshot_path) = start_mount(&ctx, args).await?;

        verify_paths(
            &[PathBuf::from("big.bin")],
            &backup_data_tmp_path,
            &snapshot_path,
        )
        .await?;

        let mut source = std::fs::File::open(backup_data_tmp_path.join("big.bin"))?;
        let mut mounted = std::fs::File::open(snapshot_path.join("big.bin"))?;
        for (offset, len) in [
            (0, 4 * 1024),
            (512 * 1024 - 100, 5 * 1024),
            (1024 * 1024 + 123, 64 * 1024),
            (size - 1024, 1024),
        ] {
            let mut expected = vec![0u8; len];
            let mut actual = vec![0u8; len];
            source.seek(SeekFrom::Start(offset))?;
            mounted.seek(SeekFrom::Start(offset))?;
            source.read_exact(&mut expected)?;
            mounted.read_exact(&mut actual)?;
            assert_eq!(actual, expected, "mismatch when reading at offset {offset}");
        }

        Ok(())
    }
}
