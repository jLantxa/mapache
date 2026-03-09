#![cfg(test)]

mod tests {
    use std::{
        path::{Path, PathBuf},
        time::Duration,
    };

    use anyhow::{Context, Result, bail};

    use mapache::commands::{self, UseSnapshot, cmd_mount, cmd_snapshot};

    use rstest::rstest;

    use crate::integration_tests::TestContext;

    /// Unmounts the filesystem from `mountpoint`
    pub fn unmount(mountpoint: &Path) -> Result<()> {
        let output = std::process::Command::new("fusermount")
            .arg("-u")
            .arg(mountpoint)
            .output()
            .with_context(|| {
                format!("Failed to execute fusermount for {}", mountpoint.display())
            })?;

        if !output.status.success() {
            eprintln!(
                "fusermount stderr: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            eprintln!(
                "fusermount stdout: {}",
                String::from_utf8_lossy(&output.stdout)
            );
            bail!("Unmount command failed with status: {}", output.status);
        }

        Ok(())
    }

    /// A RAII guard to ensure the mounted filesystem is unmounted and the mount thread is joined
    /// when it goes out of scope, even on panic or early return.
    struct MountThreadGuard {
        mountpoint: PathBuf,
        mount_thread: Option<std::thread::JoinHandle<()>>,
    }

    impl Drop for MountThreadGuard {
        fn drop(&mut self) {
            let _ = unmount(&self.mountpoint);
            if let Some(handle) = self.mount_thread.take() {
                let _ = handle.join();
            }
        }
    }

    async fn verify_paths(
        paths: &[PathBuf],
        backup_data_tmp_path: &Path,
        snapshot_path: &Path,
    ) -> Result<()> {
        for path in paths {
            use mapache::fs;

            let backup_path = backup_data_tmp_path.join(path);
            let mounted_path = snapshot_path.join(path);

            assert!(
                fs::path_exists(&mounted_path).await,
                "{:?} does not exist",
                mounted_path
            );

            let restored_meta = mounted_path.symlink_metadata()?;
            let backup_meta = backup_path.symlink_metadata()?;

            if !mounted_path.is_dir() && !mounted_path.is_symlink() {
                // Directories and symlinks always report a size of O bytes
                assert_eq!(restored_meta.len(), backup_meta.len());
            }

            // Compare file contents
            if mounted_path.is_file() {
                let mounted_data = std::fs::read(&mounted_path)?;
                let orig_data = std::fs::read(&backup_path)?;
                assert_eq!(mounted_data, orig_data);
            }

            assert_eq!(restored_meta.modified()?, backup_meta.modified()?);
        }

        Ok(())
    }

    #[tokio::test]
    #[rstest]
    #[case(false)] // The mountpoint exists before mounting
    #[case(true)] // The mountpoint is created by the command
    async fn test_mount_one_snapshot(#[case] auto_mount: bool) -> Result<()> {
        use std::{path::PathBuf, time::Instant};

        use mapache::fs;

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

        let mountpoint = ctx._tmp_dir.path().join("mount");
        if !auto_mount {
            std::fs::create_dir_all(&mountpoint)?; // Create the mountpoint
        }

        let mount_args = cmd_mount::CmdArgs {
            mountpoint: mountpoint.clone(),
            allow_other: false,
            create_mountpoint: auto_mount,
            metadata_only: false,
            data_cache_size_mib: 64.0_f32,
        };

        let global_clone = ctx.global.clone();
        let mount_thread = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async {
                if let Err(e) = commands::cmd_mount::run(&global_clone, &mount_args).await {
                    eprintln!("Background mount task failed: {}", e);
                }
            });
        });
        let _guard = MountThreadGuard {
            mountpoint: mountpoint.clone(),
            mount_thread: Some(mount_thread),
        };

        // Wait until the latest snapshot is ready
        let snapshot_path = mountpoint.join("snapshots/ids/latest");
        let wait_start = Instant::now();
        let max_wait = Duration::from_secs(5);
        while !fs::path_exists(&snapshot_path).await {
            tokio::time::sleep(Duration::from_millis(250)).await;

            if wait_start.elapsed() > max_wait {
                bail!("Waiting for FS to mount timeout ({:?})", max_wait);
            }
        }

        // Verify snapshot id directories
        let num_snapshot_dirs = mountpoint
            .join("snapshots/ids")
            .read_dir()?
            .flatten()
            .map(|entry| entry.path())
            .filter(|path| {
                path.file_name()
                    .map(|name| name.ne("latest"))
                    .unwrap_or(false)
            })
            .count();
        assert_eq!(num_snapshot_dirs, 1);

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
        ];

        verify_paths(&paths, backup_data_tmp_path, &snapshot_path).await
    }

    #[tokio::test]
    async fn test_mount_multiple_snapshots() -> Result<()> {
        use std::{path::PathBuf, time::Instant};

        use mapache::fs;

        let mut ctx = TestContext::new().await?;
        ctx.setup_backup_data()?;
        let backup_data_tmp_path = ctx.backup_data_path.as_ref().unwrap();

        // Init repo
        ctx.init_repo().await?;

        // Run snapshots
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
            .context("Failed to run cmd_snapshot 1")?;

        // Avoid timestamps within one second
        std::thread::sleep(Duration::from_millis(1500));

        let snapshot_args = cmd_snapshot::CmdArgs {
            paths: vec![
                backup_data_tmp_path.join("0"),
                backup_data_tmp_path.join("1"),
                backup_data_tmp_path.join("2"),
                // file.txt not included in this snapshot
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
            .context("Failed to run cmd_snapshot 2")?;

        let mountpoint = ctx._tmp_dir.path().join("mount");

        let mount_args = cmd_mount::CmdArgs {
            mountpoint: mountpoint.clone(),
            allow_other: false,
            create_mountpoint: true,
            metadata_only: false,
            data_cache_size_mib: 64.0_f32,
        };

        let global_clone = ctx.global.clone();
        let mount_thread = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async {
                if let Err(e) = commands::cmd_mount::run(&global_clone, &mount_args).await {
                    eprintln!("Background mount task failed: {}", e);
                }
            });
        });
        let _guard = MountThreadGuard {
            mountpoint: mountpoint.clone(),
            mount_thread: Some(mount_thread),
        };

        // Wait until the latest snapshot is ready
        let latest_snapshot_path = mountpoint.join("snapshots/by_date/latest");
        let wait_start = Instant::now();
        let max_wait = Duration::from_secs(5);
        while !fs::path_exists(&latest_snapshot_path).await {
            tokio::time::sleep(Duration::from_millis(250)).await;

            if wait_start.elapsed() > max_wait {
                bail!("Waiting for FS to mount timeout ({:?})", max_wait);
            }
        }

        let num_snapshots = ctx
            .repo_path
            .join("snapshots")
            .read_dir()?
            .flatten()
            .count();
        assert_eq!(num_snapshots, 2, "There should be two snapshots");

        // There are two snapshots, but we don't know the IDs. We access them
        // chronologically though the 'by_date' symlink.
        let mut snapshot_paths: Vec<PathBuf> = mountpoint
            .join("snapshots/by_date")
            .read_dir()?
            .flatten()
            .map(|entry| entry.path())
            .filter(|path| {
                path.file_name()
                    .map(|name| name.ne("latest"))
                    .unwrap_or(false)
            })
            .collect();

        snapshot_paths.sort_unstable(); // Sort alphabetically -> sort chronologically
        assert_eq!(
            snapshot_paths.len(),
            2,
            "There should be two by_date snapshot dirs"
        ); // Verify number of snapshot directories

        let paths1 = vec![
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
        ];

        let paths2 = vec![
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
            // file.txt not expected
        ];

        verify_paths(
            &paths1,
            backup_data_tmp_path,
            snapshot_paths.first().expect("Snapshot should exist"),
        )
        .await?;
        verify_paths(
            &paths2,
            backup_data_tmp_path,
            snapshot_paths.get(1).expect("Snapshot should exist"),
        )
        .await?;

        Ok(())
    }
}
