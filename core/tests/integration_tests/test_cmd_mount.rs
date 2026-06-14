use std::{
    io::Read,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, bail};

use mapache::commands::{self, UseSnapshot, cmd_mount, cmd_snapshot};

use crate::{
    integration_tests::{INTEGRATION_TEST_DATA, TestContext},
    synthetic::{Dataset, SyntheticData},
};

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

#[tokio::test]
async fn test_mount_basic() -> Result<()> {
    inner_test_mount(false).await
}

#[tokio::test]
async fn test_mount_auto_create() -> Result<()> {
    inner_test_mount(true).await
}

async fn inner_test_mount(auto_mount: bool) -> Result<()> {
    use std::{path::PathBuf, time::Instant};

    use mapache::fs;

    let mut ctx = TestContext::new().await?;
    let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
    let synthetic = SyntheticData::new(dataset);
    let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

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
        as_root: Some(false),
        exclude: None,
        exclude_file: None,
        tags_str: Some(String::new()),
        description: None,
        no_parent: false,
        skip_if_unchanged: Some(false),
        no_scan: Some(true),
        parent: Some(UseSnapshot::Latest),
        num_readers: Some(2),
        num_packers: Some(2),
        dry_run: false,
        with_atime: None,
        stdin: false,
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
        bundle: false,
        allow_other: false,
        create_mountpoint: auto_mount,
        metadata_only: false,
        data_cache_size_mib: 64.0_f32,
        internal_password: None,
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

    verify_paths(&paths, &backup_data_tmp_path, &snapshot_path).await
}

#[tokio::test]
async fn test_mount_multiple_snapshots() -> Result<()> {
    use std::time::Instant;

    use mapache::fs;

    let mut ctx = TestContext::new().await?;
    let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
    let synthetic = SyntheticData::new(dataset);
    let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

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
        as_root: Some(false),
        exclude: None,
        exclude_file: None,
        tags_str: Some(String::new()),
        description: None,
        no_parent: false,
        skip_if_unchanged: Some(false),
        no_scan: Some(true),
        parent: Some(UseSnapshot::Latest),
        num_readers: Some(2),
        num_packers: Some(2),
        dry_run: false,
        with_atime: None,
        stdin: false,
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
            backup_data_tmp_path.join("file.txt"),
        ],
        as_root: Some(false),
        exclude: None,
        exclude_file: None,
        tags_str: Some(String::new()),
        description: None,
        no_parent: false,
        skip_if_unchanged: Some(false),
        no_scan: Some(true),
        parent: Some(UseSnapshot::Latest),
        num_readers: Some(2),
        num_packers: Some(2),
        dry_run: false,
        with_atime: None,
        stdin: false,
    };
    commands::cmd_snapshot::run(&ctx.global, &snapshot_args)
        .await
        .context("Failed to run cmd_snapshot 2")?;

    let mountpoint = ctx._tmp_dir.path().join("mount");

    let mount_args = cmd_mount::CmdArgs {
        mountpoint: mountpoint.clone(),
        bundle: false,
        allow_other: false,
        create_mountpoint: true,
        metadata_only: false,
        data_cache_size_mib: 64.0_f32,
        internal_password: None,
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

    // Verify snapshots/ids
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
    assert_eq!(num_snapshot_dirs, 2);

    // Verify snapshots/by_date
    let num_snapshot_date_dirs = mountpoint
        .join("snapshots/by_date")
        .read_dir()?
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .map(|name| name.ne("latest"))
                .unwrap_or(false)
        })
        .count();
    assert_eq!(num_snapshot_date_dirs, 2);

    Ok(())
}

#[tokio::test]
async fn test_mount_unmount() -> Result<()> {
    use std::time::Instant;

    use mapache::fs;

    let mut ctx = TestContext::new().await?;
    let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
    let synthetic = SyntheticData::new(dataset);
    ctx.setup_backup_data(&synthetic)?;

    // Init repo
    ctx.init_repo().await?;

    let mountpoint = ctx._tmp_dir.path().join("mount");
    std::fs::create_dir_all(&mountpoint)?;

    let mount_args = cmd_mount::CmdArgs {
        mountpoint: mountpoint.clone(),
        bundle: false,
        allow_other: false,
        create_mountpoint: false,
        metadata_only: false,
        data_cache_size_mib: 64.0_f32,
        internal_password: None,
    };

    let global_clone = ctx.global.clone();
    let mount_thread = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let _ = commands::cmd_mount::run(&global_clone, &mount_args).await;
        });
    });
    let _guard = MountThreadGuard {
        mountpoint: mountpoint.clone(),
        mount_thread: Some(mount_thread),
    };

    // Wait for the FUSE mount to be ready by checking for the snapshots directory
    let snapshots_path = mountpoint.join("snapshots");
    let wait_start = Instant::now();
    let max_wait = Duration::from_secs(5);
    while !fs::path_exists(&snapshots_path).await {
        tokio::time::sleep(Duration::from_millis(250)).await;

        if wait_start.elapsed() > max_wait {
            bail!("Waiting for FS to mount timeout ({:?})", max_wait);
        }
    }

    // Unmount
    mapache::commands::cmd_mount::MapacheFS::unmount(&mountpoint)?;

    Ok(())
}
