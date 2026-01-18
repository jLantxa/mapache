#![cfg(test)]

mod tests {
    use std::path::PathBuf;

    use anyhow::{Context, Result};
    use mapache::{
        commands::{self, Compression, GlobalArgs, UseSnapshot, cmd_restore, cmd_snapshot},
        fs,
        mapache::{defaults::DEFAULT_DEFAULT_PACK_SIZE_MIB, global::set_global_opts_with_args},
        repository::repo::{Auth, SNAPSHOTS_DIR},
        restorer::Strategy,
        utils,
    };

    use tempfile::tempdir;

    use crate::{
        TEST_QUIET,
        integration_tests::{BACKUP_DATA_PATH, init_repo},
        test_utils::{self},
    };

    #[test]
    fn test_snapshot() -> Result<()> {
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

        let repo = String::from("repo");
        let repo_path = tmp_path.join(&repo);

        let global = GlobalArgs {
            repo: repo_path.to_string_lossy().to_string(),
            auth_file: Some(auth_file_path),
            key: None,
            quiet: *TEST_QUIET,
            verbosity: Some(3),
            ssh_pubkey: None,
            ssh_privatekey: None,
            pack_size_mib: DEFAULT_DEFAULT_PACK_SIZE_MIB,
            no_cache: true,
            retry_lock_duration: None,
            compression_level: Compression::Fastest,
        };
        set_global_opts_with_args(&global);

        // Init repo
        init_repo(&auth, repo_path.clone())?;

        let snapshots_dir = repo_path.join(SNAPSHOTS_DIR);
        assert_eq!(utils::count_files(&snapshots_dir)?, 0);
        let index_dir = repo_path.join("index");
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
            read_concurrency: 2,
            write_concurrency: 2,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&global, &snapshot_args)
            .context("Failed to run cmd_snapshot")?;
        assert_eq!(utils::count_files(&snapshots_dir)?, 1);
        assert_ne!(utils::count_files(&index_dir)?, 0);

        // Run restore
        let restore_path = tmp_path.join("restore");
        let restore_args = cmd_restore::CmdArgs {
            target: restore_path.clone(),
            snapshot: UseSnapshot::Latest,
            dry_run: false,
            include: None,
            exclude: None,
            strip_prefix: false,
            strategy: Strategy::Skip,
            no_verify: false,
            quit_on_error: true,
            delete: false,
            no_preserve_root: false,
        };
        commands::cmd_restore::run(&global, &restore_args).context("Failed to run cmd_restore")?;

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

    #[test]
    fn test_snapshot_dry_run() -> Result<()> {
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

        let repo = String::from("repo");
        let repo_path = tmp_path.join(&repo);

        let global = GlobalArgs {
            repo: repo_path.to_string_lossy().to_string(),
            auth_file: Some(auth_file_path),
            key: None,
            quiet: *TEST_QUIET,
            verbosity: Some(3),
            ssh_pubkey: None,
            ssh_privatekey: None,
            pack_size_mib: DEFAULT_DEFAULT_PACK_SIZE_MIB,
            no_cache: true,
            retry_lock_duration: None,
            compression_level: Compression::Fastest,
        };
        set_global_opts_with_args(&global);

        // Init repo
        init_repo(&auth, repo_path.clone())?;

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
            read_concurrency: 2,
            write_concurrency: 2,
            dry_run: true,
        };
        commands::cmd_snapshot::run(&global, &snapshot_args)
            .context("Failed to run cmd_snapshot")?;

        // `snapshots` directory should be empty
        let snapshots_dir = repo_path.join("snapshots");
        assert_eq!(utils::count_files(&snapshots_dir)?, 0);

        // `index` directory should be empty
        let index_dir = repo_path.join("index");
        assert_eq!(utils::count_files(&index_dir)?, 0);

        // Run restore
        let restore_path = tmp_path.join("restore");
        let restore_args = cmd_restore::CmdArgs {
            target: restore_path.clone(),
            snapshot: UseSnapshot::Latest,
            dry_run: false,
            include: None,
            exclude: None,
            strip_prefix: false,
            strategy: Strategy::Skip,
            no_verify: false,
            quit_on_error: true,
            delete: false,
            no_preserve_root: false,
        };

        let restore_result = commands::cmd_restore::run(&global, &restore_args);
        assert!(restore_result.is_err());

        Ok(())
    }

    #[test]
    fn test_snapshot_with_exclude() -> Result<()> {
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

        let repo = String::from("repo");
        let repo_path = tmp_path.join(&repo);

        let global = GlobalArgs {
            repo: repo_path.to_string_lossy().to_string(),
            auth_file: Some(auth_file_path),
            key: None,
            quiet: *TEST_QUIET,
            verbosity: Some(3),
            ssh_pubkey: None,
            ssh_privatekey: None,
            pack_size_mib: DEFAULT_DEFAULT_PACK_SIZE_MIB,
            no_cache: true,
            retry_lock_duration: None,
            compression_level: Compression::Fastest,
        };
        set_global_opts_with_args(&global);

        // Init repo
        init_repo(&auth, repo_path.clone())?;

        // Run snapshot
        let snapshot_args = cmd_snapshot::CmdArgs {
            paths: vec![
                backup_data_tmp_path.join("0"),
                backup_data_tmp_path.join("1"),
                backup_data_tmp_path.join("2"),
                backup_data_tmp_path.join("file.txt"),
            ],
            as_root: false,
            exclude: Some(vec![backup_data_tmp_path.join("0/01")]),
            tags_str: String::new(),
            description: None,
            no_parent: false,
            skip_if_unchanged: false,
            no_scan: false,
            parent: UseSnapshot::Latest,
            read_concurrency: 2,
            write_concurrency: 2,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&global, &snapshot_args)
            .context("Failed to run cmd_snapshot")?;

        // Run restore
        let restore_path = tmp_path.join("restore");
        let restore_args = cmd_restore::CmdArgs {
            target: restore_path.clone(),
            snapshot: UseSnapshot::Latest,
            dry_run: false,
            include: None,
            exclude: None,
            strip_prefix: false,
            strategy: Strategy::Skip,
            no_verify: false,
            quit_on_error: true,
            delete: false,
            no_preserve_root: false,
        };
        commands::cmd_restore::run(&global, &restore_args).context("Failed to run cmd_restore")?;

        let paths = vec![
            PathBuf::from("0"),
            PathBuf::from("0/file0.txt"),
            PathBuf::from("0/00"),
            PathBuf::from("0/00/file00.txt"),
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

    #[test]
    fn test_snapshot_twice() -> Result<()> {
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

        let repo = String::from("repo");
        let repo_path = tmp_path.join(&repo);

        let global = GlobalArgs {
            repo: repo_path.to_string_lossy().to_string(),
            auth_file: Some(auth_file_path),
            key: None,
            quiet: *TEST_QUIET,
            verbosity: Some(3),
            ssh_pubkey: None,
            ssh_privatekey: None,
            pack_size_mib: DEFAULT_DEFAULT_PACK_SIZE_MIB,
            no_cache: true,
            retry_lock_duration: None,
            compression_level: Compression::Fastest,
        };
        set_global_opts_with_args(&global);

        // Init repo
        init_repo(&auth, repo_path.clone())?;

        let snapshots_dir = repo_path.join(SNAPSHOTS_DIR);
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
            exclude: Some(vec![backup_data_tmp_path.join("0/01")]),
            tags_str: String::new(),
            description: None,
            no_parent: false,
            skip_if_unchanged: false,
            no_scan: false,
            parent: UseSnapshot::Latest,
            read_concurrency: 2,
            write_concurrency: 2,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&global, &snapshot_args)
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
            read_concurrency: 2,
            write_concurrency: 2,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&global, &snapshot_args)
            .context("Failed to run cmd_snapshot 2")?;
        assert_eq!(utils::count_files(&snapshots_dir)?, 2);

        let restore_path = tmp_path.join("restore");
        let restore_args = cmd_restore::CmdArgs {
            target: restore_path.clone(),
            snapshot: UseSnapshot::Latest,
            dry_run: false,
            include: None,
            exclude: None,
            strip_prefix: false,
            strategy: Strategy::Skip,
            no_verify: false,
            quit_on_error: true,
            delete: false,
            no_preserve_root: false,
        };
        commands::cmd_restore::run(&global, &restore_args).context("Failed to run cmd_restore")?;

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
            assert_eq!(restored_meta.modified()?, backup_meta.modified()?);

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

    #[test]
    fn test_snapshot_folder_as_root() -> Result<()> {
        // Save a snapshot with a single snapshot --as-root
        // The single folder is taken as the snapshot root, with all its contents
        // being in the root folder, instead of adding the folder as a child of
        // the snapshot root.

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

        let repo = String::from("repo");
        let repo_path = tmp_path.join(&repo);

        let global = GlobalArgs {
            repo: repo_path.to_string_lossy().to_string(),
            auth_file: Some(auth_file_path),
            key: None,
            quiet: *TEST_QUIET,
            verbosity: Some(3),
            ssh_pubkey: None,
            ssh_privatekey: None,
            pack_size_mib: DEFAULT_DEFAULT_PACK_SIZE_MIB,
            no_cache: true,
            retry_lock_duration: None,
            compression_level: Compression::Fastest,
        };
        set_global_opts_with_args(&global);

        // Init repo
        init_repo(&auth, repo_path.clone())?;

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
            read_concurrency: 2,
            write_concurrency: 2,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&global, &snapshot_args)
            .context("Failed to run cmd_snapshot")?;

        // Run restore
        let restore_path = tmp_path.join("restore");
        let restore_args = cmd_restore::CmdArgs {
            target: restore_path.clone(),
            snapshot: UseSnapshot::Latest,
            dry_run: false,
            include: None,
            exclude: None,
            strip_prefix: false,
            strategy: Strategy::Skip,
            no_verify: false,
            quit_on_error: true,
            delete: false,
            no_preserve_root: false,
        };
        commands::cmd_restore::run(&global, &restore_args).context("Failed to run cmd_restore")?;

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
            assert!(fs::path_exists(&restored_path));

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

    #[test]
    fn test_snapshot_intermediate_paths() -> Result<()> {
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

        let repo = String::from("repo");
        let repo_path = tmp_path.join(&repo);

        let global = GlobalArgs {
            repo: repo_path.to_string_lossy().to_string(),
            auth_file: Some(auth_file_path),
            key: None,
            quiet: *TEST_QUIET,
            verbosity: Some(3),
            ssh_pubkey: None,
            ssh_privatekey: None,
            pack_size_mib: DEFAULT_DEFAULT_PACK_SIZE_MIB,
            no_cache: true,
            retry_lock_duration: None,
            compression_level: Compression::Fastest,
        };
        set_global_opts_with_args(&global);

        // Init repo
        init_repo(&auth, repo_path.clone())?;

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
            read_concurrency: 2,
            write_concurrency: 2,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&global, &snapshot_args)
            .context("Failed to run cmd_snapshot")?;

        // Run restore
        let restore_path = tmp_path.join("restore");
        let restore_args = cmd_restore::CmdArgs {
            target: restore_path.clone(),
            snapshot: UseSnapshot::Latest,
            dry_run: false,
            include: None,
            exclude: None,
            strip_prefix: false,
            strategy: Strategy::Skip,
            no_verify: false,
            quit_on_error: true,
            delete: false,
            no_preserve_root: false,
        };
        commands::cmd_restore::run(&global, &restore_args).context("Failed to run cmd_restore")?;

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

    #[test]
    fn test_snapshot_skip_if_unchanged() -> Result<()> {
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

        let repo = String::from("repo");
        let repo_path = tmp_path.join(&repo);

        let global = GlobalArgs {
            repo: repo_path.to_string_lossy().to_string(),
            auth_file: Some(auth_file_path),
            key: None,
            quiet: *TEST_QUIET,
            verbosity: Some(3),
            ssh_pubkey: None,
            ssh_privatekey: None,
            pack_size_mib: DEFAULT_DEFAULT_PACK_SIZE_MIB,
            no_cache: true,
            retry_lock_duration: None,
            compression_level: Compression::Fastest,
        };
        set_global_opts_with_args(&global);

        // Init repo
        init_repo(&auth, repo_path.clone())?;

        let snapshots_dir = repo_path.join(SNAPSHOTS_DIR);
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
            read_concurrency: 2,
            write_concurrency: 2,
            dry_run: false,
        };

        commands::cmd_snapshot::run(&global, &snapshot_args)
            .context("Failed to run cmd_snapshot 1")?;
        assert_eq!(utils::count_files(&snapshots_dir)?, 1);

        commands::cmd_snapshot::run(&global, &snapshot_args)
            .context("Failed to run cmd_snapshot 2")?;
        assert_eq!(utils::count_files(&snapshots_dir)?, 1);

        Ok(())
    }
}
