#![cfg(test)]

mod tests {
    use std::{path::PathBuf, sync::Arc};

    use anyhow::{Context, Result};
    use mapache::{
        backend::{self, localfs::LocalFS, read_backend_dir},
        commands::{self, GlobalArgs, UseSnapshot, cmd_clean, cmd_restore, cmd_snapshot},
        mapache::{defaults::DEFAULT_DEFAULT_PACK_SIZE_MIB, global::set_global_opts_with_args},
        repository::repo::Auth,
        restorer::Strategy,
    };

    use tempfile::tempdir;

    use crate::{
        integration_tests::{BACKUP_DATA_PATH, init_repo},
        test_utils::{self},
    };

    /// Just a very basic test to verify that GC does not break the repository.
    /// This is to check against some early bugs in the garbage collector that
    /// removed files that it shouldn't.
    /// It does no harm keeping this test.
    #[test]
    fn test_gc_sanity_check() -> Result<()> {
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
            quiet: true,
            verbosity: None,
            ssh_pubkey: None,
            ssh_privatekey: None,
            pack_size_mib: DEFAULT_DEFAULT_PACK_SIZE_MIB,
            no_cache: true,
        };
        set_global_opts_with_args(&global);

        // Init repo
        init_repo(&auth, repo_path.clone())?;

        // Run snapshot twice
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
            no_scan: true,
            parent: UseSnapshot::Latest,
            read_concurrency: 2,
            write_concurrency: 2,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&global, &snapshot_args)
            .with_context(|| "Failed to run cmd_snapshot (1/2)")?;

        let snapshot_args = cmd_snapshot::CmdArgs {
            paths: vec![
                backup_data_tmp_path.join("0"),
                backup_data_tmp_path.join("1"),
                backup_data_tmp_path.join("2"),
            ],
            as_root: false,
            exclude: None,
            tags_str: String::new(),
            description: None,
            no_parent: false,
            no_scan: true,
            parent: UseSnapshot::Latest,
            read_concurrency: 2,
            write_concurrency: 2,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&global, &snapshot_args)
            .with_context(|| "Failed to run cmd_snapshot (2/2)")?;

        // Keep the last snapshot
        let forget_args = commands::cmd_forget::CmdArgs {
            forget: Vec::new(),
            force: false,
            keep_last: Some(1),
            keep_within: None,
            keep_yearly: None,
            keep_monthly: None,
            keep_weekly: None,
            keep_daily: None,
            run_gc: false,
            dry_run: false,
            tolerance: 0.0_f32,
            tags_str: Some(String::new()),
            keep_tags_str: Some(String::new()),
            verify: true,
        };
        commands::cmd_forget::run(&global, &forget_args)
            .with_context(|| "Failed to run cmd_forget")?;

        let gc_args = cmd_clean::CmdArgs {
            tolerance: 0.0_f32,
            dry_run: false,
            verify: true,
        };
        commands::cmd_clean::run(&global, &gc_args).with_context(|| "Failed to run cmd_gc")?;

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
        };
        commands::cmd_restore::run(&global, &restore_args)
            .with_context(|| "Failed to run cmd_restore")?;

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
        }

        Ok(())
    }

    /// Run clean but dry-run. Nothing should change.
    #[test]
    fn test_clean_dry_run() -> Result<()> {
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
            quiet: true,
            verbosity: None,
            ssh_pubkey: None,
            ssh_privatekey: None,
            pack_size_mib: DEFAULT_DEFAULT_PACK_SIZE_MIB,
            no_cache: true,
        };
        set_global_opts_with_args(&global);

        // Init repo
        init_repo(&auth, repo_path.clone())?;

        // Run snapshot twice
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
            no_scan: true,
            parent: UseSnapshot::Latest,
            read_concurrency: 2,
            write_concurrency: 2,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&global, &snapshot_args)
            .with_context(|| "Failed to run cmd_snapshot (1/2)")?;

        let snapshot_args = cmd_snapshot::CmdArgs {
            paths: vec![
                backup_data_tmp_path.join("0"),
                backup_data_tmp_path.join("1"),
                backup_data_tmp_path.join("2"),
            ],
            as_root: false,
            exclude: None,
            tags_str: String::new(),
            description: None,
            no_parent: false,
            no_scan: true,
            parent: UseSnapshot::Latest,
            read_concurrency: 2,
            write_concurrency: 2,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&global, &snapshot_args)
            .with_context(|| "Failed to run cmd_snapshot (2/2)")?;

        // Keep the last snapshot
        let forget_args = commands::cmd_forget::CmdArgs {
            forget: Vec::new(),
            force: false,
            keep_last: Some(1),
            keep_within: None,
            keep_yearly: None,
            keep_monthly: None,
            keep_weekly: None,
            keep_daily: None,
            run_gc: false,
            dry_run: false,
            tolerance: 0.0_f32,
            tags_str: Some(String::new()),
            keep_tags_str: Some(String::new()),
            verify: true,
        };
        commands::cmd_forget::run(&global, &forget_args)
            .with_context(|| "Failed to run cmd_forget")?;

        // Run cmd_clean and compare the repositories (using backend readdir)
        let backend = Arc::new(LocalFS::new(repo_path.clone()));
        let pre_clean_nodes = backend::read_backend_dir(backend.as_ref(), &PathBuf::new())?;
        let gc_args = cmd_clean::CmdArgs {
            tolerance: 0.0_f32,
            dry_run: true, // DRY-RUN !
            verify: true,
        };
        commands::cmd_clean::run(&global, &gc_args).with_context(|| "Failed to run cmd_gc")?;
        let post_clean_nodes = read_backend_dir(backend.as_ref(), &PathBuf::new())?;
        assert_eq!(pre_clean_nodes, post_clean_nodes);

        // Now the same, but without dry-run, the repo changes
        let pre_clean_nodes = backend::read_backend_dir(backend.as_ref(), &PathBuf::new())?;
        let gc_args = cmd_clean::CmdArgs {
            tolerance: 0.0_f32,
            dry_run: false, // No dry-run
            verify: true,
        };
        commands::cmd_clean::run(&global, &gc_args).with_context(|| "Failed to run cmd_gc")?;
        let post_clean_nodes = read_backend_dir(backend.as_ref(), &PathBuf::new())?;
        assert_ne!(pre_clean_nodes, post_clean_nodes);

        Ok(())
    }
}
