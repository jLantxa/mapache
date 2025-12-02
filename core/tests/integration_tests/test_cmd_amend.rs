#![cfg(test)]

mod tests {
    use std::{collections::BTreeSet, path::PathBuf, sync::Arc};

    use anyhow::{Context, Result};
    use mapache::{
        backend::localfs::LocalFS,
        commands::{self, GlobalArgs, UseSnapshot, cmd_amend, cmd_restore, cmd_snapshot},
        mapache::{
            defaults::{DEFAULT_DEFAULT_PACK_SIZE_MIB, TEST_REPO_CONFIG},
            global::set_global_opts_with_args,
        },
        repository::{
            repo::{Auth, Repository},
            snapshot::SnapshotStream,
        },
        restorer::Strategy,
    };

    use tempfile::tempdir;

    use crate::{
        TEST_QUIET,
        integration_tests::{BACKUP_DATA_PATH, init_repo},
        test_utils::{self},
    };

    #[test]
    fn test_amend_exclude() -> Result<()> {
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
            no_scan: true,
            parent: UseSnapshot::Latest,
            read_concurrency: 2,
            write_concurrency: 2,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&global, &snapshot_args)
            .context("Failed to run cmd_snapshot")?;

        let excluded_paths = vec![
            PathBuf::from("2"),
            PathBuf::from("file.txt"),
            PathBuf::from("0/00/file00.txt"),
        ];
        let amend_args = cmd_amend::CmdArgs {
            snapshot: UseSnapshot::Latest,
            all: false,
            keep_old: false,
            tags_str: None,
            clear_tags: false,
            description: None,
            clear_description: false,
            exclude: Some(excluded_paths.clone()),
        };
        commands::cmd_amend::run(&global, &amend_args).context("Failed to run cmd_amend")?;

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
        commands::cmd_restore::run(&global, &restore_args).context("Failed to run cmd_restore")?;

        let paths = vec![
            PathBuf::from("0"),
            PathBuf::from("0/file0.txt"),
            PathBuf::from("0/00"),
            PathBuf::from("0/01"),
            PathBuf::from("0/01/file01a.txt"),
            PathBuf::from("0/01/file01b.txt"),
            PathBuf::from("1"),
            PathBuf::from("1/10"),
            PathBuf::from("1/10/file10.txt"),
            PathBuf::from("1/10/lfile10.txt"),
        ];

        for path in &paths {
            let backup_path = backup_data_tmp_path.join(path);
            let restored_path = restore_path.join(path);
            assert!(restored_path.exists());

            let restored_meta = restored_path.symlink_metadata()?;
            let backup_meta = backup_path.symlink_metadata()?;

            if !restored_path.is_symlink() {
                assert_eq!(restored_meta.modified()?, backup_meta.modified()?);
            }

            // We excluded some paths, so the size of directories will not be consistent
            if !restore_path.is_dir() {
                assert_eq!(restored_meta.len(), backup_meta.len());
            }

            if restored_path.is_file() {
                assert_eq!(std::fs::read(&restored_path)?, std::fs::read(&backup_path)?);
            }
        }

        for path in excluded_paths {
            let restored_path = restore_path.join(path);
            assert!(!restored_path.exists());
        }

        Ok(())
    }

    #[test]
    fn test_amend_tags_and_description() -> Result<()> {
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

        let repo_path = tmp_path.join(String::from("repo"));
        let backend = Arc::new(LocalFS::new(repo_path.clone()));

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
        };
        set_global_opts_with_args(&global);

        // Init repo
        init_repo(&auth, repo_path.clone())?;
        let (repo, _, test_repo_lock_handle) =
            Repository::try_open_with_lock(Some(&auth), None, backend, TEST_REPO_CONFIG, false)?;
        drop(test_repo_lock_handle);

        // Run snapshot twice
        let snapshot_args = cmd_snapshot::CmdArgs {
            paths: vec![backup_data_tmp_path.join("0")],
            as_root: false,
            exclude: None,
            tags_str: "tag0,tag1".to_string(),
            description: Some(String::from("This snapshot will be amended")),
            no_parent: false,
            skip_if_unchanged: false,
            no_scan: true,
            parent: UseSnapshot::Latest,
            read_concurrency: 2,
            write_concurrency: 2,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&global, &snapshot_args)
            .context("Failed to run cmd_snapshot")?;

        let amend_args = cmd_amend::CmdArgs {
            snapshot: UseSnapshot::Latest,
            all: false,
            keep_old: false,
            tags_str: None,
            clear_tags: true,
            description: None,
            clear_description: true,
            exclude: None,
        };
        commands::cmd_amend::run(&global, &amend_args).context("Failed to run cmd_amend (1/2)")?;

        let mut snapshot_stream = SnapshotStream::new(repo.clone())?;
        let (_, snapshot) = snapshot_stream
            .latest()
            .expect("There should be at least one snapshot");

        assert!(snapshot.tags.is_empty());
        assert!(snapshot.description.is_none());

        let amend_args = cmd_amend::CmdArgs {
            snapshot: UseSnapshot::Latest,
            all: false,
            keep_old: false,
            tags_str: Some("new_tag".to_string()),
            clear_tags: false,
            description: Some(String::from("This description is new")),
            clear_description: false,
            exclude: None,
        };
        commands::cmd_amend::run(&global, &amend_args).context("Failed to run cmd_amend (2/2)")?;

        let mut snapshot_stream = SnapshotStream::new(repo.clone())?;
        let (_, snapshot) = snapshot_stream
            .latest()
            .expect("There should be at least one snapshot");

        let expected_tags: BTreeSet<String> =
            ["new_tag"].into_iter().map(|s| s.to_string()).collect();

        assert_eq!(snapshot.tags, expected_tags);
        assert_eq!(
            snapshot
                .description
                .expect("The description should not be None"),
            "This description is new"
        );

        Ok(())
    }
}
