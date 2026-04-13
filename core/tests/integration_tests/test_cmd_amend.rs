#![cfg(test)]

mod tests {
    use std::{collections::BTreeSet, path::PathBuf, sync::Arc};

    use anyhow::{Context, Result};
    use mapache::{
        backend::localfs::LocalFS,
        commands::{self, UseSnapshot, cmd_amend, cmd_restore, cmd_snapshot},
        mapache::defaults::TEST_REPO_CONFIG,
        repository::{repo::Repository, snapshot::SnapshotStream},
        restorer::Strategy,
    };

    use crate::integration_tests::{TestContext, assert_times_equal};

    #[tokio::test]
    async fn test_amend_exclude() -> Result<()> {
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

        let excluded_paths = vec![
            "2".to_string(),
            "file.txt".to_string(),
            "0/00/file00.txt".to_string(),
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
        commands::cmd_amend::run(&ctx.global, &amend_args)
            .await
            .context("Failed to run cmd_amend")?;

        // Run restore
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
        commands::cmd_restore::run(&ctx.global, &restore_args)
            .await
            .context("Failed to run cmd_restore")?;

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
            #[cfg(not(target_os = "windows"))]
            PathBuf::from("1/10/lfile10.txt"),
        ];

        for path in &paths {
            let backup_path = backup_data_tmp_path.join(path);
            let restored_path = restore_path.join(path);
            assert!(restored_path.exists());

            let restored_meta = restored_path.symlink_metadata()?;
            let backup_meta = backup_path.symlink_metadata()?;

            if !restored_path.is_symlink() {
                assert_times_equal(restored_meta.modified()?, backup_meta.modified()?);
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

    #[tokio::test]
    async fn test_amend_tags_and_description() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        ctx.setup_backup_data()?;
        let backup_data_tmp_path = ctx.backup_data_path.as_ref().unwrap();

        let backend = Arc::new(LocalFS::new(ctx.repo_path.clone()));

        // Init repo
        ctx.init_repo().await?;
        let (repo, _, mut test_repo_lock_handle) = Repository::try_open_with_lock(
            Some(&ctx.auth),
            None,
            backend,
            TEST_REPO_CONFIG,
            false,
            None,
        )
        .await?;
        test_repo_lock_handle.unlock().await;

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
            num_readers: 2,
            num_packers: 2,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&ctx.global, &snapshot_args)
            .await
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
        commands::cmd_amend::run(&ctx.global, &amend_args)
            .await
            .context("Failed to run cmd_amend (1/2)")?;

        let snapshot_stream = SnapshotStream::new(repo.clone()).await?;
        let (_, snapshot) = snapshot_stream
            .latest()
            .await?
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
        commands::cmd_amend::run(&ctx.global, &amend_args)
            .await
            .context("Failed to run cmd_amend (2/2)")?;

        let snapshot_stream = SnapshotStream::new(repo.clone()).await?;
        let (_, snapshot) = snapshot_stream
            .latest()
            .await?
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
