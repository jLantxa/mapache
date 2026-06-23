#![cfg(test)]

mod tests {
    use std::{collections::BTreeSet, path::PathBuf, sync::Arc};

    use anyhow::Result;
    use mapache::{
        backend::localfs::LocalFS,
        common::defaults::TEST_REPO_CONFIG,
        repository::{repo::Repository, snapshot::SnapshotStream},
    };

    use crate::{
        integration_tests::{INTEGRATION_TEST_DATA, TestContext, assert_times_equal},
        synthetic::{Dataset, SyntheticData},
    };

    #[tokio::test]
    async fn test_amend_exclude() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        // Init repo
        ctx.init_repo().await?;

        // Run snapshot
        ctx.snapshot(vec![
            backup_data_tmp_path.join("0"),
            backup_data_tmp_path.join("1"),
            backup_data_tmp_path.join("2"),
            backup_data_tmp_path.join("file.txt"),
        ])
        .await?;

        let excluded_paths = vec![
            "2".to_string(),
            "file.txt".to_string(),
            "0/00/file00.txt".to_string(),
        ];

        ctx.amend_builder()
            .exclude(excluded_paths.clone())
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
            if !restored_path.is_dir() {
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
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        let backend = Arc::new(LocalFS::new(ctx.repo_path.clone()));

        // Init repo
        ctx.init_repo().await?;
        let (repo, _, test_repo_lock_handle) =
            Repository::try_open_with_lock(&ctx.auth, None, backend, TEST_REPO_CONFIG, false, None)
                .await?;
        test_repo_lock_handle.unlock().await;

        // Run snapshot with tags and description
        ctx.snapshot_builder(vec![backup_data_tmp_path.join("0")])
            .tags("tag0,tag1".to_string())
            .description("This snapshot will be amended".to_string())
            .run(&ctx.global)
            .await?;

        // Clear tags and description
        ctx.amend_builder()
            .clear_tags(true)
            .clear_description(true)
            .run(&ctx.global)
            .await?;

        let snapshot_stream = SnapshotStream::new(repo.clone()).await?;
        let (_, snapshot) = snapshot_stream
            .latest()
            .await?
            .expect("There should be at least one snapshot");

        assert!(snapshot.tags.is_empty());
        assert!(snapshot.description.is_none());

        // Set new tags and description
        ctx.amend_builder()
            .tags("new_tag".to_string())
            .description("This description is new".to_string())
            .run(&ctx.global)
            .await?;

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
                .as_ref()
                .expect("The description should not be None"),
            "This description is new"
        );

        Ok(())
    }
}
