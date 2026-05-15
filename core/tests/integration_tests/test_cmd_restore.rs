#![cfg(test)]

mod tests {
    use std::{
        path::PathBuf,
        time::{Duration, SystemTime},
    };

    use anyhow::Result;
    use filetime::{FileTime, set_file_times};
    use mapache::restorer::Strategy;

    use crate::integration_tests::{TestContext, assert_times_equal};

    #[tokio::test]
    async fn test_restore_with_filter() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        ctx.setup_backup_data()?;
        let backup_data_tmp_path = ctx.backup_data_path.clone().unwrap();

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
        ])
        .no_scan(true)
        .run(&ctx.global)
        .await?;

        // Run restore
        let restore_path = ctx._tmp_dir.path().join("restore");
        ctx.restore_builder(restore_path.clone())
            .include(vec!["0".to_string(), "1".to_string()])
            .exclude(vec!["0/00/file00.txt".to_string(), "0/*.txt".to_string()])
            .run(&ctx.global)
            .await?;

        let restored_paths = vec![
            PathBuf::from("0"),
            PathBuf::from("0/00"),
            PathBuf::from("1"),
            PathBuf::from("1/10"),
        ];

        let excluded_paths = vec![
            PathBuf::from("0/file0.txt"),
            PathBuf::from("0/00/file00.txt"),
            PathBuf::from("2"),
            PathBuf::from("file.txt"),
        ];

        for path in &excluded_paths {
            let not_restored_path = restore_path.join(path);
            assert!(!not_restored_path.exists());
        }

        for path in &restored_paths {
            let backup_path = backup_data_tmp_path.join(path);
            let restored_path = restore_path.join(path);
            assert!(restored_path.exists());

            let restored_meta = restored_path.symlink_metadata()?;
            let backup_meta = backup_path.symlink_metadata()?;

            assert_times_equal(restored_meta.modified()?, backup_meta.modified()?);

            if restored_path.is_file() {
                assert_eq!(std::fs::read(&restored_path)?, std::fs::read(&backup_path)?);
            }

            if !restore_path.is_dir() {
                // Excluded paths decrease the size of parent directories.
                // We only test the size of files in this case
                assert_eq!(restored_meta.len(), backup_meta.len());
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_restore_dry_run() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        ctx.setup_backup_data()?;
        let backup_data_tmp_path = ctx.backup_data_path.clone().unwrap();

        // Init repo
        ctx.init_repo().await?;

        // Run snapshot
        ctx.snapshot_builder(vec![
            backup_data_tmp_path.join("0"),
            backup_data_tmp_path.join("1"),
            backup_data_tmp_path.join("2"),
            backup_data_tmp_path.join("file.txt"),
        ])
        .no_scan(true)
        .run(&ctx.global)
        .await?;

        // Run restore
        let restore_path = ctx._tmp_dir.path().join("restore");
        ctx.restore_builder(restore_path.clone())
            .dry_run(true)
            .run(&ctx.global)
            .await?;

        assert!(!restore_path.exists());

        Ok(())
    }

    #[tokio::test]
    async fn test_restore_strip_prefix() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        ctx.setup_backup_data()?;
        let backup_data_tmp_path = ctx.backup_data_path.clone().unwrap();

        // Init repo
        ctx.init_repo().await?;

        // Run snapshot
        ctx.snapshot_builder(vec![
            backup_data_tmp_path.join("0"),
            backup_data_tmp_path.join("1"),
            backup_data_tmp_path.join("2"),
            backup_data_tmp_path.join("file.txt"),
        ])
        .no_scan(true)
        .run(&ctx.global)
        .await?;

        // Run restore 1
        let restore_path = ctx._tmp_dir.path().join("restore1");
        ctx.restore_builder(restore_path.clone())
            .include(vec![
                "0/file0.txt".to_string(),
                "0/00/file00.txt".to_string(),
            ])
            .strip_prefix(true)
            .run(&ctx.global)
            .await?;

        let restored_paths = vec![PathBuf::from("file0.txt"), PathBuf::from("00/file00.txt")];
        for path in &restored_paths {
            let restored_path = restore_path.join(path);
            assert!(restored_path.exists());
        }

        // Run restore 2
        let restore_path = ctx._tmp_dir.path().join("restore2");
        ctx.restore_builder(restore_path.clone())
            .include(vec!["0/00/file00.txt".to_string()])
            .strip_prefix(true)
            .run(&ctx.global)
            .await?;

        let restored_paths = vec![PathBuf::from("file00.txt")];
        for path in &restored_paths {
            let restored_path = restore_path.join(path);
            assert!(restored_path.exists());
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_restore_delete_default() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        ctx.setup_backup_data()?;
        let backup_data_tmp_path = ctx.backup_data_path.clone().unwrap();

        // Init repo
        ctx.init_repo().await?;

        // Run snapshot
        ctx.snapshot_builder(vec![
            backup_data_tmp_path.join("0"),
            backup_data_tmp_path.join("1"),
            backup_data_tmp_path.join("2"),
            backup_data_tmp_path.join("file.txt"),
        ])
        .no_scan(true)
        .run(&ctx.global)
        .await?;

        // Run restore to create base files
        let restore_path = ctx._tmp_dir.path().join("restore");
        ctx.restore_builder(restore_path.clone())
            .strategy(Strategy::Overwrite)
            .quit_on_error(false)
            .run(&ctx.global)
            .await?;

        // Create extra files
        std::fs::create_dir_all(restore_path.join("0"))?;
        std::fs::create_dir_all(restore_path.join("1").join("10"))?;
        std::fs::File::create(restore_path.join("extra_root.txt"))?;
        std::fs::create_dir_all(restore_path.join("extra_root_dir"))?;
        std::fs::File::create(restore_path.join("0").join("extra0.txt"))?;
        std::fs::File::create(restore_path.join("1").join("10").join("extra10.txt"))?;
        assert!(restore_path.join("extra_root.txt").exists());
        assert!(restore_path.join("extra_root_dir").exists());
        assert!(restore_path.join("0").join("extra0.txt").exists());
        assert!(
            restore_path
                .join("1")
                .join("10")
                .join("extra10.txt")
                .exists()
        );

        // Restore with --delete
        ctx.restore_builder(restore_path.clone())
            .strategy(Strategy::Overwrite)
            .quit_on_error(false)
            .delete(true)
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
            let restored_path = restore_path.join(path);
            assert!(restored_path.exists());
        }

        // Assert that extra files were deleted, except at root level
        assert!(restore_path.join("extra_root.txt").exists()); // Not deleted (root level)
        assert!(restore_path.join("extra_root_dir").exists()); // Not deleted (root level)
        assert!(!restore_path.join("0").join("extra0.txt").exists());
        assert!(
            !restore_path
                .join("1")
                .join("10")
                .join("extra10.txt")
                .exists()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_restore_delete_default_with_include() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        ctx.setup_backup_data()?;
        let backup_data_tmp_path = ctx.backup_data_path.clone().unwrap();

        // Init repo
        ctx.init_repo().await?;

        // Run snapshot
        ctx.snapshot_builder(vec![
            backup_data_tmp_path.join("0"),
            backup_data_tmp_path.join("1"),
            backup_data_tmp_path.join("2"),
            backup_data_tmp_path.join("file.txt"),
        ])
        .no_scan(true)
        .run(&ctx.global)
        .await?;

        // Run restore to create base files
        let restore_path = ctx._tmp_dir.path().join("restore");
        ctx.restore_builder(restore_path.clone())
            .strategy(Strategy::Overwrite)
            .quit_on_error(false)
            .run(&ctx.global)
            .await?;

        // Create extra files
        std::fs::create_dir_all(restore_path.join("0"))?;
        std::fs::create_dir_all(restore_path.join("1").join("10"))?;
        std::fs::File::create(restore_path.join("extra_root.txt"))?;
        std::fs::create_dir_all(restore_path.join("extra_root_dir"))?;
        std::fs::File::create(restore_path.join("0").join("extra0.txt"))?;
        std::fs::File::create(restore_path.join("1").join("10").join("extra10.txt"))?;
        assert!(restore_path.join("extra_root.txt").exists());
        assert!(restore_path.join("extra_root_dir").exists());
        assert!(restore_path.join("0").join("extra0.txt").exists());
        assert!(
            restore_path
                .join("1")
                .join("10")
                .join("extra10.txt")
                .exists()
        );

        // Restore with --delete
        ctx.restore_builder(restore_path.clone())
            .include(vec!["0".to_string()])
            .strategy(Strategy::Overwrite)
            .quit_on_error(false)
            .delete(true)
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
        ];

        for path in &paths {
            let restored_path = restore_path.join(path);
            assert!(restored_path.exists());
        }

        // Assert that extra files were deleted, except at root level
        assert!(restore_path.join("extra_root.txt").exists()); // Not deleted (root level and includes)
        assert!(restore_path.join("extra_root_dir").exists()); // Not deleted (root level)
        assert!(!restore_path.join("0").join("extra0.txt").exists());
        assert!(
            restore_path
                .join("1")
                .join("10")
                .join("extra10.txt")
                .exists() // Not deleted as it is outside the includes
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_restore_delete_no_preserve_root() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        ctx.setup_backup_data()?;
        let backup_data_tmp_path = ctx.backup_data_path.clone().unwrap();

        // Init repo
        ctx.init_repo().await?;

        // Run snapshot
        ctx.snapshot_builder(vec![
            backup_data_tmp_path.join("0"),
            backup_data_tmp_path.join("1"),
            backup_data_tmp_path.join("2"),
            backup_data_tmp_path.join("file.txt"),
        ])
        .no_scan(true)
        .run(&ctx.global)
        .await?;

        // Run restore to create base files
        let restore_path = ctx._tmp_dir.path().join("restore");
        ctx.restore_builder(restore_path.clone())
            .strategy(Strategy::Overwrite)
            .quit_on_error(false)
            .no_preserve_root(true)
            .run(&ctx.global)
            .await?;

        // Create extra files
        std::fs::create_dir_all(restore_path.join("0"))?;
        std::fs::create_dir_all(restore_path.join("1").join("10"))?;
        std::fs::File::create(restore_path.join("extra_root.txt"))?;
        std::fs::create_dir_all(restore_path.join("extra_root_dir"))?;
        std::fs::File::create(restore_path.join("0").join("extra0.txt"))?;
        std::fs::File::create(restore_path.join("1").join("10").join("extra10.txt"))?;
        assert!(restore_path.join("extra_root.txt").exists());
        assert!(restore_path.join("extra_root_dir").exists());
        assert!(restore_path.join("0").join("extra0.txt").exists());
        assert!(
            restore_path
                .join("1")
                .join("10")
                .join("extra10.txt")
                .exists()
        );

        // Restore with --delete
        ctx.restore_builder(restore_path.clone())
            .strategy(Strategy::Overwrite)
            .quit_on_error(false)
            .delete(true)
            .no_preserve_root(true)
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
            let restored_path = restore_path.join(path);
            assert!(restored_path.exists());
        }

        // Assert that extra files were deleted
        assert!(!restore_path.join("extra_root.txt").exists());
        assert!(!restore_path.join("extra_root_dir").exists());
        assert!(!restore_path.join("0").join("extra0.txt").exists());
        assert!(
            !restore_path
                .join("1")
                .join("10")
                .join("extra10.txt")
                .exists()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_restore_with_conflict_resolution() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        ctx.setup_backup_data()?;
        let backup_data_tmp_path = ctx.backup_data_path.clone().unwrap();

        // Init repo and create Snapshot 1
        ctx.init_repo().await?;

        ctx.snapshot_builder(vec![backup_data_tmp_path.join("0")])
            .tags(String::from("S1"))
            .no_scan(true)
            .num_readers(1)
            .num_packers(1)
            .run(&ctx.global)
            .await?;

        let restore_path = ctx._tmp_dir.path().join("restore_conflicts");
        ctx.restore_builder(restore_path.clone())
            .quit_on_error(false)
            .run(&ctx.global)
            .await?;

        let file_to_overwrite = restore_path.join("0").join("file0.txt");
        let file_to_skip = restore_path.join("0").join("00").join("file00.txt");
        let original_content =
            std::fs::read_to_string(backup_data_tmp_path.join("0").join("file0.txt"))?;

        // Change content and metadata for both local files
        let new_content_overwrite = "Conflict content for Overwrite test";
        std::fs::write(&file_to_overwrite, new_content_overwrite)?;

        let new_content_skip = "Conflict content for Skip test";
        std::fs::write(&file_to_skip, new_content_skip)?;

        let filetime = FileTime::from(SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000_000));
        set_file_times(&file_to_overwrite, filetime, filetime)?;
        set_file_times(&file_to_skip, filetime, filetime)?;

        ctx.restore_builder(restore_path.clone())
            .include(vec!["0/file0.txt".to_string()])
            .strategy(Strategy::Overwrite)
            .quit_on_error(false)
            .run(&ctx.global)
            .await?;

        // Verify that the file was overwritten
        assert_eq!(
            std::fs::read_to_string(&file_to_overwrite)?,
            original_content
        );

        ctx.restore_builder(restore_path.clone())
            .include(vec!["0/00/file00.txt".to_string()])
            .strategy(Strategy::Skip)
            .quit_on_error(false)
            .run(&ctx.global)
            .await?;

        // Verify that the local change was kept (skipped)
        assert_eq!(std::fs::read_to_string(&file_to_skip)?, new_content_skip);

        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn test_xattrs() -> Result<()> {
        let ctx = TestContext::new().await?;
        let backup_path = ctx._tmp_dir.path().join("backup");
        std::fs::create_dir_all(&backup_path)?;

        // File with xattr
        let file_path = backup_path.join("file_with_xattr.txt");
        std::fs::write(&file_path, "hello xattr")?;
        let file_attr_name = "user.mapache_file_test";
        let file_attr_value = b"mapache_file_value";
        xattr::set(&file_path, file_attr_name, file_attr_value)?;

        // Directory with xattr
        let dir_path = backup_path.join("dir_with_xattr");
        std::fs::create_dir(&dir_path)?;
        let dir_attr_name = "user.mapache_dir_test";
        let dir_attr_value = b"mapache_dir_value";
        xattr::set(&dir_path, dir_attr_name, dir_attr_value)?;

        // Symlink with xattr (some OSs might not support xattrs on symlinks, but Linux generally does)
        let symlink_path = backup_path.join("symlink_with_xattr");
        std::os::unix::fs::symlink("file_with_xattr.txt", &symlink_path)?;
        let symlink_attr_name = "user.mapache_symlink_test";
        let symlink_attr_value = b"mapache_symlink_value";
        // standard variants do not follow symlinks by default in the xattr crate
        let symlink_xattr_supported =
            xattr::set(&symlink_path, symlink_attr_name, symlink_attr_value).is_ok();

        // Init repo
        ctx.init_repo().await?;

        // Run snapshot
        ctx.snapshot_builder(vec![backup_path.clone()])
            .root(true)
            .num_readers(1)
            .num_packers(1)
            .run(&ctx.global)
            .await?;

        // Restore
        let restore_path = ctx._tmp_dir.path().join("restore");
        ctx.restore_builder(restore_path.clone())
            .run(&ctx.global)
            .await?;

        // Verify file xattr
        let restored_file_path = restore_path.join("file_with_xattr.txt");
        assert!(restored_file_path.exists());
        let restored_file_value = xattr::get(&restored_file_path, file_attr_name)?;
        assert_eq!(restored_file_value, Some(file_attr_value.to_vec()));

        // Verify directory xattr
        let restored_dir_path = restore_path.join("dir_with_xattr");
        assert!(restored_dir_path.exists());
        let restored_dir_value = xattr::get(&restored_dir_path, dir_attr_name)?;
        assert_eq!(restored_dir_value, Some(dir_attr_value.to_vec()));

        // Verify symlink xattr
        if symlink_xattr_supported {
            let restored_symlink_path = restore_path.join("symlink_with_xattr");
            assert!(restored_symlink_path.is_symlink());
            let restored_symlink_value = xattr::get(&restored_symlink_path, symlink_attr_name)?;
            assert_eq!(restored_symlink_value, Some(symlink_attr_value.to_vec()));
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_restore_sparse_vs_eager_allocation() -> Result<()> {
        let ctx = TestContext::new().await?;
        let backup_path = ctx._tmp_dir.path().join("backup");
        std::fs::create_dir_all(&backup_path)?;

        // Create a moderately-sized test file
        let file_path = backup_path.join("large_file.bin");
        let test_size = 10 * 1024 * 1024; // 10 MiB
        let test_data = vec![42u8; test_size];
        std::fs::write(&file_path, &test_data)?;

        // Init repo and snapshot
        ctx.init_repo().await?;

        ctx.snapshot_builder(vec![file_path.clone()])
            .no_scan(true)
            .num_readers(1)
            .num_packers(1)
            .run(&ctx.global)
            .await?;

        // Test 1: Restore with sparse allocation (default)
        let restore_sparse_path = ctx._tmp_dir.path().join("restore_sparse");
        ctx.restore_builder(restore_sparse_path.clone())
            .sparse(true)
            .strategy(Strategy::Overwrite)
            .run(&ctx.global)
            .await?;

        let restored_sparse_file = restore_sparse_path.join("large_file.bin");
        assert!(
            restored_sparse_file.exists(),
            "Sparse restore: file should exist at {:?}",
            restored_sparse_file
        );
        let sparse_content = std::fs::read(&restored_sparse_file)?;
        assert_eq!(sparse_content.len(), test_data.len());
        assert_eq!(sparse_content, test_data);
        let sparse_metadata = std::fs::metadata(&restored_sparse_file)?;
        assert_eq!(sparse_metadata.len(), test_size as u64);

        // Test 2: Restore with eager allocation
        let restore_eager_path = ctx._tmp_dir.path().join("restore_eager");
        ctx.restore_builder(restore_eager_path.clone())
            .sparse(false)
            .strategy(Strategy::Overwrite)
            .run(&ctx.global)
            .await?;

        let restored_eager_file = restore_eager_path.join("large_file.bin");
        assert!(
            restored_eager_file.exists(),
            "Eager restore: file should exist at {:?}",
            restored_eager_file
        );
        let eager_content = std::fs::read(&restored_eager_file)?;
        assert_eq!(eager_content.len(), test_data.len());
        assert_eq!(eager_content, test_data);
        let eager_metadata = std::fs::metadata(&restored_eager_file)?;
        assert_eq!(eager_metadata.len(), test_size as u64);

        // Both files should have identical content
        assert_eq!(sparse_content, eager_content);

        // On Unix, check allocation: eager-allocated file should use more disk space
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let sparse_blocks = sparse_metadata.blocks();
            let eager_blocks = eager_metadata.blocks();
            // Eager allocation reserves actual blocks; sparse may use fewer initially
            // This is a best-effort check; some filesystems behave differently
            eprintln!(
                "Sparse file blocks: {}, Eager file blocks: {}",
                sparse_blocks, eager_blocks
            );
            assert!(
                eager_blocks > 0,
                "Eager-allocated file should have allocated blocks"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_restore_verify_content() -> Result<()> {
        let ctx = TestContext::new().await?;
        let backup_path = ctx._tmp_dir.path().join("backup");
        std::fs::create_dir_all(&backup_path)?;

        let file_path = backup_path.join("file.txt");
        let original_content = "Original content";
        std::fs::write(&file_path, original_content)?;

        // Init repo
        ctx.init_repo().await?;

        // Run snapshot
        ctx.snapshot_builder(vec![file_path.clone()])
            .no_scan(true)
            .num_readers(1)
            .num_packers(1)
            .run(&ctx.global)
            .await?;

        // Run restore to create base file
        let restore_path = ctx._tmp_dir.path().join("restore");
        ctx.restore_builder(restore_path.clone())
            .strategy(Strategy::Overwrite)
            .run(&ctx.global)
            .await?;

        let restored_file = restore_path.join("file.txt");
        assert_eq!(std::fs::read_to_string(&restored_file)?, original_content);

        // Get original mtime
        let original_filetime =
            FileTime::from_last_modification_time(&std::fs::metadata(&restored_file)?);

        // Modify content but keep size and mtime
        let modified_content = "Modified content"; // Same length as "Original content" is 16.
        assert_eq!(original_content.len(), modified_content.len());

        std::fs::write(&restored_file, modified_content)?;
        set_file_times(&restored_file, original_filetime, original_filetime)?;

        let new_metadata = std::fs::metadata(&restored_file)?;
        assert_eq!(new_metadata.len(), original_content.len() as u64);
        // We might have some precision issues depending on platform, but set_file_times should help.

        // Run restore again WITHOUT verify. It should skip because size and mtime match.
        ctx.restore_builder(restore_path.clone())
            .strategy(Strategy::Overwrite)
            .verify(false)
            .run(&ctx.global)
            .await?;

        assert_eq!(std::fs::read_to_string(&restored_file)?, modified_content);

        // Modify mtime but keep modified content.
        // It should detect that content DOES NOT match (automatic verification on mtime mismatch) and RESTORE it.
        let different_filetime =
            FileTime::from_last_modification_time(&std::fs::metadata(&restored_file)?);
        let future_time = FileTime::from_unix_time(different_filetime.unix_seconds() + 1000, 0);
        set_file_times(&restored_file, future_time, future_time)?;

        ctx.restore_builder(restore_path.clone())
            .strategy(Strategy::Overwrite)
            .verify(false)
            .run(&ctx.global)
            .await?;

        // It should have restored original content because hashes didn't match.
        assert_eq!(std::fs::read_to_string(&restored_file)?, original_content);

        // Test overwriting a readonly file
        // Make it readonly in the target.
        let mut perms = std::fs::metadata(&restored_file)?.permissions();
        perms.set_readonly(true);
        std::fs::set_permissions(&restored_file, perms)?;

        // Modify backup data file to force an overwrite (change content)
        std::fs::write(&file_path, "New original content")?;
        // Snapshot again
        ctx.snapshot_builder(vec![file_path.clone()])
            .no_scan(true)
            .num_readers(1)
            .num_packers(1)
            .run(&ctx.global)
            .await?;

        // Restore with overwrite strategy. It should succeed despite the file being readonly.
        ctx.restore_builder(restore_path.clone())
            .strategy(Strategy::Overwrite)
            .run(&ctx.global)
            .await?;

        assert_eq!(
            std::fs::read_to_string(&restored_file)?,
            "New original content"
        );

        Ok(())
    }
}
