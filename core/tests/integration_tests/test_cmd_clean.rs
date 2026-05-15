#![cfg(test)]

mod tests {
    use std::{path::PathBuf, sync::Arc};

    use anyhow::Result;
    use mapache::{
        backend::localfs::LocalFS, mapache::defaults::TEST_REPO_CONFIG,
        repository::repo::Repository,
    };

    use crate::integration_tests::{TestContext, assert_times_equal};

    #[tokio::test]
    async fn test_gc_sanity_check() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        ctx.setup_backup_data()?;
        let backup_data_tmp_path = ctx.backup_data_path.clone().unwrap();

        // Init repo
        ctx.init_repo().await?;

        // Run snapshot twice
        ctx.snapshot(vec![
            backup_data_tmp_path.join("0"),
            backup_data_tmp_path.join("1"),
            backup_data_tmp_path.join("2"),
            backup_data_tmp_path.join("file.txt"),
        ])
        .await?;

        ctx.snapshot(vec![
            backup_data_tmp_path.join("0"),
            backup_data_tmp_path.join("1"),
            backup_data_tmp_path.join("2"),
        ])
        .await?;

        // Keep the last snapshot
        ctx.forget_builder().keep_last(1).run(&ctx.global).await?;

        ctx.clean_builder().run(&ctx.global).await?;

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
            PathBuf::from("2"),
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
        }

        Ok(())
    }

    /// Run clean but dry-run. Nothing should change.
    #[tokio::test]
    async fn test_clean_dry_run() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        ctx.setup_backup_data()?;
        let backup_data_tmp_path = ctx.backup_data_path.clone().unwrap();

        // Init repo
        ctx.init_repo().await?;

        // Run snapshot twice
        ctx.snapshot(vec![
            backup_data_tmp_path.join("0"),
            backup_data_tmp_path.join("1"),
            backup_data_tmp_path.join("2"),
            backup_data_tmp_path.join("file.txt"),
        ])
        .await?;

        ctx.snapshot(vec![
            backup_data_tmp_path.join("0"),
            backup_data_tmp_path.join("1"),
            backup_data_tmp_path.join("2"),
        ])
        .await?;

        // Keep the last snapshot
        ctx.forget_builder().keep_last(1).run(&ctx.global).await?;

        // Run cmd_clean and compare the repositories (using backend readdir)
        let backend = Arc::new(LocalFS::new(ctx.repo_path.clone()));
        let mut pre_clean_nodes =
            mapache::backend::read_backend_dir(backend.as_ref(), &PathBuf::new()).await?;

        ctx.clean_builder().dry_run(true).run(&ctx.global).await?;

        let mut post_clean_nodes =
            mapache::backend::read_backend_dir(backend.as_ref(), &PathBuf::new()).await?;

        pre_clean_nodes.sort();
        post_clean_nodes.sort();
        assert_eq!(pre_clean_nodes, post_clean_nodes);

        // Now the same, but without dry-run, the repo changes
        let pre_clean_nodes =
            mapache::backend::read_backend_dir(backend.as_ref(), &PathBuf::new()).await?;

        ctx.clean_builder().run(&ctx.global).await?;

        let post_clean_nodes =
            mapache::backend::read_backend_dir(backend.as_ref(), &PathBuf::new()).await?;
        assert_ne!(pre_clean_nodes, post_clean_nodes);

        Ok(())
    }

    #[tokio::test]
    async fn test_gc_repacks_and_removes_garbage() -> Result<()> {
        let mut ctx = TestContext::new().await?;

        // Set a small pack size to force multiple packs and repacking
        ctx.global.pack_size_mib = 1.0; // 1MiB packs
        ctx.init_repo().await?;

        let backup_path = ctx._tmp_dir.path().join("backup");
        std::fs::create_dir(&backup_path)?;

        // Create file A (512KiB) and file B (512KiB). Total ~1MiB, should fit in one or two packs.
        let file_a = backup_path.join("file_a.bin");
        let file_b = backup_path.join("file_b.bin");
        let data_a = vec![0u8; 512 * 1024];
        let data_b = vec![1u8; 512 * 1024];
        std::fs::write(&file_a, &data_a)?;
        std::fs::write(&file_b, &data_b)?;

        ctx.snapshot_builder(vec![backup_path.clone()])
            .root(true)
            .run(&ctx.global)
            .await?;

        let snapshots_dir = ctx.repo_path.join("snapshots");
        let snapshots = std::fs::read_dir(&snapshots_dir)?
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(snapshots.len(), 1);
        let first_id = snapshots[0]
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        // Delete file B, add file C (512KiB).
        std::fs::remove_file(&file_b)?;
        let file_c = backup_path.join("file_c.bin");
        let data_c = vec![2u8; 512 * 1024];
        std::fs::write(&file_c, &data_c)?;

        ctx.snapshot_builder(vec![backup_path.clone()])
            .root(true)
            .run(&ctx.global)
            .await?;

        // Forget Snapshot 1.
        ctx.forget_builder()
            .forget(vec![first_id])
            .force(true)
            .run(&ctx.global)
            .await?;

        // After forgetting Snapshot 1, file_b's blobs are now garbage.
        let objects_dir = ctx.repo_path.join("objects");
        let pre_gc_size = mapache::utils::dir_size(&objects_dir)?;

        // Run GC.
        ctx.clean_builder().run(&ctx.global).await?;

        let post_gc_size = mapache::utils::dir_size(&objects_dir)?;

        // The size should have decreased because file_b (512KiB) is removed.
        assert!(
            post_gc_size < pre_gc_size,
            "GC should have reclaimed space. Pre: {}, Post: {}",
            pre_gc_size,
            post_gc_size
        );

        // Verify Snapshot 2 is still valid and restorable.
        ctx.verify_builder()
            .read_packs(true)
            .run(&ctx.global)
            .await?;

        let restore_path = ctx._tmp_dir.path().join("restore");
        ctx.restore_builder(restore_path.clone())
            .run(&ctx.global)
            .await?;

        assert!(restore_path.join("file_a.bin").exists());
        assert!(restore_path.join("file_c.bin").exists());
        assert!(!restore_path.join("file_b.bin").exists());

        assert_eq!(std::fs::read(restore_path.join("file_a.bin"))?, data_a);
        assert_eq!(std::fs::read(restore_path.join("file_c.bin"))?, data_c);

        Ok(())
    }

    #[tokio::test]
    async fn test_gc_tolerance() -> Result<()> {
        let mut ctx = TestContext::new().await?;

        ctx.global.pack_size_mib = 16.0;
        ctx.init_repo().await?;

        let backup_path = ctx._tmp_dir.path().join("backup");
        std::fs::create_dir(&backup_path)?;

        // Create file A (1MiB) and file B (1MiB).
        let file_a = backup_path.join("file_a.bin");
        let file_b = backup_path.join("file_b.bin");
        let data_a = vec![0u8; 1024 * 1024];
        let data_b = vec![1u8; 1024 * 1024];
        std::fs::write(&file_a, &data_a)?;
        std::fs::write(&file_b, &data_b)?;

        // Snapshot 1: {A, B}
        ctx.snapshot_builder(vec![backup_path.clone()])
            .root(true)
            .run(&ctx.global)
            .await?;

        let snapshots_dir = ctx.repo_path.join("snapshots");
        let snapshots = std::fs::read_dir(&snapshots_dir)?
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<_>, _>>()?;
        let first_id = snapshots[0]
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        // Create Snapshot 2: {A} (B is deleted)
        std::fs::remove_file(&file_b)?;
        ctx.snapshot_builder(vec![backup_path.clone()])
            .root(true)
            .run(&ctx.global)
            .await?;

        // Forget Snapshot 1
        ctx.forget_builder()
            .forget(vec![first_id])
            .force(true)
            .run(&ctx.global)
            .await?;

        // GC Logic: Ratio = garbage / 16MiB = 1MiB / 16MiB = 0.0625.

        let backend = Arc::new(LocalFS::new(ctx.repo_path.clone()));
        let (repo, _) =
            Repository::try_open_unlocked(&ctx.auth, None, backend.clone(), TEST_REPO_CONFIG)
                .await?;
        let initial_packs = repo.list_packs().await?.len();

        // Run GC with tolerance 0.1 (10%). 6.25% < 10%, should KEEP the data pack.
        ctx.clean_builder().tolerance(0.1).run(&ctx.global).await?;

        let (repo, _) =
            Repository::try_open_unlocked(&ctx.auth, None, backend.clone(), TEST_REPO_CONFIG)
                .await?;
        let post_gc_high_packs = repo.list_packs().await?.len();

        // We expect exactly ONE pack to be deleted (the unused tree pack).
        assert_eq!(
            post_gc_high_packs,
            initial_packs - 1,
            "High tolerance should have only removed the unused tree pack"
        );

        // Run GC with tolerance 0.05 (5%). 6.25% > 5%, should REPACK the data pack.
        ctx.clean_builder().tolerance(0.05).run(&ctx.global).await?;

        ctx.verify_builder()
            .read_packs(true)
            .run(&ctx.global)
            .await?;

        Ok(())
    }
}
