#![cfg(test)]

mod tests {
    use std::{path::PathBuf, sync::Arc};

    use anyhow::{Context, Result};
    use mapache::{
        backend::localfs::LocalFS,
        commands::{self, UseSnapshot, cmd_clean, cmd_restore, cmd_snapshot},
        mapache::defaults::TEST_REPO_CONFIG,
        repository::repo::Repository,
        restorer::Strategy,
    };

    use crate::integration_tests::{TestContext, assert_times_equal};

    #[tokio::test]
    async fn test_gc_sanity_check() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        ctx.setup_backup_data()?;
        let backup_data_tmp_path = ctx.backup_data_path.as_ref().unwrap();

        // Init repo
        ctx.init_repo().await?;

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
            skip_if_unchanged: false,
            no_scan: true,
            parent: UseSnapshot::Latest,
            num_readers: 2,
            num_packers: 2,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&ctx.global, &snapshot_args)
            .await
            .context("Failed to run cmd_snapshot (1/2)")?;

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
            skip_if_unchanged: false,
            no_scan: true,
            parent: UseSnapshot::Latest,
            num_readers: 2,
            num_packers: 2,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&ctx.global, &snapshot_args)
            .await
            .context("Failed to run cmd_snapshot (2/2)")?;

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
        };
        commands::cmd_forget::run(&ctx.global, &forget_args)
            .await
            .context("Failed to run cmd_forget")?;

        let gc_args = cmd_clean::CmdArgs {
            tolerance: 0.0_f32,
            dry_run: false,

            no_repack: false,
        };
        commands::cmd_clean::run(&ctx.global, &gc_args)
            .await
            .context("Failed to run cmd_gc")?;

        // Run restore
        let restore_path = ctx._tmp_dir.path().join("restore");
        let restore_args = cmd_restore::CmdArgs {
            preallocate: false,
            target: restore_path.clone(),
            snapshot: UseSnapshot::Latest,
            dry_run: false,
            verify: false,
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
        let backup_data_tmp_path = ctx.backup_data_path.as_ref().unwrap();

        // Init repo
        ctx.init_repo().await?;

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
            skip_if_unchanged: false,
            no_scan: true,
            parent: UseSnapshot::Latest,
            num_readers: 2,
            num_packers: 2,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&ctx.global, &snapshot_args)
            .await
            .context("Failed to run cmd_snapshot (1/2)")?;

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
            skip_if_unchanged: false,
            no_scan: true,
            parent: UseSnapshot::Latest,
            num_readers: 2,
            num_packers: 2,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&ctx.global, &snapshot_args)
            .await
            .context("Failed to run cmd_snapshot (2/2)")?;

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
        };
        commands::cmd_forget::run(&ctx.global, &forget_args)
            .await
            .context("Failed to run cmd_forget")?;

        // Run cmd_clean and compare the repositories (using backend readdir)
        let backend = Arc::new(LocalFS::new(ctx.repo_path.clone()));
        let mut pre_clean_nodes =
            mapache::backend::read_backend_dir(backend.as_ref(), &PathBuf::new()).await?;
        let gc_args = cmd_clean::CmdArgs {
            tolerance: 0.0_f32,
            dry_run: true, // DRY-RUN !

            no_repack: false,
        };
        commands::cmd_clean::run(&ctx.global, &gc_args)
            .await
            .context("Failed to run cmd_gc")?;
        let mut post_clean_nodes =
            mapache::backend::read_backend_dir(backend.as_ref(), &PathBuf::new()).await?;

        pre_clean_nodes.sort();
        post_clean_nodes.sort();
        assert_eq!(pre_clean_nodes, post_clean_nodes);

        // Now the same, but without dry-run, the repo changes
        let pre_clean_nodes =
            mapache::backend::read_backend_dir(backend.as_ref(), &PathBuf::new()).await?;
        let gc_args = cmd_clean::CmdArgs {
            tolerance: 0.0_f32,
            dry_run: false, // No dry-run

            no_repack: false,
        };
        commands::cmd_clean::run(&ctx.global, &gc_args)
            .await
            .context("Failed to run cmd_gc")?;
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
        ctx.global.verbosity = Some(1);
        mapache::mapache::global::set_global_opts_with_args(&ctx.global);

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

        commands::cmd_snapshot::run(
            &ctx.global,
            &cmd_snapshot::CmdArgs {
                paths: vec![backup_path.clone()],
                as_root: true,
                exclude: None,
                tags_str: String::new(),
                description: None,
                no_parent: false,
                skip_if_unchanged: false,
                no_scan: false,
                parent: UseSnapshot::Latest,
                num_readers: 1,
                num_packers: 1,
                dry_run: false,
            },
        )
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

        commands::cmd_snapshot::run(
            &ctx.global,
            &cmd_snapshot::CmdArgs {
                paths: vec![backup_path.clone()],
                as_root: true,
                exclude: None,
                tags_str: String::new(),
                description: None,
                no_parent: false,
                skip_if_unchanged: false,
                no_scan: false,
                parent: UseSnapshot::Latest,
                num_readers: 1,
                num_packers: 1,
                dry_run: false,
            },
        )
        .await?;

        // Forget Snapshot 1.
        let forget_args = commands::cmd_forget::CmdArgs {
            forget: vec![first_id],
            force: true, // Permanent delete
            tags_str: None,
            keep_last: None,
            keep_within: None,
            keep_yearly: None,
            keep_monthly: None,
            keep_weekly: None,
            keep_daily: None,
            keep_tags_str: None,
            dry_run: false,
            run_gc: false,
            tolerance: 0.0,
        };
        commands::cmd_forget::run(&ctx.global, &forget_args).await?;

        // After forgetting Snapshot 1, file_b's blobs are now garbage.
        let objects_dir = ctx.repo_path.join("objects");
        let pre_gc_size = mapache::utils::dir_size(&objects_dir)?;

        // Run GC.
        let gc_args = cmd_clean::CmdArgs {
            tolerance: 0.0,
            dry_run: false,
            no_repack: false,
        };
        commands::cmd_clean::run(&ctx.global, &gc_args).await?;

        let post_gc_size = mapache::utils::dir_size(&objects_dir)?;

        // The size should have decreased because file_b (512KiB) is removed.
        assert!(
            post_gc_size < pre_gc_size,
            "GC should have reclaimed space. Pre: {}, Post: {}",
            pre_gc_size,
            post_gc_size
        );

        // Verify Snapshot 2 is still valid and restorable.
        let verify_args = commands::cmd_verify::CmdArgs {
            read_packs: true,
            parallel: 1,
            with_cache: false,
            fail_early: false,
            sample: None,
        };
        commands::cmd_verify::run(&ctx.global, &verify_args)
            .await
            .context("Verify failed after GC")?;

        let restore_path = ctx._tmp_dir.path().join("restore");
        let restore_args = commands::cmd_restore::CmdArgs {
            preallocate: false,
            target: restore_path.clone(),
            snapshot: UseSnapshot::Latest,
            dry_run: false,
            verify: false,
            include: None,
            exclude: None,
            strip_prefix: false,
            strategy: Strategy::Skip,
            quit_on_error: true,
            delete: false,
            no_preserve_root: false,
        };
        commands::cmd_restore::run(&ctx.global, &restore_args).await?;

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
        ctx.global.verbosity = Some(1);
        mapache::mapache::global::set_global_opts_with_args(&ctx.global);

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
        commands::cmd_snapshot::run(
            &ctx.global,
            &cmd_snapshot::CmdArgs {
                paths: vec![backup_path.clone()],
                as_root: true,
                exclude: None,
                tags_str: String::new(),
                description: None,
                no_parent: false,
                skip_if_unchanged: false,
                no_scan: false,
                parent: UseSnapshot::Latest,
                num_readers: 1,
                num_packers: 1,
                dry_run: false,
            },
        )
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
        commands::cmd_snapshot::run(
            &ctx.global,
            &cmd_snapshot::CmdArgs {
                paths: vec![backup_path.clone()],
                as_root: true,
                exclude: None,
                tags_str: String::new(),
                description: None,
                no_parent: false,
                skip_if_unchanged: false,
                no_scan: false,
                parent: UseSnapshot::Latest,
                num_readers: 1,
                num_packers: 1,
                dry_run: false,
            },
        )
        .await?;

        // Forget Snapshot 1
        let forget_args = commands::cmd_forget::CmdArgs {
            forget: vec![first_id],
            force: true,
            tags_str: None,
            keep_last: None,
            keep_within: None,
            keep_yearly: None,
            keep_monthly: None,
            keep_weekly: None,
            keep_daily: None,
            keep_tags_str: None,
            dry_run: false,
            run_gc: false,
            tolerance: 0.0,
        };
        commands::cmd_forget::run(&ctx.global, &forget_args).await?;

        // GC Logic: Ratio = garbage / 16MiB = 1MiB / 16MiB = 0.0625.

        let backend = Arc::new(LocalFS::new(ctx.repo_path.clone()));
        let (repo, _) =
            Repository::try_open_unlocked(Some(&ctx.auth), None, backend.clone(), TEST_REPO_CONFIG)
                .await?;
        let initial_packs = repo.list_packs().await?.len();

        // Run GC with tolerance 0.1 (10%). 6.25% < 10%, should KEEP the data pack.
        // It will however delete the unused tree pack from Snapshot 1.
        let gc_args_high = cmd_clean::CmdArgs {
            tolerance: 0.1,
            dry_run: false,
            no_repack: false,
        };
        commands::cmd_clean::run(&ctx.global, &gc_args_high).await?;

        let (repo, _) =
            Repository::try_open_unlocked(Some(&ctx.auth), None, backend.clone(), TEST_REPO_CONFIG)
                .await?;
        let post_gc_high_packs = repo.list_packs().await?.len();

        // We expect exactly ONE pack to be deleted (the unused tree pack).
        assert_eq!(
            post_gc_high_packs,
            initial_packs - 1,
            "High tolerance should have only removed the unused tree pack"
        );

        // Run GC with tolerance 0.05 (5%). 6.25% > 5%, should REPACK the data pack.
        let gc_args_low = cmd_clean::CmdArgs {
            tolerance: 0.05,
            dry_run: false,
            no_repack: false,
        };
        commands::cmd_clean::run(&ctx.global, &gc_args_low).await?;

        let verify_args = commands::cmd_verify::CmdArgs {
            read_packs: true,
            parallel: 1,
            with_cache: false,
            fail_early: false,
            sample: None,
        };
        commands::cmd_verify::run(&ctx.global, &verify_args)
            .await
            .context("Verify failed after low tolerance GC")?;

        Ok(())
    }
}
