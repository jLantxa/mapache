#![cfg(test)]

//! Migration-specific integration tests (v1 → v2).
//!
//! Keeping these here (instead of in the v1-format tests) makes the migration
//! support a single, self-contained unit that can be dropped wholesale when
//! the v1 format is removed.
//! TODO(v1-removal): Delete this entire file when the v1 format is dropped.

mod tests {
    use std::{path::PathBuf, sync::Arc};

    use anyhow::{Context, Result};

    use mapache::{
        backend::localfs::LocalFS,
        commands::cmd_migrate,
        repository::repo::{
            INDEX_DIR, OBJECTS_DIR, Repository, SNAPSHOTS_DIR, THIS_REPOSITORY_VERSION,
        },
    };

    use crate::{
        integration_tests::{INTEGRATION_TEST_DATA, TestContext},
        synthetic::{Dataset, SyntheticData},
    };

    #[tokio::test]
    async fn test_v1_migrate_to_v2() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        // Init v1
        ctx.init_builder()
            .format(1)
            .run(&ctx.global)
            .await
            .context("Failed to init v1 repo")?;

        // Snapshot
        ctx.snapshot(vec![
            backup_data_tmp_path.join("file.txt"),
            backup_data_tmp_path.join("0"),
            backup_data_tmp_path.join("1"),
            backup_data_tmp_path.join("2"),
        ])
        .await
        .context("Failed to snapshot")?;

        // Migrate v1 -> v2
        cmd_migrate::run(&ctx.global, &cmd_migrate::CmdArgs { dry_run: false })
            .await
            .context("Migration failed")?;

        // Verify after migration
        ctx.verify_builder()
            .read_packs(true)
            .run(&ctx.global)
            .await
            .context("Verify failed after migration")?;

        // Restore after migration
        let restore_path = ctx._tmp_dir.path().join("restore_after_migrate");
        ctx.restore_builder(restore_path.clone())
            .run(&ctx.global)
            .await
            .context("Restore failed after migration")?;

        let paths = vec![
            PathBuf::from("file.txt"),
            PathBuf::from("0"),
            PathBuf::from("0/file0.txt"),
            PathBuf::from("1"),
            PathBuf::from("1/10"),
            PathBuf::from("1/10/file10.txt"),
            PathBuf::from("2"),
        ];

        for path in &paths {
            let backup_path = backup_data_tmp_path.join(path);
            let restored_path = restore_path.join(path);
            assert!(
                restored_path.exists(),
                "missing after migrate: {}",
                path.display()
            );
            assert_eq!(
                restored_path.symlink_metadata()?.len(),
                backup_path.symlink_metadata()?.len(),
                "size mismatch after migrate: {}",
                path.display(),
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_v1_migrate_preserves_dropped_snapshot() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        ctx.init_builder()
            .format(1)
            .run(&ctx.global)
            .await
            .context("Failed to init v1 repo")?;

        ctx.snapshot(vec![backup_data_tmp_path.join("file.txt")])
            .await
            .context("Failed to snapshot")?;
        let snapshot_id = ctx
            .get_snapshot_ids()?
            .into_iter()
            .next()
            .context("Snapshot was not created")?;

        ctx.forget_builder()
            .forget(vec![snapshot_id])
            .run(&ctx.global)
            .await
            .context("Failed to drop snapshot")?;

        cmd_migrate::run(&ctx.global, &cmd_migrate::CmdArgs { dry_run: false })
            .await
            .context("Migration failed")?;

        let snapshot_paths = std::fs::read_dir(ctx.repo_path.join(SNAPSHOTS_DIR))?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(snapshot_paths.len(), 1);
        assert_eq!(
            snapshot_paths[0]
                .extension()
                .and_then(|extension| extension.to_str()),
            Some("dropped")
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_v1_migrate_dry_run_does_not_modify_repo() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        // Init v1
        ctx.init_builder()
            .format(1)
            .run(&ctx.global)
            .await
            .context("Failed to init v1 repo")?;

        // Snapshot
        ctx.snapshot(vec![
            backup_data_tmp_path.join("file.txt"),
            backup_data_tmp_path.join("0"),
            backup_data_tmp_path.join("1"),
        ])
        .await
        .context("Failed to snapshot")?;

        let version_before = repository_version(&ctx).await?;
        assert_eq!(version_before, 1, "repo should start at format v1");
        let index_before = dir_file_names(&ctx.repo_path.join(INDEX_DIR))?;
        let objects_before = dir_file_names(&ctx.repo_path.join(OBJECTS_DIR))?;
        let snapshots_before = dir_file_names(&ctx.repo_path.join(SNAPSHOTS_DIR))?;

        // Dry run must succeed but write nothing.
        cmd_migrate::run(&ctx.global, &cmd_migrate::CmdArgs { dry_run: true })
            .await
            .context("migrate --dry-run failed")?;

        assert_eq!(
            repository_version(&ctx).await?,
            version_before,
            "dry run must not change the repository version"
        );
        assert_eq!(
            dir_file_names(&ctx.repo_path.join(INDEX_DIR))?,
            index_before,
            "dry run must not rewrite index files"
        );
        assert_eq!(
            dir_file_names(&ctx.repo_path.join(OBJECTS_DIR))?,
            objects_before,
            "dry run must not rewrite pack files"
        );
        assert_eq!(
            dir_file_names(&ctx.repo_path.join(SNAPSHOTS_DIR))?,
            snapshots_before,
            "dry run must not rewrite snapshot files"
        );

        // The v1 repo must still be fully usable after the dry run.
        ctx.verify_builder()
            .read_packs(true)
            .run(&ctx.global)
            .await
            .context("verify failed after dry-run")?;

        let restore_path = ctx._tmp_dir.path().join("restore_after_dry_run");
        ctx.restore_builder(restore_path.clone())
            .run(&ctx.global)
            .await
            .context("restore failed after dry-run")?;
        assert!(
            restore_path.join("file.txt").exists(),
            "file.txt must restore after dry-run"
        );

        // A real migration must still be possible afterwards.
        cmd_migrate::run(&ctx.global, &cmd_migrate::CmdArgs { dry_run: false })
            .await
            .context("real migration failed after dry-run")?;
        assert_eq!(
            repository_version(&ctx).await?,
            THIS_REPOSITORY_VERSION,
            "repo should be migrated to v2 after real migration"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_v1_migration_crash_window_stays_open_and_rerunnable() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        ctx.init_builder()
            .format(1)
            .run(&ctx.global)
            .await
            .context("Failed to init v1 repo")?;

        ctx.snapshot(vec![
            backup_data_tmp_path.join("file.txt"),
            backup_data_tmp_path.join("0"),
            backup_data_tmp_path.join("1"),
            backup_data_tmp_path.join("2"),
        ])
        .await
        .context("Failed to snapshot")?;

        // Fully migrate to v2.
        cmd_migrate::run(&ctx.global, &cmd_migrate::CmdArgs { dry_run: false })
            .await
            .context("Migration failed")?;
        assert_eq!(
            repository_version(&ctx).await?,
            THIS_REPOSITORY_VERSION,
            "repo should be fully migrated before simulating the crash"
        );

        // Reproduce the Step-4 crash window: new binary index files are
        // persisted *before* the manifest is updated, so a crash leaves a v1
        // manifest together with binary (v2) index files. Rewind the manifest
        // back to v1 to recreate that on-disk state.
        {
            let backend = Arc::new(LocalFS::new(ctx.repo_path.clone()));
            let (repo, _storage) = Repository::try_open_unlocked(
                &ctx.auth,
                None,
                backend,
                mapache::common::defaults::TEST_REPO_CONFIG,
            )
            .await
            .context("Failed to open migrated repo")?;
            let mut manifest = repo.manifest().clone();
            manifest.set_version(1);
            repo.save_manifest(&manifest)
                .await
                .context("Failed to rewind manifest to v1")?;
        }

        // The crash-window repo must still open. A hard failure here means the
        // repo is bricked: the tolerant index loader has to fall back to the
        // binary parser when a v1 manifest coexists with binary index files.
        assert_eq!(
            repository_version(&ctx).await?,
            1,
            "repo must open in the crash-window state"
        );

        // Re-running the migration must converge instead of failing on its own
        // already-migrated packs and snapshots.
        cmd_migrate::run(&ctx.global, &cmd_migrate::CmdArgs { dry_run: false })
            .await
            .context("Re-running migration after crash-window failed")?;
        assert_eq!(
            repository_version(&ctx).await?,
            THIS_REPOSITORY_VERSION,
            "repo should be migrated to v2 after the crash-window rerun"
        );

        ctx.verify_builder()
            .read_packs(true)
            .run(&ctx.global)
            .await
            .context("Verify failed after crash-window rerun")?;

        let restore_path = ctx._tmp_dir.path().join("restore_after_crash");
        ctx.restore_builder(restore_path.clone())
            .run(&ctx.global)
            .await
            .context("Restore failed after crash-window rerun")?;
        assert!(
            restore_path.join("file.txt").exists(),
            "file.txt must restore after crash-window rerun"
        );

        Ok(())
    }

    /// Open the repository without acquiring locks and return its format version.
    async fn repository_version(ctx: &TestContext) -> Result<u32> {
        let backend = Arc::new(LocalFS::new(ctx.repo_path.clone()));
        let (repo, _storage) = Repository::try_open_unlocked(
            &ctx.auth,
            None,
            backend,
            mapache::common::defaults::TEST_REPO_CONFIG,
        )
        .await?;
        Ok(repo.repo_version())
    }

    /// List the sorted file names inside a directory, used to compare repo contents.
    fn dir_file_names(dir: &PathBuf) -> Result<Vec<String>> {
        let mut names: Vec<String> = if dir.exists() {
            std::fs::read_dir(dir)?
                .map(|entry| entry.map(|e| e.file_name().to_string_lossy().into_owned()))
                .collect::<std::result::Result<Vec<_>, _>>()?
        } else {
            Vec::new()
        };
        names.sort_unstable();
        Ok(names)
    }
}
