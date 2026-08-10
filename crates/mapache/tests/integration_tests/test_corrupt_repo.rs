#![cfg(test)]

mod tests {
    use std::fs;

    use anyhow::Result;

    use mapache::repository::repo::{KEYS_DIR, OBJECTS_DIR, SNAPSHOTS_DIR};

    use crate::{
        integration_tests::{INTEGRATION_TEST_DATA, TestContext, set_write_permission},
        synthetic::{Dataset, SyntheticData},
    };
    /// Truncating a pack file should cause verify to fail.
    #[tokio::test]
    async fn test_verify_truncated_pack() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        ctx.init_repo().await?;

        ctx.snapshot_builder(vec![backup_data_tmp_path.clone()])
            .no_parent(true)
            .no_scan(true)
            .num_readers(1)
            .num_packers(1)
            .run(&ctx.global)
            .await?;

        // Find and truncate a pack file
        let objects_path = ctx.repo_path.join(OBJECTS_DIR);
        let mut truncated = false;
        for entry in fs::read_dir(objects_path)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                for subentry in fs::read_dir(entry.path())? {
                    let subentry = subentry?;
                    let path = subentry.path();
                    let meta = fs::metadata(&path)?;
                    if meta.len() > 100 {
                        set_write_permission(&path, true)?;
                        let content = fs::read(&path)?;
                        let half = content.len() / 2;
                        fs::write(&path, &content[..half])?;
                        truncated = true;
                        break;
                    }
                }
            }
            if truncated {
                break;
            }
        }

        assert!(truncated, "Should have truncated at least one pack file");

        let res = ctx
            .verify_builder()
            .read_packs(true)
            .parallel(1)
            .fail_early(true)
            .run(&ctx.global)
            .await;
        assert!(
            res.is_err(),
            "Verify should fail when a pack file is truncated"
        );

        Ok(())
    }

    /// Deleting a key file should cause verify to fail.
    #[tokio::test]
    async fn test_verify_missing_key() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        ctx.init_repo().await?;

        ctx.snapshot_builder(vec![backup_data_tmp_path.clone()])
            .no_parent(true)
            .no_scan(true)
            .num_readers(1)
            .num_packers(1)
            .run(&ctx.global)
            .await?;

        let keys_path = ctx.repo_path.join(KEYS_DIR);
        if keys_path.exists() {
            for entry in fs::read_dir(&keys_path)? {
                let entry = entry?;
                set_write_permission(entry.path(), true)?;
                fs::remove_file(entry.path())?;
            }
        }

        let res = ctx
            .verify_builder()
            .read_packs(true)
            .parallel(1)
            .fail_early(true)
            .run(&ctx.global)
            .await;
        assert!(
            res.is_err(),
            "Verify should fail when key files are deleted"
        );

        Ok(())
    }

    /// After pack corruption, a new backup should still succeed (the corrupted
    /// pack is still there but new data goes to new packs).
    #[tokio::test]
    async fn test_backup_succeeds_after_pack_corruption() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        ctx.init_repo().await?;

        // First backup
        ctx.snapshot_builder(vec![backup_data_tmp_path.clone()])
            .no_parent(true)
            .no_scan(true)
            .num_readers(1)
            .num_packers(1)
            .run(&ctx.global)
            .await?;

        // Corrupt a pack file
        let objects_path = ctx.repo_path.join(OBJECTS_DIR);
        for entry in fs::read_dir(objects_path)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                for subentry in fs::read_dir(entry.path())? {
                    let subentry = subentry?;
                    let mut content = fs::read(subentry.path())?;
                    if !content.is_empty() {
                        set_write_permission(subentry.path(), true)?;
                        content[0] = !content[0];
                        fs::write(subentry.path(), content)?;
                        // Corrupt one pack and break
                        break;
                    }
                }
                break;
            }
        }

        // Second backup with new data should still succeed.
        // Use no_parent(true) so the backup doesn't need to read old tree blobs
        // from potentially corrupted packs — only new packs are written.
        fs::write(backup_data_tmp_path.join("new_file.txt"), b"brand new data")?;

        ctx.snapshot_builder(vec![backup_data_tmp_path.clone()])
            .no_parent(true)
            .run(&ctx.global)
            .await?;

        let ids = ctx.get_snapshot_ids()?;
        assert_eq!(
            ids.len(),
            2,
            "Should have 2 snapshots after backup post-corruption"
        );

        Ok(())
    }

    /// After corrupting a snapshot file, new backups should still succeed.
    #[tokio::test]
    async fn test_corrupt_snapshot_does_not_block_new_backup() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        ctx.init_repo().await?;

        // First backup
        ctx.snapshot_builder(vec![backup_data_tmp_path.clone()])
            .no_parent(true)
            .no_scan(true)
            .num_readers(1)
            .num_packers(1)
            .run(&ctx.global)
            .await?;

        let ids1 = ctx.get_snapshot_ids()?;
        assert_eq!(ids1.len(), 1);

        // Corrupt the first snapshot
        let snapshots_path = ctx.repo_path.join(SNAPSHOTS_DIR);
        let mut corrupted = false;
        for entry in fs::read_dir(snapshots_path)? {
            let entry = entry?;
            let id_str = entry.file_name().to_string_lossy().to_string();
            if id_str == ids1[0] {
                let mut content = fs::read(entry.path())?;
                if content.len() > 5 {
                    set_write_permission(entry.path(), true)?;
                    content[0] = !content[0];
                    fs::write(entry.path(), content)?;
                    corrupted = true;
                }
            }
        }

        assert!(corrupted, "Should have corrupted the first snapshot");

        // New backup should still succeed (explicitly no parent to avoid
        // trying to load the corrupted snapshot as parent)
        fs::write(backup_data_tmp_path.join("extra.txt"), b"more data")?;
        ctx.snapshot_builder(vec![backup_data_tmp_path.clone()])
            .no_parent(true)
            .run(&ctx.global)
            .await?;

        let ids2 = ctx.get_snapshot_ids()?;
        // The new snapshot should exist (plus possibly the old one on disk)
        assert!(
            !ids2.is_empty(),
            "Should have at least one snapshot after backup post-corruption"
        );

        Ok(())
    }
}
