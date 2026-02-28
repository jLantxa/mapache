#![cfg(test)]

mod tests {
    use std::fs;

    use anyhow::{Context, Result};
    use rand::Rng;

    use mapache::{
        commands::{self, UseSnapshot, cmd_snapshot, cmd_verify},
        repository::repo::{OBJECTS_DIR, SNAPSHOTS_DIR},
    };

    use crate::integration_tests::{TestContext, set_write_permission};

    #[tokio::test]
    async fn test_verify_missing_object() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        ctx.setup_backup_data()?;
        let backup_data_tmp_path = ctx.backup_data_path.as_ref().unwrap();

        ctx.init_repo().await?;

        let snapshot_args = cmd_snapshot::CmdArgs {
            paths: vec![backup_data_tmp_path.join("file.txt")],
            as_root: false,
            exclude: None,
            tags_str: String::new(),
            description: None,
            no_parent: true,
            skip_if_unchanged: false,
            no_scan: true,
            parent: UseSnapshot::Latest,
            num_readers: 1,
            num_packers: 1,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&ctx.global, &snapshot_args).await?;

        // Verify initial state
        let verify_args = cmd_verify::CmdArgs {
            read_packs: true,
            parallel: 1,
            with_cache: false,
            fail_early: true,
            sample: None,
        };
        commands::cmd_verify::run(&ctx.global, &verify_args)
            .await
            .context("Initial verify failed")?;

        // Delete a pack file
        let mut deleted = false;
        let objects_path = ctx.repo_path.join(OBJECTS_DIR);
        for entry in fs::read_dir(objects_path)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                for subentry in fs::read_dir(entry.path())? {
                    let subentry = subentry?;
                    fs::remove_file(subentry.path())?;
                    deleted = true;
                    break;
                }
            }
            if deleted {
                break;
            }
        }

        assert!(deleted, "Should have deleted at least one pack file");

        let res = commands::cmd_verify::run(&ctx.global, &verify_args).await;
        assert!(res.is_err(), "Verify should fail when an object is missing");

        Ok(())
    }

    #[tokio::test]
    async fn test_verify_unreadable_snapshot() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        ctx.setup_backup_data()?;
        let backup_data_tmp_path = ctx.backup_data_path.as_ref().unwrap();

        ctx.init_repo().await?;

        let snapshot_args = cmd_snapshot::CmdArgs {
            paths: vec![backup_data_tmp_path.join("file.txt")],
            as_root: false,
            exclude: None,
            tags_str: String::new(),
            description: None,
            no_parent: true,
            skip_if_unchanged: false,
            no_scan: true,
            parent: UseSnapshot::Latest,
            num_readers: 1,
            num_packers: 1,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&ctx.global, &snapshot_args).await?;

        // Corrupt snapshot file (make it unparseable)
        let snapshots_path = ctx.repo_path.join(SNAPSHOTS_DIR);
        let mut corrupted = false;
        for entry in fs::read_dir(snapshots_path)? {
            let entry = entry?;
            let mut content = fs::read(entry.path())?;
            if !content.is_empty() {
                set_write_permission(&entry.path(), true)?;
                content[0] = !content[0]; // Flip bits to break JSON/encrytion
                fs::write(entry.path(), content)?;
                corrupted = true;
                break;
            }
        }

        assert!(corrupted, "Should have corrupted a snapshot file");

        let verify_args = cmd_verify::CmdArgs {
            read_packs: true,
            parallel: 1,
            with_cache: false,
            fail_early: true,
            sample: None,
        };
        let res = commands::cmd_verify::run(&ctx.global, &verify_args).await;
        assert!(
            res.is_err(),
            "Verify should fail when a snapshot is corrupt/unreadable"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_verify_bit_flip_in_snapshot() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        ctx.setup_backup_data()?;
        let backup_data_tmp_path = ctx.backup_data_path.as_ref().unwrap();

        ctx.init_repo().await?;

        let snapshot_args = cmd_snapshot::CmdArgs {
            paths: vec![backup_data_tmp_path.join("file.txt")],
            as_root: false,
            exclude: None,
            tags_str: String::new(),
            description: None,
            no_parent: true,
            skip_if_unchanged: false,
            no_scan: true,
            parent: UseSnapshot::Latest,
            num_readers: 1,
            num_packers: 1,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&ctx.global, &snapshot_args).await?;

        // Flip a bit in the middle of the snapshot file
        let snapshots_path = ctx.repo_path.join(SNAPSHOTS_DIR);
        let mut corrupted = false;
        for entry in fs::read_dir(snapshots_path)? {
            let entry = entry?;
            let mut content = fs::read(entry.path())?;
            if content.len() > 10 {
                set_write_permission(&entry.path(), true)?;
                content[10] = content[10].wrapping_add(1);
                fs::write(entry.path(), content)?;
                corrupted = true;
                break;
            }
        }

        assert!(corrupted, "Should have flipped a bit in a snapshot file");

        let verify_args = cmd_verify::CmdArgs {
            read_packs: true,
            parallel: 1,
            with_cache: false,
            fail_early: true,
            sample: None,
        };
        let res = commands::cmd_verify::run(&ctx.global, &verify_args).await;
        assert!(
            res.is_err(),
            "Verify should fail when a snapshot has bit-flips"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_verify_corrupt_pack() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        ctx.setup_backup_data()?;
        let backup_data_tmp_path = ctx.backup_data_path.as_ref().unwrap();

        ctx.init_repo().await?;

        // Create a snapshot with enough data to ensure a pack is created
        let snapshot_args = cmd_snapshot::CmdArgs {
            paths: vec![backup_data_tmp_path.clone()],
            as_root: false,
            exclude: None,
            tags_str: String::new(),
            description: None,
            no_parent: true,
            skip_if_unchanged: false,
            no_scan: true,
            parent: UseSnapshot::Latest,
            num_readers: 1,
            num_packers: 1,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&ctx.global, &snapshot_args).await?;

        // Corrupt pack files randomly
        let objects_path = ctx.repo_path.join(OBJECTS_DIR);
        let mut corrupted_count = 0;
        let mut all_packs = Vec::new();

        for entry in fs::read_dir(objects_path)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                for subentry in fs::read_dir(entry.path())? {
                    all_packs.push(subentry?.path());
                }
            }
        }

        let mut rng = rand::rng();

        for pack_path in &all_packs {
            // 50% chance to corrupt this pack
            if rng.random_bool(0.5) {
                let mut content = fs::read(pack_path)?;
                if !content.is_empty() {
                    set_write_permission(pack_path, true)?;
                    let idx = rng.random_range(0..content.len());
                    content[idx] = !content[idx];
                    fs::write(pack_path, content)?;
                    corrupted_count += 1;
                }
            }
        }

        // If by chance no packs were corrupted, force corrupt at least one
        if corrupted_count == 0 && !all_packs.is_empty() {
            let pack_path = &all_packs[0];
            let mut content = fs::read(pack_path)?;
            if !content.is_empty() {
                set_write_permission(pack_path, true)?;
                let idx = rng.random_range(0..content.len());
                content[idx] = !content[idx];
                fs::write(pack_path, content)?;
                corrupted_count += 1;
            }
        }

        assert!(corrupted_count > 0, "At least one pack should have been corrupted");

        let verify_args = cmd_verify::CmdArgs {
            read_packs: true,
            parallel: 1,
            with_cache: false,
            fail_early: true,
            sample: None,
        };

        let res = commands::cmd_verify::run(&ctx.global, &verify_args).await;
        assert!(
            res.is_err(),
            "Verify should fail when a pack file is corrupt"
        );

        Ok(())
    }
}
