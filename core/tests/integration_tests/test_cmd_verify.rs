#![cfg(test)]

mod tests {
    use std::{
        fs::OpenOptions,
        io::{Read, Seek, SeekFrom, Write},
        path::PathBuf,
        sync::Arc,
    };

    use anyhow::{Context, Ok, Result};

    use mapache::{
        backend::{BackendNode, localfs::LocalFS, read_backend_dir},
        commands::{self, UseSnapshot, cmd_snapshot, cmd_verify},
        repository::repo::{INDEX_DIR, OBJECTS_DIR},
    };

    use crate::integration_tests::{TestContext, delete_all_files_from, set_write_permission};

    #[tokio::test]
    async fn test_verify_links() -> Result<()> {
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
            exclude_file: None,
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

        let verify_args = cmd_verify::CmdArgs {
            read_packs: true,
            parallel: 8,
            with_cache: false,
            fail_early: true,
            sample: None,
        };
        let first_verify_result = commands::cmd_verify::run(&ctx.global, &verify_args).await;
        assert!(first_verify_result.is_ok(), "First verify should pass");

        let backend = Arc::new(LocalFS::new(ctx.repo_path.clone()));

        // Delete the index to make it fail the next time.
        delete_all_files_from(backend.as_ref(), &PathBuf::from(INDEX_DIR))
            .await
            .context("Failed to remove the index")?;
        let second_verify_result = commands::cmd_verify::run(&ctx.global, &verify_args).await;
        assert!(
            second_verify_result.is_err(),
            "Verify should fail without an index"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_verify_snapshots() -> Result<()> {
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
            exclude_file: None,
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

        let verify_args = cmd_verify::CmdArgs {
            read_packs: true,
            parallel: 8,
            with_cache: false,
            fail_early: true,
            sample: None,
        };
        let first_verify_result = commands::cmd_verify::run(&ctx.global, &verify_args).await;
        assert!(first_verify_result.is_ok(), "Verify should pass");

        let backend = Arc::new(LocalFS::new(ctx.repo_path.clone()));

        // Delete the packs to make it fail the next time.
        delete_all_files_from(backend.as_ref(), &PathBuf::from(OBJECTS_DIR))
            .await
            .context("Failed to remove the objects")?;
        let second_verify_result = commands::cmd_verify::run(&ctx.global, &verify_args).await;
        assert!(
            second_verify_result.is_err(),
            "Verify should fail without the packs"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_verify_packs() -> Result<()> {
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
            exclude_file: None,
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

        let verify_args = cmd_verify::CmdArgs {
            read_packs: true,
            parallel: 8,
            with_cache: false,
            fail_early: true,
            sample: None,
        };
        let first_verify_result = commands::cmd_verify::run(&ctx.global, &verify_args).await;
        assert!(first_verify_result.is_ok(), "Verify should pass");

        let backend = Arc::new(LocalFS::new(ctx.repo_path.clone()));

        // Bit flips should make it fail
        {
            let backend_nodes =
                read_backend_dir(backend.as_ref(), &ctx.repo_path.join(OBJECTS_DIR)).await?;

            let first_pack_path = &ctx.repo_path.join(
                backend_nodes
                    .iter()
                    .find(|&node| matches!(node, BackendNode::File(_, _)))
                    .expect("There should at least be one pack")
                    .path(),
            );

            set_write_permission(first_pack_path, true)?;

            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(first_pack_path)?;

            let mut first_byte = [0u8; 1];
            file.read_exact(&mut first_byte)?;
            first_byte[0] = !first_byte[0];
            file.seek(SeekFrom::Start(0))?;
            file.write_all(&first_byte)?;

            let bit_flip_verify_result = commands::cmd_verify::run(&ctx.global, &verify_args).await;
            assert!(
                bit_flip_verify_result.is_err(),
                "Verify should fail because of bit-flip (decryption error)"
            );
        }

        // Delete the packs to make it fail the next time.
        delete_all_files_from(backend.as_ref(), &PathBuf::from(OBJECTS_DIR))
            .await
            .context("Failed to remove the objects")?;
        let second_verify_result = commands::cmd_verify::run(&ctx.global, &verify_args).await;
        assert!(
            second_verify_result.is_err(),
            "Verify should fail without the packs (no root tree)"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_verify_sample() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        ctx.setup_backup_data()?;
        let backup_data_tmp_path = ctx.backup_data_path.as_ref().unwrap();

        ctx.global.verbosity = Some(1);
        mapache::mapache::global::set_global_opts_with_args(&ctx.global);

        ctx.init_repo().await?;

        // Run snapshot
        let snapshot_args = cmd_snapshot::CmdArgs {
            paths: vec![backup_data_tmp_path.join("file.txt")],
            as_root: false,
            exclude: None,
            exclude_file: None,
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

        // Verify with 50% sample
        let verify_args = cmd_verify::CmdArgs {
            read_packs: true,
            parallel: 1,
            with_cache: false,
            fail_early: true,
            sample: Some(50.0),
        };
        let verify_result = commands::cmd_verify::run(&ctx.global, &verify_args).await;
        assert!(verify_result.is_ok(), "Verify with sample should pass");

        Ok(())
    }
}
