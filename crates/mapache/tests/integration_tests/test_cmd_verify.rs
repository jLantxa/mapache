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
        repository::repo::{INDEX_DIR, OBJECTS_DIR},
    };

    use crate::{
        integration_tests::{
            INTEGRATION_TEST_DATA, TestContext, delete_all_files_from, set_write_permission,
        },
        synthetic::{Dataset, SyntheticData},
    };

    #[tokio::test]
    async fn test_verify_links() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

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

        let first_verify_result = ctx
            .verify_builder()
            .read_packs(true)
            .parallel(8)
            .fail_early(true)
            .run(&ctx.global)
            .await;
        assert!(first_verify_result.is_ok(), "First verify should pass");

        let backend = Arc::new(LocalFS::new(ctx.repo_path.clone()));

        // Delete the index to make it fail the next time.
        delete_all_files_from(backend.as_ref(), &PathBuf::from(INDEX_DIR))
            .await
            .context("Failed to remove the index")?;
        let second_verify_result = ctx
            .verify_builder()
            .read_packs(true)
            .parallel(8)
            .fail_early(true)
            .run(&ctx.global)
            .await;
        assert!(
            second_verify_result.is_err(),
            "Verify should fail without an index"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_verify_snapshots() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

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

        let first_verify_result = ctx
            .verify_builder()
            .read_packs(true)
            .parallel(8)
            .fail_early(true)
            .run(&ctx.global)
            .await;
        assert!(first_verify_result.is_ok(), "Verify should pass");

        let backend = Arc::new(LocalFS::new(ctx.repo_path.clone()));

        // Delete the packs to make it fail the next time.
        delete_all_files_from(backend.as_ref(), &PathBuf::from(OBJECTS_DIR))
            .await
            .context("Failed to remove the objects")?;
        let second_verify_result = ctx
            .verify_builder()
            .read_packs(true)
            .parallel(8)
            .fail_early(true)
            .run(&ctx.global)
            .await;
        assert!(
            second_verify_result.is_err(),
            "Verify should fail without the packs"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_verify_packs() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

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

        let first_verify_result = ctx
            .verify_builder()
            .read_packs(true)
            .parallel(8)
            .fail_early(true)
            .run(&ctx.global)
            .await;
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

            let bit_flip_verify_result = ctx
                .verify_builder()
                .read_packs(true)
                .parallel(8)
                .fail_early(true)
                .run(&ctx.global)
                .await;
            assert!(
                bit_flip_verify_result.is_err(),
                "Verify should fail because of bit-flip (decryption error)"
            );
        }

        // Delete the packs to make it fail the next time.
        delete_all_files_from(backend.as_ref(), &PathBuf::from(OBJECTS_DIR))
            .await
            .context("Failed to remove the objects")?;
        let second_verify_result = ctx
            .verify_builder()
            .read_packs(true)
            .parallel(8)
            .fail_early(true)
            .run(&ctx.global)
            .await;
        assert!(
            second_verify_result.is_err(),
            "Verify should fail without the packs (no root tree)"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_verify_sample() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        ctx.init_repo().await?;

        // Run snapshot
        ctx.snapshot_builder(vec![backup_data_tmp_path.join("file.txt")])
            .no_parent(true)
            .no_scan(true)
            .num_readers(1)
            .num_packers(1)
            .run(&ctx.global)
            .await?;

        // Verify with 50% sample
        let verify_result = ctx
            .verify_builder()
            .read_packs(true)
            .parallel(1)
            .fail_early(true)
            .sample(Some(50.0))
            .run(&ctx.global)
            .await;
        assert!(verify_result.is_ok(), "Verify with sample should pass");

        Ok(())
    }

    #[tokio::test]
    async fn test_binary_index_on_disk() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        ctx.init_repo().await?;

        ctx.snapshot(vec![
            backup_data_tmp_path.join("file.txt"),
            backup_data_tmp_path.join("0"),
        ])
        .await
        .context("snapshot failed")?;

        let index_dir = ctx.repo_path.join(INDEX_DIR);
        assert!(index_dir.exists(), "index directory should exist");

        let index_files: Vec<_> = std::fs::read_dir(&index_dir)?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_file())
            .collect();

        assert!(
            !index_files.is_empty(),
            "there should be at least one index file"
        );

        // v2 index files are binary, not JSON
        for entry in &index_files {
            let bytes = std::fs::read(entry.path())?;
            assert!(
                !bytes.is_empty(),
                "index file should not be empty: {:?}",
                entry.path()
            );
            assert_ne!(
                bytes[0],
                b'{',
                "v2 index should be binary, not JSON: {:?}",
                entry.path()
            );
        }

        // Restore should work (index is readable)
        let restore_path = ctx._tmp_dir.path().join("restore");
        ctx.restore_builder(restore_path.clone())
            .run(&ctx.global)
            .await
            .context("restore with binary index failed")?;

        assert!(restore_path.join("file.txt").exists());

        Ok(())
    }

    #[tokio::test]
    async fn test_verify_after_compression_none() -> Result<()> {
        use mapache::commands::Compression;

        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        ctx.init_repo().await?;

        let mut global = ctx.global.clone();
        global.compression_level = Compression::None;

        ctx.snapshot_builder(vec![
            backup_data_tmp_path.join("file.txt"),
            backup_data_tmp_path.join("0"),
            backup_data_tmp_path.join("1"),
        ])
        .run(&global)
        .await
        .context("snapshot with --compression none failed")?;

        ctx.verify_builder()
            .read_packs(true)
            .run(&global)
            .await
            .context("verify failed after --compression none snapshot")?;

        Ok(())
    }
}
