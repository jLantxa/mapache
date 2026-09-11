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
        backend::{
            BackendNode, Handle, StorageBackend, WriteContents, localfs::LocalFS, read_backend_dir,
        },
        repository::{
            manifest::EccConfig,
            repo::{INDEX_DIR, OBJECTS_DIR, Repository, SNAPSHOTS_DIR},
        },
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

    #[tokio::test]
    async fn test_verify_ecc_repair() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        // Init repo with ECC enabled (50% overhead).
        let ecc_config = EccConfig::from_overhead(50);
        let backend = Arc::new(LocalFS::new(ctx.repo_path.clone()));
        let _ = Repository::init(
            mapache::repository::repo::THIS_REPOSITORY_VERSION,
            &ctx.auth,
            None,
            backend.clone(),
            ecc_config,
            false,
        )
        .await
        .context("Failed to init repo with ECC")?;

        // Snapshot some data.
        ctx.snapshot_builder(vec![
            backup_data_tmp_path.join("file.txt"),
            backup_data_tmp_path.join("0"),
            backup_data_tmp_path.join("1"),
        ])
        .no_scan(true)
        .run(&ctx.global)
        .await
        .context("snapshot failed")?;

        // Verify should pass initially.
        ctx.verify_builder()
            .read_packs(true)
            .run(&ctx.global)
            .await
            .context("initial verify should pass")?;

        // Find a pack file and corrupt it.
        let objects_dir = PathBuf::from(mapache::repository::repo::OBJECTS_DIR);
        let entries = read_backend_dir(backend.as_ref(), &objects_dir).await?;
        let mut pack_path: Option<PathBuf> = None;
        for entry in entries {
            if let BackendNode::File(path, _) = entry {
                let name = path.file_name().map(|n| n.to_string_lossy().to_string());
                if let Some(name) = name
                    && !name.ends_with(".ecc")
                    && !name.ends_with(".tmp")
                {
                    pack_path = Some(path);
                    break;
                }
            }
        }
        let pack_path = pack_path.context("no pack file found")?;

        // Corrupt the first byte of the pack.
        let handle = Handle::new(&pack_path);
        let mut data = backend.read(&handle, 0, 0).await?.to_vec();
        assert!(!data.is_empty(), "pack file should not be empty");
        data[0] ^= 0xFF;
        backend.write(&handle, WriteContents::Owned(data)).await?;

        // Verify without repair should fail (bit-rot).
        let result = ctx.verify_builder().read_packs(true).run(&ctx.global).await;
        assert!(result.is_err(), "verify should fail after corruption");

        // Verify with repair should succeed (ECC fixes the corruption).
        ctx.verify_builder()
            .read_packs(true)
            .repair(true)
            .run(&ctx.global)
            .await
            .context("verify --repair should succeed after ECC repair")?;

        Ok(())
    }

    #[tokio::test]
    async fn test_verify_ecc_metadata_repair() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        // Init repo with ECC enabled (50% overhead).
        let ecc_config = EccConfig::from_overhead(50);
        let backend = Arc::new(LocalFS::new(ctx.repo_path.clone()));
        let _ = Repository::init(
            mapache::repository::repo::THIS_REPOSITORY_VERSION,
            &ctx.auth,
            None,
            backend.clone(),
            ecc_config,
            false,
        )
        .await
        .context("Failed to init repo with ECC")?;

        // Snapshot some data.
        ctx.snapshot_builder(vec![
            backup_data_tmp_path.join("file.txt"),
            backup_data_tmp_path.join("0"),
        ])
        .no_scan(true)
        .run(&ctx.global)
        .await
        .context("snapshot failed")?;

        // Verify should pass initially.
        ctx.verify_builder()
            .run(&ctx.global)
            .await
            .context("initial verify should pass")?;

        // Find a snapshot file (not its .ecc sidecar) and corrupt it.
        let snapshots_dir = PathBuf::from(SNAPSHOTS_DIR);
        let entries = read_backend_dir(backend.as_ref(), &snapshots_dir).await?;
        let mut snapshot_path: Option<PathBuf> = None;
        for entry in entries {
            if let BackendNode::File(path, _) = entry {
                let name = path.file_name().map(|n| n.to_string_lossy().to_string());
                if let Some(name) = name
                    && !name.ends_with(".ecc")
                    && !name.ends_with(".tmp")
                {
                    snapshot_path = Some(path);
                    break;
                }
            }
        }
        let snapshot_path = snapshot_path.context("no snapshot file found")?;

        // Flip a byte in the middle of the snapshot file.
        let handle = Handle::new(&snapshot_path);
        let mut data = backend.read(&handle, 0, 0).await?.to_vec();
        assert!(!data.is_empty(), "snapshot file should not be empty");
        let mid = data.len() / 2;
        data[mid] ^= 0xFF;
        backend.write(&handle, WriteContents::Owned(data)).await?;

        // Verify without repair should fail (metadata bit-rot detected).
        let result = ctx.verify_builder().run(&ctx.global).await;
        assert!(
            result.is_err(),
            "verify should fail after snapshot file corruption"
        );

        // Verify with repair should succeed (ECC fixes the metadata file).
        ctx.verify_builder()
            .repair(true)
            .run(&ctx.global)
            .await
            .context("verify --repair should succeed after ECC metadata repair")?;

        // The repaired snapshot file must be usable again.
        ctx.verify_builder()
            .run(&ctx.global)
            .await
            .context("verify should pass after metadata repair")?;

        Ok(())
    }

    #[tokio::test]
    async fn test_verify_dump_pack_blobs() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        ctx.init_repo().await?;

        ctx.snapshot_builder(vec![
            backup_data_tmp_path.join("0"),
            backup_data_tmp_path.join("1"),
            backup_data_tmp_path.join("2"),
            backup_data_tmp_path.join("file.txt"),
        ])
        .no_scan(true)
        .run(&ctx.global)
        .await?;

        let dump_path = ctx._tmp_dir.path().join("pack_blobs.txt");

        ctx.verify_builder()
            .dump_pack_blobs(dump_path.clone())
            .run(&ctx.global)
            .await
            .context("verify with --dump-pack-blobs should succeed")?;

        let contents = std::fs::read_to_string(&dump_path)?;
        let lines: Vec<&str> = contents.lines().filter(|l| !l.is_empty()).collect();
        assert!(!lines.is_empty(), "dump file should not be empty");

        for line in &lines {
            let parts: Vec<&str> = line.split(' ').collect();
            assert_eq!(
                parts.len(),
                3,
                "each line should have 3 fields (id type pack_id): {line}"
            );
            // blob id should be 64 hex chars (sha-256)
            assert_eq!(parts[0].len(), 64, "blob id should be 64 hex chars: {line}");
            // blob type must be one of the known pack-stored types
            assert!(
                matches!(parts[1], "Data" | "Tree" | "Zero"),
                "unexpected blob type: {line}"
            );
            // pack id should also be 64 hex chars
            assert_eq!(parts[2].len(), 64, "pack id should be 64 hex chars: {line}");
        }

        Ok(())
    }
}
