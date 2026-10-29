#![cfg(test)]

mod tests {
    use std::{path::PathBuf, sync::Arc};

    use anyhow::{Context, Result};
    use mapache::{
        backend::localfs::LocalFS,
        repository::repo::{INDEX_DIR, OBJECTS_DIR},
    };

    use crate::{
        integration_tests::{
            INTEGRATION_TEST_DATA, TestContext, delete_all_files_from, set_write_permission,
        },
        synthetic::{Dataset, SyntheticData},
    };

    #[tokio::test]
    async fn test_rebuild_index() -> Result<()> {
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

        let verify_builder = ctx
            .verify_builder()
            .read_packs(true)
            .parallel(8)
            .fail_early(true);

        assert!(
            verify_builder.clone().run(&ctx.global).await.is_ok(),
            "First verify should pass"
        );

        let backend = Arc::new(LocalFS::new(ctx.repo_path.clone()));

        // Rebuild index and verify
        ctx.rebuild_index_builder().run(&ctx.global).await?;

        assert!(
            verify_builder.clone().run(&ctx.global).await.is_ok(),
            "Verify should pass after index is rebuilt"
        );

        // Delete the index to make it fail the next time.
        delete_all_files_from(backend.as_ref(), &PathBuf::from(INDEX_DIR))
            .await
            .context("Failed to remove the index")?;

        assert!(
            verify_builder.clone().run(&ctx.global).await.is_err(),
            "Verify should fail without an index"
        );

        // Rebuild index and verify
        ctx.rebuild_index_builder().run(&ctx.global).await?;

        assert!(
            verify_builder.run(&ctx.global).await.is_ok(),
            "Verify should pass after index is rebuilt"
        );

        Ok(())
    }

    /// When any pack fails to parse, rebuild-index must abort and keep the old
    /// index instead of persisting a partial index and deleting the original,
    /// which would orphan the blobs of the unreadable packs.
    #[tokio::test]
    async fn test_rebuild_index_preserves_old_index_on_pack_error() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
        let synthetic = SyntheticData::new(dataset);
        let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;

        // Init repo
        ctx.init_repo().await?;

        // Run snapshot
        ctx.snapshot_builder(vec![backup_data_tmp_path.join("0")])
            .no_scan(true)
            .run(&ctx.global)
            .await?;

        let index_dir = ctx.repo_path.join(INDEX_DIR);
        let list_index_files = || -> Result<Vec<String>> {
            let mut names: Vec<String> = std::fs::read_dir(&index_dir)?
                .map(|r| r.map(|e| e.file_name().to_string_lossy().into_owned()))
                .collect::<std::io::Result<Vec<_>>>()?;
            names.sort();
            Ok(names)
        };
        let index_before = list_index_files()?;
        assert!(!index_before.is_empty(), "repo should have an index");

        // Corrupt every pack so its footer can no longer be parsed.
        let objects_path = ctx.repo_path.join(OBJECTS_DIR);
        let mut corrupted = false;
        for entry in std::fs::read_dir(objects_path)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                for subentry in std::fs::read_dir(entry.path())? {
                    let subentry = subentry?;
                    let path = subentry.path();
                    let content = std::fs::read(&path)?;
                    if content.len() > 100 {
                        set_write_permission(&path, true)?;
                        std::fs::write(&path, vec![0xAB; content.len()])?;
                        corrupted = true;
                    }
                }
            }
        }
        assert!(corrupted, "Should have corrupted at least one pack file");

        // Rebuild must fail hard…
        let output = ctx.run_mapache(&["rebuild-index"])?;
        assert!(
            !output.status.success(),
            "rebuild-index should fail when packs are unreadable\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );

        // …and leave the original index untouched.
        let index_after = list_index_files()?;
        assert_eq!(
            index_before, index_after,
            "old index must be preserved when rebuild aborts"
        );

        Ok(())
    }

    /// A zero blob must stay recoverable by rebuild-index after GC removes the
    /// pack that held its original footer entry.
    #[tokio::test]
    async fn test_rebuild_index_preserves_zero_blob_without_footer() -> Result<()> {
        rebuild_after_zero_blob_clean(false).await
    }

    /// Same, but without an index to carry over: GC must have re-emitted the
    /// zero blob's footer entry into a surviving pack.
    #[tokio::test]
    async fn test_rebuild_index_from_scratch_recovers_zero_blob_after_clean() -> Result<()> {
        rebuild_after_zero_blob_clean(true).await
    }

    async fn rebuild_after_zero_blob_clean(delete_index: bool) -> Result<()> {
        let ctx = TestContext::new().await?;
        ctx.init_repo().await?;

        let data_dir = ctx._tmp_dir.path().join("data");
        std::fs::create_dir_all(&data_dir)?;
        let zero_file = data_dir.join("zero.bin");
        let other_file = data_dir.join("other.bin");
        std::fs::write(&zero_file, vec![0u8; 4096])?;
        std::fs::write(
            &other_file,
            (0..4096u32).map(|i| (i * 31 + 7) as u8).collect::<Vec<_>>(),
        )?;

        ctx.snapshot_builder(vec![data_dir.clone()])
            .no_scan(true)
            .run(&ctx.global)
            .await?;

        // The second snapshot dedups the zero blob, so its only footer entry
        // stays in the first data pack, which holds nothing else live once forgotten.
        std::fs::remove_file(&other_file)?;
        ctx.snapshot_builder(vec![data_dir.clone()])
            .no_scan(true)
            .run(&ctx.global)
            .await?;
        ctx.forget_builder().keep_last(1).run(&ctx.global).await?;
        ctx.clean_builder().run(&ctx.global).await?;

        let verify_builder = ctx.verify_builder().read_packs(true).fail_early(true);
        assert!(
            verify_builder.clone().run(&ctx.global).await.is_ok(),
            "verify should pass after clean"
        );

        if delete_index {
            let backend = LocalFS::new(ctx.repo_path.clone());
            delete_all_files_from(&backend, &PathBuf::from(INDEX_DIR))
                .await
                .context("Failed to remove the index")?;
        }

        ctx.rebuild_index_builder().run(&ctx.global).await?;

        assert!(
            verify_builder.run(&ctx.global).await.is_ok(),
            "verify should pass after rebuild-index"
        );

        Ok(())
    }
}
