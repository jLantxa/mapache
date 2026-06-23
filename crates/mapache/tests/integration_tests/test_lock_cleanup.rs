#![cfg(test)]

use std::{path::Path, sync::Arc};

use anyhow::Result;

use mapache::{
    backend::localfs::LocalFS,
    common::defaults::TEST_REPO_CONFIG,
    repository::repo::{LOCKS_DIR, Repository},
};

use crate::{
    integration_tests::{INTEGRATION_TEST_DATA, TestContext},
    synthetic::{Dataset, SyntheticData},
};

async fn wait_for_no_locks(locks_dir: &Path) -> Result<()> {
    for _ in 0..100 {
        if mapache::utils::count_files(locks_dir)? == 0 {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    anyhow::bail!("Lock file should be deleted");
}

#[tokio::test]
async fn test_lock_handle_drop() -> Result<()> {
    let ctx = TestContext::new().await?;
    ctx.init_repo().await?;
    let locks_dir = ctx.repo_path.join(LOCKS_DIR);

    {
        let (_repo, _, _lock_handle) = Repository::try_open_with_lock(
            &ctx.auth,
            None,
            Arc::new(LocalFS::new(ctx.repo_path.clone())),
            TEST_REPO_CONFIG,
            true,
            None,
        )
        .await?;
        assert_eq!(mapache::utils::count_files(&locks_dir)?, 1);
    }

    wait_for_no_locks(&locks_dir).await?;
    Ok(())
}

#[tokio::test]
async fn test_commands_lock_cleanup() -> Result<()> {
    let mut ctx = TestContext::new().await?;
    let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
    let synthetic = SyntheticData::new(dataset);
    let backup_data_tmp_path = ctx.setup_backup_data(&synthetic)?;
    ctx.init_repo().await?;
    let locks_dir = ctx.repo_path.join(LOCKS_DIR);

    ctx.snapshot_builder(vec![backup_data_tmp_path.clone()])
        .tags("[]".to_string())
        .num_readers(1)
        .num_packers(1)
        .run(&ctx.global)
        .await?;
    wait_for_no_locks(&locks_dir).await?;

    let snapshot_id_hex = ctx
        .get_snapshot_ids()?
        .first()
        .cloned()
        .expect("Snapshot should have been created");

    ctx.log_builder().compact(true).run(&ctx.global).await?;
    wait_for_no_locks(&locks_dir).await?;

    ctx.stats_builder().run(&ctx.global).await?;
    wait_for_no_locks(&locks_dir).await?;

    ctx.verify_builder().run(&ctx.global).await?;
    wait_for_no_locks(&locks_dir).await?;

    ctx.cat_builder(mapache::commands::cmd_cat::Object::Snapshot(
        snapshot_id_hex.clone(),
    ))
    .run(&ctx.global)
    .await?;
    wait_for_no_locks(&locks_dir).await?;

    Ok(())
}
