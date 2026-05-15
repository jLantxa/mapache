#![cfg(test)]

use std::{path::Path, sync::Arc};

use anyhow::Result;
use tokio::time::{Duration, sleep};

use mapache::{
    mapache::defaults::TEST_REPO_CONFIG,
    repository::repo::{LOCKS_DIR, Repository},
};

use crate::integration_tests::TestContext;

async fn wait_for_no_locks(locks_dir: &Path) -> Result<()> {
    for _ in 0..100 {
        if mapache::utils::count_files(locks_dir)? == 0 {
            return Ok(());
        }
        sleep(Duration::from_millis(20)).await;
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
            Arc::new(mapache::backend::localfs::LocalFS::new(
                ctx.repo_path.clone(),
            )),
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
    ctx.init_repo().await?;
    let locks_dir = ctx.repo_path.join(LOCKS_DIR);

    // Snapshot
    ctx.setup_backup_data()?;
    ctx.snapshot_builder(vec![ctx.backup_data_path.as_ref().unwrap().clone()])
        .tags("[]".to_string())
        .num_readers(1)
        .num_packers(1)
        .run(&ctx.global)
        .await?;
    wait_for_no_locks(&locks_dir).await?;

    // Get the snapshot ID
    let snapshot_id_hex = ctx
        .get_snapshot_ids()?
        .first()
        .cloned()
        .expect("Snapshot should have been created");

    // Log
    ctx.log_builder().compact(true).run(&ctx.global).await?;
    wait_for_no_locks(&locks_dir).await?;

    // Stats
    ctx.stats_builder().run(&ctx.global).await?;
    wait_for_no_locks(&locks_dir).await?;

    // Verify
    ctx.verify_builder().run(&ctx.global).await?;
    wait_for_no_locks(&locks_dir).await?;

    // Cat
    ctx.cat_builder(mapache::commands::cmd_cat::Object::Snapshot(
        snapshot_id_hex.clone(),
    ))
    .run(&ctx.global)
    .await?;
    wait_for_no_locks(&locks_dir).await?;

    Ok(())
}
