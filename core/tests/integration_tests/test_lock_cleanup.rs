#![cfg(test)]

use crate::integration_tests::TestContext;
use anyhow::Result;
use mapache::mapache::defaults::TEST_REPO_CONFIG;
use mapache::repository::repo::{LOCKS_DIR, Repository};
use std::path::Path;
use std::sync::Arc;
use tokio::time::{Duration, sleep};

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
    use mapache::commands::*;

    let mut ctx = TestContext::new().await?;
    ctx.init_repo().await?;
    let locks_dir = ctx.repo_path.join(LOCKS_DIR);

    // Snapshot
    ctx.setup_backup_data()?;
    let snapshot_args = cmd_snapshot::CmdArgs {
        paths: vec![ctx.backup_data_path.as_ref().unwrap().clone()],
        as_root: false,
        exclude: None,
        tags_str: "[]".to_string(),
        description: None,
        no_parent: false,
        no_scan: false,
        skip_if_unchanged: false,
        parent: UseSnapshot::Latest,
        num_readers: 1,
        num_packers: 1,
        dry_run: false,
    };
    cmd_snapshot::run(&ctx.global, &snapshot_args).await?;
    wait_for_no_locks(&locks_dir).await?;

    // Get the snapshot ID
    let snapshot_id_hex = {
        let backend = Arc::new(mapache::backend::localfs::LocalFS::new(
            ctx.repo_path.clone(),
        ));
        let (repo, _) =
            Repository::try_open_unlocked(&ctx.auth, None, backend, TEST_REPO_CONFIG).await?;
        let ids = repo.list_snapshot_ids().await?;
        ids.first()
            .expect("Snapshot should have been created")
            .to_hex()
    };

    // Log
    let log_args = cmd_log::CmdArgs {
        snapshot: None,
        dropped: false,
        all: false,
        compact: true,
        tags_str: None,
    };
    cmd_log::run(&ctx.global, &log_args).await?;
    wait_for_no_locks(&locks_dir).await?;

    // Stats
    let stats_args = cmd_stats::CmdArgs { full: false };
    cmd_stats::run(&ctx.global, &stats_args).await?;
    wait_for_no_locks(&locks_dir).await?;

    // Verify
    let verify_args = cmd_verify::CmdArgs {
        read_packs: false,
        parallel: 1,
        with_cache: false,
        fail_early: false,
        sample: None,
    };
    cmd_verify::run(&ctx.global, &verify_args).await?;
    wait_for_no_locks(&locks_dir).await?;

    // Cat
    let cat_args = cmd_cat::CmdArgs {
        object: cmd_cat::Object::Snapshot(snapshot_id_hex.clone()),
    };
    cmd_cat::run(&ctx.global, &cat_args).await?;
    wait_for_no_locks(&locks_dir).await?;

    Ok(())
}
