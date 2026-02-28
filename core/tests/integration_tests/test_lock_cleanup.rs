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
        sleep(Duration::from_millis(50)).await;
    }
    anyhow::bail!("Lock file should be deleted");
}

#[tokio::test]
async fn test_lock_handle_drop() -> Result<()> {
    let ctx = TestContext::new().await?;
    ctx.init_repo().await?;

    let backend = Arc::new(mapache::backend::localfs::LocalFS::new(
        ctx.repo_path.clone(),
    ));

    let locks_dir = ctx.repo_path.join(LOCKS_DIR);

    {
        let (_repo, _, _lock_handle) = Repository::try_open_with_lock(
            Some(&ctx.auth),
            None,
            backend.clone(),
            TEST_REPO_CONFIG,
            true,
            None,
        )
        .await?;

        assert_eq!(
            mapache::utils::count_files(&locks_dir)?,
            1,
            "Lock file should exist"
        );
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

    // 1. Snapshot
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
    let snapshot_id = {
        let backend = Arc::new(mapache::backend::localfs::LocalFS::new(
            ctx.repo_path.clone(),
        ));
        let (repo, _) =
            Repository::try_open_unlocked(Some(&ctx.auth), None, backend, TEST_REPO_CONFIG).await?;
        let ids = repo.list_snapshot_ids().await?;
        *ids.first().expect("Snapshot should have been created")
    };
    let snapshot_id_hex = snapshot_id.to_hex();

    // 2. Log
    let log_args = cmd_log::CmdArgs {
        snapshot: None,
        dropped: false,
        all: false,
        compact: true,
        tags_str: None,
    };
    cmd_log::run(&ctx.global, &log_args).await?;
    wait_for_no_locks(&locks_dir).await?;

    // 3. Stats
    let stats_args = cmd_stats::CmdArgs { full: false };
    cmd_stats::run(&ctx.global, &stats_args).await?;
    wait_for_no_locks(&locks_dir).await?;

    // 4. Verify
    let verify_args = cmd_verify::CmdArgs {
        read_packs: false,
        parallel: 1,
        with_cache: false,
        fail_early: false,
        sample: None,
    };
    cmd_verify::run(&ctx.global, &verify_args).await?;
    wait_for_no_locks(&locks_dir).await?;

    // 5. Ls
    let ls_args = cmd_ls::CmdArgs {
        snapshot: UseSnapshot::Latest,
        path: None,
        recursive: false,
        long: false,
        human_readable: false,
    };
    cmd_ls::run(&ctx.global, &ls_args).await?;
    wait_for_no_locks(&locks_dir).await?;

    // 6. Find
    let find_args = cmd_find::CmdArgs {
        target: "backup".to_string(),
        snapshot: Some(UseSnapshot::Latest),
    };
    cmd_find::run(&ctx.global, &find_args).await?;
    wait_for_no_locks(&locks_dir).await?;

    // 7. Cat
    let cat_args = cmd_cat::CmdArgs {
        object: cmd_cat::Object::Snapshot(snapshot_id_hex.clone()),
    };
    cmd_cat::run(&ctx.global, &cat_args).await?;
    wait_for_no_locks(&locks_dir).await?;

    // 8. Diff
    let diff_args = cmd_diff::CmdArgs {
        source_snapshot_id: snapshot_id_hex.clone(),
        target_snapshot_id: snapshot_id_hex.clone(),
    };
    cmd_diff::run(&ctx.global, &diff_args).await?;
    wait_for_no_locks(&locks_dir).await?;

    // 9. Forget
    let forget_args = cmd_forget::CmdArgs {
        forget: vec![snapshot_id_hex.clone()],
        force: false,
        tags_str: None,
        keep_last: None,
        keep_within: None,
        keep_yearly: None,
        keep_monthly: None,
        keep_weekly: None,
        keep_daily: None,
        keep_tags_str: None,
        dry_run: true,
        run_gc: false,
        tolerance: 0.0,
    };
    cmd_forget::run(&ctx.global, &forget_args).await?;
    wait_for_no_locks(&locks_dir).await?;

    // 10. Clean
    let clean_args = cmd_clean::CmdArgs {
        tolerance: 100.0,
        no_repack: true,
        dry_run: true,
    };
    cmd_clean::run(&ctx.global, &clean_args).await?;
    wait_for_no_locks(&locks_dir).await?;

    // 11. RebuildIndex
    let rebuild_index_args = cmd_rebuild_index::CmdArgs { dry_run: true };
    cmd_rebuild_index::run(&ctx.global, &rebuild_index_args).await?;
    wait_for_no_locks(&locks_dir).await?;

    // 12. Amend
    let amend_args = cmd_amend::CmdArgs {
        snapshot: UseSnapshot::Latest,
        all: false,
        keep_old: false,
        tags_str: None,
        clear_tags: false,
        description: Some("new description".to_string()),
        clear_description: false,
        exclude: None,
    };
    cmd_amend::run(&ctx.global, &amend_args).await?;
    wait_for_no_locks(&locks_dir).await?;

    // 13. Recall
    let recall_args = cmd_recall::CmdArgs {
        id: snapshot_id_hex.clone(),
    };
    let _ = cmd_recall::run(&ctx.global, &recall_args).await;
    wait_for_no_locks(&locks_dir).await?;

    // 14. Restore
    let restore_path = ctx._tmp_dir.path().join("restore");
    let restore_args = cmd_restore::CmdArgs {
        snapshot: UseSnapshot::Latest,
        target: restore_path,
        exclude: None,
        include: None,
        strip_prefix: false,
        strategy: mapache::restorer::Strategy::Fail,
        delete: false,
        no_preserve_root: false,
        quit_on_error: false,
        dry_run: true,
    };
    cmd_restore::run(&ctx.global, &restore_args).await?;
    wait_for_no_locks(&locks_dir).await?;

    // 15. Rechunk
    let rechunk_args = cmd_rechunk::CmdArgs {};
    let _ = cmd_rechunk::run(&ctx.global, &rechunk_args).await;
    wait_for_no_locks(&locks_dir).await?;

    Ok(())
}
