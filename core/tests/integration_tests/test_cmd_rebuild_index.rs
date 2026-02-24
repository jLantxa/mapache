#![cfg(test)]

mod tests {
    use std::{path::PathBuf, sync::Arc};

    use anyhow::{Context, Ok, Result};

    use mapache::{
        backend::localfs::LocalFS,
        commands::{self, UseSnapshot, cmd_rebuild_index, cmd_snapshot, cmd_verify},
        repository::repo::INDEX_DIR,
    };

    use crate::integration_tests::{TestContext, delete_all_files_from};

    #[tokio::test]
    async fn test_rebuild_index() -> Result<()> {
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

        // Rebuild index and verify
        // This time there is an old index that will be replaced.
        let rebuild_index_args = cmd_rebuild_index::CmdArgs { dry_run: false };
        commands::cmd_rebuild_index::run(&ctx.global, &rebuild_index_args)
            .await
            .context("Failed to run cmd_rebuild_index")?;

        let final_verify_result = commands::cmd_verify::run(&ctx.global, &verify_args).await;
        assert!(
            final_verify_result.is_ok(),
            "Verify should pass after index is rebuilt"
        );

        // Delete the index to make it fail the next time.
        delete_all_files_from(backend.as_ref(), &PathBuf::from(INDEX_DIR))
            .await
            .context("Failed to remove the index")?;
        let second_verify_result = commands::cmd_verify::run(&ctx.global, &verify_args).await;
        assert!(
            second_verify_result.is_err(),
            "Verify should fail without an index"
        );

        // Rebuild index and verify
        // This time there's no old index.
        let rebuild_index_args = cmd_rebuild_index::CmdArgs { dry_run: false };
        commands::cmd_rebuild_index::run(&ctx.global, &rebuild_index_args)
            .await
            .context("Failed to run cmd_rebuild_index")?;

        let final_verify_result = commands::cmd_verify::run(&ctx.global, &verify_args).await;
        assert!(
            final_verify_result.is_ok(),
            "Verify should pass after index is rebuilt"
        );

        Ok(())
    }
}
