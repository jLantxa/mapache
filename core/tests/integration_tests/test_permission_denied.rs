#![cfg(test)]

#[cfg(unix)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use anyhow::{Context, Result};
    use mapache::{
        commands::{self, UseSnapshot, cmd_snapshot},
        repository::repo::SNAPSHOTS_DIR,
        utils,
    };

    use crate::integration_tests::TestContext;

    #[tokio::test]
    async fn test_snapshot_permission_denied() -> Result<()> {
        let ctx = TestContext::new().await?;
        let backup_data_tmp_path = ctx._tmp_dir.path().join("backup");
        std::fs::create_dir_all(&backup_data_tmp_path)?;

        let accessible_dir = backup_data_tmp_path.join("accessible");
        std::fs::create_dir(&accessible_dir)?;
        std::fs::write(accessible_dir.join("file1.txt"), "content1")?;

        let inaccessible_dir = backup_data_tmp_path.join("inaccessible");
        std::fs::create_dir(&inaccessible_dir)?;
        std::fs::write(inaccessible_dir.join("file2.txt"), "content2")?;

        // Set permissions to 000 (no access)
        let mut perms = std::fs::metadata(&inaccessible_dir)?.permissions();
        perms.set_mode(0o000);
        std::fs::set_permissions(&inaccessible_dir, perms.clone())?;

        // Ensure we restore permissions after the test so tempdir can cleanup
        let _cleanup = scopeguard::guard(inaccessible_dir.clone(), |p| {
            let mut perms = std::fs::metadata(&p).unwrap().permissions();
            perms.set_mode(0o755);
            let _ = std::fs::set_permissions(&p, perms);
        });

        // Init repo
        ctx.init_repo().await?;

        // Run snapshot - this should now SUCCEED and skip the inaccessible dir
        let snapshot_args = cmd_snapshot::CmdArgs {
            paths: vec![backup_data_tmp_path.clone()],
            as_root: true,
            exclude: None,
            tags_str: String::new(),
            description: None,
            no_parent: false,
            skip_if_unchanged: false,
            no_scan: false,
            parent: UseSnapshot::Latest,
            num_readers: 1,
            num_packers: 1,
            dry_run: false,
        };

        commands::cmd_snapshot::run(&ctx.global, &snapshot_args)
            .await
            .context("Snapshot should succeed by skipping inaccessible paths")?;

        // Verify that only the accessible part was backed up
        let snapshots_dir = ctx.repo_path.join(SNAPSHOTS_DIR);
        assert_eq!(utils::count_files(&snapshots_dir)?, 1);

        Ok(())
    }
}
