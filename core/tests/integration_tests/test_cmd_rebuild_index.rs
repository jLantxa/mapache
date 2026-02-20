#![cfg(test)]

mod tests {
    use std::{path::PathBuf, sync::Arc};

    use anyhow::{Context, Ok, Result};
    use tempfile::tempdir;

    use mapache::{
        backend::localfs::LocalFS,
        commands::{
            self, Compression, GlobalArgs, UseSnapshot, cmd_rebuild_index, cmd_snapshot, cmd_verify,
        },
        mapache::{defaults::DEFAULT_DEFAULT_PACK_SIZE_MIB, global::set_global_opts_with_args},
        repository::repo::{Auth, INDEX_DIR},
    };

    use crate::{
        TEST_QUIET,
        integration_tests::{BACKUP_DATA_PATH, delete_all_files_from, init_repo},
        test_utils,
    };

    #[tokio::test]
    async fn test_rebuild_index() -> Result<()> {
        // Verify that all referenced objects are indexed.
        // A missing index would make it fail.

        let tmp_dir = tempdir()?;
        let tmp_path = tmp_dir.path();
        let auth = Auth {
            username: "mapachito".to_string(),
            password: "password".to_string(),
        };
        let auth_file_path = tmp_path.join("auth");
        std::fs::write(
            &auth_file_path,
            format!("{}\n{}", auth.username, auth.password),
        )?;

        let backup_data_path = test_utils::get_test_data_path(BACKUP_DATA_PATH);
        let backup_data_tmp_path = tmp_path.join("backup");
        test_utils::extract_tar_xz_archive(&backup_data_path, &backup_data_tmp_path)?;

        let repo = String::from("repo");
        let repo_path = tmp_path.join(&repo);

        let global = GlobalArgs {
            repo: repo_path.to_string_lossy().to_string(),
            auth_file: Some(auth_file_path),
            key: None,
            quiet: *TEST_QUIET,
            json: false,
            verbosity: Some(3),
            ssh_privatekey: None,
            pack_size_mib: DEFAULT_DEFAULT_PACK_SIZE_MIB,
            no_cache: true,
            retry_lock_duration: None,
            compression_level: Compression::Fastest,
        };
        set_global_opts_with_args(&global);

        // Init repo
        init_repo(&auth, repo_path.clone()).await?;

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
        commands::cmd_snapshot::run(&global, &snapshot_args)
            .await
            .context("Failed to run cmd_snapshot")?;

        let verify_args = cmd_verify::CmdArgs {
            read_packs: true,
            parallel: 8,
            with_cache: false,
            fail_early: true,
            sample: None,
        };
        let first_verify_result = commands::cmd_verify::run(&global, &verify_args).await;
        assert!(first_verify_result.is_ok(), "First verify should pass");

        let backend = Arc::new(LocalFS::new(repo_path.clone()));

        // Rebuild index and verify
        // This time there is an old index that will be replaced.
        let rebuild_index_args = cmd_rebuild_index::CmdArgs { dry_run: false };
        commands::cmd_rebuild_index::run(&global, &rebuild_index_args)
            .await
            .context("Failed to run cmd_rebuild_index")?;

        let final_verify_result = commands::cmd_verify::run(&global, &verify_args).await;
        assert!(
            final_verify_result.is_ok(),
            "Verify should pass after index is rebuilt"
        );

        // Delete the index to make it fail the next time.
        delete_all_files_from(backend.as_ref(), &PathBuf::from(INDEX_DIR))
            .await
            .context("Failed to remove the index")?;
        let second_verify_result = commands::cmd_verify::run(&global, &verify_args).await;
        assert!(
            second_verify_result.is_err(),
            "Verify should fail without an index"
        );

        // Rebuild index and verify
        // This time there's no old index.
        let rebuild_index_args = cmd_rebuild_index::CmdArgs { dry_run: false };
        commands::cmd_rebuild_index::run(&global, &rebuild_index_args)
            .await
            .context("Failed to run cmd_rebuild_index")?;

        let final_verify_result = commands::cmd_verify::run(&global, &verify_args).await;
        assert!(
            final_verify_result.is_ok(),
            "Verify should pass after index is rebuilt"
        );

        Ok(())
    }
}
