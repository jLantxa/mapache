#![cfg(test)]

mod tests {
    use anyhow::Result;
    use mapache::{
        commands::{self, Compression, GlobalArgs, UseSnapshot, cmd_snapshot},
        mapache::{defaults::DEFAULT_DEFAULT_PACK_SIZE_MIB, global::set_global_opts_with_args},
        repository::repo::Auth,
    };

    use tempfile::tempdir;

    use crate::{
        TEST_QUIET,
        integration_tests::{BACKUP_DATA_PATH, init_repo, run_bin},
        test_utils::{self},
    };

    #[tokio::test]
    async fn test_run_cat() -> Result<()> {
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

        let repo_path = tmp_path.join("repo");

        let global = GlobalArgs {
            repo: repo_path.to_string_lossy().to_string(),
            auth_file: Some(auth_file_path.clone()),
            key: None,
            quiet: *TEST_QUIET,
            json: false,
            verbosity: Some(1),
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
            paths: vec![backup_data_tmp_path.join("file.txt")],
            as_root: false,
            exclude: None,
            tags_str: String::new(),
            description: None,
            no_parent: true,
            skip_if_unchanged: false,
            no_scan: false,
            parent: UseSnapshot::Latest,
            num_readers: 1,
            num_packers: 1,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&global, &snapshot_args).await?;

        // Test cmd_cat manifest via binary
        let output = run_bin(&[
            "cat",
            "manifest",
            "--repo",
            &repo_path.to_string_lossy(),
            "--auth-file",
            &auth_file_path.to_string_lossy(),
        ])?;

        assert!(output.status.success());
        let stdout = String::from_utf8(output.stdout)?;
        assert!(stdout.contains("\"version\""));
        assert!(stdout.contains("\"id\""));

        Ok(())
    }
}
