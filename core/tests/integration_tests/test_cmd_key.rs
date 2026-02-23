#![cfg(test)]

mod tests {
    use anyhow::Result;
    use mapache::{
        commands::{Compression, GlobalArgs},
        mapache::{defaults::DEFAULT_DEFAULT_PACK_SIZE_MIB, global::set_global_opts_with_args},
        repository::repo::{Auth, KEYS_DIR},
    };

    use tempfile::tempdir;

    use crate::{
        TEST_QUIET,
        integration_tests::{init_repo, run_bin},
    };

    #[tokio::test]
    async fn test_run_key_subcommands_and_check_stdout() -> Result<()> {
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

        // Test key list
        let output = run_bin(&[
            "key",
            "--repo",
            &repo_path.to_string_lossy(),
            "--auth-file",
            &auth_file_path.to_string_lossy(),
            "list",
        ])?;

        assert!(output.status.success());
        let stdout = String::from_utf8(output.stdout)?;
        assert!(stdout.contains("mapachito"));
        assert!(stdout.contains("Key ID"));

        // Test key add (non-interactive by bypassing UI calls via run_add implementation details)
        // Since we can't easily test interactive 'add' via binary, we verify the command structure
        // but perform the logic using core functions if we wanted deep testing.
        // However, we can test 'delete' by creating a second key manually or just using the first.

        // Test key delete
        let keys_dir = repo_path.join(KEYS_DIR);
        let keys = std::fs::read_dir(&keys_dir)?
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(keys.len(), 1);
        let key_id = keys[0].file_name().unwrap().to_str().unwrap().to_string();

        // Verification of 'delete' subcommand
        let output = run_bin(&[
            "key",
            "--repo",
            &repo_path.to_string_lossy(),
            "--auth-file",
            &auth_file_path.to_string_lossy(),
            "delete",
            &key_id,
        ])?;

        assert!(output.status.success());
        assert!(!keys_dir.join(&key_id).exists());

        // Verification of 'add' and 'change-password' is skipped in integration tests
        // due to mandatory interactive prompts in cmd_key.rs.
        // Those are covered by unit tests in keys.rs logic.

        Ok(())
    }
}
