#![cfg(test)]

mod tests {
    use anyhow::{Context, Result};
    use mapache::{
        commands::{self, GlobalArgs, cmd_unlock},
        mapache::{
            defaults::{DEFAULT_DEFAULT_PACK_SIZE_MIB, TEST_REPO_CONFIG},
            global::set_global_opts_with_args,
        },
        repository::repo::{Auth, LOCKS_DIR, Repository},
    };

    use std::sync::Arc;
    use tempfile::tempdir;

    use crate::{TEST_QUIET, integration_tests::init_repo};

    #[tokio::test]
    async fn test_cmd_unlock() -> Result<()> {
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
            auth_file: Some(auth_file_path),
            key: None,
            quiet: *TEST_QUIET,
            json: false,
            verbosity: Some(1),
            ssh_privatekey: None,
            pack_size_mib: DEFAULT_DEFAULT_PACK_SIZE_MIB,
            no_cache: true,
            retry_lock_duration: None,
            compression_level: mapache::commands::Compression::Fastest,
        };
        set_global_opts_with_args(&global);

        // Init repo
        init_repo(&auth, repo_path.clone()).await?;

        let backend = Arc::new(mapache::backend::localfs::LocalFS::new(repo_path.clone()));

        // Open with lock and KEEP the handle alive to maintain the lock
        let (_repo, _, _lock_handle) = Repository::try_open_with_lock(
            Some(&auth),
            None,
            backend,
            TEST_REPO_CONFIG,
            true,
            None,
        )
        .await?;

        let locks_dir = repo_path.join(LOCKS_DIR);
        assert_eq!(mapache::utils::count_files(&locks_dir)?, 1);

        // Test cmd_unlock without force (should not delete as it's not expired)
        let unlock_args = cmd_unlock::CmdArgs { force: false };
        commands::cmd_unlock::run(&global, &unlock_args)
            .await
            .context("cmd_unlock failed")?;
        assert_eq!(mapache::utils::count_files(&locks_dir)?, 1);

        // Test cmd_unlock with force
        let unlock_args_force = cmd_unlock::CmdArgs { force: true };
        commands::cmd_unlock::run(&global, &unlock_args_force)
            .await
            .context("cmd_unlock force failed")?;
        assert_eq!(mapache::utils::count_files(&locks_dir)?, 0);

        Ok(())
    }
}
