#![cfg(test)]

mod tests {
    use std::sync::Arc;

    use mapache::{
        backend::localfs::LocalFS,
        commands::{self, GlobalArgs, cmd_init::CmdArgs},
        mapache::{
            defaults::{DEFAULT_DEFAULT_PACK_SIZE_MIB, TEST_REPO_CONFIG},
            global::set_global_opts_with_args,
        },
        repository::repo::{Auth, Repository},
    };

    use anyhow::{Context, Result};
    use tempfile::tempdir;

    use crate::TEST_QUIET;

    #[test]
    fn test_init() -> Result<()> {
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

        let repo = String::from("repo");
        let repo_path = tmp_path.join(&repo);

        let global = GlobalArgs {
            repo: repo_path.to_string_lossy().to_string(),
            auth_file: Some(auth_file_path),
            key: None,
            quiet: *TEST_QUIET,
            verbosity: Some(3),
            ssh_pubkey: None,
            ssh_privatekey: None,
            pack_size_mib: DEFAULT_DEFAULT_PACK_SIZE_MIB,
            no_cache: true,
            retry_lock_duration: None,
        };
        let args = CmdArgs {};
        set_global_opts_with_args(&global);

        // Init repo
        commands::cmd_init::run(&global, &args).context("Failed to run cmd_init")?;

        // Assert layout
        assert!(repo_path.join("manifest").exists());
        assert!(repo_path.join("index").exists());
        assert!(repo_path.join("keys").exists());
        assert!(repo_path.join("snapshots").exists());
        assert!(repo_path.join("objects").exists());
        for i in 0x00..=0xff {
            assert!(repo_path.join("objects").join(format!("{i:02x}")).exists());
        }

        let keys = repo_path.join("keys").read_dir()?;
        assert_eq!(keys.into_iter().count(), 1);

        // Try to open repo
        let backend = Arc::new(LocalFS::new(repo_path));
        Repository::try_open_with_lock(Some(&auth), None, backend, TEST_REPO_CONFIG, false, None)
            .context("Failed to open repository")?;

        Ok(())
    }

    #[test]
    fn test_init_and_open_with_ext_keyfile() -> Result<()> {
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

        let repo = String::from("repo");
        let repo_path = tmp_path.join(&repo);
        let keyfile_path = tmp_path.join("ext_keyfile");

        let global = GlobalArgs {
            repo: repo_path.to_string_lossy().to_string(),
            auth_file: Some(auth_file_path),
            key: Some(keyfile_path.clone()),
            quiet: *TEST_QUIET,
            verbosity: Some(3),
            ssh_pubkey: None,
            ssh_privatekey: None,
            pack_size_mib: DEFAULT_DEFAULT_PACK_SIZE_MIB,
            no_cache: true,
            retry_lock_duration: None,
        };
        let args = CmdArgs {};
        set_global_opts_with_args(&global);

        // Init repo
        commands::cmd_init::run(&global, &args).context("Failed to run cmd_init")?;

        // Assert layout
        assert!(repo_path.join("manifest").exists());
        assert!(repo_path.join("index").exists());
        assert!(repo_path.join("keys").exists());
        assert!(repo_path.join("snapshots").exists());
        assert!(repo_path.join("objects").exists());
        for i in 0x00..=0xff {
            assert!(repo_path.join("objects").join(format!("{i:02x}")).exists());
        }

        assert!(keyfile_path.exists());
        let keys = repo_path.join("keys").read_dir()?;
        assert_eq!(keys.into_iter().count(), 0);

        // Try to open repo
        let backend = Arc::new(LocalFS::new(repo_path));
        Repository::try_open_with_lock(
            Some(&auth),
            Some(&keyfile_path),
            backend,
            TEST_REPO_CONFIG,
            false,
            None,
        )
        .context("Failed to open repository")?;

        Ok(())
    }
}
