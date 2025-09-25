// mapache is a secure, de-duplicating, incremental backup tool.
// Copyright (C) 2025  Javier Lancha Vázquez <javier.lancha@gmail.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

#![cfg(test)]

mod tests {
    use std::sync::Arc;

    use mapache::{
        backend::localfs::LocalFS,
        commands::{self, GlobalArgs, cmd_init::CmdArgs},
        global::{defaults::DEFAULT_DEFAULT_PACK_SIZE_MIB, set_global_opts_with_args},
        repository::repo::{Auth, RepoConfig, Repository},
    };

    use anyhow::{Context, Result};
    use tempfile::tempdir;

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
            quiet: true,
            verbosity: None,
            ssh_pubkey: None,
            ssh_privatekey: None,
            pack_size_mib: DEFAULT_DEFAULT_PACK_SIZE_MIB,
        };
        let args = CmdArgs {};
        set_global_opts_with_args(&global);

        // Init repo
        commands::cmd_init::run(&global, &args).with_context(|| "Failed to run cmd_init")?;

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
        Repository::try_open_with_lock(Some(&auth), None, backend, RepoConfig::default(), false)
            .with_context(|| "Failed to open repository")?;

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
            quiet: true,
            verbosity: None,
            ssh_pubkey: None,
            ssh_privatekey: None,
            pack_size_mib: DEFAULT_DEFAULT_PACK_SIZE_MIB,
        };
        let args = CmdArgs {};
        set_global_opts_with_args(&global);

        // Init repo
        commands::cmd_init::run(&global, &args).with_context(|| "Failed to run cmd_init")?;

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
            RepoConfig::default(),
            false,
        )
        .with_context(|| "Failed to open repository")?;

        Ok(())
    }
}
