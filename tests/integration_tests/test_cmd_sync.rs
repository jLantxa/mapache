// mapache is an incremental backup tool
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
    use std::{path::PathBuf, sync::Arc};

    use anyhow::{Context, Result};
    use mapache::{
        backend::{StorageBackend, localfs::LocalFS},
        commands::{self, GlobalArgs, UseSnapshot, cmd_snapshot, cmd_sync},
        global::{defaults::DEFAULT_DEFAULT_PACK_SIZE_MIB, set_global_opts_with_args},
        repository::{
            repo::Auth,
            sync::{self, BackendNode},
        },
    };

    use tempfile::tempdir;

    use crate::{
        integration_tests::{BACKUP_DATA_PATH, init_repo},
        test_utils::{self},
    };

    #[test]
    fn test_sync_no_delete() -> Result<()> {
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
            quiet: true,
            verbosity: None,
            ssh_pubkey: None,
            ssh_privatekey: None,
            pack_size_mib: DEFAULT_DEFAULT_PACK_SIZE_MIB,
        };
        set_global_opts_with_args(&global);

        // Init repo
        init_repo(&auth, repo_path.clone())?;

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
            rescan: false,
            parent: UseSnapshot::Latest,
            read_concurrency: 2,
            write_concurrency: 5,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&global, &snapshot_args)
            .with_context(|| "Failed to run cmd_snapshot")?;

        let dst_repo_path = tmp_path.join("sync_dst");
        let sync_args = cmd_sync::CmdArgs {
            target: dst_repo_path.to_string_lossy().to_string(),
            delete: false,
        };
        cmd_sync::run(&global, &sync_args).with_context(|| "Failed to run cmd_sync")?;

        let src_backend = Arc::new(LocalFS::new(repo_path));
        let dst_backend = Arc::new(LocalFS::new(dst_repo_path));

        let forward_cmp = |n0: &BackendNode, n1: &BackendNode| n0.path().cmp(n1.path());
        let mut src_nodes = sync::read_backend_dir(src_backend.as_ref(), &PathBuf::new())?;
        let mut dst_nodes = sync::read_backend_dir(dst_backend.as_ref(), &PathBuf::new())?;
        src_nodes.sort_unstable_by(forward_cmp);
        dst_nodes.sort_unstable_by(forward_cmp);

        assert_eq!(src_nodes, dst_nodes);

        Ok(())
    }

    #[test]
    fn test_sync_with_delete() -> Result<()> {
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
            quiet: true,
            verbosity: None,
            ssh_pubkey: None,
            ssh_privatekey: None,
            pack_size_mib: DEFAULT_DEFAULT_PACK_SIZE_MIB,
        };
        set_global_opts_with_args(&global);

        // Init repo
        init_repo(&auth, repo_path.clone())?;

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
            rescan: false,
            parent: UseSnapshot::Latest,
            read_concurrency: 2,
            write_concurrency: 5,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&global, &snapshot_args)
            .with_context(|| "Failed to run cmd_snapshot")?;

        let dst_repo_path = tmp_path.join("sync_dst");

        let src_backend = Arc::new(LocalFS::new(repo_path));
        let dst_backend = Arc::new(LocalFS::new(dst_repo_path.clone()));
        dst_backend.create()?;

        // Add some dummy files to dst repo
        std::fs::create_dir_all(dst_repo_path.join("snapshots"))?;
        std::fs::write(
            dst_repo_path.join("snapshots").join("dummy_snapshot"),
            b"Dummy content",
        )?;
        std::fs::create_dir_all(dst_repo_path.join("objects").join("ff"))?;
        std::fs::write(
            dst_repo_path.join("objects").join("ff").join("dummy_pack"),
            b"Dummy content",
        )?;
        std::fs::create_dir_all(dst_repo_path.join("index"))?;
        std::fs::write(
            dst_repo_path.join("index").join("dummy_index"),
            b"Dummy content",
        )?;

        let sync_args = cmd_sync::CmdArgs {
            target: dst_repo_path.to_string_lossy().to_string(),
            delete: true,
        };
        cmd_sync::run(&global, &sync_args).with_context(|| "Failed to run cmd_sync")?;

        let forward_cmp = |n0: &BackendNode, n1: &BackendNode| n0.path().cmp(n1.path());
        let mut src_nodes = sync::read_backend_dir(src_backend.as_ref(), &PathBuf::new())?;
        let mut dst_nodes = sync::read_backend_dir(dst_backend.as_ref(), &PathBuf::new())?;
        src_nodes.sort_unstable_by(forward_cmp);
        dst_nodes.sort_unstable_by(forward_cmp);

        assert_eq!(src_nodes, dst_nodes);

        Ok(())
    }
}
