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
    use std::path::PathBuf;

    use anyhow::{Context, Result};
    use mapache::{
        commands::{self, GlobalArgs, UseSnapshot, cmd_restore, cmd_snapshot},
        global::set_global_opts_with_args,
    };
    use tempfile::tempdir;

    use crate::{
        integration_tests::{BACKUP_DATA_PATH, init_repo},
        test_utils,
    };

    #[test]
    fn test_restore_with_filter() -> Result<()> {
        let tmp_dir = tempdir()?;
        let tmp_path = tmp_dir.path();
        let password = "mapachito";
        let password_path = tmp_path.join("password");
        std::fs::write(&password_path, password)?;

        let backup_data_path = test_utils::get_test_data_path(BACKUP_DATA_PATH);
        let backup_data_tmp_path = tmp_path.join("backup");
        test_utils::extract_tar_xz_archive(&backup_data_path, &backup_data_tmp_path)?;

        let repo = String::from("repo");
        let repo_path = tmp_path.join(&repo);

        let global = GlobalArgs {
            repo: repo_path.to_string_lossy().to_string(),
            password_file: Some(password_path),
            key: None,
            quiet: true,
            verbosity: None,
            ssh_pubkey: None,
            ssh_privatekey: None,
        };
        set_global_opts_with_args(&global);

        // Init repo
        init_repo(password, repo_path.clone())?;

        // Run snapshot
        let snapshot_args = cmd_snapshot::CmdArgs {
            paths: vec![
                backup_data_tmp_path.join("0"),
                backup_data_tmp_path.join("1"),
                backup_data_tmp_path.join("2"),
                backup_data_tmp_path.join("file.txt"),
            ],
            exclude: Some(vec![backup_data_tmp_path.join("0/01")]),
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

        // Run restore
        let restore_path = tmp_path.join("restore");
        let restore_args = cmd_restore::CmdArgs {
            target: restore_path.clone(),
            snapshot: UseSnapshot::Latest,
            dry_run: false,
            include: Some(vec![PathBuf::from("0"), PathBuf::from("1")]),
            exclude: Some(vec![PathBuf::from("0/00/file00.txt")]),
            strip_prefix: false,
            resolution: mapache::restorer::Resolution::Skip,
        };
        commands::cmd_restore::run(&global, &restore_args)
            .with_context(|| "Failed to run cmd_restore")?;

        let restored_paths = vec![
            PathBuf::from("0"),
            PathBuf::from("0/file0.txt"),
            PathBuf::from("0/00"),
            PathBuf::from("1"),
            PathBuf::from("1/10"),
        ];

        let excluded_paths = vec![
            PathBuf::from("0/00/file00.txt"),
            PathBuf::from("2"),
            PathBuf::from("file.txt"),
        ];

        for path in &excluded_paths {
            let not_restored_path = restore_path.join(path);
            assert!(!not_restored_path.exists());
        }

        for path in &restored_paths {
            let backup_path = backup_data_tmp_path.join(path);
            let restored_path = restore_path.join(path);
            assert!(restored_path.exists());

            let restored_meta = restored_path.symlink_metadata()?;
            let backup_meta = backup_path.symlink_metadata()?;

            assert_eq!(restored_meta.modified()?, backup_meta.modified()?);

            if restored_path.is_file() {
                assert_eq!(std::fs::read(&restored_path)?, std::fs::read(&backup_path)?);
            }

            if !restore_path.is_dir() {
                // Excluded paths decrease the size of parent directories.
                // We only test the size of files in this case
                assert_eq!(restored_meta.len(), backup_meta.len());
            }
        }

        Ok(())
    }

    #[test]
    fn test_restore_dry_run() -> Result<()> {
        let tmp_dir = tempdir()?;
        let tmp_path = tmp_dir.path();
        let password = "mapachito";
        let password_path = tmp_path.join("password");
        std::fs::write(&password_path, password)?;

        let backup_data_path = test_utils::get_test_data_path(BACKUP_DATA_PATH);
        let backup_data_tmp_path = tmp_path.join("backup");
        test_utils::extract_tar_xz_archive(&backup_data_path, &backup_data_tmp_path)?;

        let repo = String::from("repo");
        let repo_path = tmp_path.join(&repo);

        let global = GlobalArgs {
            repo: repo_path.to_string_lossy().to_string(),
            password_file: Some(password_path),
            key: None,
            quiet: true,
            verbosity: None,
            ssh_pubkey: None,
            ssh_privatekey: None,
        };
        set_global_opts_with_args(&global);

        // Init repo
        init_repo(password, repo_path.clone())?;

        // Run snapshot
        let snapshot_args = cmd_snapshot::CmdArgs {
            paths: vec![
                backup_data_tmp_path.join("0"),
                backup_data_tmp_path.join("1"),
                backup_data_tmp_path.join("2"),
                backup_data_tmp_path.join("file.txt"),
            ],
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

        // Run restore
        let restore_path = tmp_path.join("restore");
        let restore_args = cmd_restore::CmdArgs {
            target: restore_path.clone(),
            snapshot: UseSnapshot::Latest,
            dry_run: true,
            include: None,
            exclude: None,
            strip_prefix: false,
            resolution: mapache::restorer::Resolution::Skip,
        };
        commands::cmd_restore::run(&global, &restore_args)
            .with_context(|| "Failed to run cmd_restore")?;

        assert!(!restore_path.exists());

        Ok(())
    }

    #[test]
    fn test_restore_strip_prefix() -> Result<()> {
        let tmp_dir = tempdir()?;
        let tmp_path = tmp_dir.path();
        let password = "mapachito";
        let password_path = tmp_path.join("password");
        std::fs::write(&password_path, password)?;

        let backup_data_path = test_utils::get_test_data_path(BACKUP_DATA_PATH);
        let backup_data_tmp_path = tmp_path.join("backup");
        test_utils::extract_tar_xz_archive(&backup_data_path, &backup_data_tmp_path)?;

        let repo = String::from("repo");
        let repo_path = tmp_path.join(&repo);

        let global = GlobalArgs {
            repo: repo_path.to_string_lossy().to_string(),
            password_file: Some(password_path),
            key: None,
            quiet: true,
            verbosity: None,
            ssh_pubkey: None,
            ssh_privatekey: None,
        };
        set_global_opts_with_args(&global);

        // Init repo
        init_repo(password, repo_path.clone())?;

        // Run snapshot
        let snapshot_args = cmd_snapshot::CmdArgs {
            paths: vec![
                backup_data_tmp_path.join("0"),
                backup_data_tmp_path.join("1"),
                backup_data_tmp_path.join("2"),
                backup_data_tmp_path.join("file.txt"),
            ],
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

        // Run restore 1
        let restore_path = tmp_path.join("restore1");
        let restore_args = cmd_restore::CmdArgs {
            target: restore_path.clone(),
            snapshot: UseSnapshot::Latest,
            dry_run: false,
            include: Some(vec![
                PathBuf::from("0/file0.txt"),
                PathBuf::from("0/00/file00.txt"),
            ]),
            exclude: None,
            strip_prefix: true,
            resolution: mapache::restorer::Resolution::Skip,
        };
        commands::cmd_restore::run(&global, &restore_args)
            .with_context(|| "Failed to run cmd_restore 1")?;

        let restored_paths = vec![PathBuf::from("file0.txt"), PathBuf::from("00/file00.txt")];
        for path in &restored_paths {
            let restored_path = restore_path.join(path);
            assert!(restored_path.exists());
        }

        // Run restore 1
        let restore_path = tmp_path.join("restore2");
        let restore_args = cmd_restore::CmdArgs {
            target: restore_path.clone(),
            snapshot: UseSnapshot::Latest,
            dry_run: false,
            include: Some(vec![PathBuf::from("0/00/file00.txt")]),
            exclude: None,
            strip_prefix: true,
            resolution: mapache::restorer::Resolution::Skip,
        };
        commands::cmd_restore::run(&global, &restore_args)
            .with_context(|| "Failed to run cmd_restore 2")?;

        let restored_paths = vec![PathBuf::from("file00.txt")];
        for path in &restored_paths {
            let restored_path = restore_path.join(path);
            assert!(restored_path.exists());
        }

        Ok(())
    }
}
