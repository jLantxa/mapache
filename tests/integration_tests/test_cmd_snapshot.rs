// [backup] is an incremental backup tool
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
    use backup::{
        backend::localfs::LocalFS,
        commands::{self, GlobalArgs, UseSnapshot, cmd_restore, cmd_snapshot},
        repository,
    };
    
    use tempfile::tempdir;

    use crate::test_utils;

    const BACKUP_DATA_PATH: &str = "backup_data.tar.xz";

    fn init_repo(password: &str, repo_path: PathBuf) -> Result<()> {
        let backend = Arc::new(LocalFS::new(repo_path));
        repository::init(Some(password.to_owned()), None, backend)
            .with_context(|| "Failed to init repo")
    }

    #[test]
    fn test_snapshot() -> Result<()> {
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
        };

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
            exclude: Vec::new(),
            description: None,
            full_scan: false,
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
            include: None,
            exclude: None,
            resolution: backup::restorer::Resolution::Skip,
        };
        commands::cmd_restore::run(&global, &restore_args)
            .with_context(|| "Failed to run cmd_restore")?;

        // Check restored files
        assert!(restore_path.join("0").exists());
        assert!(restore_path.join("0").join("file0.txt").exists());
        assert!(restore_path.join("0").join("00").exists());
        assert!(
            restore_path
                .join("0")
                .join("00")
                .join("file00.txt")
                .exists()
        );
        assert!(restore_path.join("0").join("01").exists());
        assert!(restore_path.join("0").join("l01").exists());
        assert!(
            restore_path
                .join("0")
                .join("01")
                .join("file01a.txt")
                .exists()
        );
        assert!(
            restore_path
                .join("0")
                .join("01")
                .join("file01b.txt")
                .exists()
        );
        assert!(restore_path.join("1").exists());
        assert!(restore_path.join("1").join("10").exists());
        assert!(restore_path.join("2").exists());
        assert!(restore_path.join("file.txt").exists());

        // TODO: Check contents and metadata

        Ok(())
    }
}
