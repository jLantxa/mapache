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
    use std::{collections::BTreeSet, path::PathBuf, sync::Arc};

    use anyhow::{Context, Result};
    use mapache::{
        backend::localfs::LocalFS,
        commands::{self, GlobalArgs, UseSnapshot, cmd_amend, cmd_restore, cmd_snapshot},
        repository::{snapshot::SnapshotStreamer, try_open},
    };

    use tempfile::tempdir;

    use crate::{
        integration_tests::{BACKUP_DATA_PATH, init_repo},
        test_utils::{self},
    };

    #[test]
    fn test_amend_exclude() -> Result<()> {
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

        // Init repo
        init_repo(password, repo_path.clone())?;

        // Run snapshot twice
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

        let excluded_paths = vec![PathBuf::from("2"), PathBuf::from("file.txt")];
        let amend_args = cmd_amend::CmdArgs {
            snapshot: UseSnapshot::Latest,
            all: false,
            tags_str: None,
            clear_tags: false,
            description: None,
            clear_description: false,
            exclude: Some(excluded_paths.clone()),
        };
        commands::cmd_amend::run(&global, &amend_args)
            .with_context(|| "Failed to run cmd_amend")?;

        // Run restore
        let restore_path = tmp_path.join("restore");
        let restore_args = cmd_restore::CmdArgs {
            target: restore_path.clone(),
            snapshot: UseSnapshot::Latest,
            dry_run: false,
            include: None,
            exclude: None,
            strip_prefix: false,
            resolution: mapache::restorer::Resolution::Skip,
            no_verify: false,
        };
        commands::cmd_restore::run(&global, &restore_args)
            .with_context(|| "Failed to run cmd_restore")?;

        let paths = vec![
            PathBuf::from("0"),
            PathBuf::from("0/file0.txt"),
            PathBuf::from("0/00"),
            PathBuf::from("0/00/file00.txt"),
            PathBuf::from("0/01"),
            PathBuf::from("0/01/file01a.txt"),
            PathBuf::from("0/01/file01b.txt"),
            PathBuf::from("1"),
            PathBuf::from("1/10"),
        ];

        for path in &paths {
            let backup_path = backup_data_tmp_path.join(path);
            let restored_path = restore_path.join(path);
            assert!(restored_path.exists());

            let restored_meta = restored_path.symlink_metadata()?;
            let backup_meta = backup_path.symlink_metadata()?;

            assert_eq!(restored_meta.len(), backup_meta.len());
            assert_eq!(restored_meta.modified()?, backup_meta.modified()?);

            if restored_path.is_file() {
                assert_eq!(std::fs::read(&restored_path)?, std::fs::read(&backup_path)?);
            }
        }

        for path in excluded_paths {
            let restored_path = restore_path.join(path);
            assert!(!restored_path.exists());
        }

        Ok(())
    }

    #[test]
    fn test_amend_tags_and_description() -> Result<()> {
        let tmp_dir = tempdir()?;
        let tmp_path = tmp_dir.path();
        let password = "mapachito";
        let password_path = tmp_path.join("password");
        std::fs::write(&password_path, password)?;

        let backup_data_path = test_utils::get_test_data_path(BACKUP_DATA_PATH);
        let backup_data_tmp_path = tmp_path.join("backup");
        test_utils::extract_tar_xz_archive(&backup_data_path, &backup_data_tmp_path)?;

        let repo_path = tmp_path.join(String::from("repo"));
        let backend = Arc::new(LocalFS::new(repo_path.clone()));

        let global = GlobalArgs {
            repo: repo_path.to_string_lossy().to_string(),
            password_file: Some(password_path),
            key: None,
            quiet: true,
            verbosity: None,
            ssh_pubkey: None,
            ssh_privatekey: None,
        };

        // Init repo
        init_repo(password, repo_path.clone())?;
        let (repo, _) = try_open(Some(password.to_string()), None, backend)?;

        // Run snapshot twice
        let snapshot_args = cmd_snapshot::CmdArgs {
            paths: Vec::new(),
            exclude: None,
            tags_str: "tag0,tag1".to_string(),
            description: Some(String::from("This snapshot will be amended")),
            rescan: false,
            parent: UseSnapshot::Latest,
            read_concurrency: 2,
            write_concurrency: 5,
            dry_run: false,
        };
        commands::cmd_snapshot::run(&global, &snapshot_args)
            .with_context(|| "Failed to run cmd_snapshot")?;

        let amend_args = cmd_amend::CmdArgs {
            snapshot: UseSnapshot::Latest,
            all: false,
            tags_str: None,
            clear_tags: true,
            description: None,
            clear_description: true,
            exclude: None,
        };
        commands::cmd_amend::run(&global, &amend_args)
            .with_context(|| "Failed to run cmd_amend")?;

        let mut snapshot_streamer = SnapshotStreamer::new(repo.clone())?;
        let (_, snapshot) = snapshot_streamer
            .latest()
            .expect("There should be at least one snapshot");

        assert!(snapshot.tags.is_empty());
        assert!(snapshot.description.is_none());

        let amend_args = cmd_amend::CmdArgs {
            snapshot: UseSnapshot::Latest,
            all: false,
            tags_str: Some("new_tag".to_string()),
            clear_tags: false,
            description: Some(String::from("This description is new")),
            clear_description: false,
            exclude: None,
        };
        commands::cmd_amend::run(&global, &amend_args)
            .with_context(|| "Failed to run cmd_amend")?;

        let mut snapshot_streamer = SnapshotStreamer::new(repo.clone())?;
        let (_, snapshot) = snapshot_streamer
            .latest()
            .expect("There should be at least one snapshot");

        let expected_tags: BTreeSet<String> =
            ["new_tag"].into_iter().map(|s| s.to_string()).collect();

        assert_eq!(snapshot.tags, expected_tags);
        assert_eq!(
            snapshot
                .description
                .expect("The description should not be None"),
            "This description is new"
        );

        Ok(())
    }
}
