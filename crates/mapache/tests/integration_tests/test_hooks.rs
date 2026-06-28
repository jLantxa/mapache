#![cfg(test)]
#![cfg(not(target_os = "windows"))]

mod tests {
    use std::path::PathBuf;

    use anyhow::Result;
    use tempfile::tempdir;

    use mapache::repository::repo::SNAPSHOTS_DIR;

    use crate::{
        integration_tests::{INTEGRATION_TEST_DATA, TestContext},
        synthetic::{Dataset, SyntheticData},
    };

    struct HookTest {
        ctx: TestContext,
        config_dir: tempfile::TempDir,
    }

    impl HookTest {
        async fn new() -> Result<Self> {
            let mut test_ctx = TestContext::new().await?;
            let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
            let synthetic = SyntheticData::new(dataset);
            test_ctx.setup_backup_data(&synthetic)?;
            test_ctx.init_repo().await?;
            let config_dir = tempdir()?;
            std::fs::write(config_dir.path().join("config.toml"), b"")?;
            Ok(Self {
                ctx: test_ctx,
                config_dir,
            })
        }

        fn config_path(&self) -> PathBuf {
            self.config_dir.path().join("config.toml")
        }

        fn write_config(&self, toml_content: &str) {
            std::fs::write(self.config_path(), toml_content).unwrap();
        }

        fn marker(&self, name: &str) -> PathBuf {
            self.config_dir.path().join(name)
        }

        fn run(&self, args: &[&str]) -> Result<std::process::Output> {
            let bin = env!("CARGO_BIN_EXE_mapache");
            let mut cmd = std::process::Command::new(bin);
            cmd.arg("--with-config");
            cmd.arg(self.config_path());
            cmd.arg(args[0]);
            cmd.arg("--quiet");
            cmd.arg("--repo");
            cmd.arg(&self.ctx.repo_path);
            cmd.arg("--auth-file");
            cmd.arg(&self.ctx.auth_file_path);
            for a in &args[1..] {
                cmd.arg(a);
            }
            cmd.arg("--no-cache");
            let output = cmd.output()?;
            if !output.status.success() {
                eprintln!("stderr: {}", String::from_utf8_lossy(&output.stderr));
            }
            Ok(output)
        }

        async fn create_snapshot(&self) -> Result<()> {
            let backup_dir = self.ctx.backup_data_path.clone().unwrap();
            let result = self.run(&[
                "snapshot",
                &backup_dir.join("0").to_string_lossy(),
                &backup_dir.join("1").to_string_lossy(),
                &backup_dir.join("file.txt").to_string_lossy(),
            ])?;
            assert!(result.status.success(), "baseline snapshot must succeed");
            Ok(())
        }
    }

    #[tokio::test]
    async fn snapshot_success() -> Result<()> {
        let harness = HookTest::new().await?;
        let backup_dir = harness.ctx.backup_data_path.clone().unwrap();
        let path_0 = &backup_dir.join("0").to_string_lossy().to_string();
        let path_file = &backup_dir.join("file.txt").to_string_lossy().to_string();

        let pre_marker = harness.marker("snap_pre");
        harness.write_config(&format!(
            "[hooks.snapshot.pre]\ncommand = \"touch {}\"\n",
            pre_marker.display()
        ));
        assert!(
            harness
                .run(&["snapshot", path_0, path_file])?
                .status
                .success()
        );
        assert!(pre_marker.exists(), "pre-hook should run");

        let post_marker = harness.marker("snap_post");
        harness.write_config(&format!(
            "[hooks.snapshot.post]\ncommand = \"touch {}\"\n",
            post_marker.display()
        ));
        assert!(
            harness
                .run(&["snapshot", path_0, path_file])?
                .status
                .success()
        );
        assert!(post_marker.exists(), "post-hook should run");

        let result_file = harness.marker("snap_result");
        harness.write_config(&format!(
            "[hooks.snapshot.post]\ncommand = \"echo $MAPACHE_RESULT > {}\"\n",
            result_file.display()
        ));
        assert!(
            harness
                .run(&["snapshot", path_0, path_file])?
                .status
                .success()
        );
        assert_eq!(std::fs::read_to_string(&result_file)?.trim(), "success");

        Ok(())
    }

    #[tokio::test]
    async fn snapshot_failure_and_timeout() -> Result<()> {
        let harness = HookTest::new().await?;
        let backup_dir = harness.ctx.backup_data_path.clone().unwrap();

        // pre-hook fails → command aborted
        harness.write_config("[hooks.snapshot.pre]\ncommand = \"false\"\n");
        let output = harness.run(&["snapshot", &backup_dir.join("0").to_string_lossy()])?;
        assert!(!output.status.success(), "pre-hook should abort");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("pre-hook failed") || stderr.contains("exited with"));

        // timeout
        harness.write_config("[hooks.snapshot.pre]\ncommand = \"sleep 10\"\ntimeout = 1\n");
        let output = harness.run(&["snapshot", &backup_dir.join("0").to_string_lossy()])?;
        assert!(!output.status.success(), "should timeout");
        assert!(String::from_utf8_lossy(&output.stderr).contains("timed out"));

        Ok(())
    }

    #[tokio::test]
    async fn snapshot_dry_run() -> Result<()> {
        let harness = HookTest::new().await?;
        let backup_dir = harness.ctx.backup_data_path.clone().unwrap();
        let path_0 = &backup_dir.join("0").to_string_lossy().to_string();
        let path_file = &backup_dir.join("file.txt").to_string_lossy().to_string();

        let pre_marker = harness.marker("snap_dry_pre");
        harness.write_config(&format!(
            "[hooks.snapshot.pre]\ncommand = \"touch {}\"\n",
            pre_marker.display()
        ));
        assert!(
            harness
                .run(&["snapshot", "--dry-run", path_0, path_file])?
                .status
                .success()
        );
        assert!(!pre_marker.exists(), "pre-hook should not run on dry-run");

        let post_marker = harness.marker("snap_dry_post");
        harness.write_config(&format!(
            "[hooks.snapshot.post]\ncommand = \"touch {}\"\n",
            post_marker.display()
        ));
        assert!(
            harness
                .run(&["snapshot", "--dry-run", path_0, path_file])?
                .status
                .success()
        );
        assert!(!post_marker.exists(), "post-hook should not run on dry-run");

        Ok(())
    }

    #[tokio::test]
    async fn restore_hooks() -> Result<()> {
        let harness = HookTest::new().await?;
        harness.create_snapshot().await?;
        let snapshot_ids = harness.ctx.get_snapshot_ids()?;
        let snapshot_id = &snapshot_ids[0];

        // pre-hook → ok
        let pre_marker = harness.marker("rest_pre");
        harness.write_config(&format!(
            "[hooks.restore.pre]\ncommand = \"touch {}\"\n",
            pre_marker.display()
        ));
        let restore_dir = harness.ctx._tmp_dir.path().join("r1");
        assert!(
            harness
                .run(&[
                    "restore",
                    snapshot_id,
                    "--target",
                    &restore_dir.to_string_lossy()
                ])?
                .status
                .success()
        );
        assert!(pre_marker.exists());

        // pre-hook → fail
        harness.write_config("[hooks.restore.pre]\ncommand = \"false\"\n");
        let restore_dir_2 = harness.ctx._tmp_dir.path().join("r2");
        assert!(
            !harness
                .run(&[
                    "restore",
                    snapshot_id,
                    "--target",
                    &restore_dir_2.to_string_lossy()
                ])?
                .status
                .success()
        );

        // post-hook → ok
        let post_marker = harness.marker("rest_post");
        harness.write_config(&format!(
            "[hooks.restore.post]\ncommand = \"touch {}\"\n",
            post_marker.display()
        ));
        let restore_dir_3 = harness.ctx._tmp_dir.path().join("r3");
        assert!(
            harness
                .run(&[
                    "restore",
                    snapshot_id,
                    "--target",
                    &restore_dir_3.to_string_lossy()
                ])?
                .status
                .success()
        );
        assert!(post_marker.exists());

        // post-hook → MAPACHE_RESULT
        let result_file = harness.marker("rest_res");
        harness.write_config(&format!(
            "[hooks.restore.post]\ncommand = \"echo $MAPACHE_RESULT > {}\"\n",
            result_file.display()
        ));
        let restore_dir_4 = harness.ctx._tmp_dir.path().join("r4");
        assert!(
            harness
                .run(&[
                    "restore",
                    snapshot_id,
                    "--target",
                    &restore_dir_4.to_string_lossy()
                ])?
                .status
                .success()
        );
        assert_eq!(std::fs::read_to_string(&result_file)?.trim(), "success");

        Ok(())
    }

    #[tokio::test]
    async fn forget_hooks() -> Result<()> {
        let harness = HookTest::new().await?;
        harness.create_snapshot().await?;

        // pre-hook → ok
        let pre_marker = harness.marker("forget_pre");
        harness.write_config(&format!(
            "[hooks.forget.pre]\ncommand = \"touch {}\"\n",
            pre_marker.display()
        ));
        let snapshot_id = &harness.ctx.get_snapshot_ids()?[0];
        let output = harness.run(&["forget", "--force", snapshot_id])?;
        assert!(output.status.success());
        assert!(pre_marker.exists());
        assert_eq!(
            std::fs::read_dir(harness.ctx.repo_path.join(SNAPSHOTS_DIR))?.count(),
            0
        );

        // pre-hook → fail
        harness.create_snapshot().await?;
        let snapshot_id = &harness.ctx.get_snapshot_ids()?[0];
        harness.write_config("[hooks.forget.pre]\ncommand = \"false\"\n");
        let output = harness.run(&["forget", "--force", snapshot_id])?;
        assert!(!output.status.success());

        // post-hook → ok
        harness.create_snapshot().await?;
        let post_marker = harness.marker("forget_post");
        harness.write_config(&format!(
            "[hooks.forget.post]\ncommand = \"touch {}\"\n",
            post_marker.display()
        ));
        let snapshot_id = &harness.ctx.get_snapshot_ids()?[0];
        assert!(
            harness
                .run(&["forget", "--force", snapshot_id])?
                .status
                .success()
        );
        assert!(post_marker.exists());

        // post-hook → MAPACHE_RESULT
        harness.create_snapshot().await?;
        let result_file = harness.marker("forget_res");
        harness.write_config(&format!(
            "[hooks.forget.post]\ncommand = \"echo $MAPACHE_RESULT > {}\"\n",
            result_file.display()
        ));
        let snapshot_id = &harness.ctx.get_snapshot_ids()?[0];
        assert!(
            harness
                .run(&["forget", "--force", snapshot_id])?
                .status
                .success()
        );
        assert_eq!(std::fs::read_to_string(&result_file)?.trim(), "success");

        Ok(())
    }

    #[tokio::test]
    async fn clean_hooks() -> Result<()> {
        let harness = HookTest::new().await?;
        harness.create_snapshot().await?;

        let pre_marker = harness.marker("clean_pre");
        harness.write_config(&format!(
            "[hooks.clean.pre]\ncommand = \"touch {}\"\n",
            pre_marker.display()
        ));
        assert!(harness.run(&["clean"])?.status.success());
        assert!(pre_marker.exists());

        harness.write_config("[hooks.clean.pre]\ncommand = \"false\"\n");
        assert!(!harness.run(&["clean"])?.status.success());

        let post_marker = harness.marker("clean_post");
        harness.write_config(&format!(
            "[hooks.clean.post]\ncommand = \"touch {}\"\n",
            post_marker.display()
        ));
        assert!(harness.run(&["clean"])?.status.success());
        assert!(post_marker.exists());

        let result_file = harness.marker("clean_res");
        harness.write_config(&format!(
            "[hooks.clean.post]\ncommand = \"echo $MAPACHE_RESULT > {}\"\n",
            result_file.display()
        ));
        assert!(harness.run(&["clean"])?.status.success());
        assert_eq!(std::fs::read_to_string(&result_file)?.trim(), "success");

        Ok(())
    }

    #[tokio::test]
    async fn verify_hooks() -> Result<()> {
        let harness = HookTest::new().await?;
        harness.create_snapshot().await?;

        let pre_marker = harness.marker("verify_pre");
        harness.write_config(&format!(
            "[hooks.verify.pre]\ncommand = \"touch {}\"\n",
            pre_marker.display()
        ));
        assert!(harness.run(&["verify"])?.status.success());
        assert!(pre_marker.exists());

        harness.write_config("[hooks.verify.pre]\ncommand = \"false\"\n");
        assert!(!harness.run(&["verify"])?.status.success());

        let post_marker = harness.marker("verify_post");
        harness.write_config(&format!(
            "[hooks.verify.post]\ncommand = \"touch {}\"\n",
            post_marker.display()
        ));
        assert!(harness.run(&["verify"])?.status.success());
        assert!(post_marker.exists());

        let result_file = harness.marker("verify_res");
        harness.write_config(&format!(
            "[hooks.verify.post]\ncommand = \"echo $MAPACHE_RESULT > {}\"\n",
            result_file.display()
        ));
        assert!(harness.run(&["verify"])?.status.success());
        assert_eq!(std::fs::read_to_string(&result_file)?.trim(), "success");

        Ok(())
    }
}
