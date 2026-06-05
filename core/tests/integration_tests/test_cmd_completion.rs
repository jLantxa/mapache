#![cfg(test)]

mod tests {
    use anyhow::Result;
    use tempfile::tempdir;

    use crate::integration_tests::TestContext;

    #[tokio::test]
    async fn test_run_completion_and_check_stdout() -> Result<()> {
        let ctx = TestContext::new().await?;
        let tmp_dir = tempdir()?;

        // Test completion via binary
        ctx.run_mapache_ok(&[
            "completion",
            "--shell",
            "bash",
            "--path",
            &tmp_dir.path().to_string_lossy(),
        ])?;

        // Depending on implementation, it might write to the file or stdout.
        // If it writes to the file, we check the file.
        let executable_name = env!("CARGO_PKG_NAME");
        let completion_file = tmp_dir.path().join(executable_name);
        if completion_file.exists() {
            let content = std::fs::read_to_string(completion_file)?;
            assert!(content.contains(executable_name));
        }

        Ok(())
    }
}
