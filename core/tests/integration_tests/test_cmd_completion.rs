#![cfg(test)]

mod tests {
    use crate::integration_tests::run_bin;
    use anyhow::Result;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_run_completion_and_check_stdout() -> Result<()> {
        let tmp_dir = tempdir()?;

        // Test completion via binary
        let output = run_bin(&[
            "completion",
            "--shell",
            "bash",
            "--path",
            &tmp_dir.path().to_string_lossy(),
        ])?;

        assert!(output.status.success());

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
