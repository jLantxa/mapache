#![cfg(test)]

mod tests {
    use anyhow::Result;
    use tempfile::tempdir;

    use crate::integration_tests::TestContext;

    #[tokio::test]
    async fn test_run_bash_completion() -> Result<()> {
        let ctx = TestContext::new().await?;
        let tmp_dir = tempdir()?;

        ctx.run_mapache_ok(&[
            "completion",
            "--shell",
            "bash",
            "--path",
            &tmp_dir.path().to_string_lossy(),
        ])?;

        let completion_file = tmp_dir.path().join("mapache.bash");
        assert!(completion_file.is_file(), "completion file was not created");

        let content = std::fs::read_to_string(completion_file)?;
        assert!(content.contains("mapache"));
        assert!(content.contains("snapshot"));
        assert!(content.contains("restore"));
        assert!(content.contains("verify"));

        Ok(())
    }
}
