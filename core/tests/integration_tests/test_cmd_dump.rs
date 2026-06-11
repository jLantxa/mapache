#![cfg(test)]

mod tests {
    use anyhow::Result;

    use crate::integration_tests::TestContext;

    #[tokio::test]
    async fn test_dump_file() -> Result<()> {
        let ctx = TestContext::new().await?;
        let tmp = ctx._tmp_dir.path().join("custom");
        std::fs::create_dir_all(&tmp)?;

        let content = b"Hello, mapache dump!\nThis is a test.\n";
        let file_path = tmp.join("hello.txt");
        std::fs::write(&file_path, content)?;

        ctx.init_repo().await?;
        ctx.snapshot(vec![file_path]).await?;

        let output = ctx.run_mapache(&["dump", "latest", "--path", "hello.txt"])?;
        assert!(output.status.success());
        assert_eq!(output.stdout.as_slice(), content);

        Ok(())
    }

    #[tokio::test]
    async fn test_dump_path_not_found() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        ctx.setup_backup_data()?;
        let backup_data_tmp_path = ctx.backup_data_path.clone().unwrap();

        ctx.init_repo().await?;
        ctx.snapshot(vec![backup_data_tmp_path.join("file.txt")])
            .await?;

        let output = ctx.run_mapache(&["dump", "latest", "--path", "nonexistent.txt"])?;
        assert!(!output.status.success());

        Ok(())
    }

    #[tokio::test]
    async fn test_dump_directory_errors() -> Result<()> {
        let mut ctx = TestContext::new().await?;
        ctx.setup_backup_data()?;
        let backup_data_tmp_path = ctx.backup_data_path.clone().unwrap();

        ctx.init_repo().await?;
        ctx.snapshot(vec![backup_data_tmp_path.join("0")]).await?;

        let output = ctx.run_mapache(&["dump", "latest", "--path", "0"])?;
        assert!(!output.status.success());

        Ok(())
    }

    #[tokio::test]
    async fn test_dump_snapshot_not_found() -> Result<()> {
        let ctx = TestContext::new().await?;
        ctx.init_repo().await?;

        let output = ctx.run_mapache(&["dump", "00000000", "--path", "foo.txt"])?;
        assert!(!output.status.success());

        Ok(())
    }
}
