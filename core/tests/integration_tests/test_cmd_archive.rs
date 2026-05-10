use crate::integration_tests::TestContext;
use anyhow::Result;
use mapache::commands::cmd_archive;

#[tokio::test]
async fn test_archive_and_extract() -> Result<()> {
    let mut ctx = TestContext::new().await?;
    ctx.setup_backup_data()?;
    let backup_data_path = ctx.backup_data_path.as_ref().unwrap();

    let archive_path = ctx._tmp_dir.path().join("test_archive.mapache");
    let extract_path = ctx._tmp_dir.path().join("extracted");

    let archive_args = cmd_archive::CmdArgs {
        archive: true,
        input: vec![backup_data_path.clone()],
        output: Some(archive_path.clone()),
        compression_level: mapache::commands::Compression::Balanced,
        workers: 2,
        internal_password: Some("test_password".to_string()),
        ..Default::default()
    };

    cmd_archive::run(&archive_args).await?;
    assert!(archive_path.exists());

    let extract_args = cmd_archive::CmdArgs {
        extract: true,
        input: vec![archive_path.clone()],
        output: Some(extract_path.clone()),
        compression_level: mapache::commands::Compression::Balanced,
        workers: 2,
        internal_password: Some("test_password".to_string()),
        ..Default::default()
    };

    cmd_archive::run(&extract_args).await?;

    let backup_dir_name = backup_data_path.file_name().unwrap();
    verify_extracted_content(backup_data_path, &extract_path.join(backup_dir_name))?;

    Ok(())
}

fn verify_extracted_content(source: &std::path::Path, target: &std::path::Path) -> Result<()> {
    for entry in std::fs::read_dir(source)? {
        let entry = entry?;
        let source_path = entry.path();
        let target_path = target.join(entry.file_name());

        assert!(
            target_path.exists(),
            "Target path {:?} missing",
            target_path
        );

        let source_meta = std::fs::symlink_metadata(&source_path)?;
        let target_meta = std::fs::symlink_metadata(&target_path)?;

        assert_eq!(source_meta.file_type(), target_meta.file_type());

        if source_meta.is_file() {
            assert_eq!(source_meta.len(), target_meta.len());
            let source_content = std::fs::read(&source_path)?;
            let target_content = std::fs::read(&target_path)?;
            assert_eq!(
                source_content, target_content,
                "Content mismatch for {:?}",
                source_path
            );
        } else if source_meta.is_dir() {
            verify_extracted_content(&source_path, &target_path)?;
        }
    }
    Ok(())
}
