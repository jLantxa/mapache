use crate::integration_tests::{TestContext, assert_times_equal};
use anyhow::Result;
use mapache::commands::cmd_bundle;

#[tokio::test]
async fn test_bundle_and_extract() -> Result<()> {
    let mut ctx = TestContext::new().await?;
    ctx.setup_backup_data()?;
    let backup_data_path = ctx.backup_data_path.as_ref().unwrap();

    let bundle_path = ctx._tmp_dir.path().join("test_bundle.mapache");
    let extract_path = ctx._tmp_dir.path().join("extracted");

    let bundle_args = cmd_bundle::CmdArgs {
        bundle: true,
        input: vec![backup_data_path.clone()],
        output: Some(bundle_path.clone()),
        compression_level: mapache::commands::Compression::Balanced,
        workers: 2,
        internal_password: Some("test_password".to_string()),
        ..Default::default()
    };

    cmd_bundle::run(&bundle_args).await?;
    assert!(bundle_path.exists());

    let extract_args = cmd_bundle::CmdArgs {
        extract: true,
        input: vec![bundle_path.clone()],
        output: Some(extract_path.clone()),
        compression_level: mapache::commands::Compression::Balanced,
        workers: 2,
        internal_password: Some("test_password".to_string()),
        ..Default::default()
    };

    cmd_bundle::run(&extract_args).await?;

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

        assert_times_equal(source_meta.modified()?, target_meta.modified()?);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                source_meta.permissions().mode() & 0o777,
                target_meta.permissions().mode() & 0o777,
                "Permission mismatch for {:?}",
                source_path
            );
        }

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
