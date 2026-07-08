use anyhow::Result;

use crate::{
    integration_tests::{INTEGRATION_TEST_DATA, TestContext},
    synthetic::{Dataset, SyntheticData},
};

#[tokio::test]
async fn test_bundle_and_extract() -> Result<()> {
    let mut ctx = TestContext::new().await?;
    let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
    let synthetic = SyntheticData::new(dataset);
    let backup_data_path = ctx.setup_backup_data(&synthetic)?;

    let bundle_path = ctx._tmp_dir.path().join("test_bundle.mapache");
    let extract_path = ctx._tmp_dir.path().join("extracted");

    ctx.bundle_builder()
        .bundle(true)
        .input(vec![backup_data_path.clone()])
        .output(bundle_path.clone())
        .password("test_password".to_string())
        .run()
        .await?;

    assert!(bundle_path.exists());

    ctx.bundle_builder()
        .extract(true)
        .input(vec![bundle_path.clone()])
        .output(extract_path.clone())
        .password("test_password".to_string())
        .run()
        .await?;

    let backup_dir_name = backup_data_path.file_name().unwrap();
    synthetic.verify_all_exact(&extract_path.join(backup_dir_name))?;

    Ok(())
}

#[tokio::test]
async fn test_bundle_with_exclude() -> Result<()> {
    let mut ctx = TestContext::new().await?;
    let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
    let synthetic = SyntheticData::new(dataset);
    let backup_data_path = ctx.setup_backup_data(&synthetic)?;

    let bundle_path = ctx._tmp_dir.path().join("exclude_bundle.mapache");
    let extract_path = ctx._tmp_dir.path().join("extracted_exclude");

    // Exclude a directory (0/00) and a glob pattern (*.txt under 0/01)
    let excludes = vec![
        backup_data_path.join("0/00"),
        backup_data_path.join("0/01/*.txt"),
    ];

    ctx.bundle_builder()
        .bundle(true)
        .input(vec![backup_data_path.clone()])
        .output(bundle_path.clone())
        .password("test_password".to_string())
        .exclude(excludes)
        .run()
        .await?;

    assert!(bundle_path.exists());

    ctx.bundle_builder()
        .extract(true)
        .input(vec![bundle_path.clone()])
        .output(extract_path.clone())
        .password("test_password".to_string())
        .run()
        .await?;

    let backup_dir_name = backup_data_path.file_name().unwrap();
    let extracted = extract_path.join(backup_dir_name);

    // Non-excluded paths should exist
    assert!(extracted.join("file.txt").exists());
    assert!(extracted.join("0/file0.txt").exists());
    assert!(extracted.join("1/10/file10.txt").exists());

    // Excluded directory 0/00 and its contents should be absent
    assert!(!extracted.join("0/00").exists());
    assert!(!extracted.join("0/00/file00.txt").exists());

    // Excluded glob 0/01/*.txt files should be absent.
    // The directory 0/01 itself is not excluded, only its .txt children.
    assert!(extracted.join("0/01").exists());
    assert!(!extracted.join("0/01/file01a.txt").exists());
    assert!(!extracted.join("0/01/file01b.txt").exists());

    Ok(())
}
