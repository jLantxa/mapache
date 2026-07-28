use anyhow::Result;

use mapache::commands::UseSnapshot;

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
        .run(&ctx.global.clone())
        .await?;

    assert!(bundle_path.exists());

    ctx.bundle_builder()
        .extract(true)
        .input(vec![bundle_path.clone()])
        .output(extract_path.clone())
        .password("test_password".to_string())
        .run(&ctx.global.clone())
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
        .run(&ctx.global.clone())
        .await?;

    assert!(bundle_path.exists());

    ctx.bundle_builder()
        .extract(true)
        .input(vec![bundle_path.clone()])
        .output(extract_path.clone())
        .password("test_password".to_string())
        .run(&ctx.global.clone())
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

#[tokio::test]
async fn test_export_snapshot_and_import() -> Result<()> {
    let mut ctx = TestContext::new().await?;
    let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
    let synthetic = SyntheticData::new(dataset);
    let backup_data_path = ctx.setup_backup_data(&synthetic)?;

    // Init source repo and create a snapshot
    ctx.init_repo().await?;

    ctx.snapshot_builder(vec![backup_data_path.clone()])
        .no_scan(true)
        .run(&ctx.global)
        .await?;

    let src_ids = ctx.get_snapshot_ids()?;
    assert_eq!(src_ids.len(), 1, "Source should have exactly one snapshot");
    let snap_prefix = &src_ids[0][..8];

    // Export snapshot to bundle
    let bundle_path = ctx._tmp_dir.path().join("exported.mapache");
    ctx.bundle_builder()
        .export_snapshot(UseSnapshot::SnapshotId(snap_prefix.to_string()))
        .output(bundle_path.clone())
        .password("bundle_pass".to_string())
        .run(&ctx.global.clone())
        .await?;

    assert!(bundle_path.exists(), "Bundle file should exist");

    // Extract bundle and verify files match the original data
    let extract_path = ctx._tmp_dir.path().join("extracted_from_export");
    ctx.bundle_builder()
        .extract(true)
        .input(vec![bundle_path.clone()])
        .output(extract_path.clone())
        .password("bundle_pass".to_string())
        .run(&ctx.global.clone())
        .await?;

    let backup_dir_name = backup_data_path.file_name().unwrap();
    synthetic.verify_all_exact(&extract_path.join(backup_dir_name))?;

    Ok(())
}

#[tokio::test]
async fn test_import_restore_and_verify() -> Result<()> {
    let mut ctx = TestContext::new().await?;
    let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
    let synthetic = SyntheticData::new(dataset);
    let backup_data_path = ctx.setup_backup_data(&synthetic)?;

    // Create a bundle directly from filesystem
    let bundle_path = ctx._tmp_dir.path().join("import_test.mapache");
    ctx.bundle_builder()
        .bundle(true)
        .input(vec![backup_data_path.clone()])
        .output(bundle_path.clone())
        .password("pass".to_string())
        .run(&ctx.global.clone())
        .await?;

    // Init repo and import the bundle
    ctx.init_repo().await?;

    ctx.bundle_builder()
        .import(true)
        .input(vec![bundle_path.clone()])
        .password("pass".to_string())
        .run(&ctx.global.clone())
        .await?;

    let ids = ctx.get_snapshot_ids()?;
    assert_eq!(
        ids.len(),
        1,
        "Should have exactly one snapshot after import"
    );

    // Restore the imported snapshot and verify files match the original data
    let restore_path = ctx._tmp_dir.path().join("restored_from_import");
    ctx.restore_builder(restore_path.clone())
        .run(&ctx.global)
        .await?;

    let backup_dir_name = backup_data_path.file_name().unwrap();
    synthetic.verify_all_exact(&restore_path.join(backup_dir_name))?;

    Ok(())
}

#[tokio::test]
async fn test_import_dedup() -> Result<()> {
    let mut ctx = TestContext::new().await?;
    let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
    let synthetic = SyntheticData::new(dataset);
    let backup_data_path = ctx.setup_backup_data(&synthetic)?;

    // Init source repo, create snapshot with the entire directory
    ctx.init_repo().await?;

    ctx.snapshot_builder(vec![backup_data_path.clone()])
        .no_scan(true)
        .run(&ctx.global)
        .await?;

    let src_ids = ctx.get_snapshot_ids()?;
    let snap_prefix = &src_ids[0][..8];

    // Export to bundle
    let bundle_path = ctx._tmp_dir.path().join("dedup_test.mapache");
    ctx.bundle_builder()
        .export_snapshot(UseSnapshot::SnapshotId(snap_prefix.to_string()))
        .output(bundle_path.clone())
        .password("pass".to_string())
        .run(&ctx.global.clone())
        .await?;

    // Create another snapshot with the SAME data (should share blobs)
    ctx.snapshot_builder(vec![backup_data_path.clone()])
        .no_scan(true)
        .run(&ctx.global)
        .await?;

    // Import the bundle — all blobs should already exist
    ctx.bundle_builder()
        .import(true)
        .input(vec![bundle_path.clone()])
        .password("pass".to_string())
        .run(&ctx.global.clone())
        .await?;

    // Should have 3 snapshots: 2 originals + 1 from import
    let ids = ctx.get_snapshot_ids()?;
    assert_eq!(
        ids.len(),
        3,
        "Should have 3 snapshots after import with dedup"
    );

    // Restore the imported snapshot and verify full data integrity
    let restore_path = ctx._tmp_dir.path().join("restored_dedup");
    ctx.restore_builder(restore_path.clone())
        .run(&ctx.global)
        .await?;

    let backup_dir_name = backup_data_path.file_name().unwrap();
    synthetic.verify_all_exact(&restore_path.join(backup_dir_name))?;

    Ok(())
}

#[tokio::test]
async fn test_bundle_with_as_root() -> Result<()> {
    let mut ctx = TestContext::new().await?;
    let dataset = Dataset::new().with_structure(INTEGRATION_TEST_DATA);
    let synthetic = SyntheticData::new(dataset);
    let backup_data_path = ctx.setup_backup_data(&synthetic)?;

    let bundle_path = ctx._tmp_dir.path().join("as_root_bundle.mapache");
    let extract_path = ctx._tmp_dir.path().join("extracted_as_root");

    ctx.bundle_builder()
        .bundle(true)
        .input(vec![backup_data_path.join("0")])
        .output(bundle_path.clone())
        .password("test_password".to_string())
        .root(true)
        .run(&ctx.global.clone())
        .await?;

    assert!(bundle_path.exists());

    ctx.bundle_builder()
        .extract(true)
        .input(vec![bundle_path.clone()])
        .output(extract_path.clone())
        .password("test_password".to_string())
        .run(&ctx.global.clone())
        .await?;

    // With --as-root, the bundle root is the children of "0",
    // so extract_path should contain file0.txt, 00/, 01/ etc. directly.
    assert!(extract_path.join("file0.txt").exists());
    assert!(extract_path.join("00").exists());
    assert!(extract_path.join("00/file00.txt").exists());
    assert!(extract_path.join("01").exists());
    assert!(extract_path.join("01/file01a.txt").exists());
    assert!(extract_path.join("01/file01b.txt").exists());

    // Verify file contents match originals
    let original = backup_data_path.join("0/file0.txt");
    let extracted = extract_path.join("file0.txt");
    assert_eq!(std::fs::read(&extracted)?, std::fs::read(&original)?);

    Ok(())
}
