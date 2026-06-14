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
