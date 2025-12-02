#![cfg(test)]

use std::sync::LazyLock;

mod integration_tests;

const MAPACHE_TEST_QUIET: &str = "MAPACHE_TEST_QUIET";

const TEST_QUIET: LazyLock<bool> = LazyLock::new(|| match std::env::var(MAPACHE_TEST_QUIET) {
    Ok(s) => s.parse::<bool>().unwrap_or(true),
    Err(_) => true,
});

mod test_utils {
    use std::path::{Path, PathBuf};

    use anyhow::{Context, Result};
    use tar::Archive;
    use xz2::read::XzDecoder;

    pub(crate) const TESTDATA_PATH: &str = "testdata";

    pub(crate) fn get_test_data_path(filename: &str) -> PathBuf {
        PathBuf::from(TESTDATA_PATH).join(filename)
    }

    /// Extracts a .tar.xz archive to a path
    pub(crate) fn extract_tar_xz_archive(tar_xz_path: &Path, extract_to_dir: &Path) -> Result<()> {
        std::fs::create_dir_all(extract_to_dir)?;

        let file = std::fs::File::open(tar_xz_path)?;
        let xz_decoder = XzDecoder::new(file);
        let mut archive = Archive::new(xz_decoder);
        archive
            .unpack(extract_to_dir)
            .context("Failed to unpack tar")?;

        Ok(())
    }
}
