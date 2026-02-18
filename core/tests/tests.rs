#![cfg(test)]

use std::sync::LazyLock;

mod integration_tests;

/// Environment variable to set the --quiet global option during testing.
const MAPACHE_TEST_QUIET: &str = "MAPACHE_TEST_QUIET";

static TEST_QUIET: LazyLock<bool> = LazyLock::new(|| match std::env::var(MAPACHE_TEST_QUIET) {
    Ok(s) => s.parse::<bool>().unwrap_or(true),
    Err(_) => true,
});

mod test_utils {
    use std::path::{Path, PathBuf};

    use anyhow::Result;
    use tar::Archive;
    use xz2::read::XzDecoder;

    pub(crate) const TESTDATA_PATH: &str = "testdata";

    pub(crate) fn get_test_data_path(filename: &str) -> PathBuf {
        PathBuf::from(TESTDATA_PATH).join(filename)
    }

    /// Extracts a .tar.xz archive to a path.
    pub(crate) fn extract_tar_xz_archive(tar_xz_path: &Path, extract_to_dir: &Path) -> Result<()> {
        let file = std::fs::File::open(tar_xz_path)?;
        let xz_decoder = XzDecoder::new(file);
        let mut archive = Archive::new(xz_decoder);

        std::fs::create_dir_all(extract_to_dir)?;

        for entry_result in archive.entries()? {
            let mut entry = entry_result?;

            // Creation of symlinks in Windows is problematic, so we skip them.
            #[cfg(target_os = "windows")]
            if entry.header().entry_type().is_symlink() {
                continue;
            }

            entry.unpack_in(extract_to_dir)?;
        }

        Ok(())
    }
}
