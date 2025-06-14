// [backup] is an incremental backup tool
// Copyright (C) 2025  Javier Lancha Vázquez <javier.lancha@gmail.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

#![cfg(test)]


mod integration_tests;

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
            .with_context(|| "Failed to unpack tar")?;

        Ok(())
    }
}
