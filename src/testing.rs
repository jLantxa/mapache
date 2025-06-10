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

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

pub const TEST_DATA_PATH: &str = "testdata";

/// Returns the complete path of file from the test data folders
pub fn get_test_path(name: &str) -> PathBuf {
    return Path::new(TEST_DATA_PATH).join(name);
}

pub fn create_password_file(path: &Path, file_name: &str, password: &str) -> Result<PathBuf> {
    let p: PathBuf = path.join(file_name);
    std::fs::write(&p, password).with_context(|| "Failed to create password file")?;
    Ok(p)
}
